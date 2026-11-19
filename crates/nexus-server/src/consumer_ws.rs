//! The consumer WebSocket face (`GET /v1/consumer`, spec §5.4 / NEX-3): post,
//! watch, and stop watching several tasks over one authenticated socket.
//!
//! # Why a second face, and why it owns nothing
//!
//! Each HTTP surface carries exactly one subscription per request —
//! `POST /v1/chat/completions` streams the answer to the request that posted it,
//! `/v1/jobs/{id}/events` follows exactly one job — so a consumer that wants
//! several answers at once, or a verdict before any event bytes, has no way to
//! ask. The pool actor already speaks `Submit`/`Attach`/`Detach`/`Cancel` per
//! consumer and already fans one task out to N subscribers
//! (`nexus-core/src/actor.rs:195-312`), so this module is only a translation
//! layer: socket message → `ActorMsg`, `Effect::Relay` → frame. It holds no job
//! state, which is what keeps the HTTP and WebSocket surfaces from drifting on
//! what a task's stream contains.
//!
//! # The two ordering rules the design follows from
//!
//!   * **Verdict before bytes, in submission order** (NEX-3.3). A `submit` is
//!     handled *inline*: the reader does not take another message until this
//!     task's `accepted`/`rejected` has been handed to the writer task, and the
//!     event pump for that task is spawned only afterwards. Since the writer
//!     drains one FIFO queue, the verdict necessarily precedes every event frame
//!     for that task, and N submits in one TCP segment get N verdicts in order.
//!     That is also what lets a client correlate a *pre-admission* refusal to the
//!     request it belongs to: the verdict names a task id minted here, before the
//!     actor ever saw it.
//!   * **Exactly one `end` per task** (NEX-3.4). The pump task and the session
//!     teardown share one `AtomicBool` per task; whoever wins the CAS writes the
//!     `end` frame. A task that was never accepted needs no `end` — its `rejected`
//!     verdict *is* the terminal marker, and `retry_after_secs` rides on that
//!     frame rather than on an `end`.
//!
//! # Lifetime coupling (NEX-6)
//!
//! One `ConsumerId` per (connection, task), minted here — the actor keys
//! `consumers` by it, so closing one subscription never disturbs another
//! connection watching the same task (NEX-3.4/NEX-6.5). A pump task and a
//! subscription live and die together, which is the invariant the detach
//! bookkeeping rests on:
//!
//!   * a pump that ends on its own (terminal event, lag, or a socket that stopped
//!     accepting writes) sends `Detach` itself;
//!   * a pump the *session* stops (client `detach`, or connection close) has its
//!     `Detach` sent by the session, for exactly the tasks still marked attached.
//!
//! `on_detach` is idempotent (`actor.rs:1296`, an unknown job answers
//! `Vec::new()`), so the rare overlap costs nothing; double-detaching is
//! preferred to leaving a phantom consumer behind, because a non-empty `consumers`
//! set is what stops the NEX-6.6 subscriber-less window from ever running.
//!
//! `detach:true` (NEX-6.6) is the declared-detached opt-in and flips
//! `cancel_on_consumer_gone`. A detached task that outlives this connection keeps
//! running, bounded by `limits.detached_orphan_ttl_ms`; an attached one is
//! orphaned and deleted by the close itself (NEX-6.1/NEX-6.3).
//!
//! # What bounds one stream, and what bounds one connection
//!
//! Two lifetimes meet in this module, and the hub's resident cost is the sum of both:
//!
//!   * **One task's stream is bounded by the pump that carries it, and by nothing
//!     else.** When the pump finishes it takes its own bookkeeping with it: its slot in
//!     the job's fan-out (retired by `state::Subscriber`'s `Drop`, synchronously, so
//!     even an aborted pump cannot strand a subscriber queue), the session's `watches`
//!     entry for that task (posted back through the session's reap postbox, which is
//!     the only way the session learns a stream ended by itself), and this connection's
//!     mirror in the observe registry. Before that, all three outlived the stream and
//!     were released only when the socket closed — so the cost of a consumer socket grew
//!     with every task it had *ever* watched, not with the ones it was watching, which is
//!     the shape the resident-memory curve was reporting.
//!   * **The connection is bounded by liveness, not by the client's politeness** — see
//!     `consumer_keepalive_windows`. A consumer that vanishes without a `Close` (a tab
//!     killed behind a proxy, a laptop that slept, an edge that dropped the path
//!     quietly) used to leave this session parked in `stream.next()` forever, holding
//!     every byte listed above plus its writer task and its actor-side consumer slots.
//!
//! Neither bound is a deadline on job progress. A task that produces nothing for ten
//! minutes is an alive stream on an alive socket, and both survive it; what ends them is
//! the peer's own runtime going silent, or the stream running out.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::TenantId;
use nexus_core::protocol::decode_consumer_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::CancelReason;
use nexus_core::protocol::ConsumerFrame;
use nexus_core::protocol::EndReason;
use nexus_core::protocol::HubConsumerFrame;
use nexus_core::protocol::WireCaps;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::consumer;
use crate::consumer::Follow;
use crate::state;
use crate::state::Gate;
use crate::state::Hub;

/// Frames the hub may have queued in front of one consumer socket before a pump
/// parks. This is the hub-side half of NEX-5.3: a consumer that stops reading
/// feels it as a paused stream, and the hub's memory stays bounded by
/// `tasks × WRITER_QUEUE` frames rather than growing with the answer.
const WRITER_QUEUE: usize = 256;

/// How long a pump gets to write its final `end` frame once the socket is gone.
/// Pumps are parked on a `recv`, so this only has to cover scheduling.
const PUMP_DRAIN: Duration = Duration::from_millis(200);

/// The retry hint for "the hub itself did not answer", matching the `503 +
/// Retry-After: 2` the HTTP surfaces send (`consumer.rs:621-633`).
const HUB_UNAVAILABLE_RETRY_SECS: u64 = 2;

/// Floor on the hub's liveness probe period — the same floor the supplier face uses,
/// because a hub that serves many sockets cannot poll them faster than this.
const MIN_PING_MS: u64 = 250;

/// Ceiling on the probe period: no consumer socket goes unprobed for longer than
/// this, whatever the configured ruler says.
const MAX_PING_MS: u64 = 5_000;

/// A period no read loop will ever reach, used when the probe is configured off so
/// the `select!` still has a branch to poll (a guard on an `Option<Interval>` would
/// put an `unwrap` in the middle of the loop that keeps every subscription alive).
const NEVER: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);

/// The hub's liveness probe for a **consumer** socket: `(probe period, the stretch of
/// total silence that means the path is gone)`, from `limits.consumer_keepalive_ms`.
///
/// The judgement is the one the supplier face already makes (`worker_ws.rs`'s
/// `keepalive_windows`), and it copies its shape for the same reasons: a probe every
/// quarter of the ruler, clamped into `[MIN_PING_MS, MAX_PING_MS]`, and a verdict only
/// after `max(3 × probe, ruler + probe)` of hearing **nothing at all** — not one
/// consumer frame, not one `Pong`. At the default 60 s ruler that is a 5 s probe and a
/// 65 s verdict, i.e. thirteen unanswered probes behind every reap; at the `250` ms the
/// reap tests run on it is a 250 ms probe and a 750 ms verdict. Which of the two terms
/// binds depends on where the ruler sits relative to the clamps — `Limits`' own field
/// documentation works all three settings through, and
/// `the_probe_windows_are_the_arithmetic_the_comments_claim` pins them.
///
/// The window is wide on purpose: this verdict applies NEX-6.1/6.3 to whatever the
/// socket is attached to, which cancels **and deletes** an attached task, so a false
/// positive costs a second GPU run for an answer that was already being produced.
///
/// Two rules it must never break, which is why the ruler is measured in pings and not
/// in job progress:
///   * **A slow stream is not silence.** Every inbound frame resets the clock,
///     including a bare `Pong`, which a live WebSocket peer sends on its runtime's
///     behalf without any application code. A consumer waiting ten minutes for a queued
///     task sends nothing and stays attached; only a path that is gone stops the pings.
///   * **A live socket is never the reason for a verdict.** The probe asks, and the
///     peer's own runtime answers.
///
/// A ruler of `0` disables the probe — the connection then lives exactly as long as
/// its client says it does, which is what this face did before and what retained a
/// vanished consumer (and everything it held) for the life of the process.
#[must_use]
fn consumer_keepalive_windows(limits: &nexus_core::Limits) -> (Duration, Duration) {
    if limits.consumer_keepalive_ms == 0 {
        return (NEVER, NEVER);
    }
    let ruler_ms = limits.consumer_keepalive_ms.max(1);
    let probe_ms = (ruler_ms / 4).clamp(MIN_PING_MS, MAX_PING_MS);
    let silence_ms = probe_ms
        .saturating_mul(3)
        .max(ruler_ms.saturating_add(probe_ms));
    (
        Duration::from_millis(probe_ms),
        Duration::from_millis(silence_ms),
    )
}

/// `GET /v1/consumer` — the upgrade entry point.
///
/// Spec §7 makes credentials per direction and never shared — the consumer
/// direction presents the API key, never the pool token — and that check happens
/// **before** the upgrade. A missing or unknown API key is therefore an HTTP
/// `401` a plain client can act on, never a socket that opens and then closes
/// for a reason only visible in a frame.
pub async fn upgrade(
    ws: WebSocketUpgrade,
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
) -> Response {
    match consumer::tenant_of(&hub, &headers) {
        Ok(tenant) => ws.on_upgrade(move |socket| session(socket, hub, tenant)),
        Err(mut response) => {
            // `tenant_of` builds the OpenAI-shaped body (§7.1); the challenge
            // header belongs to the transport, so it is added on this side.
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"nexus\""),
            );
            response
        }
    }
}

/// One thing the writer task may put on the socket.
///
/// One task owns every write (frames, pongs, the close), so the order on the wire
/// is the order in the queue — the property NEX-3.3's verdict-before-bytes rule
/// depends on.
enum ConsumerWrite {
    /// An already-encoded consumer frame. Encoding happens where the frame is
    /// built, so a serialization failure cannot take the writer down.
    Frame(String),
    /// Answer to a client ping.
    Pong(Vec<u8>),
    /// The hub's own liveness probe (see `consumer_keepalive_windows`): a `Ping` with
    /// no payload, whose only job is to make a live peer produce a `Pong`. It is a
    /// queue item rather than a direct write for the same reason everything else on
    /// this socket is one — one task owns the ordering, and a ping must not overtake
    /// or interleave with a half-written frame.
    Ping,
    /// Flush what is queued, then close. The final `end` frames are in front of
    /// this in the same queue, so a clean close cannot truncate them.
    Close,
}

/// One task this connection is watching.
struct Watch {
    /// The consumer identity this connection is registered under for this task.
    consumer: ConsumerId,
    /// Whether this connection currently counts as an attached consumer, shared with
    /// the pump so the pump can clear it when the stream ends on its own. Two things
    /// read it: the teardown detaches exactly the tasks still marked attached
    /// (NEX-6.6: a task the client already detached has to survive the close), and
    /// `attach` refuses a second live stream for one task. Clearing it at
    /// end-of-stream is what lets a client re-attach to a task whose answer already
    /// ran out — the resume NEX-3.4's `through_seq` exists for.
    attached: Arc<AtomicBool>,
    /// End-of-stream once-flag, shared with the pump (NEX-3.4).
    ended: Arc<AtomicBool>,
    /// Asks the pump to stop and name the reason. Taken out once signalled, so a
    /// second `detach` cannot signal a dying pump again.
    stop: Option<oneshot::Sender<EndReason>>,
    /// The pump itself, joined during teardown so its `end` frame can flush.
    pump: Option<JoinHandle<()>>,
}

impl Watch {
    /// Tells the pump to close this stream with `reason`. Idempotent: the sender
    /// is taken, so only the first caller reaches the pump.
    fn signal(&mut self, reason: EndReason) {
        if let Some(tx) = self.stop.take() {
            if tx.send(reason).is_err() {
                // The pump already finished on its own and wrote its `end`; claim
                // the flag so nothing tries to write a second one.
                self.ended.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Waits for the pump to finish writing, then **ends** it: it cannot write to a
    /// socket that is going away, and it owes the actor nothing the session has not
    /// already said.
    ///
    /// The grace is a courtesy, not the mechanism. Waiting alone used to *abandon* a
    /// pump that did not wake in `PUMP_DRAIN`, and an abandoned pump is still a live
    /// task: it kept `Arc<Hub>`, its clone of the socket's write queue (which is also
    /// what kept the writer task from finishing), both relay receivers with everything
    /// still buffered in them, and its slot in the job's fan-out. On a socket that
    /// watched many tasks, "abandon" was retention by another name.
    ///
    /// Aborting is safe at every one of that task's await points. Every write the pump
    /// performs is one whole, already-encoded frame handed to the writer task through
    /// the queue, so a cut in the middle of `send` cannot put half a frame on the wire
    /// — the frame is either queued or not at all. And the bookkeeping it would have
    /// done still happens: dropping the task's future drops its `Subscriber`, whose
    /// `Drop` takes its queue out of the relay's fan-out synchronously, and the session
    /// sends the `Detach` the aborted pump would have sent (`on_detach` is idempotent,
    /// so the rare overlap costs nothing).
    async fn drain(self) {
        if let Some(task) = self.pump {
            // Asked for before the task is moved into the timeout: `abort` is the only
            // thing that can end a pump that never wakes up.
            let abort = task.abort_handle();
            if tokio::time::timeout(PUMP_DRAIN, task).await.is_err() {
                tracing::debug!("consumer pump still parked at teardown; aborting it");
                abort.abort();
            }
        }
    }
}

/// Everything a frame handler needs that belongs to the connection rather than to
/// one task. Grouped so handlers stay under the argument limit and share one view
/// of the mutable per-task map.
struct Session<'a> {
    /// Shared hub state: config, ids, pool handles, api keys.
    hub: &'a Arc<Hub>,
    /// The authenticated tenant, fixed for the connection by the pre-upgrade key.
    tenant: TenantId,
    /// Caps on what we send back — the same pair the worker face uses (§9). The
    /// inbound half stays with the read loop, which is the only reader of what the
    /// peer sends.
    caps_out: WireCaps,
    /// The single FIFO in front of the socket.
    sink: mpsc::Sender<ConsumerWrite>,
    /// Tasks this connection is watching **right now**.
    ///
    /// The invariant this map exists to hold: one entry per live stream, not one per
    /// task this socket has ever run. A stream that ends on its own (terminal event,
    /// lag, a socket that stopped accepting writes) is invisible to the session — only
    /// the pump knows it happened — so the pump posts its own `(task, consumer)` on
    /// [`Self::reap`] as it finishes and the read loop drops the entry here.
    ///
    /// Without that step the map was one entry per submission for the life of the
    /// socket: a fresh `ConsumerId`, its `stop` oneshot, the `JoinHandle` of a task
    /// that had already returned (with its result held), and the flags they pin. That
    /// is bookkeeping proportional to *cumulative* jobs rather than to live streams,
    /// and a browser tab that posted one task a minute never gave any of it back.
    watches: HashMap<JobId, Watch>,
    /// Postbox for "this stream has ended", from the pump that ended it. Unbounded on
    /// purpose: each pump posts at most one, so its depth is bounded by the number of
    /// streams that have ended and not yet been reclaimed — the exact set this
    /// mechanism exists to shrink — and posting can never park a pump.
    reap: mpsc::UnboundedSender<(JobId, ConsumerId)>,
    /// This connection's id in the hub's observe registry. Every attach and detach
    /// of a task mirrors into it, and the `ConnGuard` in `session` removes the
    /// record when the socket goes; the pumps carry the same id so a stream that
    /// ends by itself stops being reported.
    conn: ConsumerId,
}

impl Session<'_> {
    /// Drops the entry for a stream whose pump has finished, if that pump is still the
    /// one holding the task.
    ///
    /// The consumer id is the whole check, and it is load-bearing: a client that
    /// re-`attach`es to a task whose answer already ran out (the resume NEX-3.4's
    /// `through_seq` exists for) installs a **new** entry under the same job id, and
    /// the finished pump's claim must not reclaim the stream that replaced it. Ids are
    /// minted per (connection, task) and never reused, so equality is exact.
    fn reap(&mut self, job: &JobId, consumer: &ConsumerId) {
        if self
            .watches
            .get(job)
            .is_some_and(|watch| watch.consumer == *consumer)
        {
            self.watches.remove(job);
        }
    }
}

/// This connection's record in the hub's monitoring registry, removed on drop.
///
/// # Why a guard rather than a teardown call
///
/// The registry's whole purpose is to say which consumer sockets are up **right
/// now**, so the one failure mode that matters is an exit the session did not
/// walk through in order: a panic in a frame handler, a `JoinHandle::abort` during
/// shutdown, or an early `return` someone adds six months from now. A trailing
/// `hub.close_consumer_conn(..)` statement would be skipped by all three and leave
/// a connection listed as live — with its tenant and its watched jobs — for the
/// rest of the process's life. `Drop` runs on every path, and it can call into the
/// registry synchronously only because that registry is a `std::sync::Mutex`
/// (see `state::ConsumerConnection` for why that lock is the right one here).
struct ConnGuard {
    /// Hub the record lives in.
    hub: Arc<Hub>,
    /// The id this session was registered under.
    id: ConsumerId,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.hub.close_consumer_conn(&self.id);
    }
}

async fn session(socket: WebSocket, hub: Arc<Hub>, tenant: TenantId) {
    let caps_in = hub.cfg.limits.worker_caps();
    let caps_out = hub.cfg.limits.hub_caps();
    let (mut sink, mut stream) = socket.split();

    // 0. Register the connection with the hub's monitoring registry.
    //
    // This runs after `upgrade` has already authenticated the key, so a refused
    // upgrade never appears as a live connection, and it runs before the first
    // frame is read, so a monitor polling during a slow client's first submit still
    // sees the socket it is connected to. `peer` is `None` because the hub serves
    // `axum::serve(listener, app)` without `ConnectInfo`; inventing a service
    // wrapper for one optional monitor field is not worth touching the router
    // contract the runtime tests reuse verbatim.
    //
    // The guard, not a teardown statement, is what makes abnormal disconnects
    // correct: see `ConnGuard`.
    let conn = ConnGuard {
        hub: Arc::clone(&hub),
        id: hub.accept_consumer_conn(tenant.clone(), None),
    };

    // 1. Outbound pump. Dropping its `stall` clone is how the reader learns the
    //    socket can no longer be written: every pump would otherwise park behind
    //    a queue nobody drains, which NEX-5.3 wants reported, not absorbed.
    let (write_tx, mut write_rx) = mpsc::channel::<ConsumerWrite>(WRITER_QUEUE);
    let (stall_tx, mut stall_rx) = mpsc::channel::<()>(1);
    let writer = tokio::spawn(async move {
        while let Some(item) = write_rx.recv().await {
            let message = match item {
                ConsumerWrite::Frame(line) => Message::Text(line.into()),
                ConsumerWrite::Pong(payload) => Message::Pong(payload.into()),
                ConsumerWrite::Ping => Message::Ping(Vec::<u8>::new().into()),
                // Flush what the peer is owed, then hang up. Everything still queued
                // behind this was written first, which is what makes a clean close
                // unable to truncate the final `end` frames.
                ConsumerWrite::Close => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
        drop(stall_tx);
    });

    // Streams that have ended leave `session.watches` through this postbox: the pump
    // knows the moment its stream is over and the session does not, and a map that
    // only ever shrinks on the next inbound frame would still be holding everything a
    // quiet socket had ever watched. See `Session::watches`.
    let (reap_tx, mut reap_rx) = mpsc::unbounded_channel::<(JobId, ConsumerId)>();

    let mut session = Session {
        hub: &hub,
        tenant,
        caps_out,
        sink: write_tx.clone(),
        watches: HashMap::new(),
        reap: reap_tx,
        conn: conn.id.clone(),
    };

    // 2. Inbound loop: one frame at a time, handled to completion. That is what
    //    keeps a submit's verdict ahead of the next task's bytes.
    //
    // The probe windows (`consumer_keepalive_windows`) are this face's only
    // knowledge of whether the peer still exists. Without them the loop ended on an
    // inbound frame error, a `Close`, or a failed write — all three of which need the
    // peer, or something that answers on its behalf, to say something first. A browser
    // tab killed behind a proxy, a laptop that slept, or an edge that dropped the path
    // quietly produced none of those, and this session then held its writer task, one
    // pump per task it had watched, and one subscriber per relay, for the life of the
    // process.
    let (probe_period, silence_after) = consumer_keepalive_windows(&hub.cfg.limits);
    let mut probe = tokio::time::interval(probe_period);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_inbound = tokio::time::Instant::now();
    'read: loop {
        let message = tokio::select! {
            biased;
            // A stream that ended by itself. Drained in one pass — `recv` woke us for
            // one, the rest are already queued — so a burst of endings costs one
            // scheduling round rather than one per stream.
            ended = reap_rx.recv() => {
                if let Some((job, consumer)) = ended {
                    session.reap(&job, &consumer);
                }
                while let Ok((job, consumer)) = reap_rx.try_recv() {
                    session.reap(&job, &consumer);
                }
                continue 'read;
            }
            // The writer is gone, so nothing this connection could be told would
            // arrive; stop and let the teardown detach.
            _ = stall_rx.recv() => break 'read,
            // The probe. Silence is measured from the last inbound **frame of any
            // kind**, so this branch only ever speaks for a peer that has produced
            // nothing at all — no submit, no detach, no `Pong` — for the whole window.
            _ = probe.tick() => {
                let silent = last_inbound.elapsed();
                if silent >= silence_after {
                    tracing::warn!(
                        tenant = %session.tenant,
                        conn = %session.conn,
                        silence_ms = silent.as_millis() as u64,
                        probe_ms = probe_period.as_millis() as u64,
                        watches = session.watches.len(),
                        "consumer went silent past the keepalive window; closing the socket"
                    );
                    // Every byte this socket was holding goes the same way a clean
                    // close takes: the teardown signals and aborts the pumps, reports
                    // the actors, and the `ConnGuard` removes the registry record.
                    break 'read;
                }
                // `try_send`, not `send`: a full write queue means this consumer has
                // stopped reading, and parking the read loop on a ping would stop it
                // hearing the frames that would have proven it alive. So a ping that
                // cannot enter the queue is simply skipped, and the next tick asks
                // again — the verdict stays a statement about who answered, never about
                // a moment of backpressure. A writer that has actually stopped dropping
                // `stall_tx` is the case that ends the loop, and `stall_rx` above sees
                // it on the same iteration it happens.
                if session.sink.try_send(ConsumerWrite::Ping).is_err() {
                    tracing::debug!(conn = %session.conn, "write queue cannot take a probe; skipping this tick");
                }
                continue 'read;
            }
            got = stream.next() => match got {
                Some(Ok(message)) => message,
                // A client FIN or a broken frame: either way there is nothing
                // left to read, and this is NEX-6.1's independent notice of the
                // consumer leaving — no failed write has to happen first.
                Some(Err(_)) | None => break 'read,
            },
        };
        // Any inbound frame at all is liveness, control frames included: a `Pong` the
        // peer's WebSocket runtime sent without its application knowing anything is
        // exactly as good evidence as a `submit`. This is the rule that keeps a slow
        // stream alive, and it is the same one the supplier face runs.
        last_inbound = tokio::time::Instant::now();
        match message {
            Message::Text(text) => {
                let frame = match decode_consumer_frame(text.trim(), &caps_in) {
                    Ok(frame) => frame,
                    Err(err) => {
                        // A consumer has no handshake to lose, so a bad frame is
                        // answered in band with the shared machine code and the
                        // connection stays up: one typo must not cost the other
                        // tasks this socket carries.
                        let note = HubConsumerFrame::connection_error(err.code(), &err.to_string());
                        push(&session.sink, session.caps_out, note).await;
                        continue;
                    }
                };
                match frame {
                    // Passed whole: `handle_submit` reads the task's fields off the
                    // frame, which is what keeps NEX-3.3's ordering in one place.
                    ConsumerFrame::Submit { .. } => handle_submit(&mut session, frame).await,
                    ConsumerFrame::Attach { job, from_seq } => {
                        // `false`: the attach verb changes nothing about a task's
                        // contract, so the verdict echoes the attached reading.
                        handle_attach(&mut session, job, from_seq, false).await;
                    }
                    ConsumerFrame::Detach { job } => handle_detach(&mut session, job).await,
                    ConsumerFrame::Cancel { job } => handle_cancel(&mut session, job).await,
                }
            }
            // The consumer dialect is JSON in text messages; binary means the
            // peer misunderstood the protocol, not that one task failed.
            Message::Binary(_) => {
                let note = HubConsumerFrame::connection_error(
                    "bad_frame",
                    "consumer frames are JSON text messages",
                );
                push(&session.sink, session.caps_out, note).await;
            }
            Message::Ping(payload) => {
                if session
                    .sink
                    .send(ConsumerWrite::Pong(payload.to_vec()))
                    .await
                    .is_err()
                {
                    break 'read;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break 'read,
        }
    }

    // 3. Teardown, in the order that keeps both invariants: first tell every pump
    //    to close its stream (each writes exactly one `end`, NEX-3.4) and wait for
    //    the frames to reach the queue; then tell the actor about the consumers
    //    this connection still held (NEX-6.1/NEX-6.6); then close the socket.
    let Session {
        hub, sink, watches, ..
    } = session;
    let mut leaving = Vec::with_capacity(watches.len());
    for (job, mut watch) in watches {
        // Held tasks get `closed`; a task the client already detached got its
        // `detached` end from `handle_detach` and has no pump left to signal.
        watch.signal(EndReason::Closed);
        // Swapped, not read, and swapped before the drain: see `handle_detach`. A pump
        // that exits while this waits clears the flag too, so the worst case is the
        // actor hearing `Detach` twice for one consumer, which `on_detach` absorbs.
        let attached = watch.attached.swap(false, Ordering::SeqCst);
        let consumer = watch.consumer.clone();
        watch.drain().await;
        if attached {
            leaving.push((job, consumer));
        }
    }
    for (job, consumer) in leaving {
        if let Some(handle) = hub.route_job(&job).await {
            let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
        }
    }
    let _ = sink.send(ConsumerWrite::Close).await;
    drop(sink);
    let _ = writer.await;
}

/// Hand one task's outcome to the actor, answer with the verdict, then start the
/// pump that streams it. Returns once the verdict is queued — that is the NEX-3.3
/// contract, and the reason the next message is not read before this happens.
async fn handle_submit(session: &mut Session<'_>, submit: ConsumerFrame) {
    // The read loop routes only a `Submit` here. Naming the pattern (rather than
    // destructuring eight parameters) keeps the frame's own field names next to the
    // submission they build, and the `else` arm means a new inbound verb cannot
    // silently fall through into a submission.
    let ConsumerFrame::Submit {
        kind,
        payload,
        pool,
        requires,
        priority,
        detach,
        idempotency_key,
        // The frame-plane twin of the HTTP `x-supplier-id` header (`protocol.rs:616-627`):
        // a hub-minted id the consumer is asking to be queued on, or `None` for "any
        // supplier that serves this model". Never validated here — the actor's
        // `supplier_pin_allows` is the one that decides whether the pin is live, and an
        // id it does not recognise costs the submission nothing.
        supplier,
    } = submit
    else {
        return;
    };
    // NEX-6.6 vs NEX-6.1, decided by the frame rather than by the route: `false`
    // (the default on this face) means this socket *is* the subscription, so
    // hanging up orphans and deletes the task.
    let request = consumer::JobRequest {
        kind,
        pool,
        payload,
        requires,
        priority,
        detach: Some(detach),
    };
    // A consumer that names an idempotency key wants deduplication. One that does
    // not must not get the content-hash key the HTTP surface derives: the hub
    // cannot report *which* task a duplicate attached to on this face, so the
    // verdict would name a job the client never posted. A fresh key per submit is
    // the honest reading of "post me a task", and it is minted once so the
    // refusal below can name the same id it was derived from.
    let refusal_job = session.hub.ids.job_id();
    let fallback_key = format!("ws:{}", refusal_job.as_ref());
    let (submission, pool_id) = match consumer::build_submission(
        session.hub,
        &session.tenant,
        &request,
        // No route-level override on this face: the frame's own `pool` field is
        // already in `request.pool`, whereas HTTP has a second `?pool=` spelling.
        None,
        idempotency_key.as_deref(),
        fallback_key,
        // The one-admission-path parity spec §9.1 item 1 records, in this direction too:
        // the pin the HTTP header carries and the pin this frame carries reach the same
        // field of the same `Submission`, so the two faces queue a consumer on a supplier
        // identically.
        supplier,
    ) {
        Ok(built) => built,
        Err((status, code, message)) => {
            // Refused before the actor ever saw a task. The id named here was
            // minted for the verdict alone: NEX-3.3 wants the refusal tied to the
            // request, and in-order verdicts are what makes that tie usable.
            let frame = HubConsumerFrame::Rejected {
                job: refusal_job,
                status: status.as_u16(),
                code: code.to_string(),
                message,
                retry_after_secs: None,
            };
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    };

    // §5.2 at the runtime level: a key this hub has already routed is **followed**, not
    // re-submitted. Doing it here rather than letting the actor's duplicate branch answer
    // is the whole of the fix — that branch replays on the *original* id
    // (`actor.rs:1045-1064`), while this face registers its relay, its verdict and its
    // pump under the id it mints, so a verdict naming a different job would never resolve
    // and the client would read `hub_unavailable` for a task that is answering fine.
    //
    // `handle_attach` is the same follow the `attach` verb performs: verdict, `meta`, then
    // the replay of the original's ring. If the record went away in the meantime it
    // answers `unknown_job`, which is the truth about a task this hub can no longer
    // replay, and the client's next attempt starts a fresh execution (NEX-6.3).
    //
    // One consequence to keep honest: the actor's `nexus_idempotent_attach_total` no
    // longer moves for these retries, because the actor is never told about a duplicate
    // the hub can answer itself. The hub cannot compensate — `PoolGauges::from_actor`
    // (state.rs) rebuilds the mirrored registry from the actor on every dispatch.
    if let Some((_pool, existing)) = session
        .hub
        .idempotency_target(&submission.idempotency_key)
        .await
    {
        // `from_seq: 0` — a resubmission asks for the whole answer, which is the only
        // thing it can mean when the client never named a resume point. `detach` is
        // echoed rather than forced to `false`: this frame *is* a submission, so it
        // declares its own contract (NEX-6.6), unlike a bare attach.
        handle_attach(session, existing, 0, detach).await;
        return;
    }

    // Register the relay, the gate and this consumer's private replay queue
    // *before* the actor hears about the job — the ordering `consumer::submit`
    // documents, shared rather than reimplemented here. The `JobRelay` is bound rather
    // than dropped for one read only: the accepted frame's `supplier` falls back to the
    // relay's mirror of the lease, which is the same effect the HTTP faces name. The pump
    // below still does not hold it: if another surface prunes the row, the fan-out closes
    // and this stream ends in band with `closed`, which is the truth about what the hub
    // can still deliver.
    let (job, consumer, handle, relay, gate, live, replay) =
        consumer::submit(session.hub, &pool_id, submission).await;
    let accepted = match verdict_of(session.hub, gate).await {
        Gate::Accepted { worker } => HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: request.kind.clone(),
            pool: pool_id.clone(),
            // Echo of the contract this connection is under, so a client can tell
            // which lifetime rules it bought (NEX-6.6).
            detached: detach,
            // The supplier this task was connected to, or **absent from the frame** when
            // the hub has no id yet. Unlike the streamed HTTP faces this verdict is never
            // held (the one admission path spec §9.1 item 1 records, plus NEX-3.3's
            // in-order answers: the next task's bytes are not read until this one's
            // verdict is queued, so a wait here would stall the socket). A queued submit
            // therefore answers `accepted` with
            // no `supplier`, exactly the "not connected yet" reading `protocol.rs:675-687`
            // defines; the id a consumer wants next time is the one `GET /v1/jobs/{id}`
            // reports as `lease.worker`, which is the same lease this field would have
            // named.
            supplier: worker.or_else(|| relay.supplier()),
        },
        Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        } => HubConsumerFrame::Rejected {
            job: job.clone(),
            status: status.as_u16(),
            code,
            message,
            retry_after_secs,
        },
        Gate::Closed => HubConsumerFrame::Rejected {
            job: job.clone(),
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            code: "hub_unavailable".to_string(),
            message: "the hub did not answer in time; retry".to_string(),
            retry_after_secs: Some(HUB_UNAVAILABLE_RETRY_SECS),
        },
    };
    let refused = matches!(accepted, HubConsumerFrame::Rejected { .. });
    push(&session.sink, session.caps_out, accepted).await;

    if refused {
        // A refused submission must leave nothing behind on this connection. The
        // actor either never created the record (admission refusals) or drops it
        // on its own grace path, but the routing entry, the relay and the
        // idempotency row installed above are ours to prune.
        let _ = session.hub.forget_job(&job).await;
        handle.forget_relay(&job).await;
        session.hub.forget_idempotency_for_job(&job).await;
        return;
    }

    // §5.4 item 5: `meta` opens the stream, ahead of every event. Queued here rather
    // than from the pump, because this is the only point that still provably precedes
    // the first relay item. The record is read from the actor — the hub keeps no job
    // state of its own — so the phase is the actor's, not a guess. If the task has
    // already been forgotten (a worker can finish a zero-frame task this fast) there
    // is no snapshot to report, and the events that follow are the whole truth.
    if let Some(view) = session.hub.job_snapshot(&job).await {
        let meta = HubConsumerFrame::meta(&view, session.hub.wall_ms());
        push(&session.sink, session.caps_out, meta).await;
    }

    spawn_pump(session, job, consumer, handle, live, replay);
}

/// Refusal used by both follow paths, so an unknown task is answered identically
/// whichever verb reached it.
fn attach_refused(job: &JobId, status: StatusCode, message: &str) -> HubConsumerFrame {
    HubConsumerFrame::Rejected {
        job: job.clone(),
        status: status.as_u16(),
        // The HTTP surface's own code for this case, so both faces answer the same
        // question the same way (§5.1 vs §5.4).
        code: "unknown_job".to_string(),
        message: message.to_string(),
        retry_after_secs: None,
    }
}

/// Follow a task this hub already knows: verdict first, like `submit` — `accepted` is
/// queued before the pump exists, so no event byte can precede it.
///
/// Two callers, one flow: the `attach` verb, and a `submit` whose idempotency key this
/// hub has already routed (see `handle_submit`, which delegates here rather than
/// inventing a third follow path). `detached` is the contract the verdict echoes — `false`
/// for a bare attach, because following a task changes nothing about the lifetime its
/// submission declared (NEX-6.6), and the frame's own declaration for a resubmission,
/// which really is a submission.
async fn handle_attach(session: &mut Session<'_>, job: JobId, from_seq: u64, detached: bool) {
    let Some(handle) = session.hub.route_job(&job).await else {
        let frame = attach_refused(&job, StatusCode::NOT_FOUND, "no such job on this hub");
        push(&session.sink, session.caps_out, frame).await;
        return;
    };
    // Tenancy: `on_attach` keys nothing by tenant (the actor trusts its caller), so
    // this face has to check it — a guessed job id must not stream somebody else's
    // prompt. Same reading as `GET /v1/jobs/{id}/events` (consumer.rs:791-802).
    let view = match session.hub.job_snapshot(&job).await {
        Some(view) if view.tenant == session.tenant => view,
        Some(_) | None => {
            let frame = attach_refused(&job, StatusCode::NOT_FOUND, "no such job for this tenant");
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    };
    if let Some(watch) = session.watches.get(&job) {
        // Only a *live* stream is a duplicate. A task whose answer already ran out has
        // no pump and no consumer left — the pump cleared the flag on its way out — so
        // re-attaching it is the resume NEX-3.4's `through_seq` exists for, and the new
        // entry simply replaces the finished one.
        if watch.attached.load(Ordering::SeqCst) {
            // A second attach on the connection that already watches the task
            // would add a second consumer and a second pump for one stream:
            // refuse rather than duplicate the answer on one socket.
            let frame = HubConsumerFrame::Rejected {
                job,
                status: StatusCode::CONFLICT.as_u16(),
                code: "already_attached".to_string(),
                message: "this connection already watches that job".to_string(),
                retry_after_secs: None,
            };
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    }

    let relay = handle
        .relay(&job, session.hub.cfg.limits.consumer_backpressure_frames)
        .await;
    // The slot is created here, with the pump that will read it, and it leaves the
    // fan-out when that pump is dropped — see `state::Subscriber`.
    let live =
        state::Subscriber::attach(&relay, session.hub.cfg.limits.consumer_backpressure_frames)
            .await;
    let (replay_tx, replay_rx) =
        mpsc::channel::<state::RelayItem>(session.hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = session.hub.ids.consumer_id();
    if state::send(
        handle.as_ref(),
        ActorMsg::Attach {
            job: job.clone(),
            consumer: consumer.clone(),
            from_seq,
        },
    )
    .await
    .is_err()
    {
        // C2: the `live` slot goes with this function's `Subscriber`, but `replay_rx` goes with
        // it too while its sender stays in the relay — and a row with a `replay_target` is not
        // idle, so the orphan sweep could never take it. Un-install the target before answering.
        relay.abandon_replay_target().await;
        let frame = HubConsumerFrame::Rejected {
            job,
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            code: "hub_unavailable".to_string(),
            message: "the pool actor is draining".to_string(),
            retry_after_secs: Some(HUB_UNAVAILABLE_RETRY_SECS),
        };
        push(&session.sink, session.caps_out, frame).await;
        return;
    }
    // Mirror the lease for the surfaces that read it off the relay (the HTTP `x-supplier-id`
    // on a later aggregate of this same job) and for this frame's own `supplier`. The
    // snapshot above is the actor's own view, so this cannot disagree with
    // `GET /v1/jobs/{id}`; a task with no lease — still queued, or terminal with the lease
    // dropped — simply has nothing to report.
    if let Some(lease) = view.lease.as_ref() {
        relay.note_supplier(&lease.worker);
    }
    // `meta` first (§5.4 item 5), built while the record is still in hand: the
    // accepted verdict below moves the fields it echoes. Queued before the pump
    // exists, so `accepted` then `meta` then events is the order on the wire.
    let meta = HubConsumerFrame::meta(&view, session.hub.wall_ms());
    push(
        &session.sink,
        session.caps_out,
        HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: view.kind,
            pool: view.pool,
            // The contract this follow is under, echoed so a client knows which lifetime
            // rules it bought (NEX-6.6). `false` for the attach verb — following a task
            // never changes them; the frame's own declaration for a resubmission.
            detached,
            // The supplier the task is running on right now, from the lease read above;
            // omitted when the task has none, which is "no supplier connected", never a
            // refusal. A joiner that follows a finished task gets the answer body, not an
            // id — the lease is gone, and inventing one would be worse than saying nothing.
            supplier: view.lease.as_ref().map(|lease| lease.worker.clone()),
        },
    )
    .await;
    push(&session.sink, session.caps_out, meta).await;

    spawn_pump(session, job, consumer, handle, live, replay_rx);
}

/// Stop watching one task without cancelling it (NEX-6.6).
async fn handle_detach(session: &mut Session<'_>, job: JobId) {
    let Some(mut watch) = session.watches.remove(&job) else {
        // Not a task this connection is watching. `error` rather than `end`: there is
        // no stream here to end, and an `end` for a task the client is not following
        // would misreport what this connection carried. A task whose answer already ran
        // out lands here too now that a finished stream reclaims its own entry, and
        // that is the honest answer rather than a new one: the pump that ended the
        // stream detached this connection from the task on the way out, so the actor
        // already counts zero consumers for it.
        let frame = HubConsumerFrame::Error {
            job: Some(job),
            code: "not_attached".to_string(),
            message: "this connection does not watch that job".to_string(),
            retryable: false,
            through_seq: None,
        };
        push(&session.sink, session.caps_out, frame).await;
        return;
    };
    let consumer = watch.consumer.clone();
    // Swapped rather than read: the pump may clear it while this teardown drains the
    // stream, and whichever side sees `true` is the one that owes the actor the
    // `Detach`. Read before the drain on purpose, so a pump finishing underneath this
    // loop can only ever produce a second (idempotent) detach, never zero.
    let was_attached = watch.attached.swap(false, Ordering::SeqCst);
    // The pump closes the stream with `detached`; the session does the actor-side
    // detach here so the two cannot race over who told the hub.
    watch.signal(EndReason::Detached);
    let pump = watch.pump.take();
    drop(watch);
    if was_attached {
        if let Some(handle) = session.hub.route_job(&job).await {
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::Detach {
                    job: job.clone(),
                    consumer,
                },
            )
            .await;
        }
    }
    if let Some(task) = pump {
        // Same contract as `Watch::drain`: wait for the `end` frame this pump owes the
        // client, and if it cannot write it, end the task rather than let it outlive the
        // stream. A pump parked on a socket that is not draining holds the hub, the
        // socket's write queue, both relay receivers and its slot in the fan-out; the
        // detach it would have sent has already been sent above.
        let abort = task.abort_handle();
        if tokio::time::timeout(PUMP_DRAIN, task).await.is_err() {
            tracing::debug!(%job, "detached pump still parked; aborting it");
            abort.abort();
        }
    }
    // The relay is the task's shared fan-out, not this connection's property:
    // another connection may be watching the same task, and the actor prunes it
    // when it forgets the job (`state.rs:1065-1083`). Deliberately left alone.
}

/// Cancel one task explicitly. Retention stays (this is a consumer's decision,
/// not a lost connection) and the stream is closed here, because `on_cancel`
/// relays no terminal event of its own (`actor.rs:1449-1477`).
async fn handle_cancel(session: &mut Session<'_>, job: JobId) {
    let unknown = HubConsumerFrame::Error {
        job: Some(job.clone()),
        code: "unknown_job".to_string(),
        message: "no such job for this tenant".to_string(),
        retryable: false,
        through_seq: None,
    };
    let Some(handle) = session.hub.route_job(&job).await else {
        push(&session.sink, session.caps_out, unknown).await;
        return;
    };
    match session.hub.job_snapshot(&job).await {
        Some(view) if view.tenant == session.tenant => {}
        Some(_) | None => {
            push(&session.sink, session.caps_out, unknown).await;
            return;
        }
    }
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Cancel {
            job: job.clone(),
            // A consumer-initiated cancellation: the same reason
            // `DELETE /v1/jobs/{id}` sends, so no new vocabulary is needed and the
            // supplier sees one cause for both surfaces.
            reason: CancelReason::ConsumerGone,
        },
    )
    .await;
    if let Some(watch) = session.watches.get_mut(&job) {
        // Close this connection's stream now rather than waiting for a supplier
        // that may never have claimed the task. A supplier `error` that arrives
        // first already wrote the end frame, and the once-flag makes this a no-op.
        watch.signal(EndReason::Closed);
    }
}

/// Start the stream for one task and register this connection as its consumer.
///
/// The task starts out attached on purpose: both entry points (a submit that was
/// accepted, and an attach) leave this connection as an attached consumer of the task,
/// which is what the teardown's detach list is built from. The pump clears that flag
/// when the stream ends, so the list never holds a task this connection already stopped
/// watching — and posts this entry's `(job, consumer)` on the session's reap postbox on
/// its way out, so the map entry leaves with the stream rather than outliving it.
fn spawn_pump(
    session: &mut Session<'_>,
    job: JobId,
    consumer: ConsumerId,
    handle: Arc<state::PoolHandle>,
    live: state::Subscriber,
    replay: mpsc::Receiver<state::RelayItem>,
) {
    let (stop_tx, stop_rx) = oneshot::channel();
    let ended = Arc::new(AtomicBool::new(false));
    // Shared with the pump: end-of-stream ends the subscription, and the pump is the
    // one that knows when it happened.
    let attached = Arc::new(AtomicBool::new(true));
    // The connection starts out watching this task, mirrored into the hub's observe
    // registry here rather than at the two verdict sites: `spawn_pump` is the single
    // point both follow paths funnel through (an accepted submit and an `attach`), so
    // a refusal, or a follow that never reached this line, can never be reported as
    // attached. Everything the pump needs is cloned before the borrow of `session`
    // below, which the `watches` insert takes mutably.
    let hub = Arc::clone(session.hub);
    let conn = session.conn.clone();
    let reap = session.reap.clone();
    hub.consumer_attach(&conn, &job);
    let task = tokio::spawn(pump(Pump {
        follow: Follow {
            handle,
            consumer: consumer.clone(),
            job: job.clone(),
            live,
            replay,
        },
        caps: session.caps_out,
        sink: session.sink.clone(),
        stop: stop_rx,
        ended: Arc::clone(&ended),
        attached: Arc::clone(&attached),
        hub,
        conn,
        reap,
    }));
    session.watches.insert(
        job,
        Watch {
            consumer,
            attached,
            ended,
            stop: Some(stop_tx),
            pump: Some(task),
        },
    );
}

/// Await the actor's first verdict with the same budget the HTTP surfaces use, so
/// a submit cannot park the connection when nothing will ever claim it
/// (`limits.no_worker_grace_ms` plus the actor's own tick slack).
async fn verdict_of(hub: &Arc<Hub>, gate: oneshot::Receiver<Gate>) -> Gate {
    let budget = Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500);
    match tokio::time::timeout(budget, gate).await {
        Ok(Ok(gate)) => gate,
        // `Ok(Err(_))` means the relay — and with it the gate sender — is gone, so
        // the hub has already moved on. Both that and the timeout are the retryable
        // "the hub did not answer" answer, never a hang.
        Ok(Err(_)) | Err(_) => Gate::Closed,
    }
}

/// One `Pump` is one task's stream on this connection, until it ends with exactly
/// one `end`.
struct Pump {
    /// The subscription: pool handle, consumer id, job, replay + live sources.
    follow: Follow,
    caps: WireCaps,
    sink: mpsc::Sender<ConsumerWrite>,
    stop: oneshot::Receiver<EndReason>,
    /// End-of-stream once-flag, shared with the session (NEX-3.4).
    ended: Arc<AtomicBool>,
    /// This connection's attachment, shared with the session: cleared here when the
    /// stream ends, because a finished stream is a gone subscription and the session's
    /// teardown must not claim it a second time.
    attached: Arc<AtomicBool>,
    /// Hub side of the observe registry: the pump reports the connection's own
    /// end-of-stream, which no session code can see.
    hub: Arc<Hub>,
    /// Connection whose watch set this stream is leaving.
    conn: ConsumerId,
    /// Tells the session to drop this task's `watches` entry on the way out — see
    /// `Session::watches`.
    reap: mpsc::UnboundedSender<(JobId, ConsumerId)>,
}

/// What one relay item means for this connection's stream.
enum Translated {
    /// Write this frame and keep going.
    Send(HubConsumerFrame),
    /// Nothing to write: a keepalive comment, or a seq the replay already
    /// delivered (so an overlapping replay cannot print one delta twice).
    Skip,
    /// Write the frame if there is one, then close the stream. `reason` is the
    /// `end` reason this item implies, and `frame` is the last event itself —
    /// `None` for the relay's own `End` marker, which has no payload.
    Last {
        frame: Option<HubConsumerFrame>,
        reason: EndReason,
    },
}

async fn pump(pump: Pump) {
    let Pump {
        follow,
        caps,
        sink,
        mut stop,
        ended,
        attached,
        hub,
        conn,
        reap,
    } = pump;
    let Follow {
        handle,
        consumer,
        job,
        mut live,
        mut replay,
    } = follow;

    let mut through: u64 = 0;
    let mut alive = true;
    let mut reason = EndReason::Closed;
    // Two-phase pump, exactly as the SSE surfaces do it: drain this consumer's
    // private replay queue first (the dispatcher closes it once the buffered
    // prefix is written, and that close is what hands the stream to the live
    // fan-out), then follow the fan-out.
    let mut replay_open = true;
    // A `oneshot` that resolved `Err` stays ready forever, which in a biased select
    // would starve the streams; the flag takes the branch out instead.
    let mut stop_open = true;

    while alive {
        tokio::select! {
            biased;
            // A `detach`/`cancel`/close from the session.
            asked = &mut stop, if stop_open => match asked {
                Ok(asked) => {
                    reason = asked;
                    alive = false;
                }
                Err(_) => stop_open = false,
            },
            item = replay.recv(), if replay_open => match item {
                Some(item) => match relayed(&job, item, &mut through) {
                    Translated::Send(frame) => alive = push(&sink, caps, frame).await,
                    Translated::Skip => {}
                    Translated::Last { frame, reason: why } => {
                        reason = why;
                        alive = last(&sink, caps, frame).await;
                    }
                },
                None => replay_open = false,
            },
            item = live.recv() => match item {
                Some(item) => match relayed(&job, item, &mut through) {
                    Translated::Send(frame) => alive = push(&sink, caps, frame).await,
                    Translated::Skip => {}
                    Translated::Last { frame, reason: why } => {
                        reason = why;
                        alive = last(&sink, caps, frame).await;
                    }
                },
                // The relay dropped this consumer (its queue stayed full past the
                // grace window) or the relay ended. The `gap` goes out first — it
                // is ahead of the `end` in the same queue — rather than being
                // swallowed by the backlog that caused it (NEX-5.2's reported
                // hole): the stream closes instead of pretending the text went on.
                None => {
                    let gap = HubConsumerFrame::Gap {
                        job: job.clone(),
                        expected: through + 1,
                        got: through + 1,
                    };
                    push(&sink, caps, gap).await;
                    reason = EndReason::Lagged;
                    alive = false;
                }
            },
            // Unreachable in practice: the live branch has no guard, so the select
            // always has something to poll. Stated so a future guard cannot turn a
            // disabled-all-branches select into a panic.
            else => alive = false,
        }
    }

    // Exactly one `end` per task (NEX-3.4): the CAS is what makes "whoever closes
    // the stream first owns the frame" true when session and pump race.
    if ended
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let frame = HubConsumerFrame::end_for(
            &job,
            reason,
            // The cursor a consumer needs to resume elsewhere (NEX-3.4).
            (through > 0).then_some(through),
        );
        push(&sink, caps, frame).await;
    }
    // The subscription is over, whatever ended it, so this connection no longer
    // counts as a consumer of this task: the teardown must not report it as one, and a
    // fresh `attach` for the same task (the resume NEX-3.4's cursor enables) is no
    // longer a duplicate.
    attached.store(false, Ordering::SeqCst);
    // The same fact, reported to the monitor: this socket stopped watching this task.
    // The session cannot do this itself, because *it* did not end the stream — a task
    // that finished, lagged, or found the socket unwritable did, and only this task
    // knows when that happened. Without this line a long-lived connection that watched
    // a hundred finished tasks would report a hundred attached jobs until it closed.
    hub.consumer_detach(&conn, &job);
    // This connection's `watches` entry for the task can go now: the pump knows the
    // stream is over, the session never does, and until this postbox existed the entry
    // — a fresh `ConsumerId`, its `stop` oneshot, and the `JoinHandle` of the task now
    // finishing — sat in the map until the socket closed. Named with this pump's own
    // consumer id, so a re-`attach` that has already installed a new stream for the
    // same task is not reclaimed by the stream it replaced.
    let _ = reap.send((job.clone(), consumer.clone()));
    // A pump that ended on its own is the subscription going away (NEX-6.1). A
    // pump the session stopped has already been detached by the session, and
    // `on_detach` is idempotent, so saying it twice is cheaper than risking a
    // phantom consumer — a non-empty `consumers` set is what keeps NEX-6.3's
    // deletion and NEX-6.6's window from ever firing.
    let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
    // `live` and `replay` drop on the way out of this function, and dropping `live`
    // takes this consumer's queue out of the job's fan-out (see `state::Subscriber`).
    // That is the step that used to be missing: a sender left behind on a fan-out that
    // had already gone terminal was never noticed by anything, because nothing was
    // going to push to it again, and it kept whatever frames were still buffered in it
    // alive for the rest of the process's life.
}

/// Translate one relay item into this connection's frame.
fn relayed(job: &JobId, item: state::RelayItem, through: &mut u64) -> Translated {
    let event = match item {
        // The actor's keepalive comment: this transport has its own ping/pong, so
        // there is nothing to write. The SSE pumps skip it for the same reason.
        state::RelayItem::Comment(_) => return Translated::Skip,
        state::RelayItem::End => {
            // The fan-out's own marker: the task reached a terminal state on the hub.
            // Whether that was a `finish` or an `error` is not knowable here — this
            // consumer may have attached after the terminal event, or the event may
            // have been suppressed below as a replay overlap — so the stream closes
            // with `closed`, the one reason that is true either way.
            return Translated::Last {
                frame: None,
                reason: EndReason::Closed,
            };
        }
        state::RelayItem::Event(event) => event,
    };
    // Seqs already delivered are skipped. `Gap`, `Error`, `Meta` and `Replay`
    // carry no content seq and always go out.
    let seq = match &event {
        ConsumerEvent::Delta { seq, .. }
        | ConsumerEvent::Event { seq, .. }
        | ConsumerEvent::Log { seq, .. }
        | ConsumerEvent::Artifact { seq, .. }
        | ConsumerEvent::Finish { seq, .. } => Some(*seq),
        _ => None,
    };
    if let Some(seq) = seq {
        if seq <= *through {
            return Translated::Skip;
        }
        *through = seq;
    }
    let frame = HubConsumerFrame::relayed(job, event.clone());
    match &event {
        // A terminal event: the frame goes out, then the stream ends. `Finish` is
        // a complete answer; every `error` — supplier failure, cancellation, hub
        // shutdown — ends the stream as failed (NEX-5.2's in-band failure).
        ConsumerEvent::Finish { .. } => Translated::Last {
            frame: Some(frame),
            reason: EndReason::Finished,
        },
        ConsumerEvent::Error { .. } => Translated::Last {
            frame: Some(frame),
            reason: EndReason::Errored,
        },
        _ => Translated::Send(frame),
    }
}

/// Queue one frame, encoded under the outbound caps.
///
/// Returns whether the socket may still be written: `false` means the writer task
/// has stopped, which ends every pump rather than parking it on a queue nobody
/// drains. A frame that cannot fit the caps is logged and skipped — reachable only
/// by something that passed the ingress caps and grew in translation, never by a
/// delta (the worker face caps `Frame.text` at 256 KiB, far under the outbound
/// line cap).
async fn push(sink: &mpsc::Sender<ConsumerWrite>, caps: WireCaps, frame: HubConsumerFrame) -> bool {
    let line = match encode_line(&frame, &caps) {
        Ok(line) => line,
        Err(err) => {
            tracing::warn!(%err, "consumer frame exceeded the wire caps and was dropped");
            return true;
        }
    };
    sink.send(ConsumerWrite::Frame(line)).await.is_ok()
}

/// Writes the last event of a stream, if the item carried one, and reports that
/// the pump is done (`false` is the loop's new `alive`).
///
/// The result of the write is deliberately ignored: the `end` frame goes out after
/// the loop whatever the socket did with this event, so a consumer that vanished
/// mid-answer still sees the terminal frame in front of it or nothing at all —
/// never an open stream with nobody on it.
async fn last(
    sink: &mpsc::Sender<ConsumerWrite>,
    caps: WireCaps,
    frame: Option<HubConsumerFrame>,
) -> bool {
    if let Some(frame) = frame {
        let _ = push(sink, caps, frame).await;
    }
    false
}

#[cfg(test)]
mod tests {
    //! The reap windows, pinned as arithmetic rather than observed through a socket.
    //! `runtime_e2e::a_silent_consumer_socket_is_reaped_and_releases_what_it_held` proves the
    //! mechanism ends a session and releases what the session held; what this module proves is
    //! that the numbers the field documentation promises a reader are the numbers the code
    //! computes — which is the part an operator actually relies on when deciding how much risk a
    //! false verdict carries.
    use std::time::Duration;

    use nexus_core::Limits;

    use super::consumer_keepalive_windows;

    /// C4: the shipped ruler is 60 s, i.e. a 5 s probe and a 65 s verdict, and the two terms of
    /// `max(3 × probe, ruler + probe)` bind in different places depending on where the ruler sits
    /// relative to the clamps — the setting the reap tests use is the one case where `3 × probe`
    /// is the binding term.
    #[test]
    fn the_probe_windows_are_the_arithmetic_the_comments_claim() {
        // The default. `Limits::default()` is what a hub with no `NEXUS_CONSUMER_KEEPALIVE_MS`
        // runs on, so this is the window every deployed box is judged by.
        assert_eq!(
            consumer_keepalive_windows(&Limits::default()),
            (Duration::from_secs(5), Duration::from_secs(65)),
            "probe `clamp(60000 / 4, 250, 5000) = 5000`, verdict `max(15000, 60000 + 5000) = 65000`"
        );

        // The ruler C4 replaced, kept as a comparison because the difference is the argument:
        // same probe, verdict 20 s sooner. A false verdict at this setting cancels **and deletes**
        // a live task, which is what made 25 s too tight to ship.
        let ruler_20 = Limits {
            consumer_keepalive_ms: 20_000,
            ..Limits::default()
        };
        assert_eq!(
            consumer_keepalive_windows(&ruler_20),
            (Duration::from_secs(5), Duration::from_secs(25)),
            "`max(15000, 20000 + 5000) = 25000`, which is what the reviewer measured"
        );

        // What the reap tests set, chosen so the test can wait for a verdict in well under a
        // second: the probe is pinned at its floor, so the other term is the one that binds.
        let ruler_250 = Limits {
            consumer_keepalive_ms: 250,
            ..Limits::default()
        };
        assert_eq!(
            consumer_keepalive_windows(&ruler_250),
            (Duration::from_millis(250), Duration::from_millis(750)),
            "the one setting where `3 × probe = 750` beats `250 + 250 = 500`"
        );

        // Off is a value, not a mistake: a period no read loop will ever reach, rather than a
        // zero that turns the session's `select!` into a spin.
        let off = Limits {
            consumer_keepalive_ms: 0,
            ..Limits::default()
        };
        assert_eq!(
            consumer_keepalive_windows(&off),
            (super::NEVER, super::NEVER),
            "`0` means this socket lives exactly as long as its client says it does"
        );
    }
}
