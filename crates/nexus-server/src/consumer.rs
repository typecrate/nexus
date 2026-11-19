//! Consumer-facing HTTP surface (task §7): the OpenAI-shaped adapters, the
//! generic job API, and the operational routes.
//!
//! # The open-early contract, and where the 503 still fits (task §7.2)
//!
//! The handler sends `Submit` and then **awaits the first verdict** before it
//! writes a single body byte:
//!
//! * `Accepted` → headers + the role delta go out immediately (before any worker
//!   has claimed the job, since claiming is pull-driven), then `: ping` comments
//!   while it waits, then verbatim deltas.
//! * `Reject`   → a plain JSON error with the actor's status, including
//!   `503 + Retry-After` when no worker became ready in the grace window, and
//!   `400` for an unsatisfiable capability request.
//!
//! That ordering is what makes acceptance test 6 true: no bytes are spent until
//! the actor has decided the request is serviceable.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use axum::response::IntoResponse;
use axum::response::Response;
use nexus_core::adapters;
use nexus_core::auth::bearer;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PoolId;
use nexus_core::ids::TenantId;
use nexus_core::ids::WorkerId;
use nexus_core::model::Priority;
use nexus_core::protocol::CancelReason;
use nexus_core::relay::parse_resume_seq;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use nexus_core::Submission;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

use crate::state;
use crate::state::Gate;
use crate::state::Hub;
use crate::state::JobRelay;
use crate::state::RelayItem;

/// Header carrying a conversation id for affinity (task §17.4 resolution: the hub
/// trusts a client-supplied `messages` array plus this hint; it stores no history).
const SESSION_HEADER: &str = "x-session-id";
/// The supplier pin, in both directions: the consumer may ask to be queued on a
/// particular supplier, and the hub answers with the one it actually connected.
///
/// One name for both halves on purpose — a client that saved the value from an earlier
/// answer sends it back unchanged and lands on the same supplier, which is the whole
/// round trip the feature exists for. Optional in both directions: absent means "any
/// available supplier for this model", and an id this hub has never issued (or whose
/// supplier has since disconnected) is a hint the actor drops, never an error
/// (`actor.rs`'s `supplier_pin_allows`).
///
/// `pub(crate)` so `main.rs`'s CORS policy names the same spelling in its
/// `Access-Control-Expose-Headers` list: one constant for a wire name that has to match
/// in three places (request, response, CORS).
pub(crate) const SUPPLIER_HEADER: &str = "x-supplier-id";
/// Explicit idempotency key (task §5.2).
const IDEMPOTENCY_HEADER: &str = "idempotency-key";
// The `?pool=` override has no constant here on purpose: its wire name is the
// `ConsumerQuery::pool` field name, so a second spelling could only drift.
/// The SSE replay cursor (`task §7.5`). `http::header` ships no constant for it,
/// so the name is declared once here instead of being re-typed at every read.
const LAST_EVENT_ID: axum::http::HeaderName = axum::http::HeaderName::from_static("last-event-id");

/// How long a non-streaming request waits for its answer before giving up.
const AGGREGATE_TIMEOUT_SECS: u64 = 300;

/// Query parameters shared by the consumer endpoints.
#[derive(Debug, Deserialize, Default)]
pub struct ConsumerQuery {
    /// Pool override; the kind normally decides.
    #[serde(default)]
    pub pool: Option<String>,
    /// Replay floor for `/events` (`Last-Event-ID` wins).
    #[serde(default)]
    pub from_seq: Option<String>,
    /// `Last-Event-ID`, forwarded by proxies that cannot set headers.
    #[serde(default)]
    pub last_event_id: Option<String>,
}

/// Body of `POST /v1/jobs`.
#[derive(Debug, Deserialize)]
pub struct JobRequest {
    /// Registry kind, e.g. `image.workflow`.
    pub kind: String,
    /// Optional pool override.
    #[serde(default)]
    pub pool: Option<String>,
    /// Opaque payload, forwarded to the worker verbatim.
    pub payload: Value,
    /// Optional capability requirements the client declares.
    #[serde(default)]
    pub requires: Option<nexus_core::protocol::Requires>,
    /// Optional priority class name.
    #[serde(default)]
    pub priority: Option<String>,
    /// NEX-6.6's explicit opt-in, **required** on this route (spec §9.1 item 3:
    /// detachment "must be stated as such on the request; it never applies to a
    /// plain 'stream me the answer' submission").
    ///
    /// Only `true` is accepted here, and the two refusals are separate facts:
    ///   * absent — nothing was stated. An omitted field is not a declaration, so
    ///     defaulting it would hand every plain `POST /v1/jobs` detached-lifetime
    ///     semantics as a route artifact, which is precisely what NEX-6.6 forbids.
    ///     Refused with `400 detach_required`.
    ///   * `true` — stated: "post it now, I will pick the result up later". The task
    ///     keeps running when nothing is attached, bounded by
    ///     `limits.detached_orphan_ttl_ms`.
    ///   * `false` — the plain "stream me the answer" contract of NEX-6.1, which
    ///     this route cannot honour: it answers `202` and hangs up, so a coupled
    ///     submission would be orphaned and deleted before the client could poll it.
    ///     Refused with the same code rather than silently picking one reading.
    ///
    /// `GET /v1/consumer` is where `detach: false` is meaningful, because there the
    /// connection really is the subscription; on that face the field stays optional
    /// and defaults to attached (`consumer_ws.rs:375-382`).
    ///
    /// `Option<bool>` rather than `bool` with `#[serde(default)]`: a plain `bool`
    /// would make an absent field a serde failure, and the route would answer
    /// `400 bad_json` for a body that is perfectly valid JSON. The refusal belongs
    /// in the handler, where the code and the message can name the contract.
    #[serde(default)]
    pub detach: Option<bool>,
}

/// `POST /v1/chat/completions`.
pub async fn chat_completions(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    openai_endpoint(hub, &headers, &body, adapters::KIND_CHAT).await
}

/// `POST /v1/completions`.
pub async fn completions(State(hub): State<Arc<Hub>>, headers: HeaderMap, body: Bytes) -> Response {
    openai_endpoint(hub, &headers, &body, adapters::KIND_COMPLETION).await
}

/// Shared implementation of the two OpenAI-shaped routes.
async fn openai_endpoint(hub: Arc<Hub>, headers: &HeaderMap, raw: &[u8], kind: &str) -> Response {
    let tenant = match tenant_of(&hub, headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(body) = serde_json::from_slice::<Value>(raw) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "the request body is not valid JSON",
            false,
        );
    };
    let Ok(request) = adapters::ChatRequest::parse(&body, kind) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "the body is not a usable chat or completions request",
            false,
        );
    };
    let Some(pool_id) = pool_for_kind(&hub, kind) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "pool_unconfigured",
            "this kind has no pool in the configuration",
            true,
        );
    };
    let session = header_value(headers, SESSION_HEADER).map(str::to_string);
    // Tenant-namespaced, for the reason given in `create_job`.
    let idempotency =
        header_value(headers, IDEMPOTENCY_HEADER).map(|key| format!("{}:{key}", tenant.as_ref()));
    let supplier = supplier_hint(headers);
    let submission = request.to_submission(
        &hub.cfg,
        &tenant,
        pool_id.clone(),
        idempotency.as_deref(),
        session,
        // The 6th argument is the `x-supplier-id` pin (`adapters::to_submission`); the
        // hub has no opinion about whether that supplier exists.
        supplier,
        hub.cfg.tenant_priority(&tenant),
    );
    let stream = request.stream;
    let model = request.model.clone();
    // The actor arms this submission's dispatch deadline from the kind's own override
    // (`actor.rs:1250`), and the streamed faces need the same number to bound the header
    // hold below. Captured here because `submission` moves into `submit`.
    let kind = submission.kind.clone();

    // §5.2 at the runtime level, on the surface that actually carries the answer: a key
    // this hub has already routed is **followed**, not re-run. See `create_job` for the
    // full reason this is decided on the face rather than left to the actor — the
    // actor's duplicate branch replays on the original id, which is not the id this
    // request would have registered its relay and gate under, and the retry used to
    // time out as `503 hub_unavailable` while the original's replay was dropped.
    //
    // One caveat worth stating where it happens: the actor's own
    // `nexus_idempotent_attach_total` no longer moves for these retries, because the
    // actor is never told about a duplicate this hub can answer. The hub cannot
    // compensate — `PoolGauges::from_actor` (state.rs:213-222) overwrites the mirrored
    // registry from the actor on every dispatch, so a hub-side increment would be
    // erased a tick later.
    if let Some((_pool, existing)) = hub.idempotency_target(&submission.idempotency_key).await {
        if let Some((consumer, handle, live, replay, relay)) =
            join_existing_job(&hub, &existing).await
        {
            // No verdict wait, unlike a fresh submission (§7.2): an attach emits only
            // `ReplayTo` (`actor.rs:1228-1260`), and the record it follows is already
            // past `Pending`, so its stream is open by definition — the same reading
            // `GET /v1/jobs/{id}/events` takes.
            let follow = Follow {
                handle,
                consumer,
                job: existing.clone(),
                live,
                replay,
            };
            // The id comes off the relay the join seeded from the record's authoritative
            // lease (`join_existing_job`), so this face agrees with
            // `GET /v1/jobs/{id}`'s `lease.worker` without asking the actor again. `None`
            // — a queued or terminal task — means there is nothing to name, which is the
            // same rule `Gate::Accepted{worker: None}` carries for a fresh submission.
            let worker = relay.supplier();
            if !stream {
                // `false`: a joiner does not own the relay it borrowed (see the
                // parameter's documentation).
                return aggregate_response(&hub, follow, &model, false, &relay).await;
            }
            return stream_completion(&hub, follow, model, worker).await;
        }
        // Declining means the mirrored record is gone (NEX-6.3 deleted it, or its
        // retention lapsed). Drop the straggler and let `submit` start the fresh
        // execution NEX-6.3 asks for, re-mirroring the key on the way.
        hub.forget_idempotency(&submission.idempotency_key).await;
    }
    let (job, consumer, handle, relay, gate, live, replay) =
        submit(&hub, &pool_id, submission).await;

    // Await the actor's first verdict before writing anything (task §7.2).
    let verdict = tokio::time::timeout(
        Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500),
        gate,
    )
    .await;
    // The supplier the response head will name. Only the streamed surface needs it here:
    // an aggregated answer writes its head once the body is complete, and reads the id
    // from the relay at that moment (see `aggregate_response`).
    let mut worker: Option<WorkerId> = None;
    match verdict {
        Ok(Ok(Gate::Accepted { worker: leased })) => {
            // The header-hold rule (spec §7 "Supplier pinning (`x-supplier-id`)": the one
            // place it lets a response *head* move earlier than admission): see
            // `supplier_before_body` for why a queued submission's head may wait, and
            // for the bound that stops that wait from ever becoming a hang.
            if stream {
                // Armed around the hold: dropping this guard while the hold is still pending is
                // the only way the hub learns the consumer hung up inside it (see the struct's
                // doc for why the pump cannot see it). Disarmed on the next line, so from the
                // moment the head exists the pump owns the consumer's absence as usual.
                let mut tripwire = SupplierHoldDetach {
                    armed: true,
                    handle: Arc::clone(&handle),
                    job: job.clone(),
                    consumer: consumer.clone(),
                };
                worker = supplier_before_body(&hub, &relay, &kind, leased).await;
                tripwire.disarm();
            }
        }
        Ok(Ok(Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        })) => {
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, retry_after(secs));
            }
            response
                .headers_mut()
                .insert("x-nexus-job", header_of(job.as_ref()));
            return response;
        }
        // A timeout here is not "the worker is slow": it is the hub failing to
        // answer, and the honest answer is a retryable 503, not a hung stream.
        Ok(Ok(Gate::Closed)) | Ok(Err(_)) | Err(_) => {
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "hub_unavailable",
                "the hub did not answer in time; retry",
                true,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    };
    let follow = Follow {
        handle,
        consumer,
        job: job.clone(),
        live,
        replay,
    };
    if !stream {
        // The request owns this job's relay: nothing else will ever follow an id that
        // was minted for this one call.
        //
        // No header hold here, unlike the streaming arm: an aggregated answer writes its
        // head only when the body is complete, and by then the lease has happened (the
        // answer came from a supplier), so `aggregate_response` reads the id at assembly
        // time rather than guessing it now.
        return aggregate_response(&hub, follow, &model, true, &relay).await;
    }
    stream_completion(&hub, follow, model, worker).await
}

/// Open the OpenAI SSE body for one followed job: headers, the role delta, then the
/// pump that carries the rest (§7.2's ordering, §16.14's verbatim relay).
///
/// Split out of `openai_endpoint` because the idempotent-join path needs the identical
/// sequence for a job this request did not create; two copies of "role chunk before the
/// first keepalive" is how the two surfaces would drift.
async fn stream_completion(
    hub: &Arc<Hub>,
    follow: Follow,
    model: String,
    supplier: Option<WorkerId>,
) -> Response {
    let job = follow.job.clone();
    let created = hub.wall_ms();
    let (out_tx, out_rx) =
        mpsc::channel::<Result<Event, Infallible>>(hub.cfg.limits.consumer_backpressure_frames);
    // Headers first, then the role delta, then a keepalive while queued (§7.2).
    let role = json_data_event(adapters::role_chunk(&job, &model, created));
    let _ = out_tx.send(Ok(role)).await;
    tokio::spawn(pump_sse(follow, model, created, out_tx));
    let mut response = Sse::new(ReceiverStream::new(out_rx))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("ping"),
        )
        .into_response();
    add_stream_headers(&mut response);
    response_headers(&job, supplier.as_ref(), &mut response);
    response
}

/// Tag a response with the job it belongs to, so a client can poll or replay it
/// after the socket closes — spec §7 spells both halves of that: the `/v1/jobs/{id}` route
/// the id addresses on HTTP, and `through_seq` as the cursor for a later
/// `attach {from_seq}` on the socket face — and with the supplier it was connected to, so
/// it can ask for that supplier again next time (`SUPPLIER_HEADER`).
///
/// The supplier header is **omitted, never sent empty**, when the hub has no id to name:
/// a refusal connected nobody, and an id of `""` would look like a supplier the client
/// could pin on its next request. Both names are also the two headers the CORS layer
/// exposes to a browser (`main.rs`'s `observe_cors`), which is what makes them readable
/// from a dashboard on another origin.
fn response_headers(job: &JobId, supplier: Option<&WorkerId>, response: &mut Response) {
    if let Ok(value) = HeaderValue::from_str(job.as_ref()) {
        response.headers_mut().insert("x-nexus-job", value);
    }
    set_supplier(response, supplier);
}

/// Write `x-supplier-id` and nothing else: the id the hub connected this request to, or
/// nothing at all.
///
/// The "or nothing" half is the contract — an empty or invented value would read as a
/// supplier the client may pin next time, which is worse than no header, because the
/// client would keep pinning it. A hub-minted UUID is always header-safe, but the value
/// is checked rather than assumed: `HeaderValue::from_str` is the fallible boundary and
/// the failure direction is "omit", never "truncate".
fn set_supplier(response: &mut Response, supplier: Option<&WorkerId>) {
    if let Some(value) = supplier.and_then(|worker| HeaderValue::from_str(worker.as_ref()).ok()) {
        response.headers_mut().insert(SUPPLIER_HEADER, value);
    }
}

/// The frame number an event carries, or `None` for the ones that have none (`Meta`,
/// `Gap`, `Error`).
///
/// Extracted from `send_block` so the aggregating path can apply the **same** overlap rule
/// the streaming pumps do: a joiner receives its buffered prefix on its private queue *and*
/// the same seqs on the fan-out, and printing them twice is §16.4's duplicate-delta bug.
/// Before the idempotent join path existed this could not show up — a fresh submission has
/// no buffered prefix — which is why only the pumps guarded it.
fn event_seq(event: &ConsumerEvent) -> Option<u64> {
    match event {
        ConsumerEvent::Delta { seq, .. }
        | ConsumerEvent::Event { seq, .. }
        | ConsumerEvent::Log { seq, .. }
        | ConsumerEvent::Artifact { seq, .. }
        | ConsumerEvent::Finish { seq, .. } => Some(*seq),
        _ => None,
    }
}

/// Fold one relayed event into an aggregated answer, skipping any seq this consumer has
/// already contributed (see [`event_seq`]). Returns whether the event was terminal, which
/// is what ends `aggregate_response`'s live loop.
fn collect_event(
    event: ConsumerEvent,
    events: &mut Vec<ConsumerEvent>,
    chunks: &mut Vec<String>,
    through: &mut u64,
) -> bool {
    if let Some(seq) = event_seq(&event) {
        if seq <= *through {
            return false;
        }
        *through = seq;
    }
    if let ConsumerEvent::Delta { text, .. } = &event {
        chunks.push(text.clone());
    }
    let terminal = matches!(
        &event,
        ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
    );
    events.push(event);
    terminal
}

// -------------------------------------------------------------------------------------------
// C1 — the replay/live reader every streamed-HTTP pump shares
// -------------------------------------------------------------------------------------------

/// The clock a [`StreamPump::next`] may poll: the caller's sleep, pinned in place at its own
/// scope by `tokio::pin!`, or `None` for "no deadline".
///
/// `Sleep` is `!Unpin`, so it is the caller that pins it and this is only ever a reborrow of
/// that pin — which keeps the pump free of `unsafe` (this crate has none) and keeps the sleep
/// from moving while a poll is live: holding `&mut` to it is exactly what stops its owner
/// re-assigning the slot.
type Clock<'a> = Option<std::pin::Pin<&'a mut tokio::time::Sleep>>;

/// As a future: the clock resolving, or never, so `StreamPump::next` keeps one select shape
/// whether or not the caller armed a deadline. `Self` here is `Option<&mut Pin<&mut Sleep>>`,
/// reborrowed from the caller's slot on every pass of the pump's loop.
async fn clock_fired(clock: &mut Clock<'_>) {
    match clock {
        Some(clock) => clock.as_mut().await,
        None => std::future::pending::<()>().await,
    }
}

/// Why the three streamed-HTTP answers (`POST /v1/jobs` aggregate, `GET /events` SSE,
/// `POST /v1/jobs:relay`) read their two queues through this type instead of the plain
/// `while let Some(item) = replay.recv().await` they used to.
///
/// A join is handed two queues: the private `replay` for the buffered prefix, and the `live`
/// fan-out slot. Each of them is closed by *another* task, so draining one strictly before the
/// other makes a pump's lifetime a promise about that other task's cleanup. One interleaving
/// breaks the promise:
///
/// 1. the joiner resolves the job's relay row `R` (`PoolHandle::relay`);
/// 2. the actor serves `Terminal` for the job: `end_stream()` on `R`, then `forget_relay`;
/// 3. only now does the joiner install itself as `R`'s replay target and post `Attach`;
/// 4. `on_attach` finds the job terminal and answers `ReplayTo`, which is **lookup-only**
///    (`state.rs`): `R` is no longer in `relays`, so neither the buffered prefix nor an `End`
///    is ever written to that queue.
///
/// The `Sender` half of that queue lives inside `R`, and the joiner's own `Subscriber` keeps
/// `R` alive, so `replay.recv()` parks there forever: the body never completes (keepalives
/// only), the pump task and its 256-frame queue never go away, and `JobRelay::is_idle()` stays
/// false forever — so the orphan sweep cannot take that row either. A cheaper route to the
/// same park is a client that hangs up *during* the Attach round-trip: the replay loop never
/// consulted `tx.closed()`, which the live loop had been honouring since NEX-6.1.
///
/// The WS pump was never exposed to this, because it selects over all three sources at once
/// (`consumer_ws.rs`). This type is that shape, shared, so the whole "the replay queue may
/// never close" class is removed instead of one dispatcher arm being papered over.
///
/// # Ordering is byte-identical for a live consumer
///
/// A live delta can win the select while the backlog is still open. The pump does not print it
/// then: it pushes the item onto `held` (FIFO) and keeps polling, returning backlog items as
/// they arrive, and only starts handing over `held` once the backlog reports `None`. So the
/// order stays *all backlog frames in their queue's FIFO order, then all live frames in
/// theirs*, exactly as the old strictly sequential pumps printed; the caller's overlap
/// watermark de-duplicates a prefix that overlaps the fan-out, unchanged. The one cost is that
/// `held` grows while the backlog stays open. In the healthy join it does not stay open — the
/// dispatcher writes the buffered prefix and closes the target in the same dispatch that
/// answers `Attach` — and a backlog that is stranded is stranded precisely because the task is
/// terminal, which closes the live queue and stops `held` growing. The other two exits
/// (`body_closed`, the aggregate's deadline) bound what remains.
///
/// # How a queue that never closes still ends the pump
///
/// The live queue, the response body and the caller's deadline are polled from the first poll,
/// never gated behind the backlog, so the backlog is never the only thing a pump waits on:
/// `body_closed` fires the moment hyper drops a parked body (a client that hung up during the
/// Attach round-trip), and the aggregate's deadline fires whatever else is pending. The one
/// shape that still needs the backlog itself is a stranded join with a patient client: the
/// backlog is open, the fan-out is closed, and the stream waits on keepalives until the client
/// hangs up — because an open backlog is also exactly what a join of an already-terminal job
/// looks like while its prefix is in flight, and ending the stream there would truncate a
/// complete answer. What is no longer possible is the old outcome: a pump that outlived its
/// task forever, holding a relay row no sweep could take.
struct StreamPump {
    /// The join's live fan-out slot, held rather than copied: retiring it is this pump's exit
    /// work, and `Subscriber::drop` does it whether the pump ends, fails, or is aborted.
    live: state::Subscriber,
    /// The join's private replay queue.
    replay: mpsc::Receiver<RelayItem>,
    /// The response body this pump writes, so its `closed()` can be raced against the event
    /// sources. `None` for the aggregate surface, which has no streaming body and bounds its
    /// wait with `deadline` instead.
    body: Option<mpsc::Sender<Result<Event, Infallible>>>,
    /// Live items taken out of `live` while the backlog was still open, in arrival order.
    /// Flushed before any further live item, which is what keeps the printed order identical
    /// to the old pumps' (see the type's docs).
    held: VecDeque<RelayItem>,
    /// The live fan-out said `End` while the backlog was still open, so the end of the stream is
    /// owed *later* rather than never — see the deferral in `next`. One bit is enough: `End` is
    /// the last thing a fan-out can ever send.
    live_end: bool,
    /// The replay queue has yielded `None`: the backlog is closed, so live items may be
    /// printed. Also the aggregate's phase marker — see `replay_done`.
    replay_done: bool,
    /// The live queue has yielded `None` — the task is terminal (`Terminal` dropped `live`) or
    /// the relay ended. Nothing more can arrive from the fan-out at that point.
    live_done: bool,
    /// The response body's receiver is gone: hyper dropped the parked body because the client
    /// hung up (NEX-6.1).
    body_closed: bool,
    /// The caller's deadline expired (the aggregate surface only).
    timed_out: bool,
    /// The live queue closed without a terminal event: the stream ended on the actor's terms,
    /// which the streaming pumps report in band rather than pretending the text continued.
    ended: bool,
}

impl StreamPump {
    /// Wrap a join's two queues. `body` is the pump's own channel to the response, passed in
    /// because NEX-6.1's notice lives there, not in the relay.
    fn new(
        live: state::Subscriber,
        replay: mpsc::Receiver<RelayItem>,
        body: Option<mpsc::Sender<Result<Event, Infallible>>>,
    ) -> Self {
        Self {
            live,
            replay,
            body,
            held: VecDeque::new(),
            live_end: false,
            replay_done: false,
            live_done: false,
            body_closed: false,
            timed_out: false,
            ended: false,
        }
    }

    /// True from the moment the backlog closes — the same point where the old pumps moved from
    /// their replay loop to their live loop. `aggregate_response` uses it as that phase marker,
    /// because the two phases read `End` differently: the buffered prefix ignores it, the live
    /// fan-out treats it as an incomplete stream.
    fn replay_done(&self) -> bool {
        self.replay_done
    }

    /// True when `next` returned `None` because the caller's deadline expired.
    fn timed_out(&self) -> bool {
        self.timed_out
    }

    /// True when `next` returned `None` because the live queue closed without a terminal event.
    fn ended(&self) -> bool {
        self.ended
    }

    /// `next` with no deadline, which is what every streaming pump wants: it ends when the task
    /// is terminal or the client hangs up, never because the hub grew impatient. Keeping the
    /// pinned-clock type out of the call sites is only about reading — the pumps say what they
    /// mean, and the clock stays where it is owned.
    async fn next_item(&mut self) -> Option<RelayItem> {
        let mut no_clock: Clock<'_> = None;
        self.next(&mut no_clock).await
    }

    /// Next item, biased backlog-first, or `None` at an exit — see the flags above.
    async fn next(&mut self, clock: &mut Clock<'_>) -> Option<RelayItem> {
        loop {
            // 1. Live items taken before the backlog closed are printed as soon as the backlog
            //    is done with, in arrival order — the order the old sequential pumps printed
            //    them in. Printing them earlier would reorder the stream.
            if self.replay_done {
                if let Some(item) = self.held.pop_front() {
                    return Some(item);
                }
                // 1b. A live `End` taken while the backlog was still open ends the stream *here*
                //     rather than at the instant it was received: the backlog and everything
                //     held behind it are printed first, and only then is the stream over. It
                //     could not go in `held` with the deltas, because the pumps read `End` as
                //     "stop printing" — printing it on arrival would cut off a prefix that was
                //     still on its way, and for a join of a job that went terminal during its own
                //     attach that prefix is the whole answer.
                if self.live_end {
                    // `ended` stays false on purpose: this stream stopped where it was told to,
                    // so the caller owes no in-band "it stopped short of its ending" notice.
                    return None;
                }
            }
            // 2. Both queues closed without a terminal event: the fan-out is finished with this
            //    join and the backlog is done with it too, so nothing more can arrive.
            //    `replay_done` is *required* here rather than optional: an open replay queue
            //    means the dispatcher still owns the target (the `ReplayTo` arm takes it out of
            //    the relay and drops it in the same arm), so frames may still be owed — ending on
            //    `live_done` alone would truncate a join whose prefix is in flight, which is
            //    exactly what a join of an already-terminal job looks like. What C1 removes is the
            //    dependency on the backlog alone: the body and the caller's deadline stay
            //    reachable while the live queue is polled alongside it.
            if self.replay_done && self.live_done {
                self.ended = true;
                return None;
            }
            // Read the flags into locals: the select's branches capture `&mut self.replay` and
            // `&mut self.live`, and a guard borrowing all of `self` would collide with them.
            let (replay_open, live_open, body_open, armed) = (
                !self.replay_done,
                !self.live_done,
                self.body.is_some() && !self.body_closed,
                clock.is_some(),
            );
            let branch = tokio::select! {
                biased;
                item = self.replay.recv(), if replay_open => Branch::Replay(item),
                item = self.live.recv(), if live_open => Branch::Live(item),
                () = Self::body_gone(self.body.as_ref()), if body_open => Branch::Gone,
                // Reborrowed from the caller's slot on every pass, so the select taking the
                // future by value does not consume the clock.
                () = clock_fired(clock), if armed => Branch::Deadline,
            };
            match branch {
                Branch::Replay(item) => match item {
                    Some(item) => return Some(item),
                    None => self.replay_done = true,
                },
                // Held rather than printed while the backlog is open; printed straight away
                // once it has closed.
                Branch::Live(Some(item)) => {
                    if self.replay_done {
                        return Some(item);
                    }
                    if matches!(item, RelayItem::End) {
                        self.live_end = true;
                    } else {
                        self.held.push_back(item);
                    }
                }
                Branch::Live(None) => self.live_done = true,
                Branch::Gone => {
                    self.body_closed = true;
                    return None;
                }
                Branch::Deadline => {
                    self.timed_out = true;
                    return None;
                }
            }
        }
    }

    /// Non-blocking twin of `next`, same priority order. `None` here means "nothing buffered
    /// right now", not "the stream ended" — the aggregate's timeout sweep calls it until it
    /// runs dry, so an answer that could have included buffered events does not silently drop
    /// them because the clock fired.
    fn try_next(&mut self) -> Option<RelayItem> {
        // Same precedence as `next`: while the backlog is open it comes first, so a swept
        // answer is in the order the streamed answer would have been in.
        if !self.replay_done {
            if let Ok(item) = self.replay.try_recv() {
                return Some(item);
            }
        }
        if let Some(item) = self.held.pop_front() {
            return Some(item);
        }
        // A deferred `End` is deliberately not re-declared here: the only caller is the
        // aggregate's timeout sweep, which builds a JSON answer out of *events*. An `End` would
        // tell it nothing it does not already know from running dry.
        self.live.try_recv()
    }

    /// Resolves when the response body this pump writes has been dropped: hyper drops a parked
    /// body as soon as the client hangs up (measured in
    /// `runtime_e2e::an_idle_sse_hangup_cancels_the_worker_without_a_failed_write`), and
    /// dropping it closes this channel. A pump with no streaming body has already disabled the
    /// branch through `body_open`, so this is never polled there.
    async fn body_gone(body: Option<&mpsc::Sender<Result<Event, Infallible>>>) {
        if let Some(body) = body {
            body.closed().await;
        }
    }
}

/// Which branch of `StreamPump::next` won. A named type rather than tuples, because the four
/// arms mean four different things to the caller and the match has to say so.
enum Branch {
    /// An item from the private replay queue, or `None` once that queue has closed.
    Replay(Option<RelayItem>),
    /// An item from the live fan-out, or `None` once that queue has closed.
    Live(Option<RelayItem>),
    /// The response body is gone (NEX-6.1).
    Gone,
    /// The caller's deadline fired.
    Deadline,
}

/// Collect the whole stream and answer once, for `stream: false` (test 17).
///
/// `prune_relay` is the difference between a request that **owns** the relay it is
/// reading and one that borrowed it. A fresh submission mints a job id that nothing
/// else will ever follow, so its entry goes with the request. An idempotent join does
/// **not** own it: the relay belongs to the job, and removing the map entry while the
/// original submission's pump is still attached moves the fan-out to a fresh relay
/// under a live stream (`PoolHandle::relay` creates on demand), which would silence the
/// original consumer's deltas.
///
/// `relay` is the same job's [`JobRelay`] the face registered (not a second one: it is the
/// `Arc` the dispatcher wrote this job's supplier id into), and it is only read for that
/// id, at the moment the head is written. Taking the handle instead of the id is what lets
/// a `stream: false` request name a supplier the submission-time verdict could not: the
/// queued case's `Assigned` lands while this pump is still collecting. Note the two
/// near-identical names — `relay` is the fan-out, `replay` (moved into `Follow` above) is
/// this consumer's private prefix queue.
async fn aggregate_response(
    hub: &Arc<Hub>,
    follow: Follow,
    model: &str,
    prune_relay: bool,
    relay: &Arc<JobRelay>,
) -> Response {
    let Follow {
        handle,
        consumer,
        job,
        live,
        replay,
    } = follow;
    let mut events: Vec<ConsumerEvent> = Vec::new();
    let mut chunks: Vec<String> = Vec::new();
    // The joiner's overlap watermark, same rule as the streaming pumps: the buffered
    // prefix arrives on the private queue and the fan-out repeats those seqs.
    let mut through: u64 = 0;
    // A joiner's buffered prefix arrives on its private queue, so the assembled
    // answer contains every delta rather than only the ones after the join.
    let mut outcome = "end";
    // The buffered prefix can already carry the terminal event: a retry of a job that
    // finished while the hub still keeps its ring. Stopping there is what `pump_sse` does
    // for the streaming surface, and for the same reason — waiting for a second terminal on
    // the fan-out would answer `503 no_ready_worker` for an answer that is already complete.
    //
    // C1: one pass over both queues instead of "all of replay, then all of live", because the
    // old replay-first loop parked here forever when nothing closed the joiner's queue (see
    // `StreamPump`). The clock is armed from the first poll, so the whole wait — buffered
    // prefix included — is bounded by `AGGREGATE_TIMEOUT_SECS`; previously only the live phase
    // was bounded, and a stranded prefix was not a timeout but a hang. A slow answer is a
    // retryable `503` either way, so a bounded prefix phase loses nothing.
    let mut pump = StreamPump::new(live, replay, None);
    // Pinned here, at the pump's own scope, because `Sleep` is `!Unpin` and the pump only ever
    // reborrows the pin (see `Clock`).
    let clock = tokio::time::sleep(Duration::from_secs(AGGREGATE_TIMEOUT_SECS));
    tokio::pin!(clock);
    let mut deadline = Some(clock);
    while let Some(item) = pump.next(&mut deadline).await {
        match item {
            // An aggregated answer has nowhere to put a `: ping`; the SSE surface owns those.
            RelayItem::Comment(_) => {}
            // Only the live phase reads `End` as "the stream ended without a terminal event".
            // The buffered prefix used to be read until it closed, and `replay_done()` is that
            // same phase marker the old two-loop version branched on.
            RelayItem::End if pump.replay_done() => break,
            RelayItem::End => {}
            RelayItem::Event(event) => {
                if collect_event(event, &mut events, &mut chunks, &mut through) {
                    outcome = "terminal";
                    break;
                }
            }
        }
    }
    if pump.timed_out() {
        outcome = "timeout";
        // The clock firing must not throw away frames that were already buffered for this
        // join: they are the answer's content, and dropping them here would answer `503` to a
        // request that had its terminal sitting in a queue.
        while let Some(item) = pump.try_next() {
            if let RelayItem::Event(event) = item {
                if collect_event(event, &mut events, &mut chunks, &mut through) {
                    outcome = "terminal";
                    break;
                }
            }
        }
    }
    // §6: the request socket was the subscription. Whether or not an answer
    // arrived, the actor has to learn it closed, or a `stream: false` client
    // that gave up leaves the job occupying a worker slot.
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Detach {
            job: job.clone(),
            consumer,
        },
    )
    .await;
    if prune_relay {
        handle.forget_relay(&job).await;
    }
    if events
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Error { .. }))
    {
        return json_error(
            StatusCode::BAD_GATEWAY,
            "stream_incomplete",
            "the worker stopped before finishing",
            false,
        );
    }
    if outcome != "terminal" {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_ready_worker",
            "the hub produced no answer in time; retry",
            true,
        );
    }
    let assembled = adapters::assemble_completion(&job, model, hub.wall_ms(), &events);
    // The supplier id is read **here**, not at admission: an aggregated answer only writes
    // its head once the terminal event has arrived, and a terminal answer has always been
    // leased by then, so the relay's mirror of the `Assigned` (state.rs's
    // `JobRelay::supplier`) is settled. A `stream: false` request whose task never got a
    // slot never reaches this line at all — it answers the `no_ready_worker` refusal above,
    // which carries no supplier for the same reason the streamed refusals do not.
    let supplier = relay.supplier();
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-nexus-chunks", chunks.len().to_string())
        // An assembled completion is a `Value`, so serialization cannot fail here;
        // the `"{}"` fallback is the house style for that case (`io.rs:968`), not a
        // silent downgrade of a real error path.
        .body(axum::body::Body::from(
            serde_json::to_string(&assembled).unwrap_or_else(|_| "{}".to_string()),
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    set_supplier(&mut response, supplier.as_ref());
    response
}

/// `POST /v1/jobs` — the generic async job API (task §7.1, §7.3).
pub async fn create_job(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Query(query): Query<ConsumerQuery>,
    body: Bytes,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(request) = serde_json::from_slice::<JobRequest>(&body) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "expected {kind, payload}",
            false,
        );
    };
    // NEX-6.6's letter: on this route the field is required, and only `true` is a
    // valid answer. The two refusals are separate facts that share one code, because
    // they are the same lesson: this route answers `202` and hangs up, so it can
    // neither *declare* detachment on a silent client's behalf (absent) nor honour the
    // coupled contract (`false`) — a coupled task posted here would be orphaned and
    // deleted before the client could poll it. Surfaces that do hold the connection
    // open are named in the message, because "then how do I stream the answer?" is the
    // next question a client asks.
    if !matches!(request.detach, Some(true)) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "detach_required",
            "this route keeps no subscription; post with \"detach\": true, or use a surface that holds the connection open (the streaming completion routes, or GET /v1/consumer)",
            false,
        );
    }
    // Admission — kind, pool, requirements, priority, idempotency — is shared with
    // the consumer WebSocket so both faces refuse the same task the same way
    // (§5.4). `?pool=` is HTTP-only, hence the override argument.
    // No explicit `Idempotency-Key` on the request: the submission is unique, so
    // a later identical POST runs its own execution rather than reading this
    // one's answer back (a completion is never a cached response).
    let fallback_key = nexus_core::store::unique_idempotency_key(tenant.as_ref());
    let (mut submission, pool_id) = match build_submission(
        &hub,
        &tenant,
        &request,
        query.pool.as_deref(),
        header_value(&headers, IDEMPOTENCY_HEADER),
        fallback_key,
        // The same optional pin the OpenAI surface reads, so the detached job API can be
        // queued on a supplier too (spec §9.1 item 1: one admission path whatever the
        // transport, so the same task is refused the same way on every face).
        supplier_hint(&headers),
    ) {
        Ok(built) => built,
        Err((status, code, message)) => return json_error(status, code, &message, false),
    };
    submission.session_hint = header_value(&headers, SESSION_HEADER).map(str::to_string);
    // §5.2 at the runtime level: a key this hub has already routed is not new work, and
    // the honest answer to a retry is the record that already exists — its id, its
    // state, its links. Answering it here rather than letting the actor do it matters
    // because the actor's duplicate branch replays on the **original** id
    // (`actor.rs:1045-1064`, `Effect::Accepted { job: existing, attached: true, .. }`)
    // while this face has to register its relay and its verdict gate under the id it
    // mints (`submit`): a verdict that names a different job never resolves that gate,
    // and the retry used to time out as `503 hub_unavailable`.
    //
    // NEX-6.3's delete-on-orphan is not weakened: the mirror is pruned by
    // `Effect::JobForgotten` (`state.rs`'s arm for it), expires on the actor's own
    // `limits.idempotency_ttl_ms`, and the record is re-read through the actor below
    // before it is trusted. A task that was orphaned and deleted has neither a mirror
    // entry nor a snapshot, so its retry runs fresh.
    if let Some((_pool, existing)) = hub.idempotency_target(&submission.idempotency_key).await {
        match hub.job_snapshot(&existing).await {
            // Same tenant by construction — mirrored keys are namespaced
            // `{tenant}:{key}` in `build_submission` — but the view is the proof, and a
            // mismatch means the entry is not this caller's to reuse.
            Some(view) if view.tenant == tenant => {
                // Same shape as the fresh `202` below, built from the actor's view of
                // the job that is answering: the client's links point at the task that
                // exists, not at the id this request would have minted.
                let artifact_path = Some(format!("/v1/artifacts/art-{existing}"));
                let mut response = json_response(
                    StatusCode::ACCEPTED,
                    &adapters::accepted_body(&view, artifact_path),
                );
                // The id comes from the same lease `GET /v1/jobs/{id}` reports, read off
                // the view this arm already fetched; a task that is queued or terminal has
                // no lease and gets no header. Only the supplier is written here — this
                // arm's `x-nexus-job` behaviour is untouched, and the id it would name is
                // already in the body as `job_id`.
                set_supplier(
                    &mut response,
                    view.lease.as_ref().map(|lease| &lease.worker),
                );
                return response;
            }
            Some(_) | None => {
                // The record is gone (NEX-6.3 deleted it, or its retention lapsed) or it
                // belongs to somebody else: drop the straggler and submit as new work.
                hub.forget_idempotency(&submission.idempotency_key).await;
            }
        }
    }
    let (job, consumer, handle, _relay, gate, _live, _replay) =
        submit(&hub, &pool_id, submission).await;
    // The supplier the `202` may name, from the verdict alone. This route does **not**
    // hold its answer the way the streamed faces do: a detached post is a receipt, and
    // parking a receipt on a busy slot until the claim deadline lapses would turn an
    // acknowledgement into a queue wait. A client that wants the id of a task the verdict
    // could not name yet reads it from the actor, whose lease is the authority. Every arm
    // but the accepting one returns, so this is declared rather than initialised: the only
    // value it can ever hold is a verdict's.
    let worker: Option<WorkerId>;
    match tokio::time::timeout(
        Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500),
        gate,
    )
    .await
    {
        Ok(Ok(Gate::Accepted { worker: leased })) => {
            worker = leased;
            // The phantom consumer, removed. `ActorMsg::Submit` always registers
            // the submitting consumer, but on this route nobody pumps that
            // subscription — the request ends as soon as the `202` is written. Left
            // in `record.consumers` it made the task look watched forever, which is
            // what kept NEX-6.3's deletion and NEX-6.6's subscriber-less window
            // from ever applying to an async job (`actor.rs:1296` for the detach,
            // and the `consumers.is_empty()` test of the NEX-6.6 sweep at
            // `actor.rs:3232-3246`). Detaching here is what makes `detach: true`
            // mean what it says: watched by nobody, bounded by
            // `limits.detached_orphan_ttl_ms`, and a later `/events` attach
            // restarts the window. The OpenAI streaming routes do **not** do this:
            // there the pump is the subscription and its own `Detach` is the event.
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::Detach {
                    job: job.clone(),
                    consumer,
                },
            )
            .await;
        }
        Ok(Ok(Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        })) => {
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, retry_after(secs));
            }
            return response;
        }
        _ => {
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "hub_unavailable",
                "the hub did not answer in time; retry",
                true,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    }
    // The task is accepted and detached; the client polls `/events` or the
    // consumer WebSocket to watch it, and each of those subscriptions has its own
    // lifetime (`state.rs`'s relay fan-out is per job, not per request).
    let view = hub.job_snapshot(&job).await;
    let artifact_path = view.as_ref().map(|_| format!("/v1/artifacts/art-{}", job));
    let body = adapters::accepted_body(
        &view.unwrap_or_else(|| {
            placeholder_view(job.clone(), pool_id.clone(), request.kind.clone())
        }),
        artifact_path,
    );
    // `handle` stays alive through the hub; the client polls `/events`.
    let mut response = json_response(StatusCode::ACCEPTED, &body);
    set_supplier(&mut response, worker.as_ref());
    response
}

/// `GET /v1/jobs` — a coarse listing, bounded by the hub's own retention.
pub async fn list_jobs(State(hub): State<Arc<Hub>>, headers: HeaderMap) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let mut jobs = Vec::new();
    for handle in hub.pools.values() {
        for job in handle.followed_jobs().await {
            if let Some(view) = hub.job_snapshot(&job).await {
                // A listing is still a read of somebody's prompt metadata, so it
                // is filtered to the calling tenant (§9).
                if view.tenant == tenant {
                    jobs.push(adapters::job_status_body(&view));
                }
            }
        }
    }
    json_response(StatusCode::OK, &json!({ "object": "list", "data": jobs }))
}

/// `GET /v1/jobs/{id}` — authoritative state, read through the actor.
pub async fn job_status(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {
            json_response(StatusCode::OK, &adapters::job_status_body(&view))
        }
        // A foreign job reads as `404`, not `403`: job ids are otherwise
        // enumerable across tenants by probing statuses.
        Some(_) => json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job for this tenant",
            false,
        ),
        None => json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job, or its retention window passed",
            false,
        ),
    }
}

/// `DELETE /v1/jobs/{id}` — cancel (task §6).
pub async fn cancel_job(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(handle) = hub.route_job(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    // Any valid API key used to be able to cancel anyone's job: the tenant was
    // authenticated and then discarded. The owning tenant is authoritative
    // because the view comes back through the actor that owns the job.
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {}
        Some(_) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "unknown_job",
                "no such job for this tenant",
                false,
            )
        }
        None => return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false),
    }
    if state::send(
        handle.as_ref(),
        ActorMsg::Cancel {
            job: job.clone(),
            reason: CancelReason::ConsumerGone,
        },
    )
    .await
    .is_err()
    {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub_unavailable",
            "the pool actor is draining",
            true,
        );
    }
    json_response(
        StatusCode::OK,
        &json!({ "job_id": job.to_string(), "cancelling": true }),
    )
}

/// `GET /v1/jobs/{id}/events` — SSE progress for any kind (task §7.1).
pub async fn job_events(
    State(hub): State<Arc<Hub>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ConsumerQuery>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(handle) = hub.route_job(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {}
        Some(_) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "unknown_job",
                "no such job for this tenant",
                false,
            )
        }
        None => return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false),
    }
    let from_seq = parse_resume_seq(
        headers
            .get(&LAST_EVENT_ID)
            .and_then(|v| v.to_str().ok())
            .or(query.from_seq.as_deref())
            .or(query.last_event_id.as_deref()),
    );
    let relay = handle
        .relay(&job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    // The replay lands in this consumer's private queue, then the live fan-out
    // takes over: a joiner's buffer must not be broadcast to everyone.
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = hub.ids.consumer_id();
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
        // C2: this face is gone, and so is `replay_rx` — but the sender half still sits in the
        // relay, and a row with a `replay_target` is not idle, so the orphan sweep could never
        // take it. Un-install the target before answering.
        relay.abandon_replay_target().await;
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub_unavailable",
            "the pool actor is draining",
            true,
        );
    }
    let (out_tx, out_rx) =
        mpsc::channel::<Result<Event, Infallible>>(hub.cfg.limits.consumer_backpressure_frames);
    // Registered here, immediately before the pump that owns it: the slot leaves the
    // fan-out when that pump is dropped, so a watcher that hangs up cannot leave its
    // queue behind on this job's relay (`Follow`'s `live` field carries the slot).
    let live = state::Subscriber::attach(&relay, hub.cfg.limits.consumer_backpressure_frames).await;
    tokio::spawn(pump_relay(
        Follow {
            handle,
            consumer,
            job: job.clone(),
            live,
            replay: replay_rx,
        },
        out_tx,
    ));
    let mut response = Sse::new(ReceiverStream::new(out_rx))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("ping"),
        )
        .into_response();
    add_stream_headers(&mut response);
    // No supplier on this head: this is an attach, not a submission, so no verdict named
    // one for it. A client that needs the id asks the actor, whose lease is the single
    // authority (`GET /v1/jobs/{id}`'s `lease.worker`).
    response_headers(&job, None, &mut response);
    response
}

/// `GET /v1/jobs/{id}/result` — the produced artifact bytes, resumable (test 15).
pub async fn job_result(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(range): Query<crate::io::RangeQuery>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(view) = hub.job_snapshot(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    if view.tenant != tenant {
        return json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job for this tenant",
            false,
        );
    }
    let Some(artifact) = view.artifact.clone() else {
        return json_error(
            if view.terminal.map(|t| t.retryable()).unwrap_or(true) {
                StatusCode::CONFLICT
            } else {
                StatusCode::NOT_FOUND
            },
            "artifact_unavailable",
            "this job has not produced a complete artifact",
            true,
        );
    };
    let from = range.from.unwrap_or(0);
    let to = range.to.unwrap_or(artifact.size_bytes);
    crate::io::serve_artifact(
        &artifact.path,
        artifact.size_bytes,
        &artifact.content_type,
        artifact.stored_sha256.as_deref(),
        from,
        to,
    )
    .await
}

/// `GET /v1/models` — live, from ready workers only (task §7.1).
pub async fn models(State(hub): State<Arc<Hub>>) -> Response {
    let readiness = hub.readiness().await;
    let models: std::collections::BTreeSet<String> = readiness.models.iter().cloned().collect();
    json_response(
        StatusCode::OK,
        &adapters::models_document(&models, hub.wall_ms()),
    )
}

/// `GET /healthz` — degraded capacity is reported, never hidden (task §8, §11).
pub async fn healthz(State(hub): State<Arc<Hub>>) -> Response {
    let readiness = hub.readiness().await;
    let body = json!({
        "status": if readiness.ready_workers == 0 { "degraded" } else { "ok" },
        "ready_workers": readiness.ready_workers,
        "loading_workers": readiness.loading_workers,
        "queue_depth": readiness.queue_depth,
        "mailbox_pending": readiness.mailbox_pending,
        "warm_models": readiness.models,
        "capacity_signal": if readiness.ready_workers == 0 && readiness.loading_workers > 0 { "cold_start" } else if readiness.ready_workers == 0 { "no_workers" } else { "warm" },
        "protocol": nexus_core::PROTOCOL_VERSION,
    });
    json_response(
        if readiness.ready_workers == 0 {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        },
        &body,
    )
}

/// `GET /metrics` — Prometheus text (task §13's mandatory set).
pub async fn metrics(State(hub): State<Arc<Hub>>) -> Response {
    let text = hub.render_metrics().await;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(axum::body::Body::from(text))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ---------------------------------------------------------------------- plumbing

/// Resolve the caller's tenant from the API key, failing closed (task §9: an
/// API key per consumer, never one secret shared across both directions).
/// Returns the response to send when the key is missing or unknown.
//
// `result_large_err` measures the cost of moving a big `Err` out of a return
// value. `Response` is an opaque ~128-byte axum type, but every caller here
// returns the error verbatim (`Err(response) => return response`) instead of
// matching on it, so the only alternative — `Box<Response>` — adds an allocation
// on the unauthenticated path and an `into_response()` hop in nine places for a
// move the compiler already elides. Scoped to this signature for that reason.
#[allow(clippy::result_large_err)]
pub(crate) fn tenant_of(hub: &Arc<Hub>, headers: &HeaderMap) -> Result<TenantId, Response> {
    let key = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )
    .or_else(|| header_value(headers, "x-api-key"));
    hub.api_keys.verify(key).map_err(|err| {
        json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            &err.to_string(),
            false,
        )
    })
}

/// Pool that serves a kind.
fn pool_for_kind(hub: &Arc<Hub>, kind: &str) -> Option<PoolId> {
    hub.cfg.kind(kind).and_then(|k| PoolId::parse(&k.pool).ok())
}

/// A header value as `&str`.
fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|v| !v.trim().is_empty())
}

/// The optional `x-supplier-id` pin, read like every other consumer header here.
///
/// Deliberately **not** [`WorkerId::parse`]. The pin is a hint the hub answers with
/// whichever supplier it actually connected, so a value that is not in the safe alphabet
/// must cost the request nothing (`actor.rs`'s `supplier_pin_allows` reads an id this hub
/// never issued as "no pin", and an id it did issue but can no longer reach as a lapsed
/// pin). Validating at the face would turn "unknown supplier" into a `400` the client
/// never asked for, and would put the two consumer faces out of step: the WebSocket
/// carries the same field unvalidated (`protocol.rs:1147-1153`, where `supplier` is
/// exempted from the id check that `job` gets).
///
/// Trimmed, and an all-whitespace value counts as absent — the same reading
/// [`header_value`] already gives an empty header.
fn supplier_hint(headers: &HeaderMap) -> Option<WorkerId> {
    header_value(headers, SUPPLIER_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(WorkerId::new_unchecked)
}

/// The supplier's own claim window for `kind`, which is the only bound the header hold
/// below may use.
///
/// Mirrors the actor's resolution of the same value (`actor.rs:1250` and `4035`:
/// per-kind override, else the global limit) so a hub configured with a 30 s claim
/// deadline holds a response head for 30 s and no longer. Not a new knob — it is the
/// deadline the actor already arms for this job, read from the same config field.
fn claim_window_ms(hub: &Arc<Hub>, kind: &str) -> u64 {
    hub.cfg
        .kind(kind)
        .and_then(|kind| kind.claim_deadline_ms)
        .unwrap_or(hub.cfg.limits.claim_deadline_ms)
}

/// Tripwire for the supplier hold described by [`supplier_before_body`]: it releases the
/// lease if the endpoint future is cancelled **while that hold is still pending**.
///
/// WHY A `select!` AGAINST THE CHANNEL IS NOT ENOUGH HERE. The obvious
/// `select! { worker = hold => …, _ = tx.closed() => … }` cannot see the case it is meant
/// to cover. `tx` is the worker-event sender of the *pump*, and the pump exists only once
/// [`stream_completion`] has built the SSE body — which happens after the hold returns,
/// because the body's channel and its `ReceiverStream` are constructed inside that
/// function. Nothing anywhere polls that receiver during the hold, and
/// `mpsc::Sender::closed` fires when the **receiver** is dropped, not when a peer's socket
/// dies. So the signal the pump uses for a hangup (NEX-6.1) is structurally unavailable
/// before the pump exists, however often it is polled — which is what the paragraph
/// "WHAT HAPPENS IF THE CONSUMER DISAPPEARS MID-HOLD" below [`supplier_before_body`] now
/// says instead of claiming the wait could notice.
///
/// What *is* available is the hold's own cancellation. hyper drops the handler future as
/// soon as the connection is gone — that is how it signals completion — and measured here
/// (client aborts a stream whose head was never written, 5 000 ms claim window) the drop
/// lands in the same millisecond as the abort, while the hold is still pending. Dropping a
/// future drops the locals live at its await point, so an armed guard dropping *is* the
/// hangup, and the lease the hold is waiting on is released then rather than whenever the
/// claim window happens to close.
struct SupplierHoldDetach {
    /// Only a still-pending hold fires. Once [`Self::disarm`] has run the head has gone out
    /// (or the answer has) and the pump owns the consumer's absence; a detach from here
    /// would be a second one for the same hangup.
    armed: bool,
    handle: Arc<state::PoolHandle>,
    job: JobId,
    consumer: ConsumerId,
}

impl SupplierHoldDetach {
    /// Called the instant the hold resolves, which makes [`Self::drop`] a no-op.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SupplierHoldDetach {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // This is the actor's own "consumer vanished" rule, applied at the one window where
        // the pump could not observe the disappearance: the streamed chat face sets
        // `cancel_on_consumer_gone: true` (`request.to_submission` builds
        // `RouteSpec { streamed: true, face: Chat }`), and `on_detach` then cancels a queued
        // task and forgets the row, or hands a lease back to the queue if the worker has
        // already been told. Not a new policy — the same policy at an earlier moment.
        //
        // `Drop` cannot await, and `handle.tx` is the actor's *bounded* mailbox, so
        // `try_send` is the only legal send. Its error hands the message back rather than
        // dropping it, and that overflow case is drained by a one-shot task instead of
        // blocking the drop — so the actor still hears, just one permit later (as soon as
        // the mailbox drains far enough for `state::send` to land) rather than within the
        // millisecond the uncontended path takes.
        //
        // WHY A GUARDED `Handle` AND NOT BARE `tokio::spawn`. A `Drop` is not guaranteed
        // to run with a runtime current: the future being unwound can be dropped by a
        // caller that is itself outside any runtime (a unit test, a `block_on` teardown,
        // middleware unwinding after the reactor is gone), and bare `tokio::spawn`
        // **panics** when there is no runtime context. A panic thrown from `Drop` lands
        // on an unwind path that cannot reliably catch it, and this workspace builds
        // release with `panic = "abort"` (`Cargo.toml:101`), where it is a whole-process
        // abort paid for by whoever else was on the box. Losing the detach is the cheaper
        // failure: with no runtime there is no task that could have delivered it anyway,
        // and the held lease falls back to the already-documented worst case — the claim
        // window closes and the actor's own deadline releases the row (`on_tick`), which
        // is bounded. Behaviour with a runtime current is byte-for-byte what it was.
        let message = ActorMsg::Detach {
            job: self.job.clone(),
            consumer: self.consumer.clone(),
        };
        if let Err(error) = self.handle.tx.try_send(message) {
            let handle = Arc::clone(&self.handle);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = state::send(&handle, error.into_inner()).await;
                });
            }
        }
    }
}

/// The header hold: wait for the supplier id a verdict could not name, bounded by the
/// actor's own dispatch deadline for this kind.
///
/// WHY A WAIT IS ALLOWED AT ALL. NEX-3.3 promises "no bytes until the actor says the request
/// is serviceable", and this hold spends none of that credit: the head is not the body,
/// and the deadline below is a strictly earlier moment than any the face would answer at
/// anyway. What changed is only **when the head may flush** — for a submission the hub
/// admitted into the queue with a warm pool whose slots were all busy
/// (`actor.rs:1303`, `Effect::Accepted{worker: None}`), the head can now come out at
/// first lease instead of at admission. A `Queued` task that never gets a slot answers at
/// the claim deadline, exactly as it did before, with the in-band `queue_timeout`
/// (`actor.rs:3491`) rather than a `503`, because the grace window's `503` belongs to the
/// `Pending` case alone and that case already resolves this gate with an id or a refusal.
///
/// WHY THE BOUND IS THE ACTOR'S DEADLINE AND NOT A NEW ONE. The lease either happens or
/// the job's `dispatch_deadline_ms` lapses, and the actor owns that lapse (`actor.rs:1252`
/// arms it from `claim_deadline_ms` when a worker was warm at submit, which is precisely
/// the state that produced `worker: None` here). So the wait is the same interval the task
/// was already going to sit in the queue for: no new timer, no new configuration, and no
/// path where a hub that is never going to lease anything holds a socket open forever.
///
/// WHY THE RELAY IS THE ONLY THING ASKED. `Effect::SendWorker{Assigned}` writes the id
/// into the relay (`state.rs`'s dispatcher) and wakes this watch; nothing here queries the
/// actor, and the value the header carries is the same one `GET /v1/jobs/{id}` will report
/// as `lease.worker` — because it is the same effect, mirrored.
///
/// WHAT HAPPENS IF THE CONSUMER DISAPPEARS MID-HOLD. The hold cannot see it: the wait is on
/// a watch, not on the client, and nothing polls a body stream yet — the pump that reads
/// `tx.closed()` (NEX-6.1) is only created by [`stream_completion`], *after* this returns,
/// so on a hang-up inside the window that pump starts with the channel already closed. The
/// release therefore comes from [`SupplierHoldDetach`], which the caller arms around this
/// await: hyper drops the handler future the instant the connection dies (measured: same
/// millisecond as the abort), and the guard turns that drop into the very `ActorMsg::Detach`
/// the pump would have posted — within the millisecond, rather than whenever the bound
/// closes. Without it the abandoned task keeps its queue row, and in the worst case keeps a
/// lease, for the whole window, and a supplier the client pinned for conversation affinity
/// is unreachable for that long. No task outlives the response.
async fn supplier_before_body(
    hub: &Arc<Hub>,
    relay: &Arc<JobRelay>,
    kind: &str,
    worker: Option<WorkerId>,
) -> Option<WorkerId> {
    // The common case first: the submit turn itself leased the job, so the verdict named
    // the supplier and there is nothing to wait for.
    if worker.is_some() {
        return worker;
    }
    let mut watcher = relay.supplier_watcher();
    let budget = Duration::from_millis(claim_window_ms(hub, kind));
    match tokio::time::timeout(budget, wait_for_supplier(&mut watcher)).await {
        // Lease arrived inside the window: the header goes out with it.
        Ok(Some(worker)) => Some(worker),
        // The window lapsed, or the relay's own sender went away with the row pruned.
        // Either way the honest answer is "not yet", and an answer with no supplier id is
        // what the surface looked like before this header existed.
        Ok(None) | Err(_) => relay.supplier(),
    }
}

/// One wait on [`JobRelay::supplier_watcher`]: `Some` the first time an id appears,
/// `None` if the relay stops reporting at all.
///
/// The first `borrow` is checked before the first `changed()` on purpose: a lease taken
/// between the face's verdict and this loop must not be slept past, and `changed()` only
/// ever waits for what is strictly newer than what the receiver last saw.
async fn wait_for_supplier(
    watcher: &mut tokio::sync::watch::Receiver<Option<WorkerId>>,
) -> Option<WorkerId> {
    loop {
        if let Some(worker) = watcher.borrow().clone() {
            return Some(worker);
        }
        if watcher.changed().await.is_err() {
            return None;
        }
    }
}

// ------------------------------------------------------------------ shared admission

/// Resolve a generic job posting into the `Submission` the actor sees, and report
/// the same refusals the route would have given: `unknown_kind`, `wrong_pool`,
/// `invalid_pool`.
///
/// Shared by `POST /v1/jobs` and the consumer WebSocket (`consumer_ws.rs`) because
/// §5.4 requires the two faces to refuse the same way: one task, one admission
/// rule, whatever the transport. The OpenAI-shaped adapters do **not** call this —
/// their kind comes from the route and their payload is translated, so their
/// resolution genuinely differs (`adapters::openai_submission`).
///
/// `fallback_key` is what deduplication uses when the caller supplied no explicit
/// key: the HTTP route passes a content hash ("same prompt, same answer"), the
/// WebSocket passes a fresh per-submit id, because a socket that posts the same
/// payload twice means two tasks unless it asked otherwise. An explicit key is
/// namespaced by tenant either way: guessing another tenant's `Idempotency-Key`
/// must not attach to their job and stream their prompt's answer back (§9).
///
/// `supplier_hint` is the same optional pin in both vocabularies: the HTTP
/// `x-supplier-id` header and the WS `submit.supplier` field (`consumer_ws.rs`). It is a
/// parameter rather than something the caller assigns afterwards — unlike
/// `session_hint`, which genuinely has one face and not the other — because both faces
/// have the value in hand at the call site, and a submission built without it would
/// silently route somewhere the consumer did not ask for.
pub(crate) fn build_submission(
    hub: &Arc<Hub>,
    tenant: &TenantId,
    request: &JobRequest,
    pool_override: Option<&str>,
    idempotency_key: Option<&str>,
    fallback_key: String,
    supplier_hint: Option<WorkerId>,
) -> Result<(Submission, PoolId), (StatusCode, &'static str, String)> {
    let Some(kind_cfg) = hub.cfg.kind(&request.kind).cloned() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "unknown_kind",
            format!("kind {:?} is not registered", request.kind),
        ));
    };
    let pool_name = request
        .pool
        .as_deref()
        .or(pool_override)
        .unwrap_or(&kind_cfg.pool);
    if pool_name != kind_cfg.pool {
        return Err((
            StatusCode::BAD_REQUEST,
            "wrong_pool",
            "the pool does not serve this kind".to_string(),
        ));
    }
    let Ok(pool_id) = PoolId::parse(pool_name) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid_pool",
            "the pool name is not in the safe alphabet".to_string(),
        ));
    };
    let mut requires = request
        .requires
        .clone()
        .unwrap_or_else(|| kind_cfg.requires.clone());
    if requires.kv_estimate_tokens.is_none() {
        requires.kv_estimate_tokens = Some(adapters::estimate_tokens(&request.payload));
    }
    let idempotency = match idempotency_key {
        Some(key) => format!("{}:{key}", tenant.as_ref()),
        None => fallback_key,
    };
    Ok((
        Submission {
            kind: request.kind.clone(),
            pool: pool_id.clone(),
            tenant: tenant.clone(),
            requires,
            payload: request.payload.clone(),
            content_type: "application/json".to_string(),
            idempotency_key: idempotency,
            // Session affinity is a header concept; the WebSocket carries no
            // session hint, so the actor's affinity simply does not apply there.
            session_hint: None,
            // The supplier pin, verbatim from the face that read it. Resolved (or
            // dropped) by the actor, never here: `supplier_hint` is a hint to the
            // scheduler, and an id this hub has never issued means "no pin"
            // (`actor.rs`'s `supplier_pin_allows`), so an unknown value is a normal
            // answer rather than a refusal.
            supplier_hint,
            priority: request
                .priority
                .as_deref()
                .map(Priority::from_name)
                .unwrap_or_else(|| hub.cfg.tenant_priority(tenant)),
            idempotent: kind_cfg.idempotent,
            streaming: kind_cfg.streaming,
            // NEX-6.6, read off the request rather than off the route: a declared
            // detached task survives every consumer leaving, and a plain
            // submission does not. `create_job` has already refused the
            // combination it cannot honour, so on the HTTP route this is only
            // ever reached with the detached reading.
            cancel_on_consumer_gone: matches!(request.detach, Some(false)),
        },
        pool_id,
    ))
}

/// One consumer's view of one job: everything a pump needs in order to follow it
/// and to say so when the consumer goes away.
///
/// All three pumps — the OpenAI SSE body, the `/events` SSE body, and the
/// consumer WebSocket's per-task stream — take this instead of a fistful of loose
/// arguments, which is what keeps the three surfaces from drifting on what "the
/// consumer left" means: same fields, same `Detach` at the end.
pub(crate) struct Follow {
    /// The pool actor that owns the job.
    pub(crate) handle: Arc<state::PoolHandle>,
    /// The consumer identity registered for this socket.
    pub(crate) consumer: ConsumerId,
    /// Job being followed.
    pub(crate) job: JobId,
    /// The job's live fan-out, registered for as long as this pump lives.
    ///
    /// A [`state::Subscriber`] rather than a bare `Receiver`: the slot inside it is
    /// what keeps this pump in the relay's fan-out, and it is retired the moment this
    /// `Follow` is dropped — however the pump ended. That is why none of the three
    /// pumps below has an unsubscribe step to forget.
    pub(crate) live: state::Subscriber,
    /// This consumer's private replay queue, closed by the dispatcher once the
    /// buffered prefix has been written.
    pub(crate) replay: mpsc::Receiver<RelayItem>,
}

/// Send a submission and register everything its answer needs **before** the
/// actor is told about the job.
///
/// Two orderings were previously wrong and are the reason this returns more than
/// a gate:
///   * the broadcast subscription must exist before `Submit` is sent, because
///     `tokio::sync::broadcast` delivers only what is published afterwards — a
///     worker frame relayed in the same tick that answered `Accepted` used to
///     land in that gap and be lost (§16.14's "same content" claim);
///   * this consumer's private replay queue must be installed before `Submit`,
///     because an idempotent attach delivers its buffered frames through it
///     (`Accepted { attached: true, replayed }`). With no receiver installed the
///     dispatcher dropped them, so the second client saw only whatever came
///     after it joined (§16.7).
///
/// The actor's `Accepted` / `ReplayTo` handler drops the replay sender once it
/// has written the buffered prefix, which is what closes this channel and lets
/// the pumps fall through to live events.
#[allow(clippy::type_complexity)]
pub(crate) async fn submit(
    hub: &Arc<Hub>,
    pool: &PoolId,
    submission: Submission,
) -> (
    JobId,
    ConsumerId,
    Arc<state::PoolHandle>,
    Arc<JobRelay>,
    oneshot::Receiver<Gate>,
    state::Subscriber,
    mpsc::Receiver<RelayItem>,
) {
    let job = hub.ids.job_id();
    let consumer = hub.ids.consumer_id();
    // Infallible by invariant: `pool` came out of `build_submission`, which only
    // ever returns a pool the validated config registers a kind for, and
    // `Hub::new` builds one handle per configured pool. A missing handle here is a
    // broken config/registry pairing, not an input a caller can recover from, so
    // the fail-stop stays and only the lint is scoped (same shape as `io.rs:285`).
    #[allow(clippy::expect_used)]
    let handle = hub
        .pool(pool)
        .expect("pool handle exists for a configured pool");
    let relay = handle
        .relay(&job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    let (gate_tx, gate_rx) = oneshot::channel();
    *relay.gate.lock().await = Some(gate_tx);
    let live = state::Subscriber::attach(&relay, hub.cfg.limits.consumer_backpressure_frames).await;
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    // The §5 read-mostly index is written here, once, so a later `GET
    // /v1/jobs/{id}` can be routed without a second source of job state.
    hub.bind_job(&job, pool).await;
    // The hub-side mirror of the actor's §5.2 key table (`state.rs`'s `idempotency`),
    // written with the id this face registered everything under. That mirror is what
    // lets a later retry find this job instead of minting a second id the actor's
    // answer would never name; `idempotency_target` reads it, and
    // `Effect::JobForgotten` prunes it with the record.
    hub.remember_idempotency(&submission.idempotency_key, pool, &job)
        .await;
    if state::send(
        handle.as_ref(),
        ActorMsg::Submit {
            job: job.clone(),
            submission,
            consumer: consumer.clone(),
        },
    )
    .await
    .is_err()
    {
        // The actor is gone: resolve the gate ourselves so the caller answers 503
        // instead of waiting for the timeout.
        relay.resolve(Gate::Closed).await;
    }
    (
        job,
        consumer,
        Arc::clone(handle),
        relay,
        gate_rx,
        live,
        replay_rx,
    )
}

/// Undo everything `submit` installed for a submission the actor refused or never
/// answered: the routing entry, the relay, and the idempotency row (NEX-7.2 — no per-task
/// hub state that outlives the task).
///
/// All three are written *before* the actor is told, because the verdict's replay needs
/// somewhere to land (see `submit`'s ordering comment). A submission that never became a
/// job therefore owns entries the actor will never prune itself: it has no record, so
/// neither `Effect::Terminal` nor `Effect::JobForgotten` can ever name its id. Without
/// this, every refused `POST /v1/jobs` and every refused completion leaves a relay in the
/// pool's map plus a row in each of the two hub-side indexes for the life of the process —
/// the consumer WebSocket already prunes its own refusals (`consumer_ws.rs:487-496`), and
/// this is that same step on the HTTP faces.
///
/// The gate-timeout arm is the one judgement call: an actor that answered nothing may
/// still hold the record, and dropping the *route* makes it unpollable for the rest of its
/// retention. That is the trade the WebSocket already makes, and the honest direction — the
/// client has been told the hub did not answer, and a route to a task the hub has disowned
/// is worse than the `404` that admits it.
async fn abandon_submission(hub: &Arc<Hub>, handle: &Arc<state::PoolHandle>, job: &JobId) {
    hub.forget_job(job).await;
    handle.forget_relay(job).await;
    // The third map `submit` writes. Pruning by job (rather than by the submission's key)
    // is deliberate: the reverse index only ever points at the row for this job, so a row
    // a newer submission has meanwhile taken over is left alone.
    hub.forget_idempotency_for_job(job).await;
}

/// Follow the execution an idempotency key already owns instead of submitting a second
/// one: the same three registrations `GET /v1/jobs/{id}/events` makes (relay, this
/// consumer's private replay queue, `ActorMsg::Attach` from seq 0) and nothing else.
///
/// `submit`'s shape is deliberately **not** reproduced: there is no gate, because an attach
/// emits only `Effect::ReplayTo` (`actor.rs:1228-1260`) and never `Accepted`, so a face
/// that waited for a verdict here would wait out the grace window for a job that is already
/// running. The record being past `Pending` is what makes its stream open by definition —
/// the reading `/events` has always taken, and the reason a queued original answers with
/// keepalives rather than a `503` here as well.
///
/// `None` means the mirrored record is no longer there: `route_job` lost it when the actor
/// emitted `Effect::JobForgotten`, or the snapshot probe found nothing between the mirror
/// lookup and now. The caller then submits normally, which is exactly NEX-6.3's "a deleted
/// task re-runs" — and it closes the one gap a blind attach would leave, because attaching
/// to a record the actor has dropped closes the replay queue with nothing in it and leaves
/// the pump parked on a fan-out that can no longer produce events.
///
/// Two joins racing on one job inherit `/events`' pre-existing behaviour: the second
/// overwrite of `replay_target` can strand the first joiner's buffered prefix. Only the
/// ring's contents are at stake, and every consumer still gets the live tail.
///
/// The relay is returned as part of the bundle because the `x-supplier-id` answer has to
/// travel with it: an attach emits no `Effect::Accepted`, so the dispatcher never writes
/// this job's id into a row the joiner created after the lease was already held. The
/// snapshot this function reads for the tenancy/retention check carries the same
/// authoritative `lease.worker` the header must agree with, so it seeds the cell here —
/// one read, no extra round trip, and both faces then report one number.
#[allow(clippy::type_complexity)]
pub(crate) async fn join_existing_job(
    hub: &Arc<Hub>,
    job: &JobId,
) -> Option<(
    ConsumerId,
    Arc<state::PoolHandle>,
    state::Subscriber,
    mpsc::Receiver<RelayItem>,
    Arc<JobRelay>,
)> {
    // The mirrored pool is only a hint; `job_index` is what owns the routing, and a job
    // the actor has forgotten has no entry left to follow.
    let handle = hub.route_job(job).await?;
    // Guarded against the actor's retention rather than trusting the mirror's TTL: the
    // two expiry clocks (`limits.idempotency_ttl_ms` here and in the actor) agree to the
    // millisecond, but a record deleted by NEX-6.3 and a key that has not yet expired is
    // a real combination.
    let view = hub.job_snapshot(job).await?;
    let relay = handle
        .relay(job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    // The lease is the answer, and a task with none (still queued, or terminal with the
    // lease dropped) has nothing to report — the same `None` rule `Gate::Accepted` carries
    // for a submission, never a failure.
    if let Some(lease) = view.lease.as_ref() {
        relay.note_supplier(&lease.worker);
    }
    let live = state::Subscriber::attach(&relay, hub.cfg.limits.consumer_backpressure_frames).await;
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = hub.ids.consumer_id();
    if state::send(
        handle.as_ref(),
        ActorMsg::Attach {
            job: job.clone(),
            consumer: consumer.clone(),
            from_seq: 0,
        },
    )
    .await
    .is_err()
    {
        // The pool actor is draining: undo what was just registered, so a caller that
        // falls back to a fresh submission is not shadowed by a relay nobody will replay.
        handle.forget_relay(job).await;
        return None;
    }
    Some((consumer, handle, live, replay_rx, relay))
}

/// Translate relay items into SSE blocks for the OpenAI surface.
///
/// Order matters twice over: this consumer's replay prefix (the
/// `Accepted { attached: true }` path, §5.2) goes out before the live events,
/// and seqs already delivered are skipped so a replay that overlaps the fan-out
/// cannot print the same delta twice (§16.4/§16.8). When the pump ends — because
/// the stream finished or because the consumer vanished — it sends `Detach`:
/// for a streaming kind that is what makes the last consumer leaving fan out a
/// `Cancel` to the worker (§6). Without it, a client that closes its socket
/// leaves the GPU decoding for nobody.
///
/// NEX-6.1 is why `tx.closed()` is one of the sources from the first poll: hyper drops a parked
/// response body as soon as the client hangs up (measured, not assumed —
/// `runtime_e2e::an_idle_sse_hangup_cancels_the_worker_without_a_failed_write` pins the
/// behaviour end to end), and dropping the body drops this channel's receiver. Before C1 the
/// loss surfaced only when a push into the body failed, which on an idle stream is never: the
/// task sat on a worker slot until the TCP stack or the hard cap gave up.
async fn pump_sse(
    follow: Follow,
    model: String,
    created: u64,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let Follow {
        handle,
        consumer,
        job,
        live,
        replay,
    } = follow;
    let mut through: u64 = 0;
    // C1: one pass over both queues with `tx.closed()` as a source, replacing the strict
    // replay-then-live sequence. That ordering is what made this pump immortal when nothing
    // closed the joiner's replay queue, and it also ignored the client hanging up until the
    // live phase began — NEX-6.1's notice is now honoured from the first poll. What the client
    // sees, in what order, is unchanged: see `StreamPump`.
    let mut pump = StreamPump::new(live, replay, Some(tx.clone()));
    // The streaming surface has no clock of its own: it ends when the task is terminal or the
    // client hangs up, never because the hub grew impatient.
    while let Some(item) = pump.next_item().await {
        match item {
            RelayItem::Comment(_text) => {
                // axum's `KeepAlive` already emits the `: ping` comments on this response, so
                // an actor keepalive is not duplicated.
            }
            // The stream ended on its own terms; nothing to report in band.
            RelayItem::End => break,
            RelayItem::Event(event) => {
                let terminal = matches!(
                    event,
                    ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
                );
                // A false return means the body is gone, which is `body_closed()` by another
                // route: the loop ends and no in-band frame is attempted.
                if !send_block(&job, &model, created, event, &mut through, &tx).await {
                    break;
                }
                if terminal {
                    // The OpenAI contract ends with the `[DONE]` sentinel.
                    let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
                    break;
                }
            }
        }
    }
    // `ended()` is the one exit that owes the client an explanation: both queues closed with no
    // terminal event, so the stream stopped short of its ending. A hung-up client (`body_closed`)
    // and a stream that ran to its terminal or its `End` (a `break`) both owe nothing.
    if pump.ended() {
        let payload = adapters::in_band_error(
            "consumer_lagged",
            "the relay dropped this consumer before the stream ended",
            false,
        );
        let _ = tx.send(Ok(json_data_event(payload).event("error"))).await;
    }
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Detach {
            job: job.clone(),
            consumer,
        },
    )
    .await;
}

/// Write one consumer event as an OpenAI chunk. Returns false once the client is
/// gone. `through` tracks the highest seq already written so an overlapping
/// replay is skipped; `Gap` and `Error` carry no content seq and always go out.
async fn send_block(
    job: &JobId,
    model: &str,
    created: u64,
    event: ConsumerEvent,
    through: &mut u64,
    tx: &mpsc::Sender<Result<Event, Infallible>>,
) -> bool {
    // The same watermark the aggregating path applies, through the same helper (§16.4).
    let seq = event_seq(&event);
    if let Some(seq) = seq {
        if seq <= *through {
            return true;
        }
        *through = seq;
    }
    // Every payload below is a `Value` built from types that always serialize, so
    // `json_data_event`'s fallback branch is never taken in practice — the house
    // style for that case is `unwrap_or_else`, not an `expect` (`io.rs:968`).
    let block =
        match &event {
            ConsumerEvent::Delta { text, seq } => Event::default()
                .id(seq.to_string())
                .data(adapters::openai_delta_payload(text, job, model, created)),
            ConsumerEvent::Event { seq, payload } => json_data_event(payload.clone())
                .id(seq.to_string())
                .event("event"),
            ConsumerEvent::Log { seq, text } => Event::default()
                .id(seq.to_string())
                .event("log")
                .data(text.clone()),
            ConsumerEvent::Artifact { seq, manifest } => json_data_event(manifest.clone())
                .id(seq.to_string())
                .event("artifact"),
            ConsumerEvent::Gap { expected, got } => {
                json_data_event(json!({ "expected": expected, "got": got })).event("gap")
            }
            ConsumerEvent::Finish { seq, finish, usage } => json_data_event(
                adapters::finish_chunk(job, model, created, *finish, usage.as_ref()),
            )
            .id(seq.to_string()),
            ConsumerEvent::Error {
                code,
                message,
                retryable,
                ..
            } => json_data_event(adapters::in_band_error(code, message, *retryable)).event("error"),
            other => json_data_event(other.clone()),
        };
    tx.send(Ok(block)).await.is_ok()
}

/// §7.2: intermediaries must not buffer the stream. `text/event-stream` comes
/// from axum's `Sse`; without these two headers a proxy (or nginx's default)
/// holds the deltas back and the "streaming" API is not streaming.
fn add_stream_headers(response: &mut Response) {
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
}

/// Translate relay items for the generic job API: one `event:` name per kind, and
/// `id:` set to the worker seq so `Last-Event-ID` resumes exactly (task §7.5).
///
/// The private replay queue is drained first (the dispatcher closes it once the
/// buffered prefix has been written), then the live fan-out takes over, skipping
/// any seq the replay already delivered. Like the OpenAI pump, it sends `Detach`
/// when the consumer goes away, so a `/events` watcher that disconnects is not
/// counted as an attached consumer forever.
///
/// NEX-6.1 applies to a watcher exactly as it does to a streaming answer: a
/// `/events` client that hangs up while nothing is arriving must stop counting as
/// an attached consumer, so the live loop also waits on `tx.closed()`. The body
/// being dropped *is* the connection event; it needs no failed write first.
async fn pump_relay(follow: Follow, tx: mpsc::Sender<Result<Event, Infallible>>) {
    let Follow {
        handle,
        consumer,
        job,
        live,
        replay,
    } = follow;
    let mut through: u64 = 0;
    // C1: see `StreamPump`. The old version drained the replay queue to closure before it
    // looked at the body again, so a join whose replay queue was never closed became a task that
    // never ended, holding its queue and its relay row; and a client that hung up during the
    // Attach round-trip was only noticed once the live phase started.
    let mut pump = StreamPump::new(live, replay, Some(tx.clone()));
    while let Some(item) = pump.next_item().await {
        if !forward(item, &tx, &mut through).await {
            // `forward` returns false when a write failed — which already means the body is
            // gone — or when the stream reached a terminal event. Either way the `Detach` below
            // is all that is left.
            break;
        }
    }
    // Only `ended()` owes the frame: both queues closed before a terminal event, so the watcher
    // would otherwise see a stream that simply stopped. A client that hung up cannot read it.
    if pump.ended() {
        let _ = tx
            .send(Ok(
                json_data_event(json!({ "dropped": "consumer_lagged" })).event("gap")
            ))
            .await;
    }
    let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
}

/// Forward one relay item; false means the consumer went away or the stream
/// reached a terminal event. `through` de-duplicates the live fan-out against a
/// replay that overlapped it; `Gap` and `Error` carry no content seq, so they are
/// never suppressed.
async fn forward(
    item: RelayItem,
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    through: &mut u64,
) -> bool {
    match item {
        RelayItem::End => false,
        RelayItem::Comment(_text) => {
            // Comments are produced by axum's `KeepAlive`; forwarding the actor's
            // keepalive as a named event would confuse a generic client.
            true
        }
        RelayItem::Event(event) => {
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
                    return true;
                }
                *through = seq;
            }
            let name = event.event_name().to_string();
            let terminal = matches!(
                event,
                ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
            );
            let mut builder = json_data_event(event).event(&name);
            if let Some(seq) = seq {
                builder = builder.id(seq.to_string());
            }
            let sent = tx.send(Ok(builder)).await.is_ok();
            if terminal {
                return false;
            }
            sent
        }
    }
}

/// One SSE event carrying a JSON payload.
///
/// `Event::json_data` is fallible only because it is generic over `Serialize`;
/// every caller here passes something that always serializes (a `Value`, or a
/// `ConsumerEvent` whose `Serialize` impl is derived), so the empty fallback is a
/// defensive branch rather than a real case. Production code may not `expect`
/// (workspace lint policy), and the house style for an infallible serialization is
/// `unwrap_or_else` (`io.rs:968`).
fn json_data_event<T: serde::Serialize>(payload: T) -> Event {
    Event::default()
        .json_data(payload)
        .unwrap_or_else(|_| Event::default())
}

/// A `Retry-After` value. A decimal integer always encodes, so the fallback is
/// defensive; `1` keeps the header meaningful if it is ever reached.
fn retry_after(secs: u64) -> HeaderValue {
    HeaderValue::from_str(&secs.to_string()).unwrap_or_else(|_| HeaderValue::from_static("1"))
}

/// A response header built from text the hub generated itself (a job id). Ids come
/// from the safe alphabet, so the empty fallback cannot be reached in practice.
fn header_of(text: &str) -> HeaderValue {
    HeaderValue::from_str(text).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// A JSON response with the right content type.
///
/// `pub(crate)` so `observe.rs` renders its document through the *same* content-type
/// and serialization rules as every other JSON surface: two builders of "the same"
/// response drift (one adds a charset, one does not), and the dashboard's client
/// sniffs this header to decide whether the body is JSON at all.
pub(crate) fn json_response(status: StatusCode, body: &Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string()),
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// An OpenAI-shaped error body (task §7.1: clients should be able to read it).
fn json_error(status: StatusCode, code: &str, message: &str, retryable: bool) -> Response {
    json_response(status, &adapters::http_error(code, message, retryable))
}

/// Placeholder view when a snapshot probe timed out: enough for a `202` body.
fn placeholder_view(job: JobId, pool: PoolId, kind: String) -> nexus_core::JobView {
    nexus_core::JobView {
        id: job,
        pool,
        kind,
        tenant: TenantId::new_unchecked("unknown"),
        phase: nexus_core::JobPhase::Queued,
        attempt: 1,
        terminal: None,
        priority: Priority::Default,
        payload_bytes: 0,
        payload_sha256: String::new(),
        requires: nexus_core::protocol::Requires::default(),
        timings: nexus_core::model::JobTimings::default(),
        acked_seq: 0,
        gaps: 0,
        duplicates: 0,
        replay_frames: 0,
        consumers: 0,
        delivered_bytes: 0,
        lease: None,
        artifact: None,
        usage: None,
        finish: None,
        worker_error: None,
        streaming: false,
        idempotent: true,
    }
}

#[cfg(test)]
mod tests {
    //! The pumps' queue discipline, pinned at the unit level — every assertion here is an exact
    //! relay item or an exact exit flag, never "something came out". What each test is for:
    //!
    //!   * the two `*_ends_when_the_client_hangs_up` tests are C1's regression itself, run
    //!     against the real pumps, in the exact state the race leaves behind;
    //!   * the three `StreamPump` tests pin the two properties the restructure could easily have
    //!     broken: the printed **order** (backlog before live, whatever the arrival order) and
    //!     the rule that an open backlog is not an end of stream;
    //!   * the clock test pins the aggregate surface's exit, which is the only bounded one;
    //!   * the two `an_overflowed_hold_detach_*` tests pin both halves of
    //!     `SupplierHoldDetach::drop`'s mailbox-overflow fallback: that a runtime current still
    //!     delivers the detach, and that **no** runtime current is survivable rather than fatal.
    //!
    //! The end-to-end races these stand in for live in `runtime_e2e.rs`, where a socket and a
    //! worker are real rather than simulated.
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::response::sse::Event;
    use nexus_core::ids::ConsumerId;
    use nexus_core::ids::JobId;
    use nexus_core::ids::PoolId;
    use nexus_core::ActorMsg;
    use nexus_core::ConsumerEvent;
    use tokio::sync::mpsc;

    use crate::state;
    use crate::state::JobRelay;
    use crate::state::PoolGauges;
    use crate::state::PoolHandle;
    use crate::state::RelayItem;

    use super::pump_relay;
    use super::pump_sse;
    use super::Follow;
    use super::StreamPump;
    use super::SupplierHoldDetach;

    /// One delta, so an expectation reads as the event it is.
    fn delta(seq: u64, text: &str) -> ConsumerEvent {
        ConsumerEvent::Delta {
            text: text.to_string(),
            seq,
        }
    }

    /// A handle with no actor task behind it: these tests drain the mailbox themselves, so a
    /// `Detach` is judged the instant the pump posts it instead of racing a running actor. The
    /// same rig `state::tests` uses.
    fn mailbox() -> (Arc<PoolHandle>, mpsc::Receiver<ActorMsg>) {
        let (tx, rx) = mpsc::channel::<ActorMsg>(8);
        (
            Arc::new(PoolHandle {
                id: PoolId::new_unchecked("llm.chat/qwen38"),
                tx,
                workers: tokio::sync::Mutex::new(HashMap::new()),
                relays: tokio::sync::Mutex::new(HashMap::new()),
                gauges: tokio::sync::Mutex::new(PoolGauges::default()),
                relay_rows: AtomicU64::new(0),
                relay_subscribers: AtomicU64::new(0),
            }),
            rx,
        )
    }

    /// A pump over a fresh relay, with the backlog a queue the test writes by hand. Both sources
    /// are therefore under the test's control, so "which one came out first" is a fact about the
    /// pump and not about scheduling.
    async fn open_pump() -> (StreamPump, mpsc::Sender<RelayItem>, Arc<JobRelay>) {
        let relay = Arc::new(JobRelay::new(16, 0));
        let live = state::Subscriber::attach(&relay, 16).await;
        let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(16);
        (StreamPump::new(live, replay_rx, None), replay_tx, relay)
    }

    /// The stranded join, exactly as C1's interleaving leaves it (see `StreamPump`'s docs): the
    /// fan-out is finished, this join's replay queue is installed in the row and will never be
    /// written or closed, and the row itself is kept alive by the join's own subscriber — the
    /// reason the old replay-first pump could park in it forever.
    ///
    /// Returns the mailbox to read the pump's `Detach` from, and the relay row, so a test can
    /// assert what the pump released rather than what it logged.
    async fn stranded_join() -> (Follow, mpsc::Receiver<ActorMsg>, Arc<JobRelay>) {
        let (handle, actor_rx) = mailbox();
        let relay = Arc::new(JobRelay::new(16, 0));
        // What a terminal task does to its fan-out: `finished` is set and every live queue is
        // closed. Attaching behind that used to hand back a queue that would never see an `End`,
        // which is C7b.
        relay.end_stream().await;
        let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(16);
        *relay.replay_target.lock().await = Some(replay_tx);
        let live = state::Subscriber::attach(&relay, 16).await;
        (
            Follow {
                handle,
                consumer: ConsumerId::new_unchecked("c-stranded"),
                job: JobId::new_unchecked("job-stranded"),
                live,
                replay: replay_rx,
            },
            actor_rx,
            relay,
        )
    }

    /// The detach a pump owes the actor when its consumer vanishes, read off the mailbox.
    /// `ActorMsg` has no `PartialEq`, so the two fields the actor keys its bookkeeping by are
    /// asserted directly.
    async fn detached(mut actor_rx: mpsc::Receiver<ActorMsg>) -> (JobId, ConsumerId) {
        match actor_rx.recv().await {
            Some(ActorMsg::Detach { job, consumer }) => (job, consumer),
            other => panic!("expected a Detach, got {other:?}"),
        }
    }

    /// C1, end to end inside one function: an OpenAI-streaming join whose replay queue will never
    /// close, and a client that hangs up.
    ///
    /// The old pump drained the replay queue first and that loop never consulted `tx.closed()`,
    /// so with an open backlog and a gone body it parked forever — body never finishing, the
    /// task and its 256-frame queue living on, and `JobRelay::is_idle` staying false so the
    /// orphan sweep could not take the row either. **On the pre-C1 tree this test fails**: the
    /// `Detach` never arrives and the 2 s bound below fires instead.
    #[tokio::test]
    async fn an_sse_join_with_an_unclosable_backlog_ends_when_the_client_hangs_up() {
        let (follow, actor_rx, relay) = stranded_join().await;
        let consumer = follow.consumer.clone();
        let job = follow.job.clone();
        let (body_tx, body_rx) = mpsc::channel::<Result<Event, Infallible>>(16);
        let pump = tokio::spawn(pump_sse(follow, "model".to_string(), 0, body_tx));

        // The client hangs up: hyper drops the parked response body, and dropping it closes the
        // pump's channel. That close *is* the connection event NEX-6.1 waits for.
        drop(body_rx);

        let (got_job, got_consumer) =
            tokio::time::timeout(Duration::from_secs(2), detached(actor_rx))
                .await
                .expect("the pump must post its Detach, not park on the backlog");
        assert_eq!(
            (got_job, got_consumer),
            (job, consumer),
            "the detach names this join's job and consumer, exactly"
        );
        tokio::time::timeout(Duration::from_secs(2), pump)
            .await
            .expect("the pump task ends")
            .expect("and does not panic");
        // The leak assertion in its strictest form: the row is owned only by this test now, so
        // the join released the relay, its replay queue and its fan-out slot together.
        assert_eq!(
            Arc::strong_count(&relay),
            1,
            "the stranded relay row went with its pump instead of outliving it"
        );
    }

    /// The same race on `GET /events`, the watcher surface production actually uses: the pump
    /// must post its `Detach` (which is what turns a vanished watcher into NEX-6.1's `Cancel`)
    /// and must leave its slot behind. Pre-C1, this is the same park.
    #[tokio::test]
    async fn an_events_join_with_an_unclosable_backlog_ends_when_the_client_hangs_up() {
        let (follow, actor_rx, relay) = stranded_join().await;
        let consumer = follow.consumer.clone();
        let job = follow.job.clone();
        let (body_tx, body_rx) = mpsc::channel::<Result<Event, Infallible>>(16);
        let pump = tokio::spawn(pump_relay(follow, body_tx));

        drop(body_rx);

        let (got_job, got_consumer) =
            tokio::time::timeout(Duration::from_secs(2), detached(actor_rx))
                .await
                .expect("the watcher must post its Detach, not park on the backlog");
        assert_eq!(
            (got_job, got_consumer),
            (job, consumer),
            "the detach names this watcher's job and consumer, exactly"
        );
        tokio::time::timeout(Duration::from_secs(2), pump)
            .await
            .expect("the pump task ends")
            .expect("and does not panic");
        assert_eq!(
            Arc::strong_count(&relay),
            1,
            "and the watcher's relay row went with it"
        );
    }

    /// The ordering C1 must not break. The live delta is queued **before** the prefix frame, and
    /// the old pumps printed the prefix first because they drained the backlog before touching
    /// live. `held` and the deferred `End` are what keep that promise once both queues are
    /// polled together, so this asserts the printed order, not the arrival order.
    #[tokio::test(flavor = "current_thread")]
    async fn a_live_delta_waits_behind_the_backlog_it_arrived_before() {
        let (mut pump, replay_tx, relay) = open_pump().await;
        relay.push(delta(1, "live")).await;

        // One poll: the pump has to take the delta (that queue closing is one of its exits), but
        // it holds it rather than printing it, so this poll produces nothing.
        assert!(
            tokio::time::timeout(Duration::ZERO, pump.next_item())
                .await
                .is_err(),
            "a live delta taken while the backlog is open is held, not printed"
        );

        replay_tx
            .send(RelayItem::Event(delta(0, "prefix")))
            .await
            .expect("the backlog has room");
        assert_eq!(
            pump.next_item().await,
            Some(RelayItem::Event(delta(0, "prefix"))),
            "the prefix written second is printed first"
        );
        drop(replay_tx);
        assert_eq!(
            pump.next_item().await,
            Some(RelayItem::Event(delta(1, "live"))),
            "and the live delta follows the closed backlog, as the sequential pumps printed it"
        );

        relay.end_stream().await;
        assert_eq!(
            pump.next_item().await,
            Some(RelayItem::End),
            "the live `End` still ends the stream, with nothing owed before it"
        );
        assert_eq!(pump.next_item().await, None, "and then the join is over");
        assert!(pump.ended, "which is the exit that reports itself in band");
    }

    /// The other half of the ordering rule, and the one that decides whether a join is *truncated*:
    /// a live `End` taken while the backlog is still open must be deferred, because the pumps read
    /// `End` as "stop printing". Print it on arrival and the buffered prefix — the whole answer for
    /// a join of a job that went terminal during its attach — is dropped.
    #[tokio::test(flavor = "current_thread")]
    async fn a_live_end_does_not_cut_off_a_backlog_that_is_still_open() {
        let (mut pump, replay_tx, relay) = open_pump().await;
        relay.end_stream().await;

        // The fan-out is closed now, and the backlog is open and empty: the prefix has not been
        // written yet, so this is not the end of the stream.
        assert!(
            tokio::time::timeout(Duration::ZERO, pump.next_item())
                .await
                .is_err(),
            "an open backlog is not an end of stream, even with the fan-out gone"
        );

        // The dispatcher's turn, arriving after the `End` was already taken: the transcript, then
        // the queue closes.
        replay_tx
            .send(RelayItem::Event(delta(0, "first")))
            .await
            .expect("the backlog has room");
        replay_tx
            .send(RelayItem::Event(delta(1, "second")))
            .await
            .expect("the backlog has room");
        drop(replay_tx);

        assert_eq!(
            pump.next_item().await,
            Some(RelayItem::Event(delta(0, "first"))),
            "the whole transcript arrives"
        );
        assert_eq!(
            pump.next_item().await,
            Some(RelayItem::Event(delta(1, "second"))),
            "in order"
        );
        // The deferred `End` closes the stream here, after the transcript — which is the whole
        // point of deferring it: taken at face value it would have ended the stream before either
        // frame above was printed.
        assert_eq!(
            pump.next_item().await,
            None,
            "the deferred `End` ends the stream, once the backlog has been printed"
        );
        assert!(
            !pump.ended,
            "and a stream that was told to end owes no in-band stopped-short notice"
        );
    }

    /// An armed [`SupplierHoldDetach`] sitting on a mailbox that is **already full**, which
    /// is the only state that takes the guard down its fallback path: with a permit free,
    /// `try_send` succeeds and no task is involved at all. The eight fillers are `Tick`s so
    /// a drain can tell the guard's `Detach` apart from what was already queued, and the
    /// ninth send is probed here (and discarded by the failure) so the rig cannot silently
    /// degrade into the uncontended path if `mailbox()`'s capacity ever changes.
    ///
    /// Deliberately not built with `disarm()`: an armed guard is the state a cancelled
    /// endpoint future leaves behind, and that is the drop the fallback exists for.
    fn armed_guard_over_a_full_mailbox() -> (
        SupplierHoldDetach,
        mpsc::Receiver<ActorMsg>,
        JobId,
        ConsumerId,
    ) {
        let (handle, actor_rx) = mailbox();
        for n in 0..8u64 {
            handle
                .tx
                .try_send(ActorMsg::Tick { now_ms: n })
                .expect("the rig's mailbox holds its declared 8 messages");
        }
        assert!(
            handle.tx.try_send(ActorMsg::Tick { now_ms: 9 }).is_err(),
            "the mailbox must be exactly full, or the guard never reaches the fallback"
        );
        let job = JobId::new_unchecked("job-hold-detach");
        let consumer = ConsumerId::new_unchecked("c-hold-detach");
        let guard = SupplierHoldDetach {
            armed: true,
            handle,
            job: job.clone(),
            consumer: consumer.clone(),
        };
        (guard, actor_rx, job, consumer)
    }

    /// Overflow **with** a runtime current: the one-shot fallback must still deliver the
    /// detach, exactly once, behind the fillers. The guard's `try_send` is refused, the
    /// recovered message rides a spawned task, and that task lands the moment a permit
    /// frees — so the detach is the 9th message out, in FIFO order, and it names this
    /// hold's job and consumer. Without the fallback task the overflow branch drops the
    /// `Detach` on the floor and the 2 s bound below fires instead.
    #[tokio::test(flavor = "current_thread")]
    async fn an_overflowed_hold_detach_is_still_delivered_by_the_fallback() {
        let (guard, mut actor_rx, job, consumer) = armed_guard_over_a_full_mailbox();
        // The cancellation itself: hyper drops the handler future, and the armed guard's
        // `Drop` is the detach. Nothing here is awaited, so a blocking guard would hang.
        drop(guard);

        for n in 0..8u64 {
            let message = tokio::time::timeout(Duration::from_secs(2), actor_rx.recv())
                .await
                .expect("the fillers stay readable")
                .expect("the mailbox is not closed");
            assert!(
                matches!(message, ActorMsg::Tick { now_ms } if now_ms == n),
                "the mailbox drains in the order it was filled, got {message:?}"
            );
        }

        let (got_job, got_consumer) =
            tokio::time::timeout(Duration::from_secs(2), detached(actor_rx))
                .await
                .expect("the fallback must post the Detach once a permit frees");
        assert_eq!(
            (got_job, got_consumer),
            (job, consumer),
            "the detach names this hold's job and consumer, exactly"
        );
    }

    /// Overflow with **no** runtime current — the defect the `Handle::try_current()` guard
    /// fixes. A `Drop` is not promised a runtime: the future being unwound can be dropped
    /// by a caller that is itself outside one, and bare `tokio::spawn` panics there. A
    /// panic thrown from `Drop` is not reliably catchable, and this workspace builds
    /// release with `panic = "abort"` (`Cargo.toml:101`), where it aborts the process.
    ///
    /// This is a plain `#[test]`, so no runtime exists: before the guard, the `drop` below
    /// panics and the test fails; after it, the drop returns quietly and the detach is
    /// simply lost (there is no task that could have delivered it), which is the
    /// documented bounded fallback — the actor's own claim-deadline sweep releases the row.
    /// The assertion is the exact mailbox contents: eight `Tick`s, no `Detach`.
    #[test]
    fn an_overflowed_hold_detach_does_not_panic_without_a_runtime() {
        let (guard, mut actor_rx, _job, _consumer) = armed_guard_over_a_full_mailbox();
        drop(guard);

        let mut kinds = Vec::new();
        while let Ok(message) = actor_rx.try_recv() {
            kinds.push(match message {
                ActorMsg::Detach { .. } => "detach",
                ActorMsg::Tick { .. } => "tick",
                _ => "other",
            });
        }
        assert_eq!(
            kinds,
            ["tick"; 8].as_slice(),
            "the lost detach may not displace a queued message, and nothing new may appear \
             in a mailbox with no runtime to drain it"
        );
    }

    /// The aggregate surface's exit: with no body channel to hang up, the clock is what bounds the
    /// wait, and it has to be reachable even while an unclosable backlog is the only thing the
    /// pump is polling. `aggregate_response` arms this at `AGGREGATE_TIMEOUT_SECS`; the mechanism
    /// is the same, so the test uses a clock the suite can wait for.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unclosable_backlog_still_answers_when_the_clock_fires() {
        let (mut pump, _replay_tx, relay) = open_pump().await;
        relay.end_stream().await;

        let sleep = tokio::time::sleep(Duration::from_millis(20));
        tokio::pin!(sleep);
        let mut clock = Some(sleep);
        assert_eq!(
            pump.next(&mut clock).await,
            None,
            "the pump stops rather than parking on a backlog nobody will close"
        );
        assert!(pump.timed_out, "and it says which exit it took");
        assert!(
            !pump.ended,
            "the backlog never closed, so the stream did not end"
        );
    }
}
