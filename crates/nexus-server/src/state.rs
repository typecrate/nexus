//! Shared runtime state: actor mailboxes, per-job relay fan-out, the worker
//! connection registry, and the effect dispatcher.
//!
//! # Shape of a pool (task §5, §13)
//!
//! ```text
//! HTTP/WS handler ──mpsc(bounded)──▶ [ actor task ] ──Vec<Effect>──▶ dispatcher
//!                                                              │
//!                       worker outbound mpsc ◀─────────────────┤ SendWorker
//!                       per-job broadcast   ◀──────────────────┤ Relay / Keepalive
//!                       gate / replay / snapshot waiters ◀─────┘ Accepted / Reject / …
//! ```
//!
//! The `DashMap<JobId, PoolHandle>` the task permits "only as a read-mostly index
//! so a consumer's relay task can locate its job" is [`Hub::job_index`]: it is written
//! at submit time and pruned whenever the actor reports `Effect::JobForgotten`, so it
//! holds only jobs the actor still knows about rather than one entry per submission for
//! the life of the process (NEX-7.2). It never holds job state, so the actor remains the
//! single authority.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::http::StatusCode;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PayloadRefId;
use nexus_core::ids::PoolId;
use nexus_core::ids::TenantId;
use nexus_core::ids::WorkerId;
// `Hub::cfg` is typed by name in three places below, so the type itself is needed.
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::WireCaps;
use nexus_core::ActorMsg;
use nexus_core::BrokerConfig;
use nexus_core::ConsumerEvent;
use nexus_core::Effect;
use nexus_core::JobPhase;
use nexus_core::JobView;
use nexus_core::PoolActor;
use nexus_core::PoolSnapshot;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

/// Actor mailbox depth. Bounded so a submission flood back-pressures the HTTP
/// layer instead of growing memory without end (task §8).
const MAILBOX_DEPTH: usize = 512;

/// How long a read-only probe (snapshot, payload fetch) may take before the
/// caller answers "hub busy" rather than holding the request open forever.
const PROBE_TIMEOUT_MS: u64 = 2_000;

/// One item on a worker socket's outbound queue.
#[derive(Debug, Clone)]
pub enum WorkerWrite {
    /// A JSON-lines frame.
    Frame(String),
    /// Echo of a ping payload.
    Pong(Vec<u8>),
    /// The hub's own liveness probe (see `worker_ws`'s keepalive): a `Ping` with no
    /// payload, whose only job is to make a live peer produce a `Pong`.
    Ping,
}

/// A payload fetch outcome, handed back to the HTTP handler.
#[derive(Debug, Clone)]
pub struct PayloadGrant {
    /// Body bytes.
    pub bytes: Vec<u8>,
    /// Declared content type.
    pub content_type: String,
    /// Digest of the body.
    pub sha256: String,
    /// Refusal reason when not authorized.
    pub reason: Option<String>,
}

/// Item on a job's relay channel.
#[derive(Debug, Clone, PartialEq)]
pub enum RelayItem {
    /// A semantic broker event.
    Event(ConsumerEvent),
    /// An SSE comment (`: ping`) while the job waits (task §7.2).
    Comment(&'static str),
    /// Nothing more will arrive: the handler may end its body.
    End,
}

/// What the first effect about a job says about its HTTP response.
#[derive(Debug, Clone, PartialEq)]
pub enum Gate {
    /// Accepted: headers may go out and the body may start.
    ///
    /// `worker` is the supplier the consumer was **connected to**, from
    /// `Effect::Accepted.worker` (`actor.rs:379-401`) — the `x-supplier-id` the face
    /// echoes. Three states, and the faces must tell them apart:
    ///   * `Some` — leased in the same mailbox step as the verdict (the warm-and-free
    ///     case, which is the common one) or already leased by the execution a
    ///     duplicate joined. The header can go out with the head.
    ///   * `None` **on a fresh submission** — accepted into the queue with a warm pool
    ///     whose slots are all busy. Not a failure: the `Effect::SendWorker{Assigned}`
    ///     arm below resolves the gate's *successor* question through
    ///     [`JobRelay::note_supplier`], and a face that has not flushed its head yet may
    ///     still fill it (see `consumer::supplier_before_body`).
    ///   * `None` **on a join of a terminal job** — there is nothing to report, because
    ///     the lease is gone. Never a failure either.
    Accepted {
        /// The hub-minted id of the connected supplier, when one is known.
        worker: Option<WorkerId>,
    },
    /// Refuse **before** any body bytes (task §7.2, acceptance test 6).
    Reject {
        /// HTTP status.
        status: StatusCode,
        /// Stable machine code.
        code: String,
        /// Human detail.
        message: String,
        /// `Retry-After` in seconds.
        retry_after_secs: Option<u64>,
    },
    /// The actor stopped answering: a retryable failure, never a hang (task §11).
    Closed,
}

/// How long a push into a live consumer's queue may wait before that consumer is
/// declared gone. The same judgement as `send_frame`'s worker grace: a healthy
/// hyper response body drains one item in far less than this, so a consumer that
/// cannot is dead in every way that matters — dropped explicitly (its stream
/// ends with an in-band error) rather than silently missing frames.
const CONSUMER_SEND_GRACE_MS: u64 = 25;

/// Everything the consumer side needs to follow one job.
#[derive(Debug)]
pub struct JobRelay {
    /// Live consumers, one queue each, keyed by this relay's own slot ids.
    ///
    /// Two decisions live in this one declaration, and both replace a `Vec<Sender>`
    /// that the fan-out pruned **lazily**:
    ///
    ///   * **The slot leaves when the pump leaves.** A subscriber's id is held by
    ///     [`Subscriber`], whose `Drop` retires it, and every consumer pump holds
    ///     exactly one — so a stream that ends (terminal event, lag), the session
    ///     stops (`detach`, close, a liveness verdict) or is aborted takes its sender
    ///     out at that instant. On the old shape a sender stayed until the *next*
    ///     push happened to notice it was dead: a pump the session stopped left its
    ///     `Sender` — and every frame still sitting in its `consumer_backpressure_frames`
    ///     -deep queue — behind on a fan-out that had already gone terminal and would
    ///     never push again. Per-stream state with no bound, and on one long-lived
    ///     consumer socket it was one stranded queue per task it had ever watched.
    ///   * **No lock is ever held across an await**, so the `Drop` that performs the
    ///     removal (it cannot await) can always take it. The async mutex this used to
    ///     sit behind is held by an in-flight push for a whole 25 ms grace window per
    ///     subscriber, which turns "remove it when the pump stops" back into "remove
    ///     it whenever the removal is awaitable". `push_to_consumers` therefore
    ///     snapshots the senders and releases the lock before delivering anything;
    ///     the actor applies its effects inline (`run_actor` awaits one dispatch at a
    ///     time), so two pushes on one relay never overlap and a snapshot cannot
    ///     reorder them. A consumer that subscribes *during* a push still misses that
    ///     one item — exactly what it did when it blocked on the async lock.
    ///
    /// Same `std::sync::Mutex` plus [`poison_free`] choice the consumer connection
    /// registry below documents; every critical section here is a map lookup.
    subscribers: std::sync::Mutex<BTreeMap<u64, mpsc::Sender<RelayItem>>>,
    /// Per-relay source of those slot ids. Started at `1` so `0` stays visibly "not a
    /// slot" in a log line, and scoped to this relay because the relay is what owns
    /// them.
    next_subscriber: AtomicU64,
    /// Monotonic-clock ms at which this relay was created, read only by
    /// [`prune_orphaned_relays`]: every face registers its relay a moment *before*
    /// the actor knows the job (`consumer::submit`), so age is what tells an
    /// in-flight registration apart from a row that outlived its record.
    created_ms: u64,
    /// First-answer gate, resolved once by `Accepted` or `Reject`.
    pub gate: Mutex<Option<oneshot::Sender<Gate>>>,
    /// The supplier this job was leased to, as soon as the dispatcher has seen the
    /// effect that says so — see [`JobRelay::note_supplier`].
    ///
    /// A `watch` rather than a plain cell because the streamed HTTP faces **wait on
    /// it**: `Effect::Accepted` can admit a job into the queue with no lease at all
    /// (`actor.rs:1303` — a warm pool whose slots are all busy), and the header-only
    /// delivery contract means the face has to know the id before it flushes its head.
    /// The watch is the hub-side signal that the later `Assigned` happened; the actor's
    /// deadline is what bounds the wait (`consumer::supplier_before_body`), so there is
    /// no new timer and no new knob.
    ///
    /// A `watch` also gives the "never clobber" rule for free: `send_if_modified`
    /// writes the first `Some` and refuses every later write, so a requeue's second
    /// `Assigned` cannot move an id a response already named, and a `None` verdict can
    /// never erase an id that is already known.
    supplier: tokio::sync::watch::Sender<Option<WorkerId>>,
    /// Replay delivery for the consumer that just attached, so a joiner does not
    /// make every other subscriber replay too.
    pub replay_target: Mutex<Option<mpsc::Sender<RelayItem>>>,
    /// Set once a terminal event has been broadcast, so `End` is sent once.
    pub finished: AtomicBool,
}

/// One consumer's subscription to a job's fan-out: the queue its pump reads, plus
/// the slot that keeps that pump registered in [`JobRelay`] until it is dropped.
///
/// The pairing is the whole point. A pump that receives a bare `Receiver` has to
/// remember to unregister itself, and every one of the interesting exits (task
/// aborted, future dropped mid-await, an early `return` added later) skips that
/// step. Making the registration a value that *dies with the pump* is what makes
/// "no subscriber outlives its pump" true by construction rather than by
/// discipline, and it is why no pump here calls an unsubscribe function.
#[derive(Debug)]
pub struct Subscriber {
    /// The fan-out this slot belongs to.
    relay: Arc<JobRelay>,
    /// This slot's id inside `relay.subscribers`.
    id: u64,
    /// The queue this consumer's pump reads.
    rx: mpsc::Receiver<RelayItem>,
}

impl Subscriber {
    /// Register one live consumer against `relay` and hand back its subscription.
    ///
    /// `capacity` is `limits.consumer_backpressure_frames`: a consumer that cannot
    /// keep up is dropped explicitly (its queue closes and its pump ends the stream
    /// in band), which is the reading NEX-5.3 wants.
    #[must_use]
    pub async fn attach(relay: &Arc<JobRelay>, capacity: usize) -> Self {
        let (id, rx) = relay.add_slot(capacity).await;
        Self {
            relay: Arc::clone(relay),
            id,
            rx,
        }
    }

    /// Next relay item, `None` once the fan-out has closed this queue (the backlog
    /// outlasted the send grace) or ended. Shaped like `Receiver::recv` on purpose:
    /// the three pumps all read their live source this way, and the wrapper must be
    /// invisible to them — the less they know about it, the less there is to forget
    /// on the way out.
    pub async fn recv(&mut self) -> Option<RelayItem> {
        self.rx.recv().await
    }

    /// Non-blocking twin of `recv`, for the aggregate pump's timeout sweep (`consumer.rs`).
    /// `None` means "nothing buffered this instant", not "the fan-out closed" — the pump must
    /// not read a miss as an end of stream.
    pub fn try_recv(&mut self) -> Option<RelayItem> {
        self.rx.try_recv().ok()
    }
}

impl Drop for Subscriber {
    /// Takes this slot out of the fan-out. Runs on every exit path, including an
    /// abort, which is the case an explicit `unsubscribe().await` could never cover.
    fn drop(&mut self) {
        self.relay.retire(self.id);
    }
}

impl JobRelay {
    /// New relay with a bounded fan-out, stamped with the hub's monotonic clock.
    #[must_use]
    pub fn new(_capacity: usize, created_ms: u64) -> Self {
        Self {
            subscribers: std::sync::Mutex::new(BTreeMap::new()),
            next_subscriber: AtomicU64::new(1),
            created_ms,
            gate: Mutex::new(None),
            // `None` at construction: the relay is created before the actor has even
            // seen the submission (`consumer::submit`), so "no supplier yet" is the
            // only honest starting value.
            supplier: tokio::sync::watch::channel(None).0,
            replay_target: Mutex::new(None),
            finished: AtomicBool::new(false),
        }
    }

    /// Record the supplier this job was leased to, the first time one is known.
    ///
    /// Called from the dispatcher's `Effect::Accepted{worker}` and
    /// `Effect::SendWorker{Assigned}` arms, and by the attach faces when a snapshot
    /// already shows a lease (`consumer::join_existing_job`, `consumer_ws::handle_attach`)
    /// — a join never emits `Accepted`, so the lease is the only place that answer lives.
    ///
    /// First write wins, and `None` is never passed here at all: the caller's `Option`
    /// is unpacked at the call sites, so "nothing to report" leaves the existing value
    /// untouched rather than erasing it. Synchronous by design — the dispatcher holds no
    /// lock across it, and a `watch::Sender` needs no await.
    pub(crate) fn note_supplier(&self, worker: &WorkerId) {
        self.supplier.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(worker.clone());
            true
        });
    }

    /// The supplier known for this job right now, `None` while the lease is pending.
    ///
    /// Read at **write** time by the faces, never re-queried from the actor: the relay
    /// row is the hub's own mirror of the one dispatch effect that named the worker.
    ///
    /// "Right now" is about what the hub has *learned*, not about who holds the lease at the
    /// instant of the read. The cell is first-write-wins, so after a requeue hands the task to
    /// a second supplier this still answers with the first one — which is the value the
    /// surface is for: `x-supplier-id` names the supplier the consumer was **connected to**, so
    /// it can pin that supplier again, and an id that quietly moved between two claims would
    /// tell the consumer its conversation had migrated when its own request had not. The
    /// actor's `lease.worker` remains the authority on who holds the lease now, and
    /// `GET /v1/jobs/{id}` reports that. The two can diverge in either direction: after a
    /// requeue this cell keeps the first supplier, and once the lease is gone the faces read
    /// the (now empty) lease rather than this cell and stop naming anybody
    /// (`a_known_supplier_survives_every_later_verdict` pins the first,
    /// `a_join_of_a_finished_completion_names_no_supplier` the second).
    #[must_use]
    pub(crate) fn supplier(&self) -> Option<WorkerId> {
        self.supplier.borrow().clone()
    }

    /// A watcher for [`JobRelay::supplier`], handed to a face that has to hold its
    /// response head until the id is known (`consumer::supplier_before_body`).
    ///
    /// `subscribe()` starts at the value that is current *now*, so a lease taken
    /// between the face's [`JobRelay::supplier`] read and its first `changed()` poll
    /// cannot be missed — the whole reason this is a `watch` and not a `Notify`.
    #[must_use]
    pub(crate) fn supplier_watcher(&self) -> tokio::sync::watch::Receiver<Option<WorkerId>> {
        self.supplier.subscribe()
    }

    /// Resolve the gate, if it is still open.
    pub async fn resolve(&self, gate: Gate) {
        if let Some(tx) = self.gate.lock().await.take() {
            let _ = tx.send(gate);
        }
    }

    /// Registers one consumer queue and returns its `(slot id, receiver)`.
    ///
    /// Prefer [`Subscriber::attach`], which pairs the two so a caller cannot drop the
    /// receiver while still holding a slot that will never be retired.
    async fn add_slot(&self, capacity: usize) -> (u64, mpsc::Receiver<RelayItem>) {
        let (tx, rx) = mpsc::channel(capacity.max(16));
        let id = self.next_subscriber.fetch_add(1, Ordering::Relaxed);
        poison_free(&self.subscribers).insert(id, tx);
        // A relay that is already finished will never fan out another item, so a queue
        // registered here would sit open until the pump's own deadline or a hangup said stop:
        // `end_stream` has already run, and it is the only thing that sends `End`. Take the
        // sender back so the receiver closes at once and the pump sees the same thing it would
        // have seen had it attached a moment earlier. `retire` is idempotent, so the
        // `Subscriber`'s `Drop` on a slot that is already gone stays harmless.
        if self.finished.load(Ordering::Relaxed) {
            self.retire(id);
        }
        (id, rx)
    }

    /// Drops one slot. Idempotent: `end_stream`'s `clear()` and a teardown of the
    /// whole relay both take slots away from under a `Subscriber` that has not run
    /// its `Drop` yet, and a slot that is already gone is not an error.
    fn retire(&self, id: u64) {
        poison_free(&self.subscribers).remove(&id);
    }

    /// How many consumer queues are registered right now.
    ///
    /// Two readers, and neither is optional: [`prune_orphaned_relays`] sums it into
    /// `nexus_relay_subscribers`, which is the only way the deployed hub can show a consumer
    /// queue outliving its pump at all (`subscriber_count` used to be `#[cfg(test)]`, and a
    /// monitor cannot be told to rebuild the binary with tests on); and the e2e tests assert
    /// it directly, where the metric's one-second tick cadence would only blur what happened.
    #[must_use]
    pub(crate) fn subscriber_count(&self) -> usize {
        poison_free(&self.subscribers).len()
    }

    /// Un-install an attach's replay target when the attach is **abandoned** before its pump
    /// exists: the `Attach` post failed, so the face answers `503` (HTTP) or `rejected` (WS) and
    /// hangs up, and the receiver half of this queue goes out of scope with it.
    ///
    /// Leaving the sender in place is not harmless. [`JobRelay::is_idle`] requires
    /// `replay_target` to be `None`, so the row that this attach created stops being sweepable
    /// by [`prune_orphaned_relays`] — while nothing on the hub can ever take the slot again: the
    /// only other writer is the dispatcher's `ReplayTo` arm, and that attach never reached the
    /// actor. Those rows are precisely what the sweep exists to collect, so the abandonment has
    /// to be recorded, not just returned from.
    pub(crate) async fn abandon_replay_target(&self) {
        let _ = self.replay_target.lock().await.take();
    }

    /// Whether nothing on this relay can still be waited on: no consumer queue
    /// registered, no face waiting on the verdict gate, and no attach waiting for its
    /// replay prefix. Read by [`prune_orphaned_relays`], and by the tests that pin which
    /// abandoned attaches leave a sweepable row behind.
    ///
    /// `try_lock` on the two async fields, deliberately, and "not idle" when a lock
    /// will not answer: the dispatcher's `Accepted`/`ReplayTo` arms hold
    /// `replay_target` across an await (`.lock().await.take()` as a match scrutinee
    /// keeps its guard for the whole block), so a lock that cannot be taken right now
    /// means somebody is using this relay — the safe answer for a sweep whose one job
    /// is never to delete a row out from under a live request.
    ///
    /// Lock discipline, for whoever edits this next: the `subscribers` guard is a
    /// **std** mutex guard and it is held for the whole `&&` chain, across both tokio
    /// `try_lock()` calls below. Those are different mutexes, so that is safe — but a
    /// second `poison_free(&self.subscribers)` *inside the same expression* would
    /// self-deadlock on the std guard, which `Mutex::reentrant_lock`-style protection
    /// does not exist to catch.
    pub(crate) fn is_idle(&self) -> bool {
        poison_free(&self.subscribers).is_empty()
            && self.gate.try_lock().is_ok_and(|slot| slot.is_none())
            && self
                .replay_target
                .try_lock()
                .is_ok_and(|slot| slot.is_none())
    }

    /// Push one item to every live consumer. A queue that stays full past the
    /// grace window belongs to a dead consumer: its sender is removed, so its
    /// pump sees the close and ends its stream in band. Nothing is dropped for a
    /// consumer that keeps up.
    ///
    /// Delivery stays **reliable**: an item is either in every live consumer's
    /// queue, or that consumer has been dropped explicitly past the grace window.
    /// There is no ring here to lose frames from — a completion an LLM client is
    /// reading must arrive whole, and the old lossy fan-out answered a fast
    /// producer's burst (a long prefill followed by a decode run) by corrupting the
    /// answer mid-read: the consumer ended its stream, the job was cancelled
    /// mid-generation, and the supplier finished into a lease that no longer
    /// existed.
    async fn push_to_consumers(&self, item: RelayItem) {
        // Snapshot first, deliver second: the lock must be free for a `Subscriber`
        // being dropped in between (see the field's documentation).
        let subs: Vec<(u64, mpsc::Sender<RelayItem>)> = poison_free(&self.subscribers)
            .iter()
            .map(|(id, tx)| (*id, tx.clone()))
            .collect();
        let mut dead: Vec<u64> = Vec::new();
        for (id, tx) in subs {
            match tokio::time::timeout(
                std::time::Duration::from_millis(CONSUMER_SEND_GRACE_MS),
                tx.send(item.clone()),
            )
            .await
            {
                // Delivered.
                Ok(Ok(())) => {}
                // The consumer hung up mid-send: the pump is gone.
                Ok(Err(_)) => dead.push(id),
                // Still full past the grace window: a dead consumer.
                Err(_) => dead.push(id),
            }
        }
        for id in dead {
            self.retire(id);
        }
    }

    /// Relay one event, marking the stream finished when it is terminal.
    pub async fn push(&self, event: ConsumerEvent) {
        let terminal = matches!(
            event,
            ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
        );
        self.push_to_consumers(RelayItem::Event(event)).await;
        if terminal && !self.finished.swap(true, Ordering::Relaxed) {
            self.end_stream().await;
        }
    }

    /// A keepalive comment while the job waits (task §7.2).
    pub async fn push_comment(&self, text: &'static str) {
        self.push_to_consumers(RelayItem::Comment(text)).await;
    }

    /// End every live consumer's stream: one `End`, then drop the senders, so a
    /// still-attached pump sees its queue close and ends its body promptly. A
    /// job that went terminal must never leave a consumer on keepalives.
    pub async fn end_stream(&self) {
        if self.finished.swap(true, Ordering::Relaxed) {
            return;
        }
        self.push_to_consumers(RelayItem::End).await;
        poison_free(&self.subscribers).clear();
    }
}

/// One connected worker socket, as seen by a pool.
#[derive(Debug)]
pub struct WorkerConn {
    /// Connection generation, echoed as `Welcome.epoch`.
    pub conn: u64,
    /// Outbound queue for this socket.
    pub tx: mpsc::Sender<WorkerWrite>,
}

/// One live consumer WebSocket connection, as `GET /v1/observe` reports it.
///
/// # Why the hub tracks this at all
///
/// "Which consumer is attached to which job" is *actor* state — `JobRecord::
/// consumers` is the authority, and it is keyed by the per-(connection, task)
/// `ConsumerId` the consumer face mints (`consumer_ws.rs:36-38`). What the actor
/// cannot answer is the inverse question a monitor asks: **which connections are
/// up right now**, across every pool, with the jobs each one is watching. Reading
/// that out of the actors would mean one query per job per pool and still would
/// not group them by socket, because a socket is not an actor concept — it is a
/// `session()` concept. So the session owns exactly one of these records for its
/// whole life, and it is the only writer.
///
/// # Why `std::sync::Mutex`, when every other hub map uses a tokio one
///
/// The deregistration has to happen on **every** exit path of a session, including
/// a panic, and the only construct that cannot be skipped is `Drop` — which cannot
/// await. Every critical section here is a hash lookup plus a `BTreeSet` insert or
/// remove: no lock is ever held across an `.await`, so an async lock would buy
/// nothing and cost a wakeup per attach. Poisoning is impossible by construction
/// (nothing inside a critical section can panic), and [`poison_free`] still
/// recovers rather than taking the hub down.
#[derive(Debug, Clone)]
pub struct ConsumerConnection {
    /// Connection id, minted by [`IdSource::consumer_conn_id`]. One per socket,
    /// **not** the per-task `ConsumerId` the actor keys its consumer sets by.
    pub id: ConsumerId,
    /// The tenant the pre-upgrade API key resolved to.
    pub tenant: TenantId,
    /// Wall-clock ms the socket was accepted (after auth, before any frame).
    pub since_ms: u64,
    /// Peer address, when the transport exposed one. `None` because
    /// `axum::serve(listener, app)` does not install `ConnectInfo`, and inventing
    /// a second service wrapper for one monitor field is not worth the churn.
    pub peer: Option<String>,
    /// Jobs this connection currently has a live stream for. Cleared per job when
    /// its pump ends, and dropped whole when the connection does.
    pub attached_jobs: BTreeSet<JobId>,
}

/// A hub lock that never panics on a poisoned mutex.
///
/// `Mutex::expect` is not an option under the workspace's `expect_used` lint, and
/// `unwrap_or_else(|e| e.into_inner())` is not noise: it states the policy. A
/// poison flag means some other task panicked while holding the lock; the data
/// behind these locks is a plain map whose mutations are single `insert`/`remove`
/// calls, so continuing with it is strictly better than refusing every request
/// for the rest of the process's life.
fn poison_free<T>(lock: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Per-pool runtime handle.
#[derive(Debug)]
pub struct PoolHandle {
    /// The pool's id.
    pub id: PoolId,
    /// Actor mailbox.
    pub tx: mpsc::Sender<ActorMsg>,
    /// Worker id → live connection.
    pub workers: Mutex<HashMap<WorkerId, WorkerConn>>,
    /// Job id → relay fan-out.
    pub relays: Mutex<HashMap<JobId, Arc<JobRelay>>>,
    /// Read-mostly mirror of the actor's authoritative gauges, refreshed on every
    /// dispatch so `/healthz` and `/metrics` never disagree with the actor.
    pub gauges: Mutex<PoolGauges>,
    /// `nexus_relay_rows`: how many rows `relays` holds, as of the last sweep.
    ///
    /// An atomic rather than a field of `PoolGauges` because `PoolGauges` is replaced whole
    /// from the actor on every dispatch (`PoolGauges::from_actor`), and the actor has no
    /// idea how many fan-outs the dispatcher is holding. Reading this at render time is also
    /// what keeps the exposition honest whatever the tick/dispatch interleaving was.
    pub relay_rows: AtomicU64,
    /// `nexus_relay_subscribers`: consumer queues registered across those rows, from the
    /// same locked pass, so the two series are a sample of one instant and not two.
    pub relay_subscribers: AtomicU64,
}

/// Mirror of the actor's gauges and counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolGauges {
    /// Queued jobs.
    pub queue_depth: u64,
    /// Ready workers.
    pub ready_workers: u64,
    /// Connected but not-ready workers.
    pub loading_workers: u64,
    /// Live leases.
    pub leases: u64,
    /// Warm model names, for `/v1/models`.
    pub warm_models: BTreeSet<String>,
    /// Mirror of the actor's counters **and** gauges, so `/metrics` reports the
    /// exact numbers the acceptance tests assert. Gauges matter as much as
    /// counters here: `nexus_queue_depth`, `nexus_ready_workers` and
    /// `nexus_leases` are what make §13's "metrics that must exist because
    /// debugging is otherwise blind" true, and copying only `counters()` would
    /// drop them from the exposition entirely.
    pub metrics: nexus_core::Metrics,
}

impl PoolGauges {
    /// Pull the gauges from the actor itself: one source, no drift.
    #[must_use]
    pub fn from_actor(actor: &PoolActor) -> Self {
        Self {
            queue_depth: actor.queue_depth() as u64,
            ready_workers: actor.ready_workers() as u64,
            loading_workers: actor.loading_workers() as u64,
            leases: actor.leases_len() as u64,
            warm_models: actor.warm_models(),
            metrics: actor.metrics().clone(),
        }
    }
}

/// The hub: config, credentials, one handle per pool, and the routing index.
#[derive(Debug)]
pub struct Hub {
    /// Validated configuration.
    pub cfg: BrokerConfig,
    /// Pool id → handle.
    pub pools: HashMap<PoolId, Arc<PoolHandle>>,
    /// Job id → pool, populated at submit time (the §5 read-mostly index). Bounded by
    /// the actor's own bounds: the entry is removed when the actor drops the record
    /// (`Effect::JobForgotten`, handled in `dispatch`), so a long-lived hub does not
    /// accumulate one id per submission ever made.
    pub job_index: Mutex<HashMap<JobId, PoolId>>,
    /// Hub-side mirror of the actor's §5.2 idempotency table: key → (pool, job, expiry).
    ///
    /// It exists because the actor answers a duplicate on the **original** job's id
    /// (`Effect::Accepted { job: existing, attached: true, .. }`, `actor.rs:1045-1064`)
    /// while a face registers its relay, its verdict gate and its pump under the id it
    /// minted for the request. With no way to find the original *before* minting, the
    /// retry's gate was never resolved and the answer was `503 hub_unavailable` while the
    /// original's replay was dropped on a relay nobody was reading.
    ///
    /// Bounded exactly as the actor's own table is (`limits.max_idempotency_entries`,
    /// evicting whatever expires first — the rule of `actor.rs:1199-1215`) and pruned by
    /// `Effect::JobForgotten` through [`Self::key_of_job`], so an entry can never outlive
    /// the record it indexes: a task NEX-6.3 deleted has no mirror row left, and its retry
    /// runs fresh. Expiry uses the actor's own `limits.idempotency_ttl_ms` and the actor's
    /// own clock (`monotonic_ms`, the value `run_actor` feeds it), so the two tables lapse
    /// together; if the actor's copy lapses a millisecond first, the face answers from the
    /// still-retained record with the same id and the same bytes the actor's duplicate
    /// branch would have replayed.
    pub idempotency: Mutex<BTreeMap<String, (PoolId, JobId, u64)>>,
    /// Expiry-ordered index of [`Self::idempotency`]'s keys, `(expiry_ms, key)`, so lazy
    /// expiry and bounded eviction are O(log n) instead of a scan. Mirrors
    /// `actor.rs:482`'s `idempotency_timer`.
    pub idempotency_expiry: Mutex<BTreeSet<(u64, String)>>,
    /// Job → key, so `Effect::JobForgotten` prunes the row that belongs to it without
    /// scanning the table.
    pub key_of_job: Mutex<HashMap<JobId, String>>,
    /// Consumer API keys, as digests.
    pub api_keys: nexus_core::auth::ApiKeyRegistry,
    /// Pool tokens, as digests.
    pub pool_tokens: nexus_core::auth::PoolTokenRegistry,
    /// Monotonic id source.
    pub ids: Arc<IdSource>,
    /// Artifact storage root.
    pub artifact_root: String,
    /// Waiters for payload fetch answers, keyed by reference.
    pub payload_waiters: Mutex<HashMap<PayloadRefId, oneshot::Sender<PayloadGrant>>>,
    /// Waiters for `GET /v1/jobs/{id}` answers, keyed by job.
    pub snapshot_waiters: Mutex<HashMap<JobId, Vec<oneshot::Sender<Option<JobView>>>>>,
    /// Waiters for whole-pool `ActorMsg::Observe` answers, keyed by the token the
    /// request carried (`Effect::Observed.token`).
    ///
    /// A token rather than a pool id because one pool may be watched by several
    /// monitors at once and the actor answers them one per message, in mailbox
    /// order: with a pool-keyed queue, a request that timed out and stopped
    /// listening would shift every later answer onto the wrong waiter. The token
    /// makes each answer addressable, so a stale snapshot can only ever be dropped
    /// (nobody is waiting), never delivered to a request that asked for a newer one.
    pub observe_waiters: Mutex<HashMap<u64, oneshot::Sender<PoolSnapshot>>>,
    /// Monotonic source for those tokens. Started at `1` so `0` stays visibly
    /// "not a token" in a log line.
    pub observe_tokens: AtomicU64,
    /// Live consumer WebSocket connections, keyed by connection id. Written only by
    /// `consumer_ws::session` (accept / attach / detach / close); read by
    /// `GET /v1/observe`. See [`ConsumerConnection`].
    pub consumer_conns: std::sync::Mutex<HashMap<ConsumerId, ConsumerConnection>>,
    /// Wall-clock origin, for OpenAI `created`.
    pub epoch_ms: u64,
}

/// Opaque ids minted by the hub.
///
/// Jobs stay monotonic/process-unique. Supplier and consumer identities are
/// **hub-assigned UUIDv4 values**, one per connection (and, for a consumer
/// subscription, one per connection+task): nothing on the wire declares them,
/// so two suppliers can never collide and a dropped connection's id is never
/// reused.
#[derive(Debug)]
pub struct IdSource {
    /// Next job sequence.
    pub job: AtomicU64,
    /// Next connection generation.
    pub conn: AtomicU64,
    /// Per-process salt, so job ids from two hubs cannot collide.
    pub salt: u64,
}

/// Fallback entropy source for [`uuid_v4`] when the OS RNG is unavailable.
/// Only uniqueness matters in that path; the normal path is `getrandom`.
static UUID_FALLBACK: AtomicU64 = AtomicU64::new(1);

/// A random UUIDv4 (lowercase, RFC 9562 version/variant bits set).
///
/// Supplier and consumer identities are never derived from a client value and
/// never from the clock: the OS CSPRNG is asked for 128 bits, and the clock is
/// only a last-resort uniqueness fallback if the RNG itself is unavailable.
#[must_use]
pub fn uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        // No entropy: derive 128 bits from the clock + a process-local counter.
        // This keeps ids unique within the process even in that degraded mode.
        let n = UUID_FALLBACK.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        let mut x = nanos ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        for chunk in bytes.chunks_mut(8) {
            x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let y = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            let y = (y ^ (y >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            chunk.copy_from_slice(&(y ^ (y >> 31)).to_le_bytes());
        }
    }
    // Version 4 (random) and RFC 4122 variant, exactly as a UUIDv4 requires.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

impl Default for IdSource {
    fn default() -> Self {
        Self::new()
    }
}

impl IdSource {
    /// New source, salted from the wall clock (job ids only).
    #[must_use]
    pub fn new() -> Self {
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos() as u64);
        Self {
            job: AtomicU64::new(1),
            conn: AtomicU64::new(1),
            salt,
        }
    }

    /// Next job id.
    #[must_use]
    pub fn job_id(&self) -> JobId {
        let n = self.job.fetch_add(1, Ordering::Relaxed);
        JobId::new_unchecked(format!("job-{:08x}-{n:08x}", self.salt & 0xffff_ffff))
    }

    /// Next connection generation.
    #[must_use]
    pub fn conn_id(&self) -> u64 {
        self.conn.fetch_add(1, Ordering::Relaxed)
    }

    /// A fresh supplier identity for one WebSocket connection.
    ///
    /// The hub mints this at the upgrade and sends it in `Welcome`; the
    /// supplier never declares it. A reconnect is a different connection and
    /// therefore a different id — the old one is permanently gone.
    #[must_use]
    pub fn worker_id(&self) -> WorkerId {
        WorkerId::new_unchecked(uuid_v4())
    }

    /// A fresh consumer subscription id (one per connection+task).
    #[must_use]
    pub fn consumer_id(&self) -> ConsumerId {
        ConsumerId::new_unchecked(uuid_v4())
    }

    /// A fresh consumer **connection** id: one per consumer WebSocket.
    ///
    /// The actor keys `JobRecord::consumers` by the per-(connection, task) id,
    /// and `consumer_ws` mints one of those per watched job; the observe
    /// surface reports the *socket*, which is a single thing that may be
    /// watching several jobs. Both are fresh UUIDv4 values; the field an id
    /// appears in says which axis it names.
    #[must_use]
    pub fn consumer_conn_id(&self) -> ConsumerId {
        ConsumerId::new_unchecked(uuid_v4())
    }
}

impl Hub {
    /// Build the hub, then start one actor task per configured pool.
    #[must_use]
    pub fn new(cfg: BrokerConfig, artifact_root: &str) -> Arc<Self> {
        let mut pools = HashMap::new();
        // The receiver travels **with** the handle it belongs to. The spawn loop below
        // runs after `pools` has moved into the `Hub`, so a handle that is looked up
        // there instead of carried here would need an `expect` to prove an invariant the
        // compiler can already see: `starts` is built from exactly the pool ids inserted
        // above, one handle each.
        let mut starts: Vec<(PoolId, mpsc::Receiver<ActorMsg>, Arc<PoolHandle>)> = Vec::new();
        for id in cfg.pools.keys() {
            let pid = PoolId::new_unchecked(id.clone());
            let (tx, rx) = mpsc::channel::<ActorMsg>(MAILBOX_DEPTH);
            let handle = Arc::new(PoolHandle {
                id: pid.clone(),
                tx,
                workers: Mutex::new(HashMap::new()),
                relays: Mutex::new(HashMap::new()),
                gauges: Mutex::new(PoolGauges::default()),
                relay_rows: AtomicU64::new(0),
                relay_subscribers: AtomicU64::new(0),
            });
            starts.push((pid.clone(), rx, Arc::clone(&handle)));
            pools.insert(pid, handle);
        }
        let hub = Arc::new(Self {
            epoch_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            api_keys: nexus_core::auth::ApiKeyRegistry::from_config(&cfg.auth),
            pool_tokens: nexus_core::auth::PoolTokenRegistry::from_config(&cfg.auth),
            job_index: Mutex::new(HashMap::new()),
            idempotency: Mutex::new(BTreeMap::new()),
            idempotency_expiry: Mutex::new(BTreeSet::new()),
            key_of_job: Mutex::new(HashMap::new()),
            payload_waiters: Mutex::new(HashMap::new()),
            snapshot_waiters: Mutex::new(HashMap::new()),
            observe_waiters: Mutex::new(HashMap::new()),
            observe_tokens: AtomicU64::new(1),
            consumer_conns: std::sync::Mutex::new(HashMap::new()),
            artifact_root: artifact_root.to_string(),
            ids: Arc::new(IdSource::new()),
            cfg,
            pools,
        });
        for (pid, rx, handle) in starts {
            let actor =
                PoolActor::with_artifact_root(pid.clone(), hub.cfg.clone(), &hub.artifact_root);
            tokio::spawn(run_actor(pid, actor, rx, handle, Arc::clone(&hub)));
        }
        hub
    }

    /// Handle for a pool.
    #[must_use]
    pub fn pool(&self, pool: &PoolId) -> Option<&Arc<PoolHandle>> {
        self.pools.get(pool)
    }

    /// Pool ids in a stable order.
    #[must_use]
    pub fn pool_ids(&self) -> Vec<PoolId> {
        self.pools
            .keys()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Record which pool owns a job (called by the submit handler).
    ///
    /// Paired with [`Self::forget_job`]: the index only stays bounded if every entry a
    /// submit adds is removed when the actor stops knowing the job.
    pub async fn bind_job(&self, job: &JobId, pool: &PoolId) {
        self.job_index
            .lock()
            .await
            .insert(job.clone(), pool.clone());
    }

    /// Drop the routing entry for a job the actor has forgotten
    /// (`Effect::JobForgotten`). Idempotent: an unknown job prunes to nothing and
    /// answers `false`, so the dispatcher can call it from every drop site without
    /// tracking whether an earlier effect already did the work.
    pub async fn forget_job(&self, job: &JobId) -> bool {
        self.job_index.lock().await.remove(job).is_some()
    }

    /// Route a job to its pool from the read-mostly index.
    pub async fn route_job(&self, job: &JobId) -> Option<Arc<PoolHandle>> {
        let pid = self.job_index.lock().await.get(job).cloned()?;
        self.pool(&pid).map(Arc::clone)
    }

    /// Record which job an idempotency key owns, for the reason given on
    /// [`Self::idempotency`]. Called by the faces at submit time, in the same step that
    /// writes `job_index` (`consumer::submit`), because the two must name the same job.
    ///
    /// Lock order across the three mirrors is fixed — table, expiry index, reverse index —
    /// and no guard is held across a call that takes them again, so no path can acquire
    /// them in opposite orders.
    pub async fn remember_idempotency(&self, key: &str, pool: &PoolId, job: &JobId) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        // The actor's clock, not the wall clock: `monotonic_ms` is the value `run_actor`
        // hands the actor as `now_ms`, so a TTL started here expires with the actor's own
        // entry for the same key.
        let expiry = monotonic_ms().saturating_add(self.cfg.limits.idempotency_ttl_ms);
        // A key reaching a second job replaces its first row, timer entry included;
        // `JobForgotten` normally did this first, and the reverse row only changes when
        // the job actually changed.
        if let Some((_old_pool, old_job, old_expiry)) = table.remove(key) {
            expiry_index.remove(&(old_expiry, key.to_string()));
            if old_job != *job {
                reverse.remove(&old_job);
            }
        }
        // Bound the table by evicting whatever expires first, so a long-lived key is
        // never dropped ahead of a stale one — the actor's rule (`actor.rs:1199-1215`),
        // copied rather than paraphrased so the two tables never disagree.
        while table.len() >= self.cfg.limits.max_idempotency_entries {
            let Some((oldest_expiry, oldest)) = expiry_index.iter().next().cloned() else {
                break;
            };
            expiry_index.remove(&(oldest_expiry, oldest.clone()));
            if let Some((_pool, evicted, _evicted_expiry)) = table.remove(&oldest) {
                reverse.remove(&evicted);
            }
        }
        table.insert(key.to_string(), (pool.clone(), job.clone(), expiry));
        expiry_index.insert((expiry, key.to_string()));
        reverse.insert(job.clone(), key.to_string());
    }

    /// The job an idempotency key already owns, when the hub still mirrors it.
    ///
    /// Expiry is lazy and O(log n): the timer index is ordered, so the loop pops rows that
    /// are already stale and stops at the first live one. `Effect::JobForgotten` normally
    /// removes the row long before its TTL, so reaching an expired entry here needs a
    /// record the actor kept for a full `idempotency_ttl_ms` with nobody following it.
    #[must_use]
    pub async fn idempotency_target(&self, key: &str) -> Option<(PoolId, JobId)> {
        let now = monotonic_ms();
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        while let Some((expiry, stale)) = expiry_index.iter().next().cloned() {
            if expiry > now {
                break;
            }
            expiry_index.remove(&(expiry, stale.clone()));
            if let Some((_pool, job, _)) = table.remove(&stale) {
                reverse.remove(&job);
            }
        }
        let (pool, job, expiry) = table.get(key).cloned()?;
        // The loop above only pops rows the timer index still lists, and this one can be
        // the boundary case (`expiry == now`) that a coarse clock leaves behind, so the
        // hit itself is checked too rather than answered a millisecond stale.
        if expiry <= now {
            table.remove(key);
            expiry_index.remove(&(expiry, key.to_string()));
            reverse.remove(&job);
            return None;
        }
        Some((pool, job))
    }

    /// Drop one mirror entry by key: a face that found the mirrored record already gone,
    /// so the key belongs to whatever it submits next.
    pub async fn forget_idempotency(&self, key: &str) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        if let Some((_pool, job, expiry)) = table.remove(key) {
            expiry_index.remove(&(expiry, key.to_string()));
            reverse.remove(&job);
        }
    }

    /// Drop the mirror entry for a job the actor has forgotten (`Effect::JobForgotten`).
    ///
    /// This is what keeps a deleted task's retry honest: without it a key would go on
    /// naming a record nobody holds, and NEX-6.3's fresh re-run would become a replay of a
    /// `404`. Idempotent, like `forget_job`.
    pub async fn forget_idempotency_for_job(&self, job: &JobId) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        let Some(key) = reverse.remove(job) else {
            return;
        };
        if let Some((_pool, _job, expiry)) = table.remove(&key) {
            expiry_index.remove(&(expiry, key));
        }
    }

    /// Authoritative job view, read through the owning actor's mailbox.
    ///
    /// This is a serialized read, not a lock on shared job state: the answer
    /// cannot be a torn mix of two transitions because it comes back as an effect
    /// from the same single-owner loop.
    pub async fn job_snapshot(&self, job: &JobId) -> Option<JobView> {
        let handle = self.route_job(job).await?;
        let (reply, waiting) = oneshot::channel();
        self.snapshot_waiters
            .lock()
            .await
            .entry(job.clone())
            .or_default()
            .push(reply);
        if handle
            .tx
            .send(ActorMsg::Query { job: job.clone() })
            .await
            .is_err()
        {
            self.forget_snapshot_waiter(job).await;
            return None;
        }
        // `timeout(..)` yields `Result<Result<Option<JobView>, RecvError>, Elapsed>`:
        // `.ok()` drops the timeout arm, `and_then(ok())` drops the channel-closed
        // arm, and the final `.flatten()` unwraps the `Option<JobView>` the waiter
        // sent. (`Result` is not an iterator, so `.flatten()` alone cannot step over
        // the inner `Result`.)
        let view =
            tokio::time::timeout(std::time::Duration::from_millis(PROBE_TIMEOUT_MS), waiting)
                .await
                .ok()
                .and_then(|answered| answered.ok())
                .flatten();
        if view.is_none() {
            self.forget_snapshot_waiter(job).await;
        }
        view
    }

    async fn forget_snapshot_waiter(&self, job: &JobId) {
        let mut waiters = self.snapshot_waiters.lock().await;
        if let Some(slot) = waiters.get_mut(job) {
            slot.pop();
            if slot.is_empty() {
                waiters.remove(job);
            }
        }
    }

    /// Whole-pool snapshot, read through one pool actor's mailbox.
    ///
    /// `None` means "this pool did not answer in time", never "this pool is empty":
    /// the caller lists the pool under `unresponsive_pools` and still answers `200`,
    /// because a monitor that gets nothing at all cannot tell a busy actor from a
    /// dead hub. Same serialized-read property as [`Self::job_snapshot`] — the
    /// answer comes from the single owner, so it is never a torn mix of two
    /// transitions.
    ///
    /// The budget is the same `PROBE_TIMEOUT_MS` every other read-through probe
    /// uses, and it bounds the **whole** probe: the mailbox send as well as the
    /// answer. A saturated actor whose 512-deep mailbox is full would otherwise
    /// park the monitor in `send().await` forever, which is exactly the hang the
    /// deadline exists to prevent.
    pub async fn pool_snapshot(&self, handle: &Arc<PoolHandle>) -> Option<PoolSnapshot> {
        let token = self.observe_tokens.fetch_add(1, Ordering::Relaxed);
        let (reply, waiting) = oneshot::channel();
        self.observe_waiters.lock().await.insert(token, reply);
        // One future for send + answer, so a single `timeout` covers both halves
        // (two separate timeouts would let a stalled mailbox consume the budget
        // twice and double the worst-case latency for the caller).
        let probe = async {
            handle
                .tx
                .send(ActorMsg::Observe { token })
                .await
                .map_err(|_| ())?;
            waiting.await.map_err(|_| ())
        };
        let answer =
            tokio::time::timeout(std::time::Duration::from_millis(PROBE_TIMEOUT_MS), probe)
                .await
                .ok()
                .and_then(|answered| answered.ok());
        // Prune on every non-answer, so a hub that polls a wedged actor forever
        // does not accumulate one oneshot per poll. `remove` is by token, so this
        // can never drop a waiter another monitor installed in the meantime.
        if answer.is_none() {
            self.observe_waiters.lock().await.remove(&token);
        }
        answer
    }

    /// Every pool's snapshot, queried **concurrently**, each with its own deadline.
    ///
    /// The result is in `pool_ids()` order (sorted, so the rendered document is
    /// byte-stable across polls) with the pool id alongside the answer, and
    /// `None` marks an unresponsive pool. Sequential probing would make the hub's
    /// worst case `pools × 2 s` — with a dozen pools that is a monitor timing out
    /// on a hub that is fine, so one stuck actor must cost one stuck entry and
    /// nothing else.
    pub async fn pool_snapshots(&self) -> Vec<(PoolId, Option<PoolSnapshot>)> {
        // `pool` borrows `self`, so the Arc has to be cloned out before the async
        // block captures it; the clone is also what keeps each future independent
        // of the map's lifetime.
        let probes: Vec<_> = self
            .pool_ids()
            .into_iter()
            .filter_map(|pid| self.pool(&pid).map(|handle| (pid, Arc::clone(handle))))
            .map(|(pid, handle)| async move {
                let answer = self.pool_snapshot(&handle).await;
                (pid, answer)
            })
            .collect();
        futures_util::future::join_all(probes).await
    }

    /// An actor-clock timestamp as wall-clock milliseconds.
    ///
    /// Every timestamp inside the actor — `WorkerRecord::last_seen_ms`,
    /// `JobTimings::accept_ms`, `Lease::deadline_ms` — is stamped from the clock
    /// `run_actor` injects, which is [`monotonic_ms`]: milliseconds since an
    /// `Instant` taken at process start, **not** epoch time. A monitoring document
    /// that mixed `now_ms` (epoch) with `last_seen_ms` (process-relative) would
    /// render "last seen 56 years ago", so the hub translates on the way out.
    ///
    /// The arithmetic is signed rather than two saturating `sub`s because
    /// `deadline_ms` lies in the future: a saturating subtraction would collapse
    /// every live lease deadline to "now", which is precisely the value a dashboard
    /// polls for. `0` instead of a negative epoch is the honest floor.
    ///
    /// This method samples both clocks *now*; `observe.rs` holds one sampled pair for
    /// a whole request (its `Clock`) so a slow fan-out cannot make the early rows of
    /// one document disagree with the late ones. Both go through
    /// [`actor_to_wall_ms`], the only implementation of the translation.
    #[must_use]
    pub fn to_wall_ms(&self, actor_ms: u64) -> u64 {
        actor_to_wall_ms(self.wall_ms(), monotonic_ms(), actor_ms)
    }

    // ------------------------------------------------ consumer connection registry

    /// Register an accepted consumer WebSocket and return its connection id.
    ///
    /// Called **after** the pre-upgrade API-key check has passed, so a socket that
    /// never became a session is never listed, and a refused upgrade cannot leave a
    /// ghost connection behind.
    pub fn accept_consumer_conn(&self, tenant: TenantId, peer: Option<String>) -> ConsumerId {
        let id = self.ids.consumer_conn_id();
        let record = ConsumerConnection {
            id: id.clone(),
            tenant,
            since_ms: self.wall_ms(),
            peer,
            attached_jobs: BTreeSet::new(),
        };
        poison_free(&self.consumer_conns).insert(id.clone(), record);
        id
    }

    /// Mark one job as watched by this connection. Idempotent (a `BTreeSet`),
    /// because a client that re-attaches to a finished task re-adds the same job
    /// (`runtime_e2e`'s re-attach case) and the registry must not double-count it.
    pub fn consumer_attach(&self, conn: &ConsumerId, job: &JobId) {
        if let Some(record) = poison_free(&self.consumer_conns).get_mut(conn) {
            record.attached_jobs.insert(job.clone());
        }
    }

    /// Drop one job from this connection's watch set — a `detach`, a cancellation,
    /// or the pump reaching end-of-stream on its own.
    ///
    /// Missing entries are ignored: the connection may already have closed, and
    /// `close_consumer_conn` removes the whole record, after which a pump finishing
    /// underneath the teardown is expected, not an error.
    pub fn consumer_detach(&self, conn: &ConsumerId, job: &JobId) {
        if let Some(record) = poison_free(&self.consumer_conns).get_mut(conn) {
            record.attached_jobs.remove(job);
        }
    }

    /// Remove a closed connection. Idempotent, and load-bearing for abnormal
    /// disconnects: the session's guard runs it on every exit path, so a socket that
    /// dies mid-stream cannot leave a connection listed as live forever.
    pub fn close_consumer_conn(&self, conn: &ConsumerId) {
        poison_free(&self.consumer_conns).remove(conn);
    }

    /// Live consumer connections, ordered by id so two polls of an idle hub render
    /// identical bytes.
    #[must_use]
    pub fn consumer_connections(&self) -> Vec<ConsumerConnection> {
        let mut out: Vec<ConsumerConnection> = poison_free(&self.consumer_conns)
            .values()
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Fetch a spilled payload on behalf of a worker, through the actor.
    pub async fn payload_fetch(
        &self,
        reference: &PayloadRefId,
        token: &str,
        worker: &WorkerId,
        attempt: u32,
    ) -> Option<PayloadGrant> {
        // A payload reference is the job id it was spilled for (see `PayloadBook`),
        // so routing the job routes the payload.
        let job = JobId::new_unchecked(reference.as_ref().to_string());
        let handle = self.route_job(&job).await?;
        let (reply, waiting) = oneshot::channel();
        self.payload_waiters
            .lock()
            .await
            .insert(reference.clone(), reply);
        let msg = ActorMsg::FetchPayload {
            reference: reference.clone(),
            token: token.to_string(),
            worker: worker.clone(),
            attempt,
        };
        if handle.tx.send(msg).await.is_err() {
            self.payload_waiters.lock().await.remove(reference);
            return None;
        }
        // See `job_snapshot` for the shape: `Result<Result<PayloadGrant, RecvError>,
        // Elapsed>`, so `.ok()` plus `and_then(ok())` is the whole answer.
        let grant =
            tokio::time::timeout(std::time::Duration::from_millis(PROBE_TIMEOUT_MS), waiting)
                .await
                .ok()
                .and_then(|answered| answered.ok());
        if grant.is_none() {
            self.payload_waiters.lock().await.remove(reference);
        }
        grant
    }

    /// Wall-clock milliseconds, for OpenAI's `created` field.
    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(self.epoch_ms, |d| d.as_millis() as u64)
    }

    /// Aggregate warm capacity, read from the mirrored gauges.
    pub async fn readiness(&self) -> HubReadiness {
        let mut out = HubReadiness::default();
        for handle in self.pools.values() {
            let gauges = handle.gauges.lock().await;
            out.ready_workers += gauges.ready_workers;
            out.loading_workers += gauges.loading_workers;
            out.queue_depth += gauges.queue_depth;
            out.mailbox_pending += handle.mailbox_pending() as u64;
            out.models.extend(gauges.warm_models.iter().cloned());
        }
        out
    }

    /// Prometheus text exposition, assembled from the actors' own registries.
    ///
    /// Two things the previous shape got wrong, both visible to a scraper:
    ///   * the pool label is inserted **inside** an existing label set, so a
    ///     counter recorded as `nexus_frames_total{kind="delta"}` becomes
    ///     `nexus_frames_total{pool="…",kind="delta"}`. Appending a second brace
    ///     group is not valid exposition format;
    ///   * gauges are merged as well as counters, because §13's mandatory
    ///     `queue depth` / `ready workers` / `leases` series are gauges and were
    ///     absent from `/metrics` before.
    pub async fn render_metrics(&self) -> String {
        // base name → (type, labeled samples). `BTreeMap` keeps the exposition
        // byte-stable, which the node smoke test and any `diff` of scrapes rely on.
        let mut families: BTreeMap<String, (String, BTreeMap<String, u64>)> = BTreeMap::new();
        for handle in self.pools.values() {
            let gauges = handle.gauges.lock().await;
            for (kind, entries) in [
                ("gauge", gauges.metrics.gauges()),
                ("counter", gauges.metrics.counters()),
            ] {
                for (key, value) in entries {
                    let base = nexus_core::metrics::base_name(key);
                    let labeled = with_pool_label(key, handle.id.as_ref());
                    let slot = families
                        .entry(base)
                        .or_insert_with(|| (kind.to_string(), BTreeMap::new()));
                    *slot.1.entry(labeled).or_insert(0) += *value;
                }
            }
            // The relay gauges are dispatcher state, not actor state, so they are not in the
            // mirrored registry (`PoolGauges::from_actor` replaces that whole map from the
            // actor). Read them off the handle's atomics — written by the sweep — at the same
            // instant, which is what makes the exposition current regardless of whether a tick
            // or a dispatch ran last.
            for (base, value) in [
                (
                    nexus_core::metrics::names::RELAY_ROWS,
                    handle.relay_rows.load(Ordering::Relaxed),
                ),
                (
                    nexus_core::metrics::names::RELAY_SUBSCRIBERS,
                    handle.relay_subscribers.load(Ordering::Relaxed),
                ),
            ] {
                let labeled = with_pool_label(base, handle.id.as_ref());
                let slot = families
                    .entry(base.to_string())
                    .or_insert_with(|| ("gauge".to_string(), BTreeMap::new()));
                *slot.1.entry(labeled).or_insert(0) += value;
            }
        }
        let mut out = String::new();
        for (base, (kind, samples)) in families {
            out.push_str(&format!("# TYPE {base} {kind}\n"));
            for (key, value) in samples {
                out.push_str(&format!("{key} {value}\n"));
            }
        }
        out
    }
}

/// Insert the `pool` label into a metric key, creating a label set only when the
/// key does not already have one.
#[must_use]
fn with_pool_label(key: &str, pool: &str) -> String {
    match key.split_once('{') {
        Some((base, rest)) => format!("{base}{{pool=\"{pool}\",{rest}"),
        None => format!("{key}{{pool=\"{pool}\"}}"),
    }
}

/// Warm-capacity summary across pools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HubReadiness {
    /// Total ready workers.
    pub ready_workers: u64,
    /// Total connected-but-loading workers.
    pub loading_workers: u64,
    /// Total queued jobs.
    pub queue_depth: u64,
    /// Total mailbox depth.
    pub mailbox_pending: u64,
    /// Warm model names.
    pub models: BTreeSet<String>,
}

impl PoolHandle {
    /// Get or create a job's relay.
    ///
    /// The **faces** call this: each one registers the fan-out, the verdict gate and
    /// its own private replay queue before the actor ever hears about the job
    /// (`consumer::submit`, `consumer_ws::handle_attach`), so the row has to exist by
    /// the time the first effect lands. The dispatcher never calls it — see
    /// [`lookup_relay`], which is the arm-by-arm reason get-or-create there was an
    /// unbounded insert.
    pub async fn relay(&self, job: &JobId, capacity: usize) -> Arc<JobRelay> {
        let mut relays = self.relays.lock().await;
        if let Some(existing) = relays.get(job) {
            return Arc::clone(existing);
        }
        let created = Arc::new(JobRelay::new(capacity, monotonic_ms()));
        relays.insert(job.clone(), Arc::clone(&created));
        created
    }

    /// The relay for `job` if one exists, without creating it.
    pub async fn lookup_relay(&self, job: &JobId) -> Option<Arc<JobRelay>> {
        self.relays.lock().await.get(job).cloned()
    }

    /// Drop a job's relay once its consumers are gone.
    pub async fn forget_relay(&self, job: &JobId) {
        self.relays.lock().await.remove(job);
    }

    /// The live connection generation for a worker, if it has one.
    pub async fn live_conn(&self, worker: &WorkerId) -> Option<u64> {
        self.workers.lock().await.get(worker).map(|conn| conn.conn)
    }

    /// Jobs currently being followed (shutdown drain, metrics).
    pub async fn followed_jobs(&self) -> Vec<JobId> {
        self.relays.lock().await.keys().cloned().collect()
    }

    /// Pending messages in the mailbox, the backpressure signal.
    #[must_use]
    pub fn mailbox_pending(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

/// Send one message to a pool actor. An error means the actor exited, which the
/// caller turns into a retryable failure rather than a hang.
pub async fn send(handle: &PoolHandle, msg: ActorMsg) -> anyhow::Result<()> {
    handle
        .tx
        .send(msg)
        .await
        .map_err(|_| anyhow::anyhow!("pool actor mailbox is closed"))
}

/// Push an already-encoded frame to a worker's live socket.
///
/// `caps` must be the hub's **configured** outbound caps
/// (`Limits::hub_caps()`), not `WireCaps::default()`: an `Assigned` carrying an
/// inline payload near `inline_payload_cap_bytes` is far larger than the 1 MiB
/// default line, and encoding it against the default would drop the dispatch
/// silently — the worker is never told and the lease only lapses at expiry.
pub async fn send_frame(
    handle: &Arc<PoolHandle>,
    worker: &WorkerId,
    frame: &HubFrame,
    caps: &WireCaps,
) {
    let line = match encode_line(frame, caps) {
        Ok(line) => line,
        Err(err) => {
            tracing::error!(%worker, %err, "outbound frame violated the wire cap");
            return;
        }
    };
    let mut workers = handle.workers.lock().await;
    match workers.get_mut(worker) {
        Some(conn) => {
            // An embedder that reuses a worker id (the native hub never does — it
            // mints a fresh UUIDv4 per connection) gets the actor's eviction
            // effect for the *previous* generation. The registry is what actually
            // closes that old socket, and by the time this effect runs the live
            // binding is the **new** connection. Forwarding it here would tell a
            // freshly welcomed worker it had just been evicted, and a client that
            // honours `Bye` would drop the good connection and reconnect forever.
            // The WASM host sees the frame; it owns the sockets and can tell its
            // own generations apart.
            if matches!(
                frame,
                HubFrame::Bye {
                    reason: ByeReason::Evicted
                }
            ) {
                tracing::debug!(%worker, "eviction `Bye` handled by the connection registry, not the live socket");
                return;
            }
            // A `Welcome` re-binds the worker to its new generation. The
            // registry already stores the new connection id before `Hello` is
            // sent, so this is a no-op in the normal path; it is kept because a
            // host that registers lazily still ends up bound to the epoch the
            // actor believes in.
            if let HubFrame::Welcome { epoch, .. } = frame {
                conn.conn = *epoch;
            }
            // Bounded, never indefinite. The actor applies its effects inline (see
            // `run_actor`), so parking on a socket that has stopped draining would
            // head-of-line block every other job in this pool. The queue is
            // `worker_ws::OUTBOUND_QUEUE` frames deep; a frame that cannot enter it
            // within the grace period means the connection is dead in every way that
            // matters, and the **actor** decides what that does to the jobs — the
            // same message the socket handler sends when the peer hangs up
            // (`ActorMsg::WorkerGone`, `actor.rs:1772`), so the lease/resume-grace
            // rules run and an in-flight `Assigned`/`Cancel` is resolved rather than
            // silently swallowed.
            const SEND_GRACE_MS: u64 = 25;
            match tokio::time::timeout(
                std::time::Duration::from_millis(SEND_GRACE_MS),
                conn.tx.send(WorkerWrite::Frame(line)),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    tracing::warn!(%worker, "worker socket closed during send");
                    report_dead_connection(handle, worker, conn.conn);
                }
                Err(_elapsed) => {
                    tracing::warn!(%worker, grace_ms = SEND_GRACE_MS, "worker outbound queue is stalled; reporting the connection as gone");
                    report_dead_connection(handle, worker, conn.conn);
                }
            }
        }
        None => tracing::debug!(%worker, "frame dropped: no live connection"),
    }
}

/// Encode and send one frame to a worker.
fn encode_line(frame: &HubFrame, caps: &WireCaps) -> Result<String, nexus_core::CoreError> {
    nexus_core::protocol::encode_line(frame, caps)
}

/// Report a worker connection that can no longer accept frames back to the actor.
///
/// The actor is the only authority on what a dead connection means (`on_worker_gone`
/// → resume grace / lease expiry), and it filters reports about a superseded
/// generation itself via the `conn` id it believes is live (`actor.rs:1772`). This
/// layer therefore never decides on its own whether a job is requeued.
///
/// `try_send`, deliberately: this runs *inside* the actor's own effect loop, so
/// awaiting its mailbox would re-enter the loop that is calling it. If the mailbox
/// is saturated the report is dropped with a log, and the actor's lease timer still
/// resolves the job — a late answer, never a lost one.
fn report_dead_connection(handle: &Arc<PoolHandle>, worker: &WorkerId, conn: u64) {
    if let Err(err) = handle.tx.try_send(ActorMsg::WorkerGone {
        worker: worker.clone(),
        conn,
    }) {
        tracing::warn!(%worker, %err, "pool mailbox would block: the WorkerGone report is deferred to the lease timer");
    }
}

/// The actor task: take a message, mutate state, perform the effects. Nothing else
/// holds a mutable reference to the actor, which is what makes the claim guarantee
/// structural rather than "a mutex we hope is held correctly" (task §5).
async fn run_actor(
    pool: PoolId,
    mut actor: PoolActor,
    mut rx: mpsc::Receiver<ActorMsg>,
    handle: Arc<PoolHandle>,
    hub: Arc<Hub>,
) {
    // The actor's heartbeat period also sets how often the virtual clock advances, and
    // both ends are intentional: never faster than a scheduler tick, never so slow that a
    // dead worker looks alive for a whole lease period.
    let tick_ms = actor.config().heartbeat_ms_for(&pool).clamp(50, 1_000);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracing::info!(pool = %pool, tick_ms, "pool actor started");
    loop {
        tokio::select! {
            biased;
            maybe_msg = rx.recv() => match maybe_msg {
                Some(msg) => {
                    let now = monotonic_ms();
                    let effects = actor.handle(now, msg);
                    dispatch(&hub, &handle, &actor, effects).await;
                }
                None => break,
            },
            _ = ticker.tick() => {
                let now = monotonic_ms();
                let effects = actor.handle(now, ActorMsg::Tick { now_ms: now });
                dispatch(&hub, &handle, &actor, effects).await;
                // The dispatcher's prune of `handle.relays` is effect-driven, so it can
                // only ever name a job the actor still has a record for. The sweep below
                // is the unconditional half of the same bound, and it runs here because a
                // tick is the only moment this task is guaranteed to get.
                prune_orphaned_relays(&handle, &actor, now).await;
            }
        }
    }
    // The actor is gone: end every open stream with the retryable signal, so a
    // client sees a clean failure instead of a hang (task §11, §16.18).
    let relays: Vec<Arc<JobRelay>> = handle.relays.lock().await.values().cloned().collect();
    for relay in relays {
        relay.end_stream().await;
        relay.resolve(Gate::Closed).await;
    }
    tracing::warn!(pool = %pool, "pool actor exited");
}

/// How old an idle relay row has to be before [`prune_orphaned_relays`] may take it.
///
/// Age is the only thing that tells an in-flight registration apart from an orphan:
/// every face creates its relay a moment *before* the actor knows the job
/// (`consumer::submit` documents the ordering, and the attach faces do the same), so
/// "the actor has no record" is briefly true of a perfectly healthy new submission.
/// Ten seconds is three orders of magnitude more than that window needs and still
/// tiny next to the 300 s a terminal record is kept.
const ORPHAN_RELAY_GRACE_MS: u64 = 10_000;

/// Takes every relay row that has outlived the job it fans out.
///
/// The effect-driven pruning in `dispatch` removes the row when the actor reports
/// `Effect::JobForgotten`, and since the dispatcher's own arms now only ever *look
/// up* a relay, nothing inside the dispatcher can put one back. What remains is the
/// one race the dispatcher cannot see: a **face** that registers its relay after that
/// prune has already run — an `/events` handler or a WS `attach` that read
/// `job_index` a moment before the record was deleted (`consumer.rs:812-814`,
/// `consumer_ws.rs:621-623`). Its row is keyed by an id that is never reused and that
/// no later effect will ever name, so on the old shape it survived for the life of
/// the process: one permanent row per occurrence, which is exactly the shape the RSS
/// curve reports.
///
/// The sweep is safe rather than merely eager: it takes a row only when the actor has
/// no record for the id, **and** [`JobRelay::is_idle`] says nobody can still be
/// waiting on it, **and** [`ORPHAN_RELAY_GRACE_MS`] has passed since the row was
/// created. A row that is genuinely in flight fails at least one of the three, and a
/// row the sweep takes holds nothing: no consumer queue, no unresolved gate, no
/// replay queue waiting to be written.
///
/// One cadence caveat, because the grace window is a promise about the *floor*: this runs
/// only on the **tick** branch of `run_actor`'s biased select, and that select prefers the
/// mailbox. A pool saturated with messages can therefore go well over a second between
/// ticks, so `ORPHAN_RELAY_GRACE_MS` is the earliest a row can go, not the moment it
/// always goes. Nothing unbounded follows — the tick fires as soon as the mailbox drains,
/// and every row the sweep is late for is already holding nothing.
///
/// The pass also publishes `nexus_relay_rows` and `nexus_relay_subscribers`, from the same
/// locked map it just trimmed: rows and queues sampled at one instant, so a monitor
/// comparing the two is comparing one state and not two scrapes. `Relaxed` is enough — the
/// pair is a reading of a gauge, not a lock, and no other thread synchronises against it.
async fn prune_orphaned_relays(handle: &Arc<PoolHandle>, actor: &PoolActor, now_ms: u64) {
    let mut relays = handle.relays.lock().await;
    // Collected first: `HashMap` cannot be filtered in place while the actor is read
    // per candidate, and the actor call is a map lookup rather than a lock anyway.
    let stranded: Vec<JobId> = relays
        .iter()
        .filter(|(job, relay)| {
            relay.created_ms.saturating_add(ORPHAN_RELAY_GRACE_MS) <= now_ms
                && actor.job(job).is_none()
                && relay.is_idle()
        })
        .map(|(job, _)| job.clone())
        .collect();
    for job in &stranded {
        relays.remove(job);
    }
    // After the removals on purpose: the gauge is what the hub is holding when anyone looks,
    // not what it held before the sweep did its job.
    handle
        .relay_rows
        .store(relays.len() as u64, Ordering::Relaxed);
    handle.relay_subscribers.store(
        relays
            .values()
            .map(|relay| u64::try_from(relay.subscriber_count()).unwrap_or(u64::MAX))
            .sum(),
        Ordering::Relaxed,
    );
    drop(relays);
    if !stranded.is_empty() {
        tracing::debug!(pool = %handle.id, stranded = stranded.len(), "relay rows outlived their job; swept");
    }
}

/// Monotonic millisecond clock for the actor, immune to wall-clock jumps.
///
/// `pub(crate)` because `observe.rs` needs the same reading the actors are fed: the
/// only way to translate an actor timestamp is to know what the actor's clock says
/// **right now**, and a second `Instant` origin would translate against a different
/// zero.
pub(crate) fn monotonic_ms() -> u64 {
    use std::sync::OnceLock;
    static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
    ORIGIN
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

/// Translate one actor-clock timestamp into epoch milliseconds.
///
/// `epoch_origin_ms` is the wall clock and `actor_now_ms` is [`monotonic_ms`], both
/// sampled at the **same instant**; `actor_ms` is a timestamp the actor stamped on
/// its own clock. The offset is signed and can be negative, because a lease
/// `deadline_ms` is in the future: saturating the difference would collapse every
/// live deadline onto `epoch_origin_ms`, i.e. "expires right now", which is the one
/// number a dashboard polls for. Clamping at `0` (rather than wrapping) keeps the
/// output a valid epoch ms for a hub that has been up for less time than the
/// timestamps it is replaying.
#[must_use]
pub(crate) fn actor_to_wall_ms(epoch_origin_ms: u64, actor_now_ms: u64, actor_ms: u64) -> u64 {
    let delta = actor_ms as i128 - actor_now_ms as i128;
    (i128::from(epoch_origin_ms) + delta).max(0) as u64
}

/// Perform the actor's effects, in order: worker writes first, so a consumer never
/// receives a terminal event before its owning worker was told.
async fn dispatch(
    hub: &Arc<Hub>,
    handle: &Arc<PoolHandle>,
    actor: &PoolActor,
    effects: Vec<Effect>,
) {
    for effect in effects {
        match effect {
            // Outbound caps come from the hub's own configuration so an
            // `Assigned` with a large inline payload is not dropped for
            // exceeding a default that is smaller than the inline cap.
            Effect::SendWorker { worker, ref frame } => {
                send_frame(handle, &worker, frame, &hub.cfg.limits.hub_caps()).await;
                // §7.2's second half. A job submitted while the pool had no warm
                // worker was `Accepted` but deliberately left unanswered (see the
                // `Accepted` arm below: the grace window can still end in a `503`
                // with no body bytes). Handing the job to a worker is the moment
                // the request became serviceable, so the body may open now.
                // `frame` is `&Box<HubFrame>` — the effect boxes the frame so the
                // effect enum stays small (`clippy::large_enum_variant`), and serde
                // unwraps the box, so the wire shape is unchanged.
                if let HubFrame::Assigned { job, .. } = frame.as_ref() {
                    // **Lookup, never get-or-create**: this is the same NEX-7.2 rule the
                    // `Reject` arm below states in full, and an `Assigned` is the other
                    // half of it — the only thing this arm does with the relay is resolve
                    // a gate, and a gate exists only on a row a submit face installed. An
                    // invented row here was a permanent entry that discarded the verdict.
                    if let Some(relay) = handle.lookup_relay(job).await {
                        // The authoritative later answer (see the `Accepted` arm): a job
                        // that was admitted into the queue with no lease gets its supplier
                        // here, so a face that is still holding its response head can fill
                        // the header from the relay (`JobRelay::supplier`) rather than from
                        // this verdict. `worker` is the destination of the `Assigned`
                        // itself, so it is by construction the leased worker.
                        relay.note_supplier(&worker);
                        relay
                            .resolve(Gate::Accepted {
                                worker: Some(worker),
                            })
                            .await;
                    }
                }
            }
            Effect::Accepted {
                job,
                attached,
                replayed,
                worker,
            } => {
                // Lookup-only, like the `Assigned` above and the `Reject` below: only a
                // face that installed a gate can be waiting on this verdict, and a row
                // invented here after an `Effect::JobForgotten` was one nothing removed.
                if let Some(relay) = handle.lookup_relay(&job).await {
                    // Mirror the id for the faces *before* the verdict is handed over, and
                    // independently of whether this effect resolves the gate: a job can be
                    // `Accepted` and stay unanswered (the grace window NEX-3.3's
                    // verdict-before-bytes rule creates), and a face that later follows it
                    // reads the value off the relay, not off the effect.
                    if let Some(worker) = worker.as_ref() {
                        relay.note_supplier(worker);
                    }
                    // `attached` joins an execution that is already running, so its
                    // stream is open by definition. A fresh submission whose record is
                    // still `Pending` is the §7.2 grace case: nothing may be written
                    // yet, and the gate stays unresolved until either an `Assigned`
                    // lands (arm above) or the grace expiry answers `503`.
                    if attached
                        || actor
                            .job(&job)
                            .is_some_and(|record| record.phase != JobPhase::Pending)
                    {
                        relay.resolve(Gate::Accepted { worker }).await;
                    }
                    // The private replay queue carries only this consumer's buffered
                    // prefix. A fresh submission has none, so the sender must be
                    // dropped: `pump_sse` otherwise waits in its replay loop forever
                    // and never watches the fan-out, which would delay every delta
                    // until the job terminates and swallow §6's consumer-disconnect
                    // `Detach` (the worker would keep generating for nobody).
                    let target = relay.replay_target.lock().await.take();
                    if let (Some(tx), true) = (target, attached) {
                        for event in replayed {
                            let _ = tx.send(RelayItem::Event(event)).await;
                        }
                    }
                }
            }
            Effect::Reject {
                job,
                status,
                code,
                message,
                retry_after_ms,
            } => {
                let gate = Gate::Reject {
                    status: StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                    code: code.clone(),
                    message: message.clone(),
                    retry_after_secs: retry_after_ms.map(|ms| ms.div_ceil(1_000)),
                };
                tracing::warn!(pool = %handle.id, ?job, status, %code, %message, "request rejected");
                if let Some(job) = job {
                    // **Lookup, never get-or-create** (NEX-7.2: every retained byte must have a
                    // stated reason): this was the last unbounded row *the dispatcher itself
                    // created*. A verdict can only be answered by a relay that already holds a
                    // **gate**, and only the submit faces install one — `consumer::submit`, behind
                    // `POST /v1/jobs` and the OpenAI surface (`consumer.rs:1162-1166`) and reached
                    // from the WS submit too (`consumer_ws.rs:501`). The attach faces create the
                    // row and its `replay_target` with **no gate** (`consumer.rs:812-814` `:818`,
                    // `consumer_ws.rs:621-623` `:629`).
                    //
                    // A row created here could only ever be one nobody waits on: `JobRelay::new`
                    // leaves `gate: None` (`state.rs:151-158`) and `JobRelay::resolve`
                    // (`state.rs:161-165`) is a no-op on `None`, so an invented row discarded the
                    // verdict anyway — it only cost a permanent entry. That is the stranded case:
                    // the actor answers a *worker* frame about a job it no longer has a record for,
                    // i.e. a late `ArtifactStored` whose lease is gone (`actor.rs:2759`, `409
                    // stale_attempt`), a stale worker connection (`actor.rs:2746`, `401`), a chunk
                    // over the artifact cap (`actor.rs:2775`, `413`), a `Cancel` (`actor.rs:1547`)
                    // or an `Attach` (`actor.rs:1326`) naming an expired record — all
                    // `reject(Some(job), …)`. `Effect::JobForgotten` has already pruned the row,
                    // ids are monotonic and never reused (`state.rs:454-457`), and no later effect
                    // names that id again, so each occurrence kept one row for the life of the
                    // process (`run_actor` has no housekeeping sweep to reconcile them).
                    //
                    // Cloned so the pool-wide map lock is not held across the awaits below: the
                    // guard dies with this statement, whereas the `Keepalive` arm below keeps its
                    // guard across `push_comment(..).await`.
                    if let Some(relay) = handle.lookup_relay(&job).await {
                        // Nothing will replay into this submission's private queue,
                        // so close it: a waiting pump must fall through to its
                        // verdict rather than hold the slot open.
                        drop(relay.replay_target.lock().await.take());
                        relay.resolve(gate).await;
                    } else {
                        // Nothing to answer: no face holds this job's fan-out, so there is
                        // no request left to refuse. Logged rather than acted on, mirroring
                        // the `ReplayTo` arm's "no waiting consumer" case.
                        tracing::debug!(
                            pool = %handle.id,
                            %job,
                            status,
                            %code,
                            "rejection names a job with no relay: nothing is waiting on its verdict"
                        );
                    }
                }
            }
            Effect::Relay { job, events } => {
                // Lookup-only: an event nobody follows needs no fan-out. Creating one
                // here was the highest-frequency resurrection of a row the
                // `Effect::JobForgotten` arm had just pruned — a late worker frame for a
                // forgotten job relays through this arm, and ids are never reused, so each
                // occurrence kept one empty row for the life of the process.
                if let Some(relay) = handle.lookup_relay(&job).await {
                    for event in events {
                        relay.push(event).await;
                    }
                } else {
                    tracing::debug!(pool = %handle.id, %job, "relayed events had no fan-out: no consumer follows this job");
                }
            }
            Effect::ReplayTo {
                job,
                consumer,
                events,
                lossy,
            } => {
                match handle.lookup_relay(&job).await {
                    Some(relay) => match relay.replay_target.lock().await.take() {
                        Some(tx) => {
                            if lossy {
                                // The ring already evicted part of what was asked for:
                                // say so, rather than pretending the text is complete.
                                let _ = tx
                                    .send(RelayItem::Event(ConsumerEvent::Gap {
                                        expected: 0,
                                        got: 0,
                                    }))
                                    .await;
                            }
                            for event in events {
                                let _ = tx.send(RelayItem::Event(event)).await;
                            }
                        }
                        None => {
                            tracing::debug!(pool = %handle.id, %job, %consumer, "attach replay had no waiting consumer")
                        }
                    },
                    // Same honest answer as the arm's own "no waiting consumer" case: the
                    // face that asked to attach has no replay queue installed here (it
                    // raced this job being forgotten, most likely).
                    None => {
                        tracing::debug!(pool = %handle.id, %job, %consumer, "attach replay had no relay")
                    }
                }
            }
            Effect::Keepalive { job } => {
                if let Some(relay) = handle.lookup_relay(&job).await {
                    relay.push_comment("ping").await;
                }
            }
            Effect::ReleasePayload { job, reference } => {
                tracing::debug!(pool = %handle.id, %job, ?reference, "prompt released from memory");
            }
            Effect::DeleteArtifact { artifact, path } => {
                tracing::info!(pool = %handle.id, %artifact, %path, "artifact TTL expired");
                crate::io::delete_artifact(&path).await;
            }
            Effect::Terminal {
                job,
                reason,
                retryable,
            } => {
                tracing::info!(pool = %handle.id, %job, reason = reason.as_str(), retryable, "job terminal");
                // A terminal job's fan-out has carried its last event, so the map
                // entry goes: without this, `relays` grows for the hub's lifetime
                // while the actor has already bounded its own job map by retention.
                // Live pumps hold their own `Arc<JobRelay>`, and a later `/events`
                // attach creates a fresh one, because the replay ring itself lives
                // in the actor and is unaffected.
                // Whichever path made the job terminal, a pump still attached to
                // it ends here: one `End` on every subscriber, then the senders go.
                // A body that rides keepalives after its job is terminal is the
                // hang this arm used to leave behind.
                //
                // Lookup-only, like every other arm: with the old get-or-create this arm
                // created a row for a job that had no fan-out at all (its `JobForgotten`
                // had already run, or no face ever followed it) and removed it again two
                // statements later, which was harmless but stated the wrong thing. A
                // pump that is still attached holds its own `Arc<JobRelay>` — the one this
                // lookup finds — so `end_stream` still reaches it.
                if let Some(relay) = handle.lookup_relay(&job).await {
                    relay.end_stream().await;
                    handle.forget_relay(&job).await;
                }
            }
            Effect::JobForgotten { job } => {
                // The actor has dropped the record, the replay ring and the idempotency
                // key, so the job id is unknown from here on: a later submission with the
                // old key starts a fresh execution (NEX-6.3). Every hub-side structure
                // keyed by job id has to agree, or the two read-mostly maps outlive the
                // thing they index — `job_index` was otherwise one entry per submission
                // for the lifetime of the process (NEX-7.2).
                //
                // Removal is keyed by this job alone and idempotent, so it cannot disturb
                // a live relay belonging to another job. A pump that is still attached
                // holds its own `Arc<JobRelay>` and ends on the verdict it already has;
                // a later `/events` attach answers `404 unknown_job`, which is the truth.
                let routed = hub.forget_job(&job).await;
                handle.forget_relay(&job).await;
                // The key goes with the record: an entry left behind would answer a
                // later retry with a job nobody has, where NEX-6.3 says it runs fresh.
                hub.forget_idempotency_for_job(&job).await;
                tracing::debug!(pool = %handle.id, %job, routed, "job forgotten by the actor; index, relay and idempotency mirror pruned");
            }
            Effect::Snapshot { job, view } => {
                let mut waiters = hub.snapshot_waiters.lock().await;
                if let Some(slot) = waiters.get_mut(&job) {
                    if let Some(waiter) = slot.pop() {
                        // `view` is `Box<Option<JobView>>` (see `Effect::Snapshot`): the
                        // box is an allocation detail, so the waiter receives exactly the
                        // derived view it received before, cloned out from behind it.
                        let _ = waiter.send(view.as_ref().clone());
                    }
                    if slot.is_empty() {
                        waiters.remove(&job);
                    }
                }
            }
            Effect::Observed { token, snapshot } => {
                // The waiting half lives in the hub (`Hub::pool_snapshot`), which is
                // the only place that knows who asked. A missing waiter is normal:
                // the monitor gave up at its own deadline, and the actor's answer
                // for a request nobody still wants is dropped here rather than
                // delivered to whoever happens to poll next.
                if let Some(waiter) = hub.observe_waiters.lock().await.remove(&token) {
                    let _ = waiter.send(*snapshot);
                } else {
                    tracing::debug!(pool = %handle.id, token, "observe answer had no waiter");
                }
            }
            Effect::PayloadBytes {
                reference,
                granted,
                bytes,
                content_type,
                sha256,
                reason,
            } => {
                if let Some(waiter) = hub.payload_waiters.lock().await.remove(&reference) {
                    let _ = waiter.send(PayloadGrant {
                        bytes,
                        content_type,
                        sha256,
                        reason,
                    });
                } else {
                    tracing::debug!(%reference, granted, "payload answer had no waiter");
                }
            }
            Effect::Log { level, message } => match level {
                nexus_core::LogLevel::Info => tracing::info!(pool = %handle.id, %message),
                nexus_core::LogLevel::Warn => tracing::warn!(pool = %handle.id, %message),
                nexus_core::LogLevel::Error => tracing::error!(pool = %handle.id, %message),
            },
        }
    }
    *handle.gauges.lock().await = PoolGauges::from_actor(actor);
}

/// Cancel every leased job on shutdown, so GPUs stop generating (task §6, §16.18).
pub async fn drain_pools(hub: &Arc<Hub>) {
    for handle in hub.pools.values() {
        let _ = handle
            .tx
            .send(ActorMsg::Shutdown {
                now_ms: monotonic_ms(),
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    // The relay gauges are atomics on the handle, and the sweep's tests read them the way
    // `/metrics` does rather than through a test-only accessor.
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::dispatch;
    use super::send_frame;
    use super::with_pool_label;
    use super::Hub;
    use super::PoolGauges;
    use super::PoolHandle;
    use super::WorkerConn;
    use super::WorkerWrite;
    use nexus_core::ids::JobId;
    use nexus_core::ids::PoolId;
    use nexus_core::ids::WorkerId;
    use nexus_core::protocol::CancelReason;
    use nexus_core::protocol::HubFrame;
    use nexus_core::protocol::WireCaps;
    use nexus_core::ActorMsg;
    use nexus_core::BrokerConfig;
    use nexus_core::Effect;
    use nexus_core::PoolActor;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;

    #[test]
    fn pool_label_goes_inside_the_existing_label_set() {
        // Exact expected output: a second brace group is not valid Prometheus.
        assert_eq!(
            with_pool_label(r#"nexus_frames_total{kind="delta"}"#, "llm.chat/qwen38"),
            r#"nexus_frames_total{pool="llm.chat/qwen38",kind="delta"}"#
        );
        assert_eq!(
            with_pool_label("nexus_queue_depth", "p"),
            r#"nexus_queue_depth{pool="p"}"#
        );
    }

    /// The actor applies its effects inline, so a worker whose outbound queue is
    /// full and unread (a wedged socket) must cost the pool a bounded grace, not an
    /// indefinite park — and the frame must be **reported**, not swallowed. The
    /// actor stays the only authority: it gets `WorkerGone` with the generation it
    /// believes in and decides whether that means resume grace or lease expiry.
    #[tokio::test]
    async fn a_stalled_worker_queue_is_reported_instead_of_parking_the_actor_loop() {
        // A handle with no actor task behind it: the mailbox is drained by this test,
        // so the assertion cannot race a running actor.
        let (actor_tx, mut actor_rx) = mpsc::channel::<ActorMsg>(16);
        let handle = Arc::new(PoolHandle {
            id: PoolId::new_unchecked("llm.chat/qwen38"),
            tx: actor_tx,
            workers: Mutex::new(HashMap::new()),
            relays: Mutex::new(HashMap::new()),
            gauges: Mutex::new(PoolGauges::default()),
            relay_rows: AtomicU64::new(0),
            relay_subscribers: AtomicU64::new(0),
        });

        let (worker_tx, worker_rx) = mpsc::channel::<WorkerWrite>(1);
        worker_tx
            .send(WorkerWrite::Frame("queued".to_string()))
            .await
            .expect("the first frame fits");
        handle.workers.lock().await.insert(
            WorkerId::new_unchecked("w-stalled"),
            WorkerConn {
                conn: 7,
                tx: worker_tx,
            },
        );

        let started = std::time::Instant::now();
        send_frame(
            &handle,
            &WorkerId::new_unchecked("w-stalled"),
            &HubFrame::Cancel {
                job: JobId::new_unchecked("job-1"),
                reason: CancelReason::ConsumerGone,
            },
            &WireCaps::default(),
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            elapsed >= std::time::Duration::from_millis(20),
            "the queue gets its grace period first: {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "the send is bounded, never indefinite: {elapsed:?}"
        );

        let report = actor_rx
            .recv()
            .await
            .expect("the failed send must reach the actor");
        // Matched on a reference: binding `worker`/`conn` out of the owned value
        // would partially move `report`, and the message below still formats it.
        assert!(
            matches!(&report, ActorMsg::WorkerGone { worker, conn } if worker.as_ref() == "w-stalled" && *conn == 7),
            "the report carries the connection the actor believes is live: {report:?}"
        );
        assert!(
            actor_rx.try_recv().is_err(),
            "exactly one report per failed send"
        );
        drop(worker_rx);
    }

    /// NEX-7.2 / NEX-6.3: the two hub-side maps keyed by job id must shrink when the
    /// actor stops knowing the job. Without the `Effect::JobForgotten` arm, `job_index`
    /// grows by one entry per submission for the lifetime of the process — the actor
    /// bounds its own job map, but the index and the relay fan-out do not.
    #[tokio::test]
    async fn job_index_and_relays_are_pruned_when_the_actor_forgets_a_job() {
        // A real `Hub`, so the dispatcher prunes the same maps the HTTP surface reads.
        // The actor tasks it spawns stay parked on empty mailboxes: this test drives
        // `dispatch` directly, which is the half that owns the pruning.
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        let kept = JobId::new_unchecked("job-kept");
        let gone = JobId::new_unchecked("job-gone");
        hub.bind_job(&gone, &pid).await;
        hub.bind_job(&kept, &pid).await;
        // Two live fan-outs, because pruning has to be per-job and never "clear the map".
        handle.relay(&gone, 16).await;
        handle.relay(&kept, 16).await;
        // And two idempotency rows, for the same reason: `JobForgotten` has to take the
        // deleted task's key with it while its neighbour's stays (NEX-6.3's retry runs
        // fresh, §5.2's duplicate still replays).
        hub.remember_idempotency("gone-key", &pid, &gone).await;
        hub.remember_idempotency("kept-key", &pid, &kept).await;
        assert_eq!(hub.job_index.lock().await.len(), 2);
        assert_eq!(handle.relays.lock().await.len(), 2);
        assert_eq!(hub.idempotency.lock().await.len(), 2);

        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: gone.clone() }],
        )
        .await;

        // The routing entry is gone, so a request for that job is a `404`, not a route to
        // a pool whose actor has already forgotten it.
        assert!(!hub.forget_job(&gone).await, "the entry was already pruned");
        assert!(hub.route_job(&gone).await.is_none());
        // The key goes with it: a retained row would answer the next retry with a task that
        // no longer exists, where NEX-6.3 says it runs fresh.
        assert!(hub.idempotency_target("gone-key").await.is_none());
        assert!(!hub.key_of_job.lock().await.contains_key(&gone));
        // The neighbour is untouched: a live relay for another job must survive.
        assert!(handle.relays.lock().await.contains_key(&kept));
        assert!(hub.job_index.lock().await.contains_key(&kept));
        assert_eq!(
            hub.idempotency_target("kept-key").await.map(|(_, job)| job),
            Some(kept.clone()),
            "a pruned job must not take another task's key with it"
        );

        // Idempotent, because two drop sites can report the same job: the NEX-6.3 orphan
        // path deletes the record and the retention sweep may have armed a timer first.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: gone }],
        )
        .await;
        assert_eq!(handle.relays.lock().await.len(), 1);
        assert_eq!(hub.job_index.lock().await.len(), 1);
        assert_eq!(hub.idempotency.lock().await.len(), 1);
    }

    /// The job ids this pool holds a relay for right now, comma-joined in sorted order, so
    /// every expectation below is one exact string and cannot depend on `HashMap` order.
    async fn relay_ids(handle: &Arc<PoolHandle>) -> String {
        let mut ids: Vec<String> = handle
            .relays
            .lock()
            .await
            .keys()
            .map(|job| job.as_ref().to_string())
            .collect();
        ids.sort();
        ids.join(",")
    }

    /// One `HubFrame::Assigned` for `job`, shaped like the actor's dispatch frame.
    fn assigned_to(job: &JobId) -> HubFrame {
        HubFrame::Assigned {
            job: job.clone(),
            attempt: 1,
            kind: "llm.completion".to_string(),
            tenant: nexus_core::ids::TenantId::new_unchecked("default"),
            requires: nexus_core::protocol::Requires::default(),
            payload: nexus_core::protocol::JobPayload::Inline {
                content_type: "application/json".to_string(),
                body: serde_json::json!({"prompt": "supplier"}),
            },
            lease_expires_ms: 0,
            job_deadline_ms: 0,
            start_seq: 0,
            continuation_text: None,
            artifact_put: None,
        }
    }

    /// The dispatcher's half of the `x-supplier-id` contract, stated once as the rule the
    /// faces are built on: **a supplier that is known is never lost, and one that is
    /// unknown is never invented.**
    ///
    /// `Effect::Accepted.worker` (`actor.rs:379-401`) is the answer the faces need before
    /// they write anything, and the two ways it can arrive — named by the verdict, or left
    /// to a later `Effect::SendWorker{Assigned}` — both have to land on one relay cell that
    /// a response head can read. The resolution *condition* of the arm (`attached ||
    /// phase != Pending`) is `nexus-core`'s business and its acceptance suite pins it; what
    /// is under test here is what the arm **does** with `worker` once it fires, so
    /// `attached: true` stands in for the queued record's phase and keeps the phase probe
    /// out of the way.
    #[tokio::test]
    async fn a_known_supplier_survives_every_later_verdict() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());
        let first = WorkerId::new_unchecked("w-first");
        let second = WorkerId::new_unchecked("w-second");

        // ---- 1. The verdict names the supplier: the face needs no second actor turn ----
        let warm = JobId::new_unchecked("job-warm");
        let relay = handle.relay(&warm, 16).await;
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        *relay.gate.lock().await = Some(gate_tx);
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Accepted {
                job: warm.clone(),
                attached: true,
                replayed: Vec::new(),
                worker: Some(first.clone()),
            }],
        )
        .await;
        assert_eq!(
            gate_rx.await.expect("the verdict reaches its face"),
            super::Gate::Accepted {
                worker: Some(first.clone())
            },
            "the acceptance carries the id the header echoes"
        );
        assert_eq!(
            relay.supplier().as_ref(),
            Some(&first),
            "and the relay mirrors it, so a face that reads at write time agrees with \
             its verdict instead of re-querying the actor"
        );
        // A second attempt on another supplier (the requeue shape) must not move an id a
        // consumer has already been told: the header names who it was **connected to**.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::SendWorker {
                worker: second.clone(),
                frame: Box::new(assigned_to(&warm)),
            }],
        )
        .await;
        assert_eq!(
            relay.supplier().as_ref(),
            Some(&first),
            "a later `Assigned` never rewrites the supplier the consumer was told"
        );

        // ---- 2. Nothing warm yet: the verdict has nothing to say, and the gate waits ----
        //
        // The NEX-3.3 grace shape (`attached: false`, no record, so the arm deliberately does
        // not resolve): the answer comes from the `Assigned` itself, and that arm has to
        // carry the id too, not just open the body.
        let waiting = JobId::new_unchecked("job-waiting");
        let relay = handle.relay(&waiting, 16).await;
        let (gate_tx, mut gate_rx) = tokio::sync::oneshot::channel();
        *relay.gate.lock().await = Some(gate_tx);
        let mut watcher = relay.supplier_watcher();
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Accepted {
                job: waiting.clone(),
                attached: false,
                replayed: Vec::new(),
                worker: None,
            }],
        )
        .await;
        assert_eq!(
            gate_rx.try_recv().unwrap_err(),
            tokio::sync::oneshot::error::TryRecvError::Empty,
            "an unanswered acceptance opens nothing: the grace window is still the \
             503's to spend"
        );
        assert_eq!(relay.supplier(), None, "and there is no supplier to name");
        assert!(
            !watcher.has_changed().unwrap_or(true),
            "a verdict with no supplier must not wake the face that is holding its head"
        );
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::SendWorker {
                worker: first.clone(),
                frame: Box::new(assigned_to(&waiting)),
            }],
        )
        .await;
        assert_eq!(
            gate_rx.await.expect("the `Assigned` answers the open gate"),
            super::Gate::Accepted {
                worker: Some(first.clone())
            },
            "the hand-over is now an answer, not just a body opening"
        );
        assert_eq!(relay.supplier().as_ref(), Some(&first));
        assert!(
            watcher
                .changed()
                .await
                .is_ok_and(|()| watcher.borrow().as_ref() == Some(&first)),
            "the watch wakes exactly once, with the id"
        );

        // ---- 3. Queued with a warm pool whose slots are all busy (`actor.rs:1303`) ----
        //
        // Here the verdict DOES resolve the gate — the record is `Queued`, not `Pending` —
        // while carrying no supplier, so the gate is spent with `None`. The `Assigned` that
        // follows cannot re-resolve it, and must not need to: the cell is what a face that
        // has not flushed its head yet fills from, and a face that already flushed simply
        // leaves the header off.
        let contended = JobId::new_unchecked("job-contended");
        let relay = handle.relay(&contended, 16).await;
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        *relay.gate.lock().await = Some(gate_tx);
        let mut watcher = relay.supplier_watcher();
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Accepted {
                job: contended.clone(),
                attached: true,
                replayed: Vec::new(),
                worker: None,
            }],
        )
        .await;
        assert_eq!(
            gate_rx.await.expect("the queue acceptance resolves"),
            super::Gate::Accepted { worker: None },
            "accepted, with nothing yet to put in the header"
        );
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::SendWorker {
                worker: second.clone(),
                frame: Box::new(assigned_to(&contended)),
            }],
        )
        .await;
        assert_eq!(
            relay.supplier().as_ref(),
            Some(&second),
            "the slot freed, the lease landed, and the cell filled for the face that \
             is still holding its head"
        );
        assert!(
            watcher
                .changed()
                .await
                .is_ok_and(|()| watcher.borrow().as_ref() == Some(&second)),
            "the same wake the streamed face sleeps on"
        );
        // A join of a record that has since gone terminal reports `None` — "nothing to
        // report", never a failure — and must leave the id above standing.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Accepted {
                job: contended.clone(),
                attached: true,
                replayed: Vec::new(),
                worker: None,
            }],
        )
        .await;
        assert_eq!(
            relay.supplier().as_ref(),
            Some(&second),
            "`None` is never an erasure"
        );
        // The three rows are only ever looked up, never created, by these arms.
        assert_eq!(
            relay_ids(&handle).await,
            "job-contended,job-waiting,job-warm"
        );
    }

    /// NEX-7.2 — in-memory state bounded, every retained byte with a stated reason —
    /// says the `Effect::Reject` arm must **look up** a relay, never create one.
    /// (NEX-7.3, the id this arm used to cite, is hub-restart semantics: a different
    /// clause, `hub-specification.md:237-239`.)
    ///
    /// The reachable scenario, rate-independent: a job is accepted and followed while a
    /// worker artifact `PUT` is in flight (`io.rs:229-283`); the actor then forgets it —
    /// detach, the NEX-6.3 orphan sweep or the retention sweep — and its
    /// `Effect::JobForgotten` prunes the hub-side row. The upload lands afterwards, so
    /// `on_artifact_stored` finds `leases.get(job) == None` and emits
    /// `reject(Some(job), 409, "stale_attempt")` (`actor.rs:2759`). On the old
    /// get-or-create dispatcher that rejection inserted a fresh row: gate `None`, so
    /// `JobRelay::resolve` (`state.rs:161-165`) discarded the verdict anyway, and the
    /// row itself stayed — ids are monotonic and never reused (`state.rs:454-457`) and
    /// no later effect can name an id the actor has dropped, so nothing ever removed it.
    /// One permanent row per occurrence, for the life of the process: the last
    /// unbounded row **the dispatcher itself created**, and the arm the pruning test
    /// above does not cover.
    ///
    /// The row this arm cannot resurrect is not the whole bound either. The same
    /// stranding is reachable from the other side: an `/events` face that races
    /// `Effect::JobForgotten` creates its row at `consumer.rs:812-814` (handler span
    /// `consumer.rs:789-831`) **after** the prune has run, so no later effect can ever
    /// name that id again — one permanent row per occurrence, whatever the dispatcher
    /// does. `prune_orphaned_relays` is that other half: it runs on every actor tick and
    /// takes a row once the actor has no record for its job, nothing can still wait on the
    /// relay, and `ORPHAN_RELAY_GRACE_MS` has passed since the row was created. See
    /// `the_tick_sweep_takes_relay_rows_that_outlived_their_job` below for the sweep's own
    /// cases; what this arm still contributes is the log line every refusal that finds no
    /// relay writes, `rejection names a job with no relay`.
    #[tokio::test]
    async fn a_rejection_for_a_forgotten_job_resurrects_no_relay() {
        // The rig of the pruning test: a real `Hub`, so the dispatcher works on the same
        // map the HTTP faces read, driven through `dispatch` directly so no actor task can
        // touch it behind these assertions.
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        // The job that goes stranded (`lost`), plus one neighbour that must survive every
        // step below: this fix stops an insert, it never removes a live row.
        let lost = JobId::new_unchecked("job-stranded");
        let neighbour = JobId::new_unchecked("job-neighbour");
        hub.bind_job(&lost, &pid).await;
        hub.bind_job(&neighbour, &pid).await;
        handle.relay(&lost, 16).await;
        handle.relay(&neighbour, 16).await;

        // Step 1: the actor drops the record, and the arm below prunes the row.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: lost.clone() }],
        )
        .await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-neighbour",
            "the forget pruned the stranded row and nothing else"
        );

        // Step 2: the late answer for the upload still in flight — the arm under test.
        let late_refusal = || Effect::Reject {
            job: Some(lost.clone()),
            status: 409,
            code: "stale_attempt".to_string(),
            message: "this upload does not belong to the current attempt".to_string(),
            retry_after_ms: None,
        };
        dispatch(&hub, &handle, &actor, vec![late_refusal()]).await;
        // The whole expected map. On the old get-or-create arm this is
        // `job-neighbour,job-stranded`: the resurrection, invisible to every other test
        // because the row looks exactly like a live fan-out until the process ends.
        assert_eq!(
            relay_ids(&handle).await,
            "job-neighbour",
            "a refusal nobody is waiting on creates no relay"
        );
        // More chunks of the same upload, three at a time: still the one row, or this is a
        // slower leak rather than a closed one.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![late_refusal(), late_refusal(), late_refusal()],
        )
        .await;
        assert_eq!(relay_ids(&handle).await, "job-neighbour");

        // Step 3: the half that must keep working. Every face registers its relay *and* its
        // gate before the actor hears about the job (`consumer.rs:1162-1166`), so a lookup
        // finds the row a verdict has to land on. Without this assertion the fix could pass
        // by silently dropping every refusal.
        let refused = JobId::new_unchecked("job-refused");
        let relay = handle.relay(&refused, 16).await;
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        *relay.gate.lock().await = Some(gate_tx);
        let (replay_tx, mut replay_rx) = mpsc::channel::<super::RelayItem>(16);
        *relay.replay_target.lock().await = Some(replay_tx);
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Reject {
                job: Some(refused.clone()),
                status: 429,
                code: "queue_full".to_string(),
                message: "the queue is full; retry".to_string(),
                retry_after_ms: Some(2_500),
            }],
        )
        .await;
        // Exact verdict, unchanged by the fix: `2_500 ms` answers as a `3 s` `Retry-After`.
        let verdict = gate_rx.await.expect("the waiting face gets its verdict");
        assert_eq!(
            verdict,
            super::Gate::Reject {
                status: axum::http::StatusCode::TOO_MANY_REQUESTS,
                code: "queue_full".to_string(),
                message: "the queue is full; retry".to_string(),
                retry_after_secs: Some(3),
            }
        );
        assert!(
            replay_rx.recv().await.is_none(),
            "the refusal still closes this consumer's private replay queue"
        );
        // The refused job's own row stays for its face to prune (`abandon_submission`,
        // `consumer.rs:1226-1233`); the dispatcher neither adds it nor removes it.
        assert_eq!(
            relay_ids(&handle).await,
            "job-neighbour,job-refused",
            "a refusal leaves the rows it found exactly as they were"
        );
    }

    /// NEX-7.2 on the fan-out itself: a consumer's queue leaves the relay the moment the
    /// pump that owned it stops, on **every** exit path.
    ///
    /// `JobRelay::subscribers` used to be a `Vec<Sender>` pruned by the **next push**, so a
    /// pump the session stopped — `detach`, a close, a liveness verdict, an abort — left its
    /// sender, and every frame still buffered in its `consumer_backpressure_frames`-deep
    /// queue, on a fan-out that had already gone terminal and would never push again. On one
    /// long-lived consumer socket that was one stranded queue per task it had ever watched:
    /// per-stream state with no bound, and the shape the hub's resident-memory curve was
    /// reporting. The removal is `Subscriber`'s `Drop` now, and a `Drop` cannot await — which
    /// is exactly why the slot map sits behind a `std::sync::Mutex` that
    /// `push_to_consumers` snapshots and releases before delivering anything.
    #[tokio::test]
    async fn a_retired_pump_takes_its_consumer_queue_out_of_the_fan_out() {
        // A relay on its own: the fan-out is the unit under test, and the creation stamp is
        // irrelevant to it (`0` keeps the literal honest about that).
        let relay = Arc::new(super::JobRelay::new(16, 0));

        // First consumer: attaching registers exactly one queue, and only one.
        let first = super::Subscriber::attach(&relay, 16).await;
        assert_eq!(
            relay.subscriber_count(),
            1,
            "one consumer, one registered queue"
        );

        // Second consumer of the same task (two sockets following one job is normal): its own
        // slot, never a shared one.
        let mut second = super::Subscriber::attach(&relay, 16).await;
        assert_eq!(
            relay.subscriber_count(),
            2,
            "two consumers of one task hold two slots"
        );

        // The half that must keep working after the snapshot rewrite of `push_to_consumers`
        // (it no longer delivers while holding the lock): delivery to a consumer that keeps up
        // is reliable and in order. Whole-value equality on the received queue.
        relay
            .push(super::ConsumerEvent::Delta {
                seq: 1,
                text: "one".to_string(),
            })
            .await;
        relay
            .push(super::ConsumerEvent::Delta {
                seq: 2,
                text: "two".to_string(),
            })
            .await;
        let mut got = Vec::new();
        for _ in 0..2 {
            got.push(
                second
                    .recv()
                    .await
                    .expect("a consumer that keeps up is never dropped"),
            );
        }
        assert_eq!(
            got,
            vec![
                super::RelayItem::Event(super::ConsumerEvent::Delta {
                    seq: 1,
                    text: "one".to_string(),
                }),
                super::RelayItem::Event(super::ConsumerEvent::Delta {
                    seq: 2,
                    text: "two".to_string(),
                }),
            ],
            "both frames arrived whole, in order, on the queue that is still live"
        );

        // The claim itself: a pump that goes away takes **its own** slot and leaves the other
        // one alone. The old shape kept both until a push happened to notice.
        drop(first);
        assert_eq!(
            relay.subscriber_count(),
            1,
            "a retired pump takes exactly its own slot, not the fan-out"
        );

        // An **aborted** pump, which is what `detach` and a teardown do to a stream that will
        // not drain in `PUMP_DRAIN`. The `oneshot` makes the ordering exact: the count is
        // already 2 by the time the task reports its attach, and awaiting the aborted handle
        // returns only once the task's future has been dropped, so the `Drop` below has
        // already run — no sleeping, no polling, no race.
        let relay_for_pump = Arc::clone(&relay);
        let (attached_tx, attached_rx) = tokio::sync::oneshot::channel();
        let parked = tokio::spawn(async move {
            let _live = super::Subscriber::attach(&relay_for_pump, 16).await;
            let _ = attached_tx.send(());
            // Parked forever: this pump never reaches an unwind of its own.
            std::future::pending::<()>().await;
        });
        attached_rx
            .await
            .expect("the spawned pump registered before the test cuts it off");
        assert_eq!(relay.subscriber_count(), 2, "the parked pump is registered");
        parked.abort();
        let _ = parked.await;
        assert_eq!(
            relay.subscriber_count(),
            1,
            "an abort retires the slot too — an explicit `unsubscribe().await` never could"
        );

        // `end_stream` (the job went terminal) clears the fan-out; the `Subscriber` a pump is
        // still holding then retires a slot that is already gone. Idempotent, and it must not
        // take a **new** consumer's slot with it, which is the case a positional or
        // count-based removal would get wrong.
        let third = super::Subscriber::attach(&relay, 16).await;
        assert_eq!(relay.subscriber_count(), 2);
        relay.end_stream().await;
        assert_eq!(
            relay.subscriber_count(),
            0,
            "a terminal stream leaves no consumer queue behind"
        );
        drop(third);
        drop(second);
        assert_eq!(
            relay.subscriber_count(),
            0,
            "a late `Drop` after `end_stream` is a no-op, not a panic or an underflow"
        );
    }

    /// NEX-7.2 across the rest of the dispatcher: **every** arm looks a relay up, none
    /// installs one.
    ///
    /// `a_rejection_for_a_forgotten_job_resurrects_no_relay` pins the `Reject` arm. The same
    /// get-or-create sat in six more arms — `SendWorker`'s `Assigned`, `Accepted`, `Relay`,
    /// `ReplayTo`, `Keepalive`, `Terminal` — and the highest-frequency one was `Relay`: a late
    /// worker frame for a job the actor had already forgotten relayed through it, job ids are
    /// never reused, and each occurrence left one empty row no later effect could ever name.
    /// Every one of those arms is driven at a forgotten job here, with the whole surviving map
    /// asserted as one exact id string each time.
    ///
    /// Then the same arms run against a **live** relay, because a fix that only stops inserts
    /// could pass by quietly losing every delivery: the exact events in order on the
    /// consumer's queue, the exact verdict on its waiting gate, the exact prefix (gap first)
    /// on its private replay queue, and one `End` when the job goes terminal.
    #[tokio::test]
    async fn no_dispatcher_arm_gives_a_forgotten_job_its_relay_row_back() {
        // Same rig as the neighbouring tests: a real `Hub`, `dispatch` called directly.
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        let lost = JobId::new_unchecked("job-forgotten");
        let live = JobId::new_unchecked("job-live");
        hub.bind_job(&lost, &pid).await;
        hub.bind_job(&live, &pid).await;
        handle.relay(&lost, 16).await;
        handle.relay(&live, 16).await;

        // The actor forgets `lost`, and the forget prunes its row.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: lost.clone() }],
        )
        .await;
        assert_eq!(relay_ids(&handle).await, "job-live");

        // One batch naming the forgotten job through every remaining arm at once. The
        // `SendWorker`/`Assigned` pair is here too, and its worker id is deliberately
        // unregistered: `send_frame` reports and returns, while the arm's relay lookup runs
        // either way, which is the half under test.
        let resurrection_batch = || {
            vec![
                Effect::SendWorker {
                    worker: WorkerId::new_unchecked("w-gone"),
                    frame: Box::new(HubFrame::Assigned {
                        job: lost.clone(),
                        attempt: 1,
                        kind: "llm.completion".to_string(),
                        tenant: nexus_core::ids::TenantId::new_unchecked("default"),
                        requires: nexus_core::protocol::Requires::default(),
                        payload: nexus_core::protocol::JobPayload::Inline {
                            content_type: "application/json".to_string(),
                            body: serde_json::json!({"prompt": "late"}),
                        },
                        lease_expires_ms: 0,
                        job_deadline_ms: 0,
                        start_seq: 0,
                        continuation_text: None,
                        artifact_put: None,
                    }),
                },
                Effect::Accepted {
                    job: lost.clone(),
                    attached: true,
                    replayed: vec![super::ConsumerEvent::Delta {
                        seq: 1,
                        text: "late".to_string(),
                    }],
                    // A join of a record the actor has already forgotten: the arm's relay
                    // lookup misses, so neither the id nor the replay has anywhere to go.
                    worker: Some(WorkerId::new_unchecked("w-gone")),
                },
                Effect::Relay {
                    job: lost.clone(),
                    events: vec![super::ConsumerEvent::Delta {
                        seq: 2,
                        text: "still late".to_string(),
                    }],
                },
                Effect::ReplayTo {
                    job: lost.clone(),
                    consumer: hub.ids.consumer_id(),
                    events: vec![super::ConsumerEvent::Delta {
                        seq: 3,
                        text: "buffered".to_string(),
                    }],
                    lossy: true,
                },
                Effect::Keepalive { job: lost.clone() },
                Effect::Terminal {
                    job: lost.clone(),
                    reason: nexus_core::model::TerminalReason::Completed,
                    retryable: false,
                },
            ]
        };
        dispatch(&hub, &handle, &actor, resurrection_batch()).await;
        // The whole expected map. On the old dispatcher every arm in that batch inserted the
        // row, so this is `job-forgotten,job-live` — one permanent row per occurrence.
        assert_eq!(
            relay_ids(&handle).await,
            "job-live",
            "no arm hands a forgotten job its relay row back"
        );
        // Twice more, because the leak was per occurrence: a batch that ran three times used
        // to cost exactly what a batch that ran once cost, and this must not be a slower leak.
        dispatch(&hub, &handle, &actor, resurrection_batch()).await;
        dispatch(&hub, &handle, &actor, resurrection_batch()).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-live",
            "repetition proves the bound, not just the first refusal"
        );

        // The half that must keep working, on the relay that does exist. A consumer follows
        // `live`, a face waits on its verdict gate, and its attach has a private replay queue
        // installed — exactly what `consumer_ws::handle_attach` and `consumer::submit`
        // register before the actor hears anything. Each batch below is one mailbox step, which
        // is how `run_actor` applies effects; order matters here because the `Accepted` arm
        // takes the replay target and closes it when nothing is left to replay into it.
        let relay = handle.relay(&live, 16).await;
        let mut follower = super::Subscriber::attach(&relay, 16).await;
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        *relay.gate.lock().await = Some(gate_tx);
        let (replay_tx, mut replay_rx) = mpsc::channel::<super::RelayItem>(16);
        *relay.replay_target.lock().await = Some(replay_tx);

        dispatch(
            &hub,
            &handle,
            &actor,
            vec![
                Effect::Relay {
                    job: live.clone(),
                    events: vec![
                        super::ConsumerEvent::Delta {
                            seq: 1,
                            text: "first".to_string(),
                        },
                        super::ConsumerEvent::Delta {
                            seq: 2,
                            text: "second".to_string(),
                        },
                    ],
                },
                Effect::Keepalive { job: live.clone() },
            ],
        )
        .await;

        // Exact stream on the live queue: both deltas, then the keepalive comment. Nothing
        // about the fix may reorder or drop these.
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(follower.recv().await.expect("a live fan-out delivers"));
        }
        assert_eq!(
            got,
            vec![
                super::RelayItem::Event(super::ConsumerEvent::Delta {
                    seq: 1,
                    text: "first".to_string(),
                }),
                super::RelayItem::Event(super::ConsumerEvent::Delta {
                    seq: 2,
                    text: "second".to_string(),
                }),
                super::RelayItem::Comment("ping"),
            ],
            "a live relay still receives exactly what it is owed, in order"
        );

        // The joiner's private prefix, on its own step: gap marker first, because this arm was
        // told the ring had already evicted part of what was asked for.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::ReplayTo {
                job: live.clone(),
                consumer: hub.ids.consumer_id(),
                events: vec![super::ConsumerEvent::Delta {
                    seq: 3,
                    text: "buffered".to_string(),
                }],
                lossy: true,
            }],
        )
        .await;
        let mut replayed = Vec::new();
        for _ in 0..2 {
            replayed.push(
                replay_rx
                    .recv()
                    .await
                    .expect("a waiting joiner still gets its prefix"),
            );
        }
        assert_eq!(
            replayed,
            vec![
                super::RelayItem::Event(super::ConsumerEvent::Gap {
                    expected: 0,
                    got: 0,
                }),
                super::RelayItem::Event(super::ConsumerEvent::Delta {
                    seq: 3,
                    text: "buffered".to_string(),
                }),
            ],
            "a lossy replay still says so before it replays"
        );

        // The verdict, on a step of its own: `attached` is what makes a joiner's stream open by
        // definition, so the gate resolves rather than waiting out the no-worker grace.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Accepted {
                job: live.clone(),
                attached: true,
                replayed: Vec::new(),
                // Nothing leased to report: the verdict still opens the stream, and the
                // new field travels as whatever the effect carried — here, nothing.
                worker: None,
            }],
        )
        .await;
        let verdict = gate_rx
            .await
            .expect("the accepted verdict reaches its face");
        assert_eq!(verdict, super::Gate::Accepted { worker: None });
        // The replay target this arm takes and closes is the face's own business now that the
        // prefix is delivered: a queue nobody will write to again must not stay open.
        assert!(
            replay_rx.recv().await.is_none(),
            "the prefix queue closes once the join is accepted"
        );

        // `Terminal` on a live row still ends the stream and hands the row back to the faces.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::Terminal {
                job: live.clone(),
                reason: nexus_core::model::TerminalReason::Completed,
                retryable: false,
            }],
        )
        .await;
        assert_eq!(
            follower.recv().await,
            Some(super::RelayItem::End),
            "the terminal verdict still closes this consumer's stream"
        );
        assert_eq!(
            relay_ids(&handle).await,
            "",
            "a terminal stream leaves no relay row for this pool at all"
        );
        // The follower's own `Drop` now finds an empty fan-out — the same idempotent exit the
        // pump takes on a socket that closed underneath it.
        drop(follower);
        assert_eq!(
            relay.subscriber_count(),
            0,
            "and no subscriber slot survives it"
        );
    }

    /// NEX-7.2's unconditional half: the actor's tick sweep takes the relay rows that
    /// outlived the job they fan out.
    ///
    /// The dispatcher prunes `relays` from effects, so it can only ever name a job the actor
    /// still has a record for. Everything else was permanent: a **face** that registers its
    /// relay after that prune — an `/events` handler or a WS `attach` that read `job_index` a
    /// moment before the record was deleted (`consumer.rs:812-814`) — left one row per
    /// occurrence for the life of the process, which is exactly the accumulating shape the
    /// resident-memory curve reported.
    ///
    /// `prune_orphaned_relays` runs on every tick, and takes a row only when **all three**
    /// hold: the actor has no record for the id, `JobRelay::is_idle` says nobody can still be
    /// waiting on it, and `ORPHAN_RELAY_GRACE_MS` has passed since the row was created. Every
    /// case below passes `now_ms` and a creation stamp explicitly, so the sweep is judged on
    /// exact inputs rather than on how long the machine took — and age is what separates a
    /// healthy in-flight registration (a face creates its row a moment *before* the actor
    /// knows the job) from an orphan.
    #[tokio::test]
    async fn the_tick_sweep_takes_relay_rows_that_outlived_their_job() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        // A mutable actor this test drives directly, so "the actor still has the record" is
        // the actor's own answer rather than a stand-in the test invented.
        let mut actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        // One clock reading for the whole test, ahead of the real clock's own start so the
        // stamps below cannot underflow a `u64` on a test binary that has only been running a
        // few milliseconds. Every expectation is stated as an offset from this reading, and the
        // sweep is handed the same one — the comparison it makes is `created + grace <= now`,
        // so nothing here depends on how long the machine took.
        let grace = super::ORPHAN_RELAY_GRACE_MS;
        let now = super::monotonic_ms() + grace * 3;
        // Old enough that `created + grace <= now` holds at the boundary itself.
        let old = now - grace;
        // One millisecond short of that: the same row, still in flight.
        let fresh = now - grace + 1;

        // The candidates, each differing from its neighbour in exactly one condition.
        let stranded = JobId::new_unchecked("job-stranded"); // old, jobless, idle → taken
        let in_flight = JobId::new_unchecked("job-fresh"); // not old enough → kept
        let recorded = JobId::new_unchecked("job-live"); // actor still has it → kept
        let followed = JobId::new_unchecked("job-followed"); // a consumer is attached → kept
        let gated = JobId::new_unchecked("job-gate"); // a face waits on its verdict → kept
        let rejoining = JobId::new_unchecked("job-replay"); // an attach awaits its prefix → kept
        for (job, stamp) in [
            (stranded.clone(), old),
            (in_flight.clone(), fresh),
            (recorded.clone(), old),
            (followed.clone(), old),
            (gated.clone(), old),
            (rejoining.clone(), old),
        ] {
            handle
                .relays
                .lock()
                .await
                .insert(job, Arc::new(super::JobRelay::new(16, stamp)));
        }
        assert_eq!(
            relay_ids(&handle).await,
            "job-followed,job-fresh,job-gate,job-live,job-replay,job-stranded",
            "the whole fixture, stated before the sweep touches it"
        );

        // `recorded` gets a real record. The actor refuses a submission whose capabilities no
        // connected worker can serve (`unsatisfiable_reason`, `actor.rs:3812-3818`), so the pool
        // needs a worker first — and a real one, driven through the same messages a supplier
        // socket produces, because "the actor still has the record" has to be the actor's own
        // answer rather than something the test asserted into existence.
        let worker = WorkerId::new_unchecked("w-sweep");
        actor.handle(
            now,
            ActorMsg::Hello {
                worker: worker.clone(),
                conn: 1,
                hello: nexus_core::HelloInput {
                    protocol: 1,
                    pools: vec![pid.to_string()],
                    capacity: 1,
                    requires: nexus_core::protocol::Requires {
                        model: Some("qwen38".to_string()),
                        ..nexus_core::protocol::Requires::default()
                    },
                    attestation: nexus_core::protocol::Attestation::default(),
                    tier: None,
                    engine: None,
                },
            },
        );
        actor.handle(
            now,
            ActorMsg::WorkerFrame {
                worker: worker.clone(),
                conn: 1,
                frame: nexus_core::protocol::WorkerFrame::Ready {
                    free_slots: 1,
                    free_kv_tokens: None,
                    engine_ready: true,
                    telemetry: None,
                },
            },
        );
        let effects = actor.handle(
            now,
            ActorMsg::Submit {
                job: recorded.clone(),
                submission: nexus_core::Submission {
                    kind: "llm.completion".to_string(),
                    pool: pid.clone(),
                    tenant: nexus_core::ids::TenantId::new_unchecked("default"),
                    requires: nexus_core::protocol::Requires::default(),
                    payload: serde_json::json!({"prompt": "sweep"}),
                    content_type: "application/json".to_string(),
                    idempotency_key: "sweep-1".to_string(),
                    session_hint: None,
                    // No `x-supplier-id` on this submission: any available supplier may
                    // take it, which is the pre-feature routing the sweep test depends on.
                    supplier_hint: None,
                    priority: nexus_core::Priority::Default,
                    idempotent: true,
                    streaming: true,
                    cancel_on_consumer_gone: false,
                },
                consumer: hub.ids.consumer_id(),
            },
        );
        assert!(
            !effects.is_empty(),
            "the actor answered the submission, so the record is its answer"
        );
        assert!(
            actor.job(&recorded).is_some(),
            "the actor really does hold `job-live` — that is the condition being tested: {effects:?}"
        );

        // The three "somebody is still waiting" rows, one per field `is_idle` reads.
        let follower = super::Subscriber::attach(
            &handle.lookup_relay(&followed).await.expect("fixture row"),
            16,
        )
        .await;
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        *handle
            .lookup_relay(&gated)
            .await
            .expect("fixture row")
            .gate
            .lock()
            .await = Some(gate_tx);
        let (replay_tx, replay_rx) = mpsc::channel::<super::RelayItem>(16);
        *handle
            .lookup_relay(&rejoining)
            .await
            .expect("fixture row")
            .replay_target
            .lock()
            .await = Some(replay_tx);

        // The sweep.
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-followed,job-fresh,job-gate,job-live,job-replay",
            "one row had no record, no waiter and its age; every other condition survived"
        );

        // Idempotent: the same tick again removes nothing further, so a sweep that misread a
        // condition cannot slowly eat a live pool.
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-followed,job-fresh,job-gate,job-live,job-replay",
            "the sweep is stable on what it chose to keep"
        );

        // The bound the sweep exists to provide, in the order reality produces it: the streams
        // end (the follower's `Drop`, the verdict answered, the attach's replay queue closed),
        // **then** a later tick takes the rows they used to pin. This is the sequence a
        // long-lived hub actually runs, and it is why per-job state stops accumulating here.
        drop(gate_rx);
        drop(replay_rx);
        // Clearing the two slots is what "the face finished with it" looks like from the
        // relay's side: `consumer::abandon_submission` and the attach pumps do exactly this on
        // their way out.
        *handle
            .lookup_relay(&gated)
            .await
            .expect("fixture row")
            .gate
            .lock()
            .await = None;
        *handle
            .lookup_relay(&rejoining)
            .await
            .expect("fixture row")
            .replay_target
            .lock()
            .await = None;
        drop(follower);
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-fresh,job-live",
            "once nothing waits on them, retired rows go on the next tick — and only the \
             actor's own record and a row still inside its grace survive"
        );
    }

    /// C2: an attach abandoned **after** it installed its replay target has to take the target
    /// back out, or the row it created is immortal in exactly the way the sweep was built to fix.
    ///
    /// Two paths are the same three lines deep: `/events` installs the target and then the
    /// `Attach` post fails because the actor is draining, and the WS `attach` installs, fails the
    /// same post, and answers `rejected`. In both, the receiver half of the queue goes out of
    /// scope with the handler while the sender stays in the row — and nothing else on the hub can
    /// ever reach it, because the only other writer is the dispatcher's `ReplayTo` arm and this
    /// attach never reached the actor. Meanwhile `is_idle` reads an installed target as "somebody
    /// is waiting on this relay", which is the truth about a live join and a lie about an
    /// abandoned one.
    ///
    /// Pinned against `is_idle` plus the sweep rather than against the two handlers: the shape is
    /// the invariant, and driving the handler's early return would mean parking a pool actor that
    /// has already exited — which that handler cannot tell apart from any other closed mailbox.
    #[tokio::test]
    async fn an_abandoned_attach_leaves_a_sweepable_row() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());
        let grace = super::ORPHAN_RELAY_GRACE_MS;
        let now = super::monotonic_ms() + grace * 3;
        let old = now - grace;
        let abandoned = JobId::new_unchecked("job-abandoned");
        let relay = Arc::new(super::JobRelay::new(16, old));
        handle
            .relays
            .lock()
            .await
            .insert(abandoned.clone(), Arc::clone(&relay));

        // What the abandoned handler leaves behind: the target installed, its receiver already
        // dropped by the early return, and an actor that never heard about the attach at all.
        let (replay_tx, replay_rx) = mpsc::channel::<super::RelayItem>(16);
        *relay.replay_target.lock().await = Some(replay_tx);
        drop(replay_rx);
        assert!(
            !relay.is_idle(),
            "an installed replay target is not idle, receiver or no receiver — that is the trap"
        );
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-abandoned",
            "and the sweep cannot take the row: this used to be permanent, one row per abandoned \
             attach, for the life of the process"
        );

        // What both sites now do before their early return.
        relay.abandon_replay_target().await;
        assert!(
            relay.is_idle(),
            "taking the target back is what makes the row idle again"
        );
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "",
            "so a later tick takes the row the abandoned attach created"
        );
        assert_eq!(
            handle.relay_rows.load(Ordering::Relaxed),
            0,
            "and the published gauge sees it go, which is the whole point of C3"
        );
    }

    /// C3: the sweep is the hub's only window onto its own fan-out bookkeeping, so what it
    /// publishes has to be the state that **survived** the sweep, sampled at one instant.
    ///
    /// `subscriber_count` used to be `#[cfg(test)]` and `followed_jobs` was rendered nowhere,
    /// which is how a hub could hold one queue per task for hours with nothing on `/metrics` to
    /// show it. These are the exact numbers
    /// `the_relay_bookkeeping_shows_up_on_the_metrics_page` then reads through the HTTP surface,
    /// so the two tests are two readings of one quantity rather than two independent guesses.
    #[tokio::test]
    async fn the_sweep_publishes_the_relay_gauges() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());
        let grace = super::ORPHAN_RELAY_GRACE_MS;
        let now = super::monotonic_ms() + grace * 3;
        let old = now - grace;
        // One millisecond short of the grace, so this row survives every sweep below whatever
        // else happens to it — the gauge has to keep counting rows that are merely young.
        let fresh = now - grace + 1;

        for (job, stamp) in [
            (JobId::new_unchecked("job-followed"), old),
            (JobId::new_unchecked("job-retired"), old),
            (JobId::new_unchecked("job-young"), fresh),
        ] {
            handle
                .relays
                .lock()
                .await
                .insert(job, Arc::new(super::JobRelay::new(16, stamp)));
        }
        let followed = handle
            .lookup_relay(&JobId::new_unchecked("job-followed"))
            .await
            .expect("fixture row");
        let first = super::Subscriber::attach(&followed, 16).await;
        let second = super::Subscriber::attach(&followed, 16).await;

        // Before any sweep has run, nothing is published: the actor's first tick is what arms
        // these, and `run_actor` ticks within a second of boot.
        assert_eq!(
            (
                handle.relay_rows.load(Ordering::Relaxed),
                handle.relay_subscribers.load(Ordering::Relaxed),
            ),
            (0, 0),
            "the gauges start at the values the handle was constructed with"
        );

        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-followed,job-young",
            "the swept row is gone before the numbers are taken"
        );
        assert_eq!(
            handle.relay_rows.load(Ordering::Relaxed),
            2,
            "`nexus_relay_rows`: the rows that survived, not the rows that existed"
        );
        assert_eq!(
            handle.relay_subscribers.load(Ordering::Relaxed),
            2,
            "`nexus_relay_subscribers`: the queues registered across them, from the same pass"
        );

        // A consumer hanging up moves the second series on the next tick, without touching rows:
        // this is the reading the leak used to make climb forever.
        drop(first);
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            handle.relay_rows.load(Ordering::Relaxed),
            2,
            "no row went away"
        );
        assert_eq!(
            handle.relay_subscribers.load(Ordering::Relaxed),
            1,
            "one queue did"
        );

        // And the last stream ending makes the followed row sweepable, two ticks later.
        drop(second);
        drop(followed);
        super::prune_orphaned_relays(&handle, &actor, now).await;
        assert_eq!(
            relay_ids(&handle).await,
            "job-young",
            "a job that stopped being followed goes on the next tick"
        );
        assert_eq!(
            handle.relay_rows.load(Ordering::Relaxed),
            1,
            "leaving only the row still inside its grace, which is counted"
        );
        assert_eq!(
            handle.relay_subscribers.load(Ordering::Relaxed),
            0,
            "with nobody watching it"
        );
    }

    /// NEX-7.2 on the new map: the idempotency mirror is bounded by the actor's own entry
    /// cap, evicting whatever expires first rather than the newest key, and its rows expire
    /// on the actor's own TTL so a long-lived hub does not route retries to jobs nobody has.
    #[tokio::test]
    async fn the_idempotency_mirror_is_bounded_and_expires() {
        let mut cfg = BrokerConfig::default();
        // Three rows is the whole table, and a TTL the test can outwait. `Hub::new` does
        // not run `validate`, so `1 ms` is usable here rather than refused at boot.
        cfg.limits.max_idempotency_entries = 3;
        cfg.limits.idempotency_ttl_ms = 1;
        let hub = Hub::new(cfg, "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");

        hub.remember_idempotency("k-1", &pid, &JobId::new_unchecked("job-1"))
            .await;
        hub.remember_idempotency("k-2", &pid, &JobId::new_unchecked("job-2"))
            .await;
        hub.remember_idempotency("k-3", &pid, &JobId::new_unchecked("job-3"))
            .await;
        assert_eq!(hub.idempotency.lock().await.len(), 3);
        assert_eq!(hub.idempotency_expiry.lock().await.len(), 3);
        assert_eq!(hub.key_of_job.lock().await.len(), 3);

        // A fourth row is the cap: the entry that expires first leaves, and every reverse
        // index loses exactly that one row.
        hub.remember_idempotency("k-4", &pid, &JobId::new_unchecked("job-4"))
            .await;
        assert_eq!(
            hub.idempotency
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["k-2".to_string(), "k-3".to_string(), "k-4".to_string()],
            "the oldest-expiring key is the one that goes"
        );
        assert_eq!(hub.key_of_job.lock().await.len(), 3);
        assert!(hub.idempotency_target("k-1").await.is_none());

        // Expiry: every row carries the same 1 ms TTL, so after a short wait the table is
        // empty on the next read rather than on a timer the hub would have to run.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(hub.idempotency_target("k-4").await.is_none());
        assert!(hub.idempotency.lock().await.is_empty());
        assert!(hub.idempotency_expiry.lock().await.is_empty());
        assert!(
            hub.key_of_job.lock().await.is_empty(),
            "lazy expiry clears the reverse index too, or it grows on its own"
        );
    }

    /// The bounded-shape claim, stated exactly: N submissions leave the index holding
    /// only the jobs the actor still has records for.
    #[tokio::test]
    async fn the_routing_index_holds_only_the_jobs_the_actor_still_knows() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        // Twelve submissions: twelve index entries and twelve fan-outs.
        let ids: Vec<JobId> = (1..=12)
            .map(|n| JobId::new_unchecked(format!("job-{n}")))
            .collect();
        for id in &ids {
            hub.bind_job(id, &pid).await;
            handle.relay(id, 16).await;
        }
        assert_eq!(hub.job_index.lock().await.len(), 12);

        // One dispatcher pass carrying the actor's drop reports for the first nine.
        let forgotten: Vec<Effect> = ids[..9]
            .iter()
            .map(|job| Effect::JobForgotten { job: job.clone() })
            .collect();
        dispatch(&hub, &handle, &actor, forgotten).await;

        let expected: Vec<String> = ids[9..].iter().map(|j| j.as_ref().to_string()).collect();
        let mut indexed: Vec<String> = hub
            .job_index
            .lock()
            .await
            .keys()
            .map(|j| j.as_ref().to_string())
            .collect();
        indexed.sort();
        assert_eq!(indexed, expected);
        let mut relays: Vec<String> = handle
            .relays
            .lock()
            .await
            .keys()
            .map(|j| j.as_ref().to_string())
            .collect();
        relays.sort();
        assert_eq!(relays, expected);
    }
}
