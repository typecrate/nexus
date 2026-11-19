//! Task §16 acceptance matrix, driven against the **real** pool actor — the
//! thing under test is never mocked. Everything is deterministic: time is
//! injected and claim order is mailbox order, so each test asserts exact effects.
//!
//! The native runtime's real WS/SSE round trip lives in `crates/nexus-server`
//! (`src/runtime_e2e.rs`), which dials a real listener with a real WebSocket
//! client; both this file and that one have now been executed (`cargo test
//! --workspace`, see README "Verification status"). What this file proves is the
//! load-bearing part — mutual exclusion, leases, cancellation, de-duplication,
//! replay, admission, payload spill and relay fidelity.

// The workspace lints (`unwrap_used`/`expect_used`, Cargo.toml) exist for the
// shipped paths, and the policy comment says tests "are allowed to be blunt".
// They are applied to test targets too, so the exception has to be spelled out
// here: an `expect` in an acceptance test *is* the assertion.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use nexus_core::adapters::assemble_completion;
use nexus_core::adapters::KIND_CHAT;
use nexus_core::config::BrokerConfig;
use nexus_core::ids::ArtifactId;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PoolId;
use nexus_core::ids::TenantId;
use nexus_core::ids::WorkerId;
use nexus_core::model::EngineState;
use nexus_core::model::JobPhase;
// The full derived status view (`GET /v1/jobs/{id}`), asserted whole so the terminal
// prompt release cannot quietly move a field on the surface a poller reads.
use nexus_core::model::JobTimings;
use nexus_core::model::JobView;
// The read-only monitoring snapshot (`ActorMsg::Observe`), asserted on directly here
// because the actor is the authority `GET /v1/observe` renders.
use nexus_core::model::LinkState;
use nexus_core::model::Priority;
use nexus_core::model::Submission;
use nexus_core::model::TerminalReason;
use nexus_core::model::WorkerTier;
use nexus_core::model::WorkerView;
use nexus_core::protocol::Attestation;
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::CancelReason;
use nexus_core::protocol::FinishKind;
use nexus_core::protocol::FrameKind;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::JobPayload;
use nexus_core::protocol::Requires;
use nexus_core::protocol::WorkerFrame;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use nexus_core::Effect;
use nexus_core::HelloInput;
use nexus_core::PoolActor;
use nexus_core::PoolSnapshot;
use serde_json::json;
use serde_json::Value;

/// Reference digests from SHARD_TABLE (services/molab/molab-strata-runtime.py:95-98).
const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
const SHARD_2: &str = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";

fn pool() -> PoolId {
    PoolId::new_unchecked("llm.chat/qwen38")
}

/// Task defaults, with windows shrunk where a test needs to cross a deadline.
fn config() -> BrokerConfig {
    let mut cfg = BrokerConfig::default();
    cfg.auth.allow_anonymous = true;
    cfg.auth.model_shas = BTreeSet::from([SHARD_1.to_string(), SHARD_2.to_string()]);
    cfg.limits.no_worker_grace_ms = 2_000;
    cfg.limits.resume_grace_ms = 1_500;
    cfg.limits.heartbeat_ms = 20_000;
    cfg.limits.lease_ms = 60_000;
    cfg.limits.flush_interval_ms = 75;
    cfg.limits.flush_bytes = 1_536;
    cfg.limits.inline_payload_cap_bytes = 64;
    cfg.limits.max_spilled_payloads = 8;
    cfg.limits.max_spill_bytes = 4_096;
    cfg.limits.replay_frames = 64;
    cfg.limits.replay_bytes = 65_536;
    cfg.limits.job_retention_ms = 30_000;
    cfg.limits.tenant_max_payload_bytes = 4_096;
    cfg.limits.tenant_max_concurrent = 4;
    cfg.limits.max_continuations = 0;
    cfg.validate().expect("config must validate");
    cfg
}

fn actor() -> PoolActor {
    PoolActor::new(pool(), config())
}

fn hello_msg(worker: &str, conn: u64, capacity: u32) -> ActorMsg {
    ActorMsg::Hello {
        worker: WorkerId::new_unchecked(worker),
        conn,
        hello: HelloInput {
            protocol: 1,
            pools: vec![pool().to_string()],
            capacity,
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524_288),
                capabilities: BTreeSet::from(["tools".to_string()]),
                kv_estimate_tokens: None,
            },
            attestation: Attestation {
                model_shas: BTreeSet::from([SHARD_1.to_string(), SHARD_2.to_string()]),
                weight_bytes: Some(83_863_586_496),
            },
            tier: Some(WorkerTier::Dedicated),
            engine: Some("strata".to_string()),
        },
    }
}

fn frame(worker: &str, conn: u64, frame: WorkerFrame) -> ActorMsg {
    ActorMsg::WorkerFrame {
        worker: WorkerId::new_unchecked(worker),
        conn,
        frame,
    }
}

fn ready_msg(worker: &str, conn: u64, free_slots: u32) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Ready {
            free_slots,
            free_kv_tokens: Some(524_288),
            engine_ready: true,
            telemetry: None,
        },
    )
}

fn claim_msg(worker: &str, conn: u64, jobs: u32) -> ActorMsg {
    frame(worker, conn, WorkerFrame::Claim { jobs: Some(jobs) })
}

fn delta_msg(worker: &str, conn: u64, job: &str, seq: u64, text: &str) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Frame {
            job: JobId::new_unchecked(job),
            seq,
            kind: FrameKind::Delta,
            content_type: Some("application/json".to_string()),
            text: text.to_string(),
            artifact: None,
        },
    )
}

fn done_msg(worker: &str, conn: u64, job: &str, seq: u64, usage: Option<Value>) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Done {
            job: JobId::new_unchecked(job),
            seq,
            finish: FinishKind::Stop,
            usage,
            error: None,
        },
    )
}

fn gone_msg(worker: &str, conn: u64) -> ActorMsg {
    ActorMsg::WorkerGone {
        worker: WorkerId::new_unchecked(worker),
        conn,
    }
}

fn chat_body(text: &str) -> Value {
    json!({ "model": "qwen38", "messages": [{ "role": "user", "content": text }], "stream": true })
}

/// A streaming submission for `job`. `kv_estimate_tokens` is 1024 so the KV
/// scheduling gate has a real number to compare against.
fn submit_msg(job: &str, key: &str, session: Option<&str>) -> ActorMsg {
    submit_msg_with(job, key, session, chat_body("hi"), None)
}

/// The same submission, **pinned** to `supplier` (`x-supplier-id` /
/// `submit.supplier`). `None` keeps it unpinning.
///
/// It is derived from [`submit_msg_with`] rather than written out again on purpose: a
/// pinned run and an unpinned run then differ in exactly one field, which is what makes
/// the "an absent hint changes nothing" test evidence rather than a restatement.
fn submit_pinned_msg(job: &str, key: &str, supplier: Option<&str>) -> ActorMsg {
    submit_pinned_session_msg(job, key, supplier, None)
}

/// The pinned builder **with a session hint as well**, for the records that carry both
/// consumer hints at once (acceptance t20j).
///
/// The existing pair of helpers cannot express that state: `submit_pinned_msg` always
/// leaves `session_hint: None` and `submit_msg_with` always leaves `supplier_hint: None`,
/// so every shipped test of the pin ran with the two hints apart. Both hints are set here
/// and nothing else changes, which keeps the "one field differs" property the pinned/unpinned
/// comparisons above depend on.
fn submit_pinned_session_msg(
    job: &str,
    key: &str,
    supplier: Option<&str>,
    session: Option<&str>,
) -> ActorMsg {
    let ActorMsg::Submit {
        job: id,
        consumer,
        mut submission,
    } = submit_msg_with(job, key, session, chat_body("hi"), None)
    else {
        unreachable!("submit_msg_with builds a Submit");
    };
    submission.supplier_hint = supplier.map(WorkerId::new_unchecked);
    ActorMsg::Submit {
        job: id,
        consumer,
        submission,
    }
}

fn submit_msg_with(
    job: &str,
    key: &str,
    session: Option<&str>,
    body: Value,
    tenant: Option<&str>,
) -> ActorMsg {
    ActorMsg::Submit {
        job: JobId::new_unchecked(job),
        consumer: ConsumerId::new_unchecked(format!("con-{job}")),
        submission: Submission {
            kind: KIND_CHAT.to_string(),
            pool: pool(),
            tenant: TenantId::new_unchecked(tenant.unwrap_or("acme")),
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524_288),
                capabilities: BTreeSet::new(),
                kv_estimate_tokens: Some(1_024),
            },
            payload: body,
            content_type: "application/json".to_string(),
            idempotency_key: key.to_string(),
            session_hint: session.map(str::to_string),
            supplier_hint: None,
            priority: Priority::Default,
            idempotent: true,
            streaming: true,
            cancel_on_consumer_gone: true,
        },
    }
}

/// Connect a worker and bring it to `ready`, then let it claim `slots` jobs.
fn warm(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, now: u64) {
    a.handle(now, hello_msg(worker, conn, capacity));
    a.handle(now + 1, ready_msg(worker, conn, capacity));
}

/// `warm` for a supplier advertising a **different model** than the reference
/// `qwen38`. Same pattern `warm_pool` and `t13e` use: `Hello.requires` is the offer,
/// and only the model axis varies here.
fn warm_model(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, model: &str, now: u64) {
    let mut msg = hello_msg(worker, conn, capacity);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.requires.model = Some(model.to_string());
    }
    a.handle(now, msg);
    a.handle(now + 1, ready_msg(worker, conn, capacity));
}

/// Bring a supplier up exactly the way the shipped basalt client does at
/// startup: `hello` → `ready(capacity)` → `claim(capacity)`.
///
/// `warm` alone is not enough for the slot-release tests below: it never grants
/// claim **authority**, and the defect they pin down is about parked authority
/// sitting in front of queued work, so the pool has to be mid-claim before the
/// first `Submit` arrives.
fn warm_and_claim(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, now: u64) {
    warm(a, worker, conn, capacity, now);
    a.handle(now + 2, claim_msg(worker, conn, capacity));
}

/// `warm` for a pool the actor owns that is *not* the reference llm pool.
///
/// `Hello.pools` is the join request, and `PoolActor::resolve_pools` skips every
/// name that belongs to another actor, so a worker that only names
/// `llm.chat/qwen38` joins nothing here and the pool has zero capacity (t15's
/// ComfyUI host runs on `image.render/comfyui`).
fn warm_pool(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, pool: &str, now: u64) {
    let mut msg = hello_msg(worker, conn, capacity);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.pools = vec![pool.to_string()];
    }
    a.handle(now, msg);
    a.handle(now + 1, ready_msg(worker, conn, capacity));
}

/// All `Assigned` frames as `(worker, job, attempt)`.
///
/// `Effect::SendWorker::frame` is a `Box<HubFrame>` (the effect enum boxes it so a
/// relay step does not copy ~430 bytes per frame), so every helper matches through
/// `frame.as_ref()` rather than matching the field directly.
fn assigned(effects: &[Effect]) -> Vec<(String, String, u32)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWorker { worker, frame } => match frame.as_ref() {
                HubFrame::Assigned { job, attempt, .. } => {
                    Some((worker.to_string(), job.to_string(), *attempt))
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Events relayed to consumers (live relay only).
fn relay_events(effects: &[Effect]) -> Vec<ConsumerEvent> {
    effects
        .iter()
        .flat_map(|e| match e {
            Effect::Relay { events, .. } => events.clone(),
            _ => Vec::new(),
        })
        .collect()
}

/// Frames sent to one worker.
///
/// The output lifetime is spelled out because there are two input lifetimes
/// (`effects` and `worker`) and rustc cannot infer which one the frames borrow.
fn sent_to<'a>(effects: &'a [Effect], worker: &str) -> Vec<&'a HubFrame> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWorker { worker: w, frame } if w.as_ref() == worker => Some(frame.as_ref()),
            _ => None,
        })
        .collect()
}

fn rejects(effects: &[Effect]) -> Vec<(u16, String, Option<u64>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Reject {
                status,
                code,
                retry_after_ms,
                ..
            } => Some((*status, code.clone(), *retry_after_ms)),
            _ => None,
        })
        .collect()
}

fn terminals(effects: &[Effect]) -> Vec<(String, TerminalReason, bool)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Terminal {
                job,
                reason,
                retryable,
            } => Some((job.to_string(), *reason, *retryable)),
            _ => None,
        })
        .collect()
}

/// Job ids the actor reported as **deleted** (`Effect::JobForgotten`).
///
/// NEX-6.3's "the task is then deleted" is only observable through this effect: the
/// hub's `job_index` and fan-out maps are keyed by job id and the actor is the only
/// thing that knows a record is gone.
fn forgotten(effects: &[Effect]) -> Vec<String> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::JobForgotten { job } => Some(job.to_string()),
            _ => None,
        })
        .collect()
}

/// `Effect::Accepted` as `(job, attached, replayed)`.
///
/// `attached=false` with an empty replay is the whole difference between "a fresh
/// execution" and "joined an execution that already has an answer" (§5.2, §9.1).
fn accepted(effects: &[Effect]) -> Vec<(String, bool, Vec<ConsumerEvent>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Accepted {
                job,
                attached,
                replayed,
                // The leased-supplier field is asserted where it is the point (the pin
                // tests at the end of this file); this helper stays about the verdict.
                ..
            } => Some((job.to_string(), *attached, replayed.clone())),
            _ => None,
        })
        .collect()
}

/// `Effect::Accepted` as `(job, attached, leased-supplier)`.
///
/// The third slot is what the HTTP and WS faces need for `x-supplier-id`: the supplier
/// the hub is talking about **at the moment it answers**, without another actor turn.
fn accepted_workers(effects: &[Effect]) -> Vec<(String, bool, Option<String>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Accepted {
                job,
                attached,
                worker,
                ..
            } => Some((
                job.to_string(),
                *attached,
                worker.as_ref().map(|w| w.to_string()),
            )),
            _ => None,
        })
        .collect()
}

fn deltas(events: &[ConsumerEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            ConsumerEvent::Delta { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

// --------------------------------------------------------------------------- §16

#[test]
fn t01_two_workers_one_job_yields_exactly_one_assigned() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    let effects_a = a.handle(20, claim_msg("w-a", 1, 1));
    let effects_b = a.handle(20, claim_msg("w-b", 2, 1));

    assert_eq!(
        assigned(&effects_a),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)]
    );
    assert_eq!(
        assigned(&effects_b),
        Vec::new(),
        "the second worker receives nothing"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-a")
    );
    assert_eq!(a.queue_depth(), 0);
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].in_flight.len(),
        0
    );
}

#[test]
fn t02_concurrent_claims_resolve_deterministically_in_mailbox_order() {
    // Same world, opposite arrival order: the winner follows the mailbox, so the
    // outcome is reproducible rather than "one of them, somehow".
    //
    // Each worker's own connection id travels with its claim: `w-a` is live on
    // conn 1 and `w-b` on conn 2 (both `warm` above), and a claim stamped with the
    // other socket's generation is a **stale** frame that the actor correctly drops
    // (`§11`, `is_stale`). Hardcoding conn 1 for whichever worker arrived first was
    // what made the `("w-b", "w-a")` permutation look like a lost dispatch.
    for (first, first_conn, second, second_conn) in
        [("w-a", 1u64, "w-b", 2u64), ("w-b", 2, "w-a", 1)]
    {
        let mut a = actor();
        warm(&mut a, "w-a", 1, 4, 0);
        warm(&mut a, "w-b", 2, 4, 0);
        a.handle(10, submit_msg("job-1", "k1", None));
        let e1 = a.handle(20, claim_msg(first, first_conn, 1));
        let e2 = a.handle(20, claim_msg(second, second_conn, 1));
        assert_eq!(
            assigned(&e1),
            vec![(first.to_string(), "job-1".to_string(), 1u32)]
        );
        assert_eq!(assigned(&e2), Vec::new());
        assert_eq!(a.leases_len(), 1, "no double dispatch for {first}/{second}");
    }
}

#[test]
fn t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(21, claim_msg("w-b", 2, 1));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .attempt,
        1
    );

    // Socket closes: the resume grace holds ownership, so nobody else can take it.
    let gone = a.handle(30, gone_msg("w-a", 1));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("still owned")
            .worker,
        WorkerId::new_unchecked("w-a")
    );
    assert_eq!(a.ready_workers(), 1);
    assert!(assigned(&gone).is_empty());

    // Grace expires with zero bytes delivered → requeue, preferring the other worker.
    let after = a.handle(31_501, ActorMsg::Tick { now_ms: 31_501 });
    assert_eq!(
        assigned(&after),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)]
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased again")
            .attempt,
        2
    );

    let mut seen = after;
    seen.extend(a.handle(32_000, delta_msg("w-b", 2, "job-1", 1, &"tok".repeat(600))));
    seen.extend(a.handle(32_100, delta_msg("w-b", 2, "job-1", 2, &"en".repeat(600))));
    seen.extend(a.handle(
        32_200,
        done_msg("w-b", 2, "job-1", 3, Some(json!({"completion_tokens": 6}))),
    ));
    let events = relay_events(&seen);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ConsumerEvent::Error { .. })),
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Finish { .. }))
            .count(),
        1
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"no_output\"}"),
        1
    );
    // The consumer's stream is coherent: exactly the two deltas from attempt 2, in
    // order, and nothing from the killed attempt. The length is 3 000, not 2 400:
    // `"tok".repeat(600)` is 1 800 chars and `"en".repeat(600)` is 1 200, so the two
    // halves are unequal (the 600 is the repetition count, not the byte count).
    assert_eq!(
        deltas(&events),
        vec!["tok".repeat(600), "en".repeat(600)],
        "the two attempt-2 deltas, verbatim, with no attempt-1 residue"
    );
    assert_eq!(
        deltas(&events).iter().map(String::len).sum::<usize>(),
        3_000
    );
}

#[test]
fn t04_worker_killed_after_output_terminates_in_band_with_one_finish() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let first = a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    assert_eq!(
        deltas(&relay_events(&first)).len(),
        1,
        "2 kB crosses the byte threshold and flushes"
    );

    let gone = a.handle(40, gone_msg("w-a", 1));
    let after = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    let all: Vec<Effect> = first.into_iter().chain(gone).chain(after.clone()).collect();
    let events = relay_events(&all);

    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Finish { .. }))
            .count(),
        0,
        "no finish_reason after a partial stream"
    );
    assert_eq!(deltas(&events).len(), 1, "deltas are never re-emitted");
    assert_eq!(
        terminals(&after),
        vec![(
            "job-1".to_string(),
            TerminalReason::WorkerGonePartial,
            false
        )]
    );
    assert_eq!(a.metrics().get("nexus_inband_errors_total"), 1);
    assert!(
        assigned(&after).is_empty(),
        "continuation is disabled here: the job is not re-owned"
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn t05_consumer_disconnect_cancels_the_worker_immediately() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let effects = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("con-job-1"),
        },
    );

    assert!(
        sent_to(&effects, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::ConsumerGone,
                ..
            }
        )),
        "cancel must fan out on disconnect"
    );
    assert_eq!(a.metrics().get("nexus_cancels_sent_total"), 1);
    assert_eq!(
        terminals(&effects),
        vec![("job-1".to_string(), TerminalReason::Cancelled, false)]
    );

    // The ack releases the slot, so the engine is idle again (the §16.5 assertion
    // "engine goes idle" is exactly this state).
    a.handle(
        31,
        frame(
            "w-a",
            1,
            WorkerFrame::CancelAck {
                job: JobId::new_unchecked("job-1"),
            },
        ),
    );
    assert_eq!(a.metrics().get("nexus_cancel_acks_total"), 1);
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")]
        .in_flight
        .is_empty());
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable());
    assert_eq!(a.leases_len(), 0);

    // Frames that arrive after the cancel are counted, never relayed.
    let late = a.handle(32, delta_msg("w-a", 1, "job-1", 1, "too late"));
    assert!(relay_events(&late).is_empty());
}

#[test]
fn t06_no_ready_worker_answers_503_with_retry_after_before_any_body() {
    let mut a = actor();
    a.handle(0, hello_msg("w-a", 1, 4));
    a.handle(
        1,
        frame(
            "w-a",
            1,
            WorkerFrame::Ready {
                free_slots: 4,
                free_kv_tokens: None,
                engine_ready: false,
                telemetry: None,
            },
        ),
    );
    assert_eq!(a.ready_workers(), 0);
    assert_eq!(
        a.loading_workers(),
        1,
        "cold start shows as degraded capacity, not as zero capacity"
    );

    let effects = a.handle(10, submit_msg("job-1", "k1", None));
    assert!(
        rejects(&effects).is_empty(),
        "still inside the grace window"
    );
    assert!(
        relay_events(&effects).is_empty(),
        "no body bytes before the answer"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("record").phase,
        JobPhase::Pending
    );

    // Grace expires with nothing warm: 503 + Retry-After, and no Relay at all.
    let late = a.handle(2_010, ActorMsg::Tick { now_ms: 2_010 });
    assert_eq!(
        rejects(&late),
        vec![(503, "no_ready_worker".to_string(), Some(2_000))]
    );
    assert!(relay_events(&late).is_empty());
    assert_eq!(
        terminals(&late),
        vec![("job-1".to_string(), TerminalReason::NoReadyWorker, true)]
    );
    assert_eq!(a.queue_depth(), 0);
}

#[test]
fn t06b_a_warm_worker_arriving_inside_the_grace_avoids_the_503() {
    let mut a = actor();
    a.handle(0, hello_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // The engine finishes loading at 1.5 s, inside the 2 s window.
    a.handle(1_500, ready_msg("w-a", 1, 4));
    a.handle(1_501, claim_msg("w-a", 1, 1));
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );
    let late = a.handle(2_010, ActorMsg::Tick { now_ms: 2_010 });
    assert!(rejects(&late).is_empty(), "the job was already dispatched");
}

#[test]
fn t06c_zero_connected_workers_is_a_capability_reject_not_a_503() {
    let mut a = actor();
    let effects = a.handle(10, submit_msg("job-1", "k1", None));
    assert_eq!(
        rejects(&effects),
        vec![(400, "capability_unsatisfied".to_string(), None)]
    );
}

#[test]
fn t07_duplicate_idempotency_key_attaches_and_replays_never_reruns() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "same-key", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let streamed = a.handle(
        30,
        delta_msg("w-a", 1, "job-1", 1, &"delta-text".repeat(200)),
    );
    assert_eq!(deltas(&relay_events(&streamed)).len(), 1);

    let dup = a.handle(
        40,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-2"),
            consumer: ConsumerId::new_unchecked("con-2"),
            submission: match submit_msg("job-ignored", "same-key", None) {
                ActorMsg::Submit { submission, .. } => submission,
                _ => unreachable!(),
            },
        },
    );
    match &dup[..] {
        [Effect::Accepted {
            attached,
            job,
            replayed,
            worker,
        }] => {
            assert!(attached);
            assert_eq!(
                job.as_ref(),
                "job-1",
                "the second client joins the existing execution"
            );
            // The join names the supplier the execution is already running on: a
            // duplicate of a leased job is a warm answer, and the face answering it can
            // name the supplier in the same turn it opens the stream.
            assert_eq!(worker.as_ref(), Some(&WorkerId::new_unchecked("w-a")));
            assert_eq!(
                replayed.len(),
                1,
                "and is handed what the buffer already holds"
            );
        }
        other => panic!("expected exactly one attach effect, got {other:?}"),
    }
    assert!(assigned(&dup).is_empty(), "no second execution");
    assert_eq!(a.metrics().get("nexus_idempotent_attach_total"), 1);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("record")
            .consumers
            .len(),
        2
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-2")).map(|r| r.phase),
        None,
        "job-2 was never allocated"
    );
}

#[test]
fn t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"one".repeat(600)));
    a.handle(31, delta_msg("w-a", 1, "job-1", 2, &"two".repeat(600)));
    a.handle(120, delta_msg("w-a", 1, "job-1", 3, &"three".repeat(600)));
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .seq
            .acked_through(),
        3
    );

    // The socket drops and comes back inside the grace window.
    a.handle(130, gone_msg("w-a", 1));
    let welcome = a.handle(135, hello_msg("w-a", 5, 4));
    let resumed = welcome
        .iter()
        .find_map(|e| match e {
            // `frame` is a `Box<HubFrame>`; match through it.
            Effect::SendWorker { frame, .. } => match frame.as_ref() {
                HubFrame::Welcome { resumed, .. } => Some(resumed.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("Welcome lists the resumable jobs");
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0].resume_from_seq, 3,
        "the hub tells the worker where to continue"
    );

    let resume = a.handle(
        136,
        frame(
            "w-a",
            5,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-1"),
                attempt: 1,
                acked_seq: 3,
            },
        ),
    );
    assert!(sent_to(&resume, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::Ack { seq: 3, .. })));
    assert_eq!(a.metrics().get("nexus_resume_accepted_total"), 1);
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("still leased")
            .attempt,
        1,
        "the job was never re-dispatched"
    );

    // The worker re-sends its tail: both frames are dropped as duplicates.
    let replay_2 = a.handle(140, delta_msg("w-a", 5, "job-1", 2, &"two".repeat(600)));
    let replay_3 = a.handle(141, delta_msg("w-a", 5, "job-1", 3, &"three".repeat(600)));
    for effects in [&replay_2, &replay_3] {
        assert!(
            deltas(&relay_events(effects)).is_empty(),
            "no duplicate deltas on the wire"
        );
        assert!(
            sent_to(effects, "w-a")
                .iter()
                .any(|f| matches!(f, HubFrame::Ack { seq: 3, .. })),
            "the worker is re-acked to where it belongs"
        );
    }
    assert_eq!(a.metrics().get("nexus_frames_duplicate_total"), 2);
    assert_eq!(deltas(&relay_events(&welcome)).len(), 0);

    // It then continues from seq 4 and finishes normally.
    let tail = a.handle(150, delta_msg("w-a", 5, "job-1", 4, &"four".repeat(600)));
    assert_eq!(deltas(&relay_events(&tail)).len(), 1);
    let done = a.handle(260, done_msg("w-a", 5, "job-1", 4, None));
    assert!(relay_events(&done)
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Finish { seq: 4, .. })));
}

#[test]
fn t09_consumer_reconnect_with_last_event_id_replays_without_a_gap() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let mut texts = Vec::new();
    for (seq, word) in [(1u64, "alpha"), (2, "beta"), (3, "gamma")] {
        let effects = a.handle(seq * 100, delta_msg("w-a", 1, "job-1", seq, word));
        texts.extend(deltas(&relay_events(&effects)));
    }
    // The third delta is still inside its flush window when its frame is handled:
    // §7.2's age rule is a timer, not a frame-driven heuristic, so it leaves on the
    // next tick (`limits.flush_interval_ms = 75`, and
    // `deltas_are_batched_by_size_and_age_not_per_token` pins the same behaviour for
    // a small tail). The replay ring already holds all three frames, which is the
    // point of the attach below.
    let aged = a.handle(375, ActorMsg::Tick { now_ms: 375 });
    texts.extend(deltas(&relay_events(&aged)));
    assert_eq!(
        texts,
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );

    // A second consumer drops and returns with Last-Event-ID: 1.
    let effects = a.handle(
        380,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("late"),
            from_seq: 1,
        },
    );
    let mut lossy = None;
    let mut replayed = Vec::new();
    for e in &effects {
        if let Effect::ReplayTo {
            events, lossy: l, ..
        } = e
        {
            lossy = Some(*l);
            replayed.extend(deltas(events));
        }
    }
    assert_eq!(
        lossy,
        Some(false),
        "the ring still holds everything after seq 1"
    );
    assert_eq!(
        replayed,
        vec!["beta".to_string(), "gamma".to_string()],
        "no gap in the text"
    );

    // Asking for something already evicted is reported, not silently truncated.
    let beyond = a.handle(
        390,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("older"),
            from_seq: 0,
        },
    );
    for e in &beyond {
        if let Effect::ReplayTo { events, .. } = e {
            assert_eq!(
                deltas(events).len(),
                3,
                "the whole buffer is still available at this size"
            );
        }
    }
}

#[test]
fn t10b_auto_is_dispatched_to_whichever_model_is_free() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    let body = json!({"model":"qwen38","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}],"stream":true});
    let effects = a.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-vis"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: KIND_CHAT.to_string(),
                pool: pool(),
                tenant: TenantId::new_unchecked("acme"),
                requires: nexus_core::adapters::infer_requires("qwen38", &body),
                payload: body,
                content_type: "application/json".to_string(),
                idempotency_key: "vision-key".to_string(),
                session_hint: None,
                supplier_hint: None,
                priority: Priority::Default,
                idempotent: true,
                streaming: true,
                cancel_on_consumer_gone: true,
            },
        },
    );
    assert!(rejects(&effects).is_empty());
    assert!(
        a.has_job(&JobId::new_unchecked("job-vis")),
        "a proxied request occupies a slot"
    );
    // The pull model: the job queues for the worker's claim - and the claim gets
    // it, capability mismatch and all.
    assert_eq!(
        a.job(&JobId::new_unchecked("job-vis")).expect("r").phase,
        JobPhase::Queued
    );
    let claim = a.handle(20, claim_msg("w-a", 1, 4));
    assert_eq!(
        assigned(&claim),
        vec![("w-a".to_string(), "job-vis".to_string(), 1u32)]
    );
}

#[test]
fn t11_a_loading_worker_never_receives_assigned() {
    let mut a = actor();
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // It claims while still loading: the answer is nothing at all.
    let effects = a.handle(20, claim_msg("w-load", 1, 4));
    assert!(assigned(&effects).is_empty());
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Pending
    );

    // When it reports ready, the authority it already holds pays off immediately.
    let ready = a.handle(30, ready_msg("w-load", 1, 4));
    assert_eq!(
        assigned(&ready),
        vec![("w-load".to_string(), "job-1".to_string(), 1u32)]
    );
}

#[test]
fn t12_declared_capacity_is_respected_per_worker() {
    // Five jobs have to be **admitted** for the fifth to still sit queued behind a
    // 4-slot worker. The shared fixture caps a tenant at 4 concurrent jobs (the
    // tenant-limit test depends on that number), which would answer the fifth
    // `Submit` with `429` instead of queueing it, so this test raises its own cap.
    let mut cfg = config();
    cfg.limits.tenant_max_concurrent = 8;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-4", 1, 4, 0);
    for n in 1..=5 {
        a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    let effects = a.handle(20, claim_msg("w-4", 1, 8));
    let got = assigned(&effects);
    assert_eq!(got.len(), 4, "a 4-slot worker gets exactly 4 jobs, never 5");
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-4")].in_flight.len(),
        4
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-4")].available_slots(),
        0
    );
    assert_eq!(a.queue_depth(), 1, "the fifth job stays queued");
    // A further claim while full grants no authority, so nothing over-dispatches.
    assert!(assigned(&a.handle(30, claim_msg("w-4", 1, 4))).is_empty());
}

#[test]
fn t12b_a_one_slot_worker_takes_exactly_one_job() {
    let mut a = actor();
    warm(&mut a, "w-1", 1, 1, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(11, submit_msg("job-2", "k2", None));
    let effects = a.handle(20, claim_msg("w-1", 1, 4));
    assert_eq!(assigned(&effects).len(), 1, "ComfyUI-style 1-slot engine");
    assert_eq!(a.queue_depth(), 1);
}

#[test]
fn t13_attestation_outside_the_allowlist_cannot_join_the_pool() {
    let mut a = actor();
    let mut msg = hello_msg("w-rogue", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.attestation = Attestation {
            model_shas: BTreeSet::from([
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ]),
            weight_bytes: None,
        };
    }
    let effects = a.handle(0, msg);
    assert!(sent_to(&effects, "w-rogue")
        .iter()
        .any(|f| matches!(f, HubFrame::Reject { code, .. } if code == "attestation_rejected")));
    assert!(sent_to(&effects, "w-rogue").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::AttestationRejected
        }
    )));
    assert!(
        a.workers().is_empty(),
        "an unattested worker is not capacity"
    );
    assert_eq!(a.ready_workers(), 0);
}

#[test]
fn t13b_protocol_mismatch_is_a_clean_reject() {
    let mut a = actor();
    let mut msg = hello_msg("w-old", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.protocol = 2;
    }
    let effects = a.handle(0, msg);
    assert!(sent_to(&effects, "w-old").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::VersionMismatch
        }
    )));
    assert!(a.workers().is_empty());
}

#[test]
fn t13c_a_worker_joins_by_pool_name_whatever_it_offers() {
    // Joining is by pool NAME alone: the hello's `requires` is the worker's
    // OFFER, recorded for per-job routing (`scheduler::eligible`), never a
    // floor the pool checks. An 8K supplier advertising its own model name
    // joins the same pool a 512K qwen38 host serves, and the load balancer
    // decides per job which of them a stream goes to; what still refuses a
    // join is a pool this hub does not serve.
    let mut a = actor();
    let mut msg = hello_msg("w-small", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.requires.min_context_tokens = Some(8_192);
        hello.requires.model = Some("localhost/basalt".to_string());
    }
    let effects = a.handle(0, msg);
    assert!(
        !sent_to(&effects, "w-small")
            .iter()
            .any(|f| matches!(f, HubFrame::Reject { .. })),
        "the offer is the worker's own business, not a join condition: {:?}",
        sent_to(&effects, "w-small")
    );
    assert_eq!(a.workers().len(), 1);

    let mut unknown = hello_msg("w-lost", 2, 4);
    if let ActorMsg::Hello { hello, .. } = &mut unknown {
        hello.pools = vec!["llm.chat/nowhere".to_string()];
    }
    let effects = a.handle(1, unknown);
    assert!(sent_to(&effects, "w-lost")
        .iter()
        .any(|f| matches!(f, HubFrame::Reject { code, .. } if code == "pool_unavailable")));
    assert!(!a.workers().contains_key(&WorkerId::new_unchecked("w-lost")));
}

#[test]
fn t13d_a_smoke_hub_with_no_trust_anchor_admits_an_honest_supplier_it_cannot_judge() {
    // This is the shape the daytona profile boots: `allow_anonymous`, and no digests
    // listed. The hub used to accept the socket upgrade and then close every worker
    // with `attestation_rejected` — a refusal with no repair available on the supplier
    // side, because the hub had nothing to compare the digest against. The same cell
    // pins the second half of the same supplier's path: a pack that advertises its
    // trained 262144 context joins the sample pool, because joining is by pool name
    // and no capability floor is consulted.
    let mut cfg = config();
    cfg.auth.model_shas = BTreeSet::new();
    cfg.validate()
        .expect("no anchors and no credentials is a coherent smoke hub, not a dead end");
    assert!(
        !cfg.auth.attestation_required(),
        "a hub that lists no digest has no identity rule to enforce"
    );
    let mut a = PoolActor::new(pool(), cfg);

    let mut msg = hello_msg("w-basalt", 1, 8);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.attestation = Attestation {
            model_shas: BTreeSet::from([
                "36fdc5077666f18c39552e3cba40c707f3d066119fa86b69d5d3fe632d5f89c6".to_string(),
            ]),
            weight_bytes: Some(113_491_281_169),
        };
        hello.requires.min_context_tokens = Some(262_144);
    }
    let effects = a.handle(0, msg);
    assert!(
        !sent_to(&effects, "w-basalt")
            .iter()
            .any(|f| matches!(f, HubFrame::Reject { .. })),
        "an unlisted digest and an honest 256K offer must not be refused: {:?}",
        sent_to(&effects, "w-basalt")
    );
    assert!(
        !a.workers().is_empty(),
        "the supplier joined, and a joined worker is capacity"
    );

    // Malformed is still malformed with no anchors: that is a broken supplier, not an
    // unjudged one.
    let mut broken = hello_msg("w-typo", 1, 8);
    if let ActorMsg::Hello { hello, .. } = &mut broken {
        hello.attestation = Attestation {
            model_shas: BTreeSet::from(["36fdc507".to_string()]),
            weight_bytes: None,
        };
    }
    let effects = a.handle(1, broken);
    assert!(sent_to(&effects, "w-typo")
        .iter()
        .any(|f| matches!(f, HubFrame::Reject { code, .. } if code == "attestation_rejected")));
}

#[test]
fn t14_tool_call_reasoning_and_usage_survive_the_relay_verbatim() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let chunks = [
        r#"{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"reasoning_content":"thin"},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"get_weather","arguments":"{\"ci"}}]},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\"}"}}]},"finish_reason":"tool_calls"}]}"#,
    ];
    let mut events = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        // Each frame is handled a full flush window apart, so the ordering
        // assertion below is about relay fidelity, not about batching.
        events.extend(relay_events(&a.handle(
            30 + i as u64 * 100,
            delta_msg("w-a", 1, "job-1", i as u64 + 1, chunk),
        )));
    }
    assert_eq!(
        deltas(&events),
        chunks.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
        "byte-for-byte, no re-tokenization artifacts"
    );

    let usage = json!({"prompt_tokens": 11, "completion_tokens": 7, "tool_calls": 1, "engine_specific": "kept"});
    let done = a.handle(
        430,
        frame(
            "w-a",
            1,
            WorkerFrame::Done {
                job: JobId::new_unchecked("job-1"),
                seq: 5,
                finish: FinishKind::ToolCalls,
                usage: Some(usage.clone()),
                error: None,
            },
        ),
    );
    match relay_events(&done).last().expect("a finish event") {
        ConsumerEvent::Finish {
            finish,
            usage: relayed_usage,
            ..
        } => {
            assert_eq!(*finish, FinishKind::ToolCalls);
            assert_eq!(
                relayed_usage.as_ref(),
                Some(&usage),
                "usage is relayed untouched"
            );
        }
        other => panic!("expected a finish event, got {other:?}"),
    }
    assert_eq!(a.metrics().get("nexus_tokens_total"), 7);
}

#[test]
fn t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume() {
    let image_pool = PoolId::new_unchecked("image.render/comfyui");
    let mut hub = PoolActor::with_artifact_root(image_pool.clone(), config(), "var/artifacts");
    // The worker has to *say* it is joining this pool: the reference `warm` names
    // the llm pool, which this actor does not own, and the join would be refused.
    warm_pool(&mut hub, "w-comfy", 1, 1, image_pool.as_ref(), 0);
    hub.handle(5, claim_msg("w-comfy", 1, 1));

    let effects = hub.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-img"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: "image.workflow".to_string(),
                pool: image_pool.clone(),
                tenant: TenantId::new_unchecked("acme"),
                requires: Requires::default(),
                payload: json!({"graph": "workflow-json"}),
                content_type: "application/json".to_string(),
                idempotency_key: "img-1".to_string(),
                session_hint: None,
                supplier_hint: None,
                priority: Priority::Bulk,
                idempotent: true,
                streaming: false,
                // A job-API submission survives its submitting socket (task §7.1).
                cancel_on_consumer_gone: false,
            },
        },
    );
    assert_eq!(
        assigned(&effects),
        vec![("w-comfy".to_string(), "job-img".to_string(), 1u32)]
    );
    let HubFrame::Assigned { artifact_put, .. } = sent_to(&effects, "w-comfy")[0] else {
        panic!("expected an Assigned frame carrying an upload slot");
    };
    let put = artifact_put.clone().expect("an upload slot");
    assert_eq!(put.path, "/v1/artifacts/art-job-img");
    assert_eq!(put.token, "atok-job-img-1");
    assert_eq!(put.chunk_bytes, 4 * 1024 * 1024);

    let stored = nexus_core::store::sha256_hex(b"half-one-half-two");
    hub.handle(20, stored_chunk(8, 0, false, None, None, None));

    // A digest the hub cannot confirm is surfaced in band, not swallowed (§9: the
    // declaration is measured, never trusted). It is the **first** completion attempt
    // for this artifact, because a disagreement leaves the slot open: `complete()`
    // refuses to mark an artifact whose declared bytes disagree with the hub's own
    // measurement, and an already-complete artifact takes a chunk-write refusal
    // instead. Sequencing it here is what makes the retry below the interesting part:
    // the honest declaration still completes cleanly afterwards.
    let bad = hub.handle(
        21,
        stored_chunk(
            9,
            1,
            true,
            Some(stored.clone()),
            Some(&"f".repeat(64)),
            Some(17),
        ),
    );
    assert!(relay_events(&bad)
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Error { code, .. } if code == "artifact_mismatch")));
    assert!(
        !hub.artifacts()
            .get(&nexus_core::ArtifactId::new_unchecked("art-job-img"))
            .expect("the slot survives its own refusal")
            .complete,
        "a refused declaration must not mark the artifact complete"
    );

    let done = hub.handle(
        22,
        stored_chunk(9, 1, true, Some(stored.clone()), Some(&stored), Some(17)),
    );
    let view = hub
        .job_view(&JobId::new_unchecked("job-img"))
        .expect("view");
    let artifact = view.artifact.clone().expect("artifact recorded");
    assert_eq!(artifact.size_bytes, 17);
    assert_eq!(artifact.stored_sha256.as_deref(), Some(stored.as_str()));
    assert_eq!(artifact.content_type, "image/png");
    assert!(artifact.complete);
    assert_eq!(artifact.path, "var/artifacts/job-img/art-job-img");
    // 8 + 9 from the refused attempt's chunk and 9 again from the honest retry: the
    // counter is what the worker wrote to the upload endpoint, while the book
    // deduplicates the chunk itself (the recorded total is the 17 asserted above).
    assert_eq!(hub.metrics().get("nexus_artifact_bytes_total"), 26);
    match relay_events(&done).as_slice() {
        [ConsumerEvent::Artifact { manifest, .. }] => {
            assert_eq!(manifest.sha256.as_deref(), Some(stored.as_str()));
            assert_eq!(manifest.size_bytes, Some(17));
        }
        other => panic!("expected one artifact announcement, got {other:?}"),
    }
    // An upload from a worker that no longer owns the job is refused outright.
    //
    // `w-other` has to be **connected** for this to be the ownership refusal: a
    // stranger with no live socket at all is refused one step earlier with
    // `401 stale_connection` (the §11 generation rule), which is a different answer
    // to a different question. With a live socket of its own, the only thing wrong
    // with its upload is that the lease belongs to `w-comfy`.
    warm_pool(&mut hub, "w-other", 9, 1, image_pool.as_ref(), 22);
    let stranger = hub.handle(
        23,
        ActorMsg::ArtifactStored {
            job: JobId::new_unchecked("job-img"),
            artifact: ArtifactId::new_unchecked("art-job-img"),
            worker: WorkerId::new_unchecked("w-other"),
            conn: 9,
            attempt: 1,
            chunk_index: 3,
            chunk_bytes: 1,
            complete: false,
            stored_sha256: None,
            declared: Default::default(),
            content_type: None,
        },
    );
    assert_eq!(
        rejects(&stranger),
        vec![(409, "stale_attempt".to_string(), None)],
        "a foreign worker's upload is refused, not absorbed"
    );

    // Detaching the submitting consumer must not cancel a detached job (§7.3).
    let detach = hub.handle(
        24,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-img"),
            consumer: ConsumerId::new_unchecked("c"),
        },
    );
    assert!(
        terminals(&detach).is_empty(),
        "the job keeps running so the client can poll for the artifact"
    );
}

/// One `ArtifactStored` message, with the chunk shape the runtime emits.
fn stored_chunk(
    bytes: u64,
    index: u64,
    complete: bool,
    stored: Option<String>,
    declared_sha: Option<&str>,
    declared_size: Option<u64>,
) -> ActorMsg {
    ActorMsg::ArtifactStored {
        job: JobId::new_unchecked("job-img"),
        artifact: ArtifactId::new_unchecked("art-job-img"),
        worker: WorkerId::new_unchecked("w-comfy"),
        conn: 1,
        attempt: 1,
        chunk_index: index,
        chunk_bytes: bytes,
        complete,
        stored_sha256: stored,
        declared: nexus_core::actor::DeclaredArtifact {
            sha256: declared_sha.map(str::to_string),
            size_bytes: declared_size,
        },
        content_type: if complete {
            Some("image/png".to_string())
        } else {
            None
        },
    }
}

#[test]
fn t16_large_payload_travels_by_reference_and_is_fetchable_by_its_owner() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    // The config caps inline payloads at 64 bytes.
    let big =
        json!({"model": "qwen38", "messages": [{"role": "user", "content": "x".repeat(512)}]});
    let effects = a.handle(
        10,
        submit_msg_with("job-big", "k-big", None, big.clone(), None),
    );
    let HubFrame::Assigned { payload, .. } = sent_to(&effects, "w-a")[0] else {
        panic!("expected an Assigned frame");
    };
    match payload {
        JobPayload::ByRef {
            path,
            token,
            size_bytes,
            sha256,
            content_type,
            ..
        } => {
            assert_eq!(path, "/v1/payloads/job-big");
            assert_eq!(token, "ptok-job-big-1");
            assert_eq!(
                *size_bytes, 572,
                "serialized length: 512 content chars plus JSON framing"
            );
            assert_eq!(
                sha256,
                &nexus_core::store::sha256_hex(&serde_json::to_vec(&big).expect("serialize"))
            );
            assert_eq!(content_type, "application/json");
        }
        JobPayload::Inline { .. } => panic!("a 512-char prompt must spill at a 64-byte cap"),
    }
    // The bytes are held in memory (never written to disk) and are readable only
    // by the owning worker, on the current attempt, with the right token.
    let id = nexus_core::PayloadRefId::new_unchecked("job-big");
    let entry = a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-a"),
            1,
            20,
        )
        .expect("authorized");
    let fetched: Value =
        serde_json::from_slice(&entry.bytes).expect("the spilled bytes are the payload");
    assert_eq!(fetched, big);
    assert!(a
        .payloads()
        .authorize_fetch(&id, "wrong-token", &WorkerId::new_unchecked("w-a"), 1, 20)
        .is_err());
    assert!(a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-b"),
            1,
            20
        )
        .is_err());
    assert_eq!(a.metrics().get("nexus_spill_bytes_total"), 572);
    assert_eq!(a.payloads().bytes(), 572);

    // Terminal releases the prompt immediately (the §9 retention policy).
    let done = a.handle(30, done_msg("w-a", 1, "job-big", 1, None));
    assert!(done.iter().any(|e| matches!(
        e,
        Effect::ReleasePayload {
            reference: Some(_),
            ..
        }
    )));
    assert!(a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-a"),
            1,
            31
        )
        .is_err());
    assert_eq!(a.payloads().bytes(), 0);
}

#[test]
fn t17_non_streaming_response_carries_the_same_content_as_the_stream() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let mut events = Vec::new();
    for (i, part) in ["Hel", "lo ", "wor", "ld"].iter().enumerate() {
        let chunk =
            json!({"choices": [{"index": 0, "delta": {"content": part}, "finish_reason": null}]})
                .to_string();
        events.extend(relay_events(&a.handle(
            30 + i as u64 * 100,
            delta_msg("w-a", 1, "job-1", i as u64 + 1, &chunk),
        )));
    }
    let streamed: String = deltas(&events)
        .iter()
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .filter_map(|v| {
            v["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(streamed, "Hello world");

    let usage = json!({"completion_tokens": 12});
    events.extend(relay_events(
        &a.handle(430, done_msg("w-a", 1, "job-1", 5, Some(usage.clone()))),
    ));
    let assembled = assemble_completion(&JobId::new_unchecked("job-1"), "qwen38", 430_000, &events);
    assert_eq!(
        assembled["choices"][0]["message"]["content"].as_str(),
        Some("Hello world")
    );
    assert_eq!(
        assembled["choices"][0]["finish_reason"].as_str(),
        Some("stop")
    );
    assert_eq!(assembled["usage"], usage);
    assert_eq!(assembled["object"].as_str(), Some("chat.completion"));
}

#[test]
fn t18_hub_shutdown_fails_clients_retryably_and_a_fresh_hub_resumes() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(11, submit_msg("job-2", "k2", None));

    let effects = a.handle(30, ActorMsg::Shutdown { now_ms: 30 });
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Cancel {
            reason: CancelReason::HubShutdown,
            ..
        }
    )));
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::Shutdown
        }
    )));
    let mut reasons = terminals(&effects);
    reasons.sort();
    assert_eq!(
        reasons,
        vec![
            ("job-1".to_string(), TerminalReason::HubShutdown, true),
            ("job-2".to_string(), TerminalReason::HubShutdown, true),
        ]
    );
    assert!(
        reasons.iter().all(|(_, _, retryable)| *retryable),
        "in-flight clients must get a retryable failure"
    );
    assert_eq!(a.queue_depth(), 0);
    assert!(a.is_closed());
    assert_eq!(
        rejects(&a.handle(31, submit_msg("job-3", "k3", None)))[0].0,
        503
    );
    assert_eq!(
        relay_events(&effects)
            .iter()
            .filter(|e| matches!(
                e,
                ConsumerEvent::Error {
                    retryable: true,
                    ..
                }
            ))
            .count(),
        2
    );

    // A fresh hub accepts the reconnecting worker and the retried submissions,
    // with the worker dialing in on its own (task §11, §16.18).
    let mut b = actor();
    warm(&mut b, "w-a", 9, 4, 40);
    b.handle(45, claim_msg("w-a", 9, 4));
    let first = b.handle(50, submit_msg("job-1", "k1-retry", None));
    assert_eq!(
        assigned(&first),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)]
    );
    let second = b.handle(51, submit_msg("job-2", "k2-retry", None));
    assert_eq!(
        assigned(&second),
        vec![("w-a".to_string(), "job-2".to_string(), 1u32)]
    );
}

// ----------------------------------------------------------------- beyond §16

#[test]
fn deltas_are_batched_by_size_and_age_not_per_token() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    // Nine 100-byte tokens inside one 75 ms window: nothing may go out yet.
    let mut relayed = 0;
    for seq in 1..=9 {
        relayed += deltas(&relay_events(&a.handle(
            30 + seq,
            delta_msg("w-a", 1, "job-1", seq, &"t".repeat(100)),
        )))
        .len();
    }
    assert_eq!(
        relayed, 0,
        "900 bytes is under the 1536-byte window, so zero syscalls"
    );
    // Crossing the byte threshold flushes the whole window as one batch.
    let flush = a.handle(31, delta_msg("w-a", 1, "job-1", 10, &"u".repeat(700)));
    assert_eq!(
        deltas(&relay_events(&flush)).len(),
        10,
        "one flush carries all ten deltas"
    );
    // A small tail is flushed by the age timer, not by the next token.
    let small = a.handle(40, delta_msg("w-a", 1, "job-1", 11, "tiny"));
    assert!(deltas(&relay_events(&small)).is_empty());
    let aged = a.handle(120, ActorMsg::Tick { now_ms: 120 });
    assert_eq!(deltas(&relay_events(&aged)), vec!["tiny".to_string()]);
}

#[test]
fn seq_gaps_are_reported_to_the_consumer_instead_of_swallowed() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    let effects = a.handle(140, delta_msg("w-a", 1, "job-1", 4, &"b".repeat(2_000)));
    let events = relay_events(&effects);
    assert!(
        matches!(
            events.first(),
            Some(ConsumerEvent::Gap {
                expected: 2,
                got: 4
            })
        ),
        "{events:?}"
    );
    assert_eq!(a.metrics().get("nexus_seq_gaps_total"), 1);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .seq
            .duplicates(),
        0
    );
}

#[test]
fn the_queue_has_no_admission_bound_over_budget_submissions_await() {
    let mut cfg = config();
    cfg.tenants.insert(
        "acme".to_string(),
        nexus_core::TenantConfig {
            max_concurrent: Some(64),
            ..Default::default()
        },
    );
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: nothing drains the queue, so depth shows exactly
    // what was admitted. The queue is unbounded — every submission is accepted
    // and awaits, however deep the queue already is, because the hub itself
    // has no admission rate limit.
    a.handle(0, hello_msg("w-load", 1, 4));
    for n in 1..=6 {
        let effects = a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
        assert!(
            rejects(&effects).is_empty(),
            "job-{n} must be accepted: the hub never answers 429 for capacity"
        );
    }
    assert_eq!(a.queue_depth(), 6, "all six await in the unbounded queue");
}

#[test]
fn tenant_concurrency_cap_awaits_and_releases_on_terminal() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    // Four submissions fill the tenant's four slots; the fifth is ACCEPTED —
    // the hub has no admission rate limit — and awaits in the queue.
    for n in 1..=5 {
        let effects = a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
        assert!(
            rejects(&effects).is_empty(),
            "job-{n} must be accepted: over-budget concurrency awaits, never 429s"
        );
    }
    // Four run; the fifth is queued behind the cap, not refused.
    assert_eq!(a.queue_depth(), 1, "the fifth awaits a free tenant slot");
    // One slot frees on the terminal: the awaited job is the very next thing
    // the worker pulls (§4's pull model — dispatch happens on claim).
    let done = a.handle(50, done_msg("w-a", 1, "job-1", 0, None));
    assert_eq!(terminals(&done).first().expect("terminal").0, "job-1");
    let pull = a.handle(60, claim_msg("w-a", 1, 4));
    assert_eq!(
        assigned(&pull),
        vec![("w-a".to_string(), "job-5".to_string(), 1)],
        "the freed slot dispatches the awaited fifth job"
    );
    assert_eq!(a.queue_depth(), 0);
}

#[test]
fn oversized_prompt_is_413_and_an_unregistered_kind_is_400() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    let huge = json!({"model":"qwen38","messages":[{"role":"user","content":"z".repeat(5_000)}]});
    assert_eq!(
        rejects(&a.handle(10, submit_msg_with("job-x", "kx", None, huge, None)))[0],
        (413, "payload_too_large".to_string(), None)
    );

    // Prompt-token cap, the cheap KV mitigation of §8.
    let mut cfg = config();
    cfg.limits.tenant_max_prompt_tokens = 10;
    let mut b = PoolActor::new(pool(), cfg);
    warm(&mut b, "w-a", 1, 4, 0);
    let effects = b.handle(
        10,
        submit_msg_with("job-y", "ky", None, chat_body("hi"), None),
    );
    assert_eq!(
        rejects(&effects),
        vec![(413, "prompt_too_large".to_string(), None)]
    );

    // Unknown kind: the hub refuses rather than guessing idempotency semantics.
    let mut c = actor();
    warm(&mut c, "w-a", 1, 4, 0);
    let unknown = c.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-z"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: match submit_msg("job-z", "kz", None) {
                ActorMsg::Submit { submission, .. } => Submission {
                    kind: "not.registered".to_string(),
                    ..submission
                },
                _ => unreachable!(),
            },
        },
    );
    assert_eq!(
        rejects(&unknown),
        vec![(400, "unknown_kind".to_string(), None)]
    );
}

#[test]
fn stale_connection_frames_and_closes_are_ignored() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    assert_eq!(a.leases_len(), 1);

    // The same worker id reconnects: the old socket is evicted, not doubled up.
    let welcome = a.handle(30, hello_msg("w-a", 7, 4));
    assert!(sent_to(&welcome, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::Evicted
        }
    )));
    assert_eq!(a.metrics().get("nexus_worker_evictions_total"), 1);
    assert_eq!(
        a.workers().len(),
        1,
        "one record per id (the native hub never reuses one)"
    );

    // A frame already in flight on the dead socket is dropped.
    let stale = a.handle(31, delta_msg("w-a", 1, "job-1", 1, &"x".repeat(2_000)));
    assert!(relay_events(&stale).is_empty());
    assert_eq!(a.metrics().get("nexus_stale_frames_total"), 1);
    assert_eq!(a.workers()[&WorkerId::new_unchecked("w-a")].stale_frames, 1);

    // And the dead socket closing must not disturb the live one.
    a.handle(32, gone_msg("w-a", 1));
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable());
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("intact")
            .epoch,
        7
    );
}

#[test]
fn frames_for_a_job_this_worker_no_longer_owns_are_refused_with_a_cancel() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 1, 0);
    a.handle(5, claim_msg("w-a", 1, 1));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    // Consumer cancels: the lease goes away, but the engine may still be decoding.
    a.handle(
        21,
        ActorMsg::Cancel {
            job: JobId::new_unchecked("job-1"),
            reason: CancelReason::ConsumerGone,
        },
    );
    let late = a.handle(22, delta_msg("w-a", 1, "job-1", 2, "still working"));
    assert!(
        sent_to(&late, "w-a")
            .iter()
            .any(|f| matches!(f, HubFrame::Cancel { .. })),
        "the worker is told to stop again"
    );
    assert!(
        relay_events(&late).is_empty(),
        "nothing reaches the gone consumer"
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn resume_for_a_moved_or_finished_lease_is_nacked() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, done_msg("w-a", 1, "job-1", 1, None));
    let effects = a.handle(
        40,
        frame(
            "w-a",
            1,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-1"),
                attempt: 1,
                acked_seq: 1,
            },
        ),
    );
    assert!(sent_to(&effects, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::ResumeNack { reason, .. } if !reason.is_empty())));
    assert_eq!(a.metrics().get("nexus_resume_rejected_total"), 1);
    assert_eq!(a.metrics().get("nexus_resume_accepted_total"), 0);

    // A stale attempt number is refused too, so an old generation cannot hijack
    // a retried job.
    let mut b = actor();
    warm(&mut b, "w-a", 1, 1, 0);
    warm(&mut b, "w-b", 2, 1, 0);
    b.handle(5, claim_msg("w-a", 1, 1));
    b.handle(6, claim_msg("w-b", 2, 1));
    b.handle(10, submit_msg("job-9", "k9", None));
    b.handle(20, gone_msg("w-a", 1));
    let moved = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert_eq!(
        assigned(&moved),
        vec![("w-b".to_string(), "job-9".to_string(), 2u32)]
    );
    // w-a dials back in on a new socket before trying to resume: a `Resume` stamped
    // with the generation the hub already retired is a **stale frame** and is dropped
    // without a reply (`stale_connection_frames_and_closes_are_ignored` pins that
    // rule), so the "old generation cannot hijack a retried job" race is the one this
    // assertion is about — the client reconnects and asks for the attempt it still
    // believes it owns.
    b.handle(41_550, hello_msg("w-a", 5, 1));
    let stale_resume = b.handle(
        41_600,
        frame(
            "w-a",
            5,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-9"),
                attempt: 1,
                acked_seq: 0,
            },
        ),
    );
    assert!(sent_to(&stale_resume, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::ResumeNack { .. })));
    assert_eq!(
        b.lease_of(&JobId::new_unchecked("job-9"))
            .expect("owner")
            .worker,
        WorkerId::new_unchecked("w-b"),
        "one owner survives the race"
    );
}

#[test]
fn non_idempotent_kinds_are_never_requeued_after_an_ambiguous_loss() {
    let mut cfg = config();
    cfg.kinds.insert(
        "notify.webhook".to_string(),
        nexus_core::KindConfig {
            pool: pool().to_string(),
            idempotent: false,
            streaming: true,
            ..Default::default()
        },
    );
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-hook"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: "notify.webhook".to_string(),
                pool: pool(),
                tenant: TenantId::new_unchecked("acme"),
                requires: Requires::default(),
                payload: json!({"url": "https://example.invalid/hook"}),
                content_type: "application/json".to_string(),
                idempotency_key: "hook-1".to_string(),
                session_hint: None,
                supplier_hint: None,
                priority: Priority::Default,
                idempotent: false,
                streaming: true,
                cancel_on_consumer_gone: true,
            },
        },
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-hook"))
            .expect("leased")
            .attempt,
        1
    );
    a.handle(30, gone_msg("w-a", 1));
    let effects = a.handle(31_501, ActorMsg::Tick { now_ms: 31_501 });
    assert!(
        assigned(&effects).is_empty(),
        "a side-effecting job is failed loudly, never re-run"
    );
    assert_eq!(
        terminals(&effects),
        vec![(
            "job-hook".to_string(),
            TerminalReason::NonIdempotentAmbiguous,
            false
        )]
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"non_idempotent\"}"),
        1
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only() {
    // `heartbeat_ms = 5 s` is what makes §5.1's two states separable on the
    // injected clock: dispatch pauses after three missed heartbeats (15 s of
    // silence), while a lease renewed by the last heartbeat stays alive for
    // `lease_ms` (60 s). At the reference 20 s cadence the two deadlines coincide
    // (3 x 20 s = 60 s = the lease), so "unhealthy but still leased" would be
    // unobservable rather than untrue. `Limits::validate` already insists
    // `lease_ms >= 2 x heartbeat_ms`, which is the same invariant from the other side.
    let mut cfg = config();
    cfg.limits.heartbeat_ms = 5_000;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // The lease is stamped where it is granted: `dispatch(now_ms) +
    // lease_ms_for(pool)`, i.e. 10 + 60 000. Anchoring it at the worker's last
    // heartbeat instead would shorten a lease by however long the worker had been
    // idle before the job arrived, which is not what `Welcome.lease_ms` promises.
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("l")
            .deadline_ms,
        60_010
    );

    // A heartbeat at 40 s pushes the deadline out; the job is untouched.
    a.handle(40_000, ready_msg("w-a", 1, 3));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("l")
            .deadline_ms,
        100_000
    );
    assert_eq!(a.job(&JobId::new_unchecked("job-1")).expect("r").attempt, 1);

    // Silence: after three heartbeat intervals dispatch pauses, but the lease is
    // kept until it actually expires (§5.1's "unhealthy but leased").
    let degraded = a.handle(60_001, ActorMsg::Tick { now_ms: 60_001 });
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].engine,
        EngineState::Degraded
    );
    assert!(a.lease_of(&JobId::new_unchecked("job-1")).is_some());
    assert!(!degraded
        .iter()
        .any(|e| matches!(e, Effect::Terminal { .. })));
    // `Assigned`, not `Streaming`: the phase advances to `Streaming` only when a
    // frame is relayed (`PoolActor::relay_into`, and `JobPhase::Assigned`'s own doc
    // says "leased to a worker, waiting for the first frame"). Nothing has been
    // relayed here — that is the point of the test — and `acceptance.rs:393` and
    // `:1234` assert the same `Assigned` phase for a dispatched, frame-less job.
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );

    // The lease lapses at 100 s: zero bytes delivered → requeued, and the
    // degraded worker is not handed the retry either.
    let expired = a.handle(100_000, ActorMsg::Tick { now_ms: 100_000 });
    assert!(
        a.lease_of(&JobId::new_unchecked("job-1")).is_none(),
        "the lease is gone, and no new owner yet"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Queued
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .last_worker,
        Some(WorkerId::new_unchecked("w-a"))
    );
    assert!(assigned(&expired).is_empty());
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"no_output\"}"),
        1
    );

    // The worker recovers, claims again, and retries it (attempt 2).
    let back = a.handle(100_100, ready_msg("w-a", 1, 4));
    let retried = a.handle(100_200, claim_msg("w-a", 1, 4));
    assert_eq!(assigned(&back).len() + assigned(&retried).len(), 1);
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("re-leased")
            .attempt,
        2
    );
}

#[test]
fn hard_deadline_terminates_a_stalled_job_and_cancels_the_worker() {
    let mut cfg = config();
    cfg.kinds.get_mut(KIND_CHAT).expect("kind").job_hard_cap_ms = Some(5_000);
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(
        10,
        submit_msg_with("job-1", "k1", None, chat_body("hi"), None),
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .deadline_ms,
        // `kind.job_hard_cap_ms` is measured from accept: the submit landed at
        // t = 10, so the hard cap is 10 + 5 000. The consumer's clock starts when
        // the request was accepted, not when the pool came into being.
        5_010
    );
    let effects = a.handle(5_010, ActorMsg::Tick { now_ms: 5_010 });
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Cancel {
            reason: CancelReason::Deadline,
            ..
        }
    )));
    assert_eq!(
        terminals(&effects),
        vec![("job-1".to_string(), TerminalReason::DeadlineExceeded, false)]
    );
    // The lease deadline (60 s) is later, so the tick must not double-fire it.
    assert_eq!(a.leases_len(), 0);
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_terminal_total{reason=\"deadline_exceeded\"}"),
        1
    );
}

#[test]
fn models_and_health_reflect_only_warm_capacity() {
    let mut a = actor();
    a.handle(0, hello_msg("w-load", 1, 4));
    assert!(
        a.warm_models().is_empty(),
        "a loading worker is not warm capacity"
    );
    assert_eq!(a.metrics().get("nexus_loading_workers"), 1);
    let cold = nexus_core::adapters::health_body(
        "llm.chat/qwen38",
        a.ready_workers(),
        a.loading_workers(),
        a.queue_depth(),
        a.leases_len(),
    );
    assert_eq!(cold["status"], json!("degraded"));
    assert_eq!(cold["capacity_signal"], json!("cold_start"));

    a.handle(1, ready_msg("w-load", 1, 4));
    assert_eq!(a.warm_models(), BTreeSet::from(["qwen38".to_string()]));
    assert_eq!(a.metrics().get("nexus_ready_workers"), 1);
    let warm = nexus_core::adapters::health_body(
        "llm.chat/qwen38",
        a.ready_workers(),
        a.loading_workers(),
        a.queue_depth(),
        a.leases_len(),
    );
    assert_eq!(warm["status"], json!("ok"));
    assert_eq!(
        nexus_core::adapters::models_document(&a.warm_models(), 1_700_000_000_000)["data"][0]["id"],
        json!("qwen38")
    );
}

#[test]
fn queued_streaming_jobs_receive_keepalive_pings() {
    let mut cfg = config();
    // Long enough that the grace deadline does not pre-empt the ping assertions.
    cfg.limits.no_worker_grace_ms = 60_000;
    let mut a = PoolActor::new(pool(), cfg);
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));

    let first = a.handle(10_010, ActorMsg::Tick { now_ms: 10_010 });
    assert!(first
        .iter()
        .any(|e| matches!(e, Effect::Keepalive { job } if job.as_ref() == "job-1")));
    // Not twice inside one second.
    let second = a.handle(10_400, ActorMsg::Tick { now_ms: 10_400 });
    assert!(!second.iter().any(|e| matches!(e, Effect::Keepalive { .. })));
    let third = a.handle(11_100, ActorMsg::Tick { now_ms: 11_100 });
    assert!(third.iter().any(|e| matches!(e, Effect::Keepalive { .. })));
    // Once it is assigned and streaming, the pings stop: real bytes are flowing.
    a.handle(12_000, ready_msg("w-load", 1, 4));
    a.handle(12_100, claim_msg("w-load", 1, 4));
    let streaming = a.handle(13_000, ActorMsg::Tick { now_ms: 13_000 });
    assert!(!streaming
        .iter()
        .any(|e| matches!(e, Effect::Keepalive { .. })));
}

#[test]
fn session_affinity_keeps_a_conversation_on_one_worker() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    let mut seen = Vec::new();
    for turn in 1..=6u64 {
        let effects = a.handle(
            turn * 10,
            submit_msg(&format!("job-{turn}"), &format!("k{turn}"), Some("sess-42")),
        );
        let claim_a = a.handle(turn * 10 + 1, claim_msg("w-a", 1, 4));
        let claim_b = a.handle(turn * 10 + 2, claim_msg("w-b", 2, 4));
        // Every dispatch counts, whichever message triggered it: the first turn can
        // only be handed out by the `Claim` that carries the authority, and after
        // that the retained authority lets the next `Submit` dispatch on arrival.
        let mut made = assigned(&effects);
        made.extend(assigned(&claim_a));
        made.extend(assigned(&claim_b));
        assert_eq!(made.len(), 1, "turn {turn} is dispatched exactly once");
        let (owner, job, attempt) = made[0].clone();
        assert_eq!(job, format!("job-{turn}"), "the right turn was dispatched");
        assert_eq!(attempt, 1u32, "no turn of a conversation is re-run");
        // A conversation is sequential: the answer completes before the next turn is
        // asked, which frees the slot and the tenant's concurrency budget. Leaving
        // all six turns in flight instead would bind on §8's per-worker capacity
        // (4 slots) and on `tenant_max_concurrent` before affinity had anything to
        // say about it.
        let conn = if owner == "w-a" { 1 } else { 2 };
        a.handle(
            turn * 10 + 3,
            done_msg(
                &owner,
                conn,
                &job,
                turn,
                Some(json!({"completion_tokens": turn})),
            ),
        );
        seen.extend(made);
    }
    let workers: BTreeSet<String> = seen.iter().map(|(w, _, _)| w.clone()).collect();
    assert_eq!(
        workers.len(),
        1,
        "one conversation must not hop workers, or turn 5 re-prefills 100K tokens: {seen:?}"
    );
    assert_eq!(seen.len(), 6);

    // Death of the affinity owner invalidates only that mapping (§8): the next
    // turn lands on the survivor.
    let dead = seen[0].0.clone();
    let survivor = if dead == "w-a" { "w-b" } else { "w-a" };
    let conn = if dead == "w-a" { 1 } else { 2 };
    a.handle(100, gone_msg(&dead, conn));
    let effects = a.handle(110, submit_msg("job-7", "k7", Some("sess-42")));
    a.handle(
        111,
        claim_msg(survivor, if survivor == "w-a" { 1 } else { 2 }, 4),
    );
    assert_eq!(
        assigned(&effects),
        vec![(survivor.to_string(), "job-7".to_string(), 1u32)]
    );

    // Jobs with no session hint remain open to either worker.
    let plain = a.handle(120, submit_msg("job-8", "k8", None));
    a.handle(
        121,
        claim_msg(survivor, if survivor == "w-a" { 1 } else { 2 }, 4),
    );
    assert_eq!(assigned(&plain).len(), 1);
}

#[test]
fn resume_by_continuation_is_off_by_default_and_opt_in() {
    // Default (budget 0): a mid-stream death is terminal.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(6, claim_msg("w-b", 2, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    a.handle(40, gone_msg("w-a", 1));
    let effects = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert_eq!(
        terminals(&effects).first().expect("terminal").1,
        TerminalReason::WorkerGonePartial
    );

    // Opt-in: the partial text is handed to the next owner and the consumer's
    // stream is not ended (§7.4).
    let mut b = actor();
    b.limits_mut().max_continuations = 1;
    warm(&mut b, "w-a", 1, 4, 0);
    warm(&mut b, "w-b", 2, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(6, claim_msg("w-b", 2, 4));
    b.handle(10, submit_msg("job-1", "k1", None));
    b.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    b.handle(40, gone_msg("w-a", 1));
    let after = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert!(
        relay_events(&after)
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count()
            == 0,
        "the stream stays open"
    );
    assert_eq!(
        assigned(&after),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)]
    );
    let frame = sent_to(&after, "w-b")
        .into_iter()
        .find(|f| matches!(f, HubFrame::Assigned { .. }))
        .expect("an assignment");
    match frame {
        HubFrame::Assigned {
            continuation_text, ..
        } => {
            assert_eq!(
                continuation_text.as_deref().map(str::len),
                Some(2_000),
                "the partial text travels to the new owner"
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(
        b.metrics()
            .get("nexus_lease_expiries_total{kind=\"continuation\"}"),
        1
    );
    assert_eq!(
        b.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );
}

/// §7.4's safety half: switching continuation on must not resurrect a stream whose
/// partial output is a **tool-call fragment**. The hub cannot rebuild a prompt
/// prefix out of `{"function":{"arguments":"{\"ci"}`, so the attempt stays terminal
/// even with budget available — and says why, in a log the operator can act on.
/// `continuation_allows_opaque_output` is the explicit opt-in for a pool whose
/// workers restart cleanly from an arbitrary prefix.
#[test]
fn opaque_tool_output_is_not_continued_even_when_continuation_is_on() {
    let tool_chunk = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{\"ci"}}]}}]}"#;

    let mut a = actor();
    a.limits_mut().max_continuations = 1;
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(6, claim_msg("w-b", 2, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, tool_chunk));
    a.handle(40, gone_msg("w-a", 1));
    let after = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });

    assert!(assigned(&after).is_empty(), "an opaque attempt is not re-owned: the next worker would inherit a half-written tool argument");
    assert_eq!(
        terminals(&after),
        vec![(
            "job-1".to_string(),
            TerminalReason::WorkerGonePartial,
            false
        )],
        "the refusal is the ordinary in-band terminal"
    );
    assert_eq!(
        relay_events(&after)
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count(),
        1,
        "the consumer is told on the same stream"
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"partial\"}"),
        1
    );
    assert!(
        after.iter().any(|e| matches!(e, Effect::Log { message, .. } if message.contains("opaque"))),
        "the refusal must be explainable: continuation is on, so a silent terminal looks like a bug"
    );

    // The same death with the escape hatch open: seam and all, exactly one seam.
    let mut b = actor();
    b.limits_mut().max_continuations = 1;
    b.limits_mut().continuation_allows_opaque_output = true;
    warm(&mut b, "w-a", 1, 4, 0);
    warm(&mut b, "w-b", 2, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(6, claim_msg("w-b", 2, 4));
    b.handle(10, submit_msg("job-1", "k1", None));
    b.handle(30, delta_msg("w-a", 1, "job-1", 1, tool_chunk));
    b.handle(40, gone_msg("w-a", 1));
    let opted = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });

    assert_eq!(
        assigned(&opted),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)],
        "the operator asked for the seam"
    );
    assert_eq!(
        b.metrics()
            .get("nexus_lease_expiries_total{kind=\"continuation\"}"),
        1
    );
    let HubFrame::Assigned {
        continuation_text, ..
    } = sent_to(&opted, "w-b")
        .into_iter()
        .find(|f| matches!(f, HubFrame::Assigned { .. }))
        .expect("an assignment")
    else {
        unreachable!();
    };
    assert_eq!(
        continuation_text.as_deref(),
        Some(tool_chunk),
        "the opaque prefix travels verbatim when the operator insists on it"
    );
}

/// §6's ownership rule is the **subscription**, not the wire format: an async job
/// (one the client submits through `POST /v1/jobs` and polls later) survives every
/// watcher hanging up, even when its kind happens to be a streaming one. `t05_...`
/// covers the mirror image, where the flag is `true` and the last consumer leaving
/// does cancel.
#[test]
fn an_async_job_survives_its_events_watcher_disconnecting() {
    let mut submit = submit_msg("job-async", "kasync", None);
    if let ActorMsg::Submit { submission, .. } = &mut submit {
        // A streaming kind submitted as a detached, poll-later job: the wire format
        // is SSE, the ownership contract is "this socket is not the owner".
        submission.streaming = true;
        submission.cancel_on_consumer_gone = false;
    }

    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit);
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(
        25,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("watcher"),
            from_seq: 0,
        },
    );

    let watcher_leaves = a.handle(
        28,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("watcher"),
        },
    );
    assert!(
        terminals(&watcher_leaves).is_empty(),
        "one watcher leaving must not cancel anything"
    );

    // The submitting socket goes away too, and it still is not an owner: this is
    // the case the wire format alone got wrong.
    let submitter_leaves = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("con-job-async"),
        },
    );
    assert!(
        terminals(&submitter_leaves).is_empty(),
        "a detached submission keeps running so the client can poll for the result"
    );
    assert!(
        sent_to(&submitter_leaves, "w-a")
            .iter()
            .all(|f| !matches!(f, HubFrame::Cancel { .. })),
        "no Cancel reaches the GPU"
    );
    assert_eq!(a.metrics().get("nexus_cancels_sent_total"), 0);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-async")).expect("r").phase,
        JobPhase::Assigned,
        "the job is still owned by w-a"
    );
    assert_eq!(a.leases_len(), 1, "and the worker keeps its slot");
}

#[test]
fn prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map() {
    // Two separate bounds, both deliberate (task §9's retention policy):
    //   payload TTL drops a queued prompt out of memory,
    //   job retention drops a terminal record (and with it its replay ring).
    let mut cfg = config();
    // Long enough that neither the no-worker grace nor the queue deadline fires.
    cfg.limits.no_worker_grace_ms = 600_000;
    cfg.limits.claim_deadline_ms = 600_000;
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: the job stays queued and nothing dispatches it.
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(5, claim_msg("w-load", 1, 4));
    let big =
        json!({"model": "qwen38", "messages": [{"role": "user", "content": "y".repeat(300)}]});
    a.handle(10, submit_msg_with("job-p", "kp", None, big, None));
    assert_eq!(
        a.payloads().bytes(),
        360,
        "the prompt is held in memory, not on disk"
    );

    // Payload TTL is 120 s. Sweeping it must free the bytes and say so.
    let swept = a.handle(120_010, ActorMsg::Tick { now_ms: 120_010 });
    assert!(swept.iter().any(|e| matches!(
        e,
        Effect::ReleasePayload {
            reference: Some(_),
            ..
        }
    )));
    assert_eq!(a.payloads().bytes(), 0);
    assert!(
        a.has_job(&JobId::new_unchecked("job-p")),
        "the job record survives its prompt; only the prompt expired"
    );

    // A dispatch after the prompt lapsed is refused rather than handed a body of
    // nothing, and the refusal is retryable so the client re-sends the text.
    //
    // The refusal lands on the heartbeat, not on the later `Claim`: w-load still
    // holds the authority it granted at t = 5, and `PoolActor::on_ready` drains that
    // authority the moment the engine reports ready — which is exactly what
    // `t11_a_loading_worker_never_receives_assigned` asserts for a worker with a
    // healthy prompt. The trailing `Claim` then has nothing left to dispatch.
    let refused = a.handle(120_100, ready_msg("w-load", 1, 4));
    let late_claim = a.handle(120_200, claim_msg("w-load", 1, 4));
    assert!(assigned(&refused).is_empty());
    assert!(assigned(&late_claim).is_empty());
    assert_eq!(
        terminals(&refused).first().expect("terminal").1,
        TerminalReason::QueueTimeout
    );
    assert!(
        terminals(&late_claim).is_empty(),
        "one terminal per attempt: the claim must not re-fail the job"
    );
    assert_eq!(
        relay_events(&refused)
            .iter()
            .filter(|e| matches!(
                e,
                ConsumerEvent::Error {
                    retryable: true,
                    ..
                }
            ))
            .count(),
        1,
        "the body was already open, so the failure goes out in band and says retry"
    );
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"dispatch_failed\"}"),
        1
    );

    // Retention: a terminal record is dropped after `job_retention_ms`.
    let mut b = actor();
    warm(&mut b, "w-a", 1, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(10, submit_msg("job-r", "kr", None));
    let done = b.handle(20, done_msg("w-a", 1, "job-r", 1, None));
    assert_eq!(
        terminals(&done).first().expect("terminal").1,
        TerminalReason::Completed
    );
    assert!(
        b.has_job(&JobId::new_unchecked("job-r")),
        "the record is kept for replay first"
    );
    assert_eq!(b.retention_len(), 1);
    // Retention is measured from the terminal moment (`finish_job` stamps
    // `terminal_ms + job_retention_ms`), so the record is swept at 20 + 30 000 — the
    // boundary itself, since the sweep takes everything whose deadline is `<= now`.
    let later = b.handle(30_020, ActorMsg::Tick { now_ms: 30_020 });
    // NEX-6.3's deletion is **reported**, not silent: the hub's `job_index` and relay
    // maps are keyed by job id, and `Effect::JobForgotten` is the only signal that they
    // may be pruned. It stays silent to *consumers* — the terminal event went out at
    // t = 20, and a job that is merely forgotten must not invent a second verdict.
    assert_eq!(
        forgotten(&later),
        vec!["job-r".to_string()],
        "the retention sweep deletes the record and names it"
    );
    assert!(
        relay_events(&later).is_empty() && terminals(&later).is_empty(),
        "no consumer-visible event accompanies the deletion"
    );
    assert!(!b.has_job(&JobId::new_unchecked("job-r")));
    assert_eq!(b.retention_len(), 0);
}

// ------------------------- terminal retention: the record keeps the answer, not the prompt

/// Prompt bytes the actor still **holds** for one job: the inline JSON body on the
/// record, plus the spilled copy's own bytes when the prompt travelled by reference.
///
/// This is a measurement of retained memory, not a flag the production code could set
/// to make the assertion pass. `payload_inline` *is* the retained value
/// (`model.rs:383`), and `payload_bytes` is the figure the hub charges its own spill
/// budget with — `actor.rs:1107` serializes the body once and `store.rs:243` sums those
/// same lengths — so both terms are in the unit the hub already uses for RSS
/// accounting, and a non-zero answer means the process is still paying for the prompt.
fn retained_prompt_bytes(a: &PoolActor, job: &str) -> usize {
    let record = a.job(&JobId::new_unchecked(job)).expect("record");
    let inline = record
        .payload_inline
        .as_ref()
        .map_or(0, |body| body.to_string().len());
    let spilled = record.payload_ref.as_ref().map_or(0, |reference| {
        a.payloads().payload_bytes(reference).map_or(0, <[u8]>::len)
    });
    inline + spilled
}

/// Bytes the record still keeps for §7.4's continuation draft (`partial_text`,
/// `model.rs:410`), measured the same way: the buffer's own length. Per job it is
/// capped at `partial_text_cap_bytes` (1 MiB, `limits.rs:174`), which is why it matters
/// to a resident-memory plateau.
fn retained_partial_text_bytes(a: &PoolActor, job: &str) -> usize {
    a.job(&JobId::new_unchecked(job))
        .expect("record")
        .partial_text
        .len()
}

#[test]
fn a_terminal_job_releases_its_prompt_bytes_while_it_is_still_retained_for_replay() {
    // The resident-memory defect this pins: a terminal record used to drop only its
    // **spilled** bytes (`Effect::ReleasePayload`, `actor.rs:3160`) and carried its
    // inline copy until `forget_job` 300 s later (`job_retention_ms`, `limits.rs:183`),
    // for the whole of which no reader exists.
    //
    // `config()` caps inline payloads at 64 bytes, so the ordinary harness spills and
    // never exercises the copy that leaked — the inline cap has to be raised.
    let mut cfg = config();
    cfg.limits.inline_payload_cap_bytes = 4_096;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    // 74 bytes of JSON framing around a 12-char content: the actor records the same
    // figure in `payload_bytes` (`actor.rs:1107`), so every count below is anchored to
    // it instead of being inferred from the helper.
    let body = chat_body("hello prompt");
    let submit = a.handle(
        10,
        submit_msg_with("job-mem", "k-mem", None, body.clone(), None),
    );

    // Pre-terminal half of the contract, unchanged: the worker is handed the prompt
    // verbatim, because the release happens at terminal and `dispatch` reads the field
    // before that (`actor.rs:2283`).
    let HubFrame::Assigned { payload, .. } = sent_to(&submit, "w-a")[0] else {
        panic!("expected an Assigned frame");
    };
    match payload {
        JobPayload::Inline { body: sent, .. } => assert_eq!(sent, &body),
        other => panic!("an 86-byte prompt stays inline at a 4 096-byte cap, got {other:?}"),
    }
    assert_eq!(retained_prompt_bytes(&a, "job-mem"), 86);
    assert_eq!(a.payloads().bytes(), 0, "under the cap nothing is spilled");

    a.handle(20, delta_msg("w-a", 1, "job-mem", 1, "Hello"));
    a.handle(30, delta_msg("w-a", 1, "job-mem", 2, " world"));
    assert_eq!(
        retained_partial_text_bytes(&a, "job-mem"),
        11,
        "the continuation draft is live while the job is not terminal"
    );

    let done = a.handle(40, done_msg("w-a", 1, "job-mem", 3, None));
    assert_eq!(
        terminals(&done),
        vec![("job-mem".to_string(), TerminalReason::Completed, false)]
    );

    // The record itself is still retained — that retention is what a re-attach needs,
    // and it is not what this fix touches.
    assert!(a.has_job(&JobId::new_unchecked("job-mem")));
    assert_eq!(
        a.retention_len(),
        1,
        "the retention window is armed as before"
    );
    // The two retained-byte terms are the fix's observable contract. Before it, both
    // held their pre-terminal figures here: 86 and 11.
    assert_eq!(
        retained_prompt_bytes(&a, "job-mem"),
        0,
        "a terminal record holds no prompt bytes"
    );
    assert_eq!(
        retained_partial_text_bytes(&a, "job-mem"),
        0,
        "and no continuation draft either"
    );

    // What must not move: the size and the digest are what `JobView` publishes on the
    // status and observe surfaces (`model.rs:537-539`), so releasing the bytes must not
    // rewrite the metadata a client reads back.
    let record = a.job(&JobId::new_unchecked("job-mem")).expect("record");
    assert_eq!(record.payload_bytes, 86);
    assert_eq!(
        record.payload_sha256,
        nexus_core::store::sha256_hex(&serde_json::to_vec(&body).expect("serialize"))
    );
    assert_eq!(
        a.job_view(&JobId::new_unchecked("job-mem"))
            .expect("view")
            .replay_frames,
        2,
        "the replay ring keeps the answer untouched"
    );
}

#[test]
fn a_terminal_job_still_answers_replay_join_and_status_after_its_prompt_is_dropped() {
    // The safety half: everything a late client reads after the terminal transition
    // comes from the replay ring or the derived view, never from the prompt. Driven
    // through the three real post-terminal readers — `Attach` (`actor.rs:846`), the
    // idempotency join (`actor.rs:1142`) and `Observe` (`actor.rs:934`) — with the
    // prompt already gone, so an answer that still matches is proof rather than hope.
    let mut cfg = config();
    cfg.limits.inline_payload_cap_bytes = 4_096;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    let body = chat_body("hello prompt");
    let usage = json!({"completion_tokens": 12, "prompt_tokens": 21});
    a.handle(10, submit_msg_with("job-mem", "k-mem", None, body, None));
    a.handle(20, delta_msg("w-a", 1, "job-mem", 1, "Hello"));
    a.handle(30, delta_msg("w-a", 1, "job-mem", 2, " world"));
    let done = a.handle(40, done_msg("w-a", 1, "job-mem", 3, Some(usage.clone())));
    assert_eq!(
        terminals(&done),
        vec![("job-mem".to_string(), TerminalReason::Completed, false)]
    );
    assert_eq!(retained_prompt_bytes(&a, "job-mem"), 0);
    assert_eq!(retained_partial_text_bytes(&a, "job-mem"), 0);

    // Reader 1: a consumer reconnecting from seq 0 gets the whole answer, byte for byte.
    let attach = a.handle(
        50,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-mem"),
            consumer: ConsumerId::new_unchecked("late"),
            from_seq: 0,
        },
    );
    let mut replayed: Vec<ConsumerEvent> = Vec::new();
    for effect in &attach {
        if let Effect::ReplayTo { events, lossy, .. } = effect {
            assert!(!lossy, "the ring still holds everything from seq 0");
            replayed.extend(events.clone());
        }
    }
    assert_eq!(
        replayed,
        vec![
            ConsumerEvent::Delta {
                seq: 1,
                text: "Hello".to_string(),
            },
            ConsumerEvent::Delta {
                seq: 2,
                text: " world".to_string(),
            },
            ConsumerEvent::Finish {
                seq: 3,
                finish: FinishKind::Stop,
                usage: Some(usage.clone()),
            },
        ],
        "replay after the release is the live stream's own content"
    );

    // The same events are what a non-streaming client's answer is assembled from
    // (`adapters::assemble_completion`), so the aggregate body is unchanged too.
    let assembled = assemble_completion(
        &JobId::new_unchecked("job-mem"),
        "qwen38",
        40_000,
        &replayed,
    );
    assert_eq!(
        assembled["choices"][0]["message"]["content"].as_str(),
        Some("Hello world")
    );
    assert_eq!(
        assembled["choices"][0]["finish_reason"].as_str(),
        Some("stop")
    );
    assert_eq!(assembled["usage"], usage);

    // Reader 2: a same-key submit joins the finished execution instead of re-running
    // it, and does so without touching the released prompt.
    let join = a.handle(
        60,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-late"),
            consumer: ConsumerId::new_unchecked("con-late"),
            submission: match submit_msg("job-ignored", "k-mem", None) {
                ActorMsg::Submit { submission, .. } => submission,
                _ => unreachable!(),
            },
        },
    );
    match &join[..] {
        [Effect::Accepted {
            attached,
            job,
            replayed,
            worker,
        }] => {
            assert!(attached);
            assert_eq!(
                job.as_ref(),
                "job-mem",
                "the joiner follows the finished job"
            );
            // A finished execution holds no lease, so there is no supplier to name —
            // the face must read this as "nothing to report", never as a failure.
            assert_eq!(worker, &None);
            assert_eq!(
                deltas(replayed),
                vec!["Hello".to_string(), " world".to_string()]
            );
        }
        other => panic!("expected exactly one attach effect, got {other:?}"),
    }
    assert!(
        assigned(&join).is_empty(),
        "no second execution, so no second read of a prompt that is gone"
    );
    assert_eq!(retained_prompt_bytes(&a, "job-mem"), 0);

    // Reader 3: the status/poll surface (`GET /v1/jobs/{id}`, backed by `job_view`)
    // answers with the same view it did before the release, byte for byte, with the
    // submission's size and digest intact even though the bytes are no longer held.
    let view = a.job_view(&JobId::new_unchecked("job-mem")).expect("view");
    assert_eq!(
        view,
        JobView {
            id: JobId::new_unchecked("job-mem"),
            pool: pool(),
            kind: "llm.chat".to_string(),
            tenant: TenantId::new_unchecked("acme"),
            phase: JobPhase::Finished,
            attempt: 1,
            terminal: Some(TerminalReason::Completed),
            priority: Priority::Default,
            payload_bytes: 86,
            payload_sha256: "4d2e9bfe9ac58c2c29e42e750bbfabd59350dd28dccf15fe24806646253029cf"
                .to_string(),
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524_288),
                capabilities: BTreeSet::new(),
                kv_estimate_tokens: Some(1_024),
            },
            timings: JobTimings {
                accept_ms: 10,
                dispatch_ms: Some(10),
                first_frame_ms: Some(20),
                last_frame_ms: Some(40),
            },
            acked_seq: 3,
            gaps: 0,
            duplicates: 0,
            replay_frames: 2,
            // The submitter, the `late` re-attach and the idempotent joiner above.
            consumers: 3,
            delivered_bytes: 0,
            lease: None,
            artifact: None,
            usage: Some(usage.clone()),
            finish: Some(FinishKind::Stop),
            worker_error: None,
            streaming: true,
            idempotent: true,
        },
        "the whole status view is unchanged by the release"
    );
    let snapshot = observed(&a.handle(70, ActorMsg::Observe { token: 1 }));
    assert!(
        snapshot.jobs.is_empty(),
        "and the monitoring plane lists active records only (`actor.rs:813` filters the \
         terminal ones out), so it is not a post-terminal reader of a retained prompt"
    );
}

// ------------------------------------------------ NEX-6.3 / NEX-6.6: who may keep a task

#[test]
fn a_chat_task_that_loses_its_last_consumer_is_deleted_with_its_cancel() {
    // NEX-6.3's decisive trace, resolved in code: "the task is then **deleted**: no
    // queue entry, no lease, no record, no retained stream, no idempotency entry that
    // will hand the same answer to a later stranger. A task with no attached consumer
    // has no reason to exist." The question §9.1 item 2 asks — "does the next submission
    // with the same idempotency key execute fresh, or attach to the dead execution and
    // replay `job ended: cancelled`?" — is answered **fresh**, in the same mailbox step
    // as the cancel, because the hub is single-threaded by construction.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    assert_eq!(
        a.leases_len(),
        1,
        "w-a holds the job before the socket dies"
    );

    let gone = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("con-job-1"),
        },
    );
    // The consumer-visible half is exactly what it was: NEX-6.4's order (worker first),
    // the terminal event in band.
    assert!(
        sent_to(&gone, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::ConsumerGone,
                ..
            }
        )),
        "cancel must fan out on disconnect"
    );
    assert_eq!(
        terminals(&gone),
        vec![("job-1".to_string(), TerminalReason::Cancelled, false)],
        "the consumer's stream ends in band"
    );
    // The deletion half — NEX-6.3's list, in this same step, reported so the embedder's
    // own job-id maps can go with it.
    assert_eq!(
        forgotten(&gone),
        vec!["job-1".to_string()],
        "the actor reports the deletion so the hub prunes its routing entry"
    );
    assert!(!a.has_job(&JobId::new_unchecked("job-1")), "no record");
    assert_eq!(a.leases_len(), 0, "no lease");
    assert_eq!(a.queue_depth(), 0, "no queue entry");
    assert_eq!(
        a.retention_len(),
        0,
        "no retention entry either: the job is deleted, not retired"
    );
    assert!(
        a.workers()[&WorkerId::new_unchecked("w-a")]
            .in_flight
            .is_empty(),
        "the worker holds no slot for a job that no longer exists"
    );

    // A frame still in flight from the cancelled worker is refused, never relayed into a
    // stream nobody watches, and the worker is told to stop producing.
    let late = a.handle(31, delta_msg("w-a", 1, "job-1", 2, "still producing"));
    assert!(
        relay_events(&late).is_empty(),
        "bytes that arrive after the deletion are refused, not relayed"
    );
    assert!(
        sent_to(&late, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::LeaseExpired,
                ..
            }
        )),
        "the refusal tells the worker to stop"
    );
    assert_eq!(a.metrics().get("nexus_inband_errors_total"), 1);

    // The decisive half: same idempotency key, new job id, and a **fresh** execution.
    let retry = a.handle(40, submit_msg("job-2", "k1", None));
    assert_eq!(
        accepted(&retry),
        vec![("job-2".to_string(), false, Vec::new())],
        "not attached and nothing replayed: the dead execution is not resurrected"
    );
    assert_eq!(
        a.metrics().get("nexus_idempotent_attach_total"),
        0,
        "the idempotency entry was deleted with the task, so there was nothing to attach to"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-2"))
            .expect("the retry exists")
            .attempt,
        0,
        "a brand new execution, not a continuation of the cancelled one"
    );
}

#[test]
fn a_detached_queue_without_a_subscriber_is_capped_by_the_orphan_ttl() {
    // §9.1 item 3: "a detached job is the one permitted exception [to *no consumer, no
    // task*]; cap its lifetime while it has no subscriber." The cap is
    // `detached_orphan_ttl_ms`, and because the actor runs on a virtual clock it is a
    // `Tick` statement, never a wall-clock sleep.
    let mut cfg = config();
    cfg.limits.detached_orphan_ttl_ms = 20_000;
    // Neither the no-worker grace nor the queue deadline may answer first: the deletion
    // under test has to be attributable to this cap and to nothing else.
    cfg.limits.no_worker_grace_ms = 600_000;
    cfg.limits.claim_deadline_ms = 600_000;
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: the job stays queued and nothing dispatches it.
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(5, claim_msg("w-load", 1, 4));
    let mut detached = submit_msg_with("job-d", "kd", None, chat_body(&"x".repeat(300)), None);
    if let ActorMsg::Submit { submission, .. } = &mut detached {
        // `POST /v1/jobs` semantics (NEX-6.1): this socket is not the owner, so the task
        // goes on without one.
        submission.cancel_on_consumer_gone = false;
    }
    a.handle(10, detached);
    assert_eq!(
        a.queue_depth(),
        1,
        "queued, waiting for a worker that is still loading"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-d")));
    // 300 characters of prompt is above the 64 B inline cap, so the queued body is held
    // by reference and charged to the spill budget: 374 bytes is the serialized
    // `chat_body` (56 + 300 + 18), and the deletion has to give every byte of it back.
    assert_eq!(
        a.payloads().bytes(),
        374,
        "the queued prompt occupies spill budget"
    );

    // The only socket that could ever follow this job closes at t = 20. The task survives
    // that — §8's "keep it running" — so nothing is deleted yet.
    let left = a.handle(
        20,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-d"),
            consumer: ConsumerId::new_unchecked("con-job-d"),
        },
    );
    assert!(
        forgotten(&left).is_empty(),
        "the cap runs from the detach, and 20 - 20 is inside it"
    );
    assert_eq!(a.queue_depth(), 1, "NEX-6.6 keeps the queued work alive");

    // One millisecond short of the window and the state is still there: the bound is the
    // number, not an approximation of it.
    let before = a.handle(20_019, ActorMsg::Tick { now_ms: 20_019 });
    assert!(
        forgotten(&before).is_empty(),
        "19 999 ms of a 20 000 ms window"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-d")));

    // On the boundary the whole state stops existing.
    let swept = a.handle(20_020, ActorMsg::Tick { now_ms: 20_020 });
    assert_eq!(forgotten(&swept), vec!["job-d".to_string()]);
    assert!(!a.has_job(&JobId::new_unchecked("job-d")), "no record");
    assert_eq!(a.queue_depth(), 0, "no queue entry");
    assert_eq!(
        a.retention_len(),
        0,
        "and it is not parked in retention either"
    );
    assert_eq!(
        a.payloads().bytes(),
        0,
        "the queued prompt's bytes go back to the budget"
    );
    assert_eq!(
        a.metrics().get("nexus_queue_depth"),
        0,
        "the published gauge moves with the deletion"
    );

    // The capacity probe below needs the tenant cap to be the ONLY thing
    // holding work back: bring the loading worker to ready so a slot can be
    // dispatched the moment one exists.
    a.handle(20_090, ready_msg("w-load", 1, 4));

    // Its idempotency key went with it, so the same key executes fresh: §9.1 item 2's
    // decisive question, asked of the detached shape this cap exists for. It goes first
    // because the fresh execution is itself a **live** job: once the capacity probe below
    // fills the tenant's budget, a new execution would wait in the queue and prove
    // nothing about the key.
    let retry = a.handle(20_100, submit_msg("job-e", "kd", None));
    assert_eq!(
        accepted(&retry),
        vec![("job-e".to_string(), false, Vec::new())],
        "the key is free, so a new execution starts instead of an attach"
    );

    // NEX-7.1's tenant cap is what the deletion gave back. The cap is **4** concurrent
    // jobs for `acme` (`tenant_concurrency_cap_awaits_and_releases_on_terminal` admits
    // 1..=4 and holds the 5th in the queue), so the freed slot is exactly the room
    // `job-e` plus `job-n1..=n3` now occupy: four live jobs, each dispatching the
    // moment it is submitted. Had the deleted record kept its slot, `job-n3` would
    // still be queued here; had the slot been released twice, `job-full` would
    // dispatch too.
    for n in 1..=3 {
        let id = format!("job-n{n}");
        let effects = a.handle(20_110 + n, submit_msg(&id, &format!("kn{n}"), None));
        assert_eq!(
            accepted(&effects),
            vec![(id.clone(), false, Vec::new())],
            "{id} must be admitted into the freed capacity"
        );
        assert_eq!(
            a.queue_depth(),
            0,
            "{id} dispatches immediately: the cap has room"
        );
    }
    // The hub has no admission rate limit, so the exact-slot probe is a queue
    // depth: the fifth live job is accepted and HELD by the cap — the queued
    // entry is the proof that exactly one slot was freed.
    let full = a.handle(20_200, submit_msg("job-full", "kfull", None));
    assert_eq!(
        accepted(&full),
        vec![("job-full".to_string(), false, Vec::new())],
        "the hub never refuses: over-budget concurrency awaits"
    );
    assert_eq!(
        a.queue_depth(),
        1,
        "the deleted job gave back exactly one slot: job-full is held by the cap"
    );
}

#[test]
fn a_detached_job_with_a_live_lease_outlives_the_orphan_ttl_and_stays_replayable() {
    // The restriction that makes the cap safe. Once a worker holds the lease, GPU work
    // has been committed: killing a job 25 s into a render because a poller hung up would
    // throw away the expensive part to save the bookkeeping. Claimed work is bounded by
    // `job_hard_cap_ms`, and once terminal by `job_retention_ms` — never by this clock.
    let mut cfg = config();
    cfg.limits.detached_orphan_ttl_ms = 20_000;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    let mut detached = submit_msg("job-c", "kc", None);
    if let ActorMsg::Submit { submission, .. } = &mut detached {
        submission.cancel_on_consumer_gone = false;
    }
    a.handle(10, detached);
    assert_eq!(a.leases_len(), 1, "a worker took it straight away");

    let left = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-c"),
            consumer: ConsumerId::new_unchecked("con-job-c"),
        },
    );
    assert!(
        forgotten(&left).is_empty(),
        "the lease holder is still producing: this is the case the cap must not kill"
    );
    assert!(
        sent_to(&left, "w-a")
            .iter()
            .all(|f| !matches!(f, HubFrame::Cancel { .. })),
        "no Cancel reaches the GPU"
    );
    assert_eq!(a.leases_len(), 1, "and it keeps its slot");

    // t = 25 000: past the detached cap, inside the lease (`lease_ms` 60 000) and the
    // hard cap, so the subscriber-less sweep must leave it alone.
    let held = a.handle(25_000, ActorMsg::Tick { now_ms: 25_000 });
    assert!(
        forgotten(&held).is_empty(),
        "a claimed job is bounded by `job_hard_cap_ms`, not by the subscriber-less clock"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-c"))
            .expect("the job is still known")
            .phase,
        JobPhase::Assigned,
        "still owned by w-a"
    );
    assert_eq!(a.leases_len(), 1, "the worker keeps its slot");

    // The answer arrives, and the two legitimate clocks take over: the record is retained
    // for `job_retention_ms`, and a retry with the same key attaches and replays it
    // instead of re-running the GPU — NEX-7.2's "an answer a retry must not re-run".
    a.handle(26_000, delta_msg("w-a", 1, "job-c", 1, "a frame"));
    let done = a.handle(27_000, done_msg("w-a", 1, "job-c", 2, None));
    assert_eq!(
        terminals(&done).first().expect("terminal").1,
        TerminalReason::Completed
    );
    assert!(
        forgotten(&done).is_empty(),
        "a terminal answer is retained, not deleted"
    );
    assert_eq!(
        a.retention_len(),
        1,
        "bounded by retention from the terminal moment"
    );

    let retry = a.handle(28_000, submit_msg("job-c2", "kc", None));
    let (job, attached, replayed) = accepted(&retry)
        .first()
        .expect("an Accepted effect")
        .clone();
    assert_eq!(
        job, "job-c",
        "the retry joins the execution that has the answer"
    );
    assert!(attached, "and does not start a second execution");
    assert!(
        matches!(replayed.last(), Some(ConsumerEvent::Finish { .. })),
        "the finished stream is replayed to it"
    );
    assert_eq!(a.metrics().get("nexus_idempotent_attach_total"), 1);

    // Terminal at 27 000 plus the harness's 30 000 ms retention: 57 000 is the boundary,
    // and the orphan TTL had nothing to do with any of it.
    let before = a.handle(56_999, ActorMsg::Tick { now_ms: 56_999 });
    assert!(forgotten(&before).is_empty(), "one ms short of retention");
    assert!(a.has_job(&JobId::new_unchecked("job-c")));
    let later = a.handle(57_000, ActorMsg::Tick { now_ms: 57_000 });
    assert_eq!(forgotten(&later), vec!["job-c".to_string()]);
    assert!(!a.has_job(&JobId::new_unchecked("job-c")));
    assert_eq!(a.retention_len(), 0);
}

#[test]
fn memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest() {
    // NEX-7.1 / §9.1 item 5: an empty artifact root is the operator's explicit "this
    // deployment has no scratch disk" opt-out, and bulk artifact egress is the only thing
    // this hub writes anywhere. A kind whose answer is a file can never be served, so the
    // refusal happens at **admission**: once the response body is open a clean 4xx is no
    // longer legal (§7.2), so waiting for the worker's first `PUT` would be too late.
    let mut a = PoolActor::with_artifact_root(pool(), config(), "");
    assert!(
        a.artifacts().root().is_empty(),
        "an empty root is the mode flag"
    );
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));

    // `notify.webhook` is the reference config's non-streaming kind on this pool: its
    // answer is an artifact, which is exactly the pair the mode cannot serve.
    let mut webhook = submit_msg("job-w", "kw", None);
    if let ActorMsg::Submit { submission, .. } = &mut webhook {
        submission.kind = "notify.webhook".to_string();
        submission.streaming = false;
        submission.idempotent = false;
        submission.requires = Requires::default();
    }
    let refused = a.handle(10, webhook);
    assert_eq!(
        rejects(&refused),
        vec![(400, "artifact_unavailable".to_string(), None)],
        "refused before any body byte, with the reason the operator can act on"
    );
    assert!(
        !a.has_job(&JobId::new_unchecked("job-w")),
        "refused before the record existed"
    );
    assert_eq!(a.queue_depth(), 0, "and before the queue was touched");
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"artifact_unavailable\"}"),
        1,
        "the refusal is counted under its own reason"
    );

    // A streaming kind answers on the response body, so the same mode still serves it:
    // the rule is about artifact output, not about disk in general.
    let ok = a.handle(11, submit_msg("job-s", "ks", None));
    assert_eq!(
        accepted(&ok),
        vec![("job-s".to_string(), false, Vec::new())],
        "a streaming kind needs no scratch disk"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-s")));

    // And a hub with the default root admits the artifact kind normally, which is the
    // half that keeps `PoolActor::new` deployments untouched.
    let mut d = actor();
    warm(&mut d, "w-a", 1, 4, 0);
    d.handle(5, claim_msg("w-a", 1, 4));
    let mut second = submit_msg("job-w2", "kw2", None);
    if let ActorMsg::Submit { submission, .. } = &mut second {
        submission.kind = "notify.webhook".to_string();
        submission.streaming = false;
        submission.idempotent = false;
        submission.requires = Requires::default();
    }
    assert_eq!(
        accepted(&d.handle(10, second)),
        vec![("job-w2".to_string(), false, Vec::new())],
        "with a root configured, nothing about admission changes"
    );
}

#[test]
fn the_actor_never_reports_two_owners_for_one_job() {
    // The invariant every other test leans on: exactly one live lease per job,
    // and the queue never holds a leased job.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 2, 0);
    warm(&mut a, "w-b", 2, 2, 0);
    for n in 1..=6 {
        a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    for t in 0..40u64 {
        let now = 100 + t * 13;
        a.handle(now, claim_msg("w-a", 1, 2));
        a.handle(now + 1, claim_msg("w-b", 2, 2));
        for worker in ["w-a", "w-b"] {
            let conn = if worker == "w-a" { 1 } else { 2 };
            for job in 1..=6 {
                let id = JobId::new_unchecked(format!("job-{job}"));
                if a.lease_of(&id).is_some_and(|l| l.worker.as_ref() == worker) {
                    let _ = a.handle(
                        now + 2,
                        delta_msg(worker, conn, &format!("job-{job}"), t, &"z".repeat(400)),
                    );
                    if t % 7 == 0 {
                        let _ = a.handle(
                            now + 3,
                            done_msg(worker, conn, &format!("job-{job}"), t, None),
                        );
                    }
                }
            }
        }
        let _ = a.handle(now + 5, ActorMsg::Tick { now_ms: now + 5 });
        for job in 1..=6 {
            let id = JobId::new_unchecked(format!("job-{job}"));
            let owners = a
                .workers()
                .values()
                .filter(|w| w.in_flight.contains(&id))
                .count();
            let leased = a.lease_of(&id).map(|l| l.worker.clone());
            assert!(owners <= 1, "job-{job} has {owners} owners at round {t}");
            if let Some(owner) = leased {
                assert_eq!(owners, 1, "a leased job occupies exactly one worker");
                let record = a.job(&id).expect("record");
                assert_ne!(
                    record.phase,
                    JobPhase::Queued,
                    "a leased job is not also queued"
                );
                assert_eq!(
                    record.attempt,
                    a.lease_of(&id).expect("l").attempt,
                    "the lease attempt matches the job"
                );
                assert_eq!(owner, a.lease_of(&id).expect("l").worker);
            }
        }
    }
    let in_flight: usize = a.workers().values().map(|w| w.in_flight.len()).sum();
    assert_eq!(
        a.leases_len(),
        in_flight,
        "the lease table and the worker slots agree"
    );
}

// ---------------------------------------------------------------------------
// `GET /v1/observe`'s authority: `ActorMsg::Observe` → `Effect::Observed`.
//
// The dashboard renders exactly what `PoolActor::pool_snapshot` answers with, so
// the numbers it displays are decided here, in the single owner. These two tests
// are the contract's authority half; `runtime_e2e.rs` proves the wire half.
// ---------------------------------------------------------------------------

/// The one [`Effect::Observed`] answer in a `handle` result, unwrapped.
///
/// Panicking here *is* the assertion: an `Observe` that answers with anything else
/// (or with nothing) is the bug the caller is testing for.
fn observed(effects: &[Effect]) -> PoolSnapshot {
    let mut answers: Vec<PoolSnapshot> = effects
        .iter()
        // `snapshot` is boxed so the effect enum stays small (`Effect::Observed`'s
        // doc), so the payload is read through the box.
        .filter_map(|effect| match effect {
            Effect::Observed { snapshot, .. } => Some(snapshot.as_ref().clone()),
            _ => None,
        })
        .collect();
    assert_eq!(answers.len(), 1, "one Observe, exactly one answer");
    answers.remove(0)
}

#[test]
fn observe_answers_with_the_actors_own_workers_jobs_and_leases() {
    // A one-worker pool holding one leased job: the shape the dashboard's first
    // poll is supposed to show. Times are the injected ones, so every timestamp
    // below is a literal the actor was actually handed.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 2, 0); // hello at 0, `Ready` at 1
    a.handle(5, claim_msg("w-a", 1, 2)); // authority for two jobs
    let effects = a.handle(6, submit_msg("job-1", "k1", None));
    assert_eq!(
        assigned(&effects),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)],
        "outstanding claim authority dispatches the job at submit time"
    );

    // The token travels in and comes back out unchanged. The hub correlates by it
    // because one pool may be polled by several monitors at once and the actor
    // answers in mailbox order (`state.rs`, `Hub::pool_snapshot`).
    let answered = a.handle(7, ActorMsg::Observe { token: 9_001 });
    assert_eq!(
        answered.len(),
        1,
        "an Observe mutates nothing and answers once"
    );
    match &answered[0] {
        Effect::Observed { token, .. } => assert_eq!(*token, 9_001),
        other => panic!("expected the snapshot answer, got {other:?}"),
    }
    let snap = observed(&answered);

    // The gauges are the actor's own counter methods (`ready_workers`,
    // `loading_workers`, `queue.depth`, `leases.len`), which is what stops
    // `/healthz`, `/metrics` and `/v1/observe` from answering with three lookalikes.
    assert_eq!(snap.pool, pool());
    assert_eq!(snap.ready_workers, 1);
    assert_eq!(snap.loading_workers, 0);
    assert_eq!(snap.connected_workers, 1);
    assert_eq!(snap.queue_depth, 0, "the only job is leased, not queued");
    assert_eq!(snap.leases, 1);

    // The worker row is the record, projected — including the two derived numbers
    // the UI reads: `available_slots` (the actor's, not the heartbeat's) and
    // `outstanding_claims` (2 granted minus the 1 this dispatch consumed).
    assert_eq!(
        snap.workers,
        vec![WorkerView {
            id: WorkerId::new_unchecked("w-a"),
            pools: [pool()].into_iter().collect(),
            link: LinkState::Live,
            engine: EngineState::Ready,
            epoch: 1,
            capacity: 2,
            free_slots: 2,
            free_kv_tokens: Some(524_288),
            in_flight: [JobId::new_unchecked("job-1")].into_iter().collect(),
            outstanding_claims: 1,
            available_slots: 1,
            // The last frame the actor actually received: the `Claim` at t=5. A
            // dispatch does not refresh it, because no bytes came off the wire.
            last_seen_ms: 5,
            telemetry: None,
        }]
    );

    // Jobs come back through `job_view`, i.e. joined to the lease that lives in a
    // second map — the only way "who owns this job" and "what phase is it in" can
    // agree.
    assert_eq!(snap.jobs.len(), 1);
    let job = &snap.jobs[0];
    assert_eq!(job.id, JobId::new_unchecked("job-1"));
    assert_eq!(job.pool, pool());
    assert_eq!(job.tenant, TenantId::new_unchecked("acme"));
    assert_eq!(job.phase, JobPhase::Assigned);
    assert_eq!(job.attempt, 1);
    assert_eq!(job.consumers, 1, "the one face that submitted is watching");
    assert_eq!(
        job.lease,
        Some(nexus_core::Lease {
            job: JobId::new_unchecked("job-1"),
            worker: WorkerId::new_unchecked("w-a"),
            // The lease names the connection generation it was made on (§11's
            // stale-generation guard), which is this worker's `conn`.
            epoch: 1,
            attempt: 1,
            // t=6 + the pool's effective `lease_ms` (60_000; the sample topology
            // sets no per-pool override), so the deadline is an absolute actor time.
            deadline_ms: 60_006,
            streamed_bytes: 0,
        })
    );

    // Now finish it. The record survives for `job_retention_ms` so a retry can
    // replay it, but a monitor must not be shown a finished job forever.
    a.handle(8, delta_msg("w-a", 1, "job-1", 0, "hi"));
    a.handle(9, done_msg("w-a", 1, "job-1", 1, None));
    let after = observed(&a.handle(10, ActorMsg::Observe { token: 9_002 }));
    assert!(
        a.has_job(&JobId::new_unchecked("job-1")),
        "the terminal record is still retained for replay"
    );
    assert!(
        after.jobs.is_empty(),
        "a terminal job is not monitoring state: {after:?}"
    );
    assert_eq!(after.leases, 0, "finishing dropped the lease");
    assert_eq!(after.ready_workers, 1, "the worker is still warm and ready");
    // `last_seen_ms` is deliberately not asserted here: the relay path refreshes it
    // from the worker's frames (`actor.rs:2377`), so its exact value after a `Done`
    // depends on whether that finish event carried bytes — not on the snapshot.
    assert_eq!(
        after.workers[0].in_flight.len(),
        0,
        "the slot is free again whatever the heartbeat says"
    );
    assert_eq!(
        after.workers[0].available_slots, 2,
        "capacity minus an empty in_flight is what dispatch obeys"
    );
    assert_eq!(
        after.workers[0].outstanding_claims, 1,
        "the second half of the claim is still authority the worker holds"
    );
}

#[test]
fn observe_lists_an_orphaned_worker_but_excludes_it_from_the_ready_and_connected_counts() {
    // The case a dashboard would otherwise get wrong twice over: a socket that
    // dropped mid-stream still owns its lease, and it is still resumable until
    // `resume_grace_ms` expires. So it must appear (the job has an owner), but it
    // must not be counted as capacity the hub can use.
    let mut a = actor();
    warm(&mut a, "w-b", 3, 1, 0);
    a.handle(5, claim_msg("w-b", 3, 1));
    a.handle(6, submit_msg("job-9", "k9", None));
    let before = observed(&a.handle(7, ActorMsg::Observe { token: 1 }));
    assert_eq!(before.connected_workers, 1);
    assert_eq!(before.ready_workers, 1);

    a.handle(8, gone_msg("w-b", 3));
    let after = observed(&a.handle(9, ActorMsg::Observe { token: 2 }));

    assert_eq!(
        after.workers.len(),
        1,
        "an orphan is still a record, and the UI shows it as resumable"
    );
    let worker = &after.workers[0];
    assert_eq!(worker.link, LinkState::Orphaned);
    // `orphan_worker` degrades the engine and zeroes the heartbeat's slot count
    // immediately (`actor.rs:2785-2793`), so a dropped socket is never advertised as
    // capacity — while the actor's own `in_flight` survives, because the lease still
    // names this worker and a resume must not take a second set of slots.
    assert_eq!(worker.engine, EngineState::Degraded);
    assert_eq!(worker.free_slots, 0);
    assert_eq!(worker.available_slots, 0, "nothing dispatchable is left");
    assert_eq!(
        worker.in_flight,
        [JobId::new_unchecked("job-9")].into_iter().collect(),
        "the orphan still owns the job"
    );
    // A dead socket's unfulfilled claims are void; only leased work survives.
    assert_eq!(worker.outstanding_claims, 0);
    assert_eq!(
        worker.epoch, 3,
        "the epoch is the connection generation it had"
    );
    assert_eq!(
        worker.last_seen_ms, 5,
        "the gone itself is not an inbound frame"
    );

    // The three counts a monitor sums over: an orphan is in none of them.
    assert_eq!(after.ready_workers, 0);
    assert_eq!(after.loading_workers, 0, "loading counts live sockets only");
    assert_eq!(after.connected_workers, 0);
    // The lease is held until the orphan timer fires at `8 + resume_grace_ms`, so a
    // snapshot taken right after the drop still reports it.
    assert_eq!(after.leases, 1);
    assert_eq!(after.leases, a.leases_len(), "one source, no drift");
    assert_eq!(
        after.jobs.len(),
        1,
        "the job is still active, and still names its (unreachable) owner"
    );
    assert_eq!(
        after.jobs[0].lease.as_ref().expect("lease").worker,
        WorkerId::new_unchecked("w-b")
    );
}

#[test]
fn t19_an_engine_failure_after_streaming_still_ends_the_job() {
    // The live failure this pins (2026-11-11, three jobs in a row on the field
    // hub): the worker streamed its deltas, then its engine call died — and the
    // reference supplier reports that failure with a `Done` at the job's
    // STARTING seq (`molab-basalt-runtime.py:_run_job` sends `start_seq` on every
    // error path). That seq is below every accepted delta, so the hub used to
    // refuse the Done outright: it sent the worker a Cancel, dropped the lease on
    // the ack, and left the record `streaming` forever. No reaper looks at a
    // lease-less streaming record, heartbeats keep the socket alive, and every
    // consumer — streaming or assembled — hung on the keepalive pings until it
    // gave up. The terminal declaration from the job's CURRENT lease owner ends
    // the job instead.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    a.handle(31, delta_msg("w-a", 1, "job-1", 2, "b"));

    // The engine call failed after those deltas; the supplier reports it from
    // `start_seq` with its own retryability hint.
    let done = a.handle(
        40,
        frame(
            "w-a",
            1,
            WorkerFrame::Done {
                job: JobId::new_unchecked("job-1"),
                seq: 0,
                finish: FinishKind::Error,
                usage: None,
                error: Some(nexus_core::protocol::WorkerError {
                    code: "engine_error".to_string(),
                    message: "the engine produced no SSE line within 120s".to_string(),
                    retryable: true,
                }),
            },
        ),
    );

    // The consumer gets the failure in band and exactly one terminal.
    let events = relay_events(&done);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count(),
        1,
        "the engine failure must reach the consumer in band: {events:?}"
    );
    assert_eq!(
        terminals(&done),
        vec![("job-1".to_string(), TerminalReason::Completed, false)]
    );
    // And the worker is NOT cancelled for reporting the death of its own job —
    // the old refusal sent a Cancel the worker could only ack.
    assert!(
        sent_to(&done, "w-a").is_empty(),
        "a terminal Done must not be answered with a Cancel: {:?}",
        sent_to(&done, "w-a")
    );
    assert!(a.lease_of(&JobId::new_unchecked("job-1")).is_none());
    // The lag itself is on the record for operators.
    assert!(
        done.iter().any(|e| matches!(e, Effect::Log { message, .. }
                if message.contains("accepted as terminal from the lease owner"))),
        "the lagged Done is logged, not silent: {done:?}"
    );
}

#[test]
fn t19b_a_requeued_attempts_late_done_cannot_end_the_newer_run() {
    // The half of the rule the previous test gives up: once a job has been
    // requeued, a previous attempt's late `Done` — same worker, same socket, a
    // seq below the newer attempt's accepted frames — must not end attempt 2's
    // run. This is the one case the old unconditional refusal actually protected,
    // and it is reachable without any socket change: the lease lapses while the
    // worker's engine call is still (zombie) running, the job requeues to the
    // same live socket, and the zombie's thread finishes late.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    // Attempt 1 produced nothing and stopped heartbeating: the lease lapses at
    // dispatch + lease_ms, the idempotent kind requeues (zero delivered bytes),
    // and the SAME live socket re-claims attempt 2.
    a.handle(60_100, ActorMsg::Tick { now_ms: 60_100 });
    assert!(a.lease_of(&JobId::new_unchecked("job-1")).is_none());
    // The zombie socket is still up: its next heartbeat makes it dispatchable
    // again, and its claim picks the requeued job back up on the SAME conn.
    a.handle(60_150, ready_msg("w-a", 1, 4));
    a.handle(60_200, claim_msg("w-a", 1, 1));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("reattempted")
            .attempt,
        2
    );
    // Attempt 2 streams from its own floor and adds accepted frames.
    a.handle(60_300, delta_msg("w-a", 1, "job-1", 1, &"y".repeat(2_000)));
    a.handle(60_400, delta_msg("w-a", 1, "job-1", 2, "z"));

    // Attempt 1's zombie thread wakes up and declares a clean stop from seq 0.
    let late = a.handle(60_500, done_msg("w-a", 1, "job-1", 0, None));
    assert!(
        sent_to(&late, "w-a")
            .iter()
            .any(|f| matches!(f, HubFrame::Cancel { .. })),
        "the stale attempt's Done is still refused: {:?}",
        sent_to(&late, "w-a")
    );
    assert!(terminals(&late).is_empty(), "attempt 2 is untouched");
    // And attempt 2's own finish still ends the job.
    let finish = a.handle(60_600, done_msg("w-a", 1, "job-1", 3, None));
    assert_eq!(
        terminals(&finish),
        vec![("job-1".to_string(), TerminalReason::Completed, false)]
    );
}

#[test]
fn t13e_a_foreign_model_offer_joins_and_the_job_still_finds_it() {
    let mut a = actor();
    // The basalt supplier's hello against the built-in default pool: its own
    // model label at the pack's trained context, no capabilities.
    let mut msg = hello_msg("w-basalt", 1, 8);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.requires.model = Some("localhost/basalt".to_string());
        hello.requires.min_context_tokens = Some(262_144);
        hello.requires.capabilities = BTreeSet::new();
    }
    let effects = a.handle(0, msg);
    assert!(
        !sent_to(&effects, "w-basalt")
            .iter()
            .any(|f| matches!(f, HubFrame::Reject { .. })),
        "the default-pool hello must join: {:?}",
        sent_to(&effects, "w-basalt")
    );
    a.handle(1, ready_msg("w-basalt", 1, 8));
    let effects = a.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("con-1"),
            submission: Submission {
                kind: KIND_CHAT.to_string(),
                pool: pool(),
                tenant: TenantId::new_unchecked("acme"),
                requires: Requires {
                    model: Some("localhost/basalt".to_string()),
                    min_context_tokens: Some(1_024),
                    capabilities: BTreeSet::new(),
                    kv_estimate_tokens: Some(1_024),
                },
                payload: chat_body("hi"),
                content_type: "application/json".to_string(),
                idempotency_key: "k1".to_string(),
                session_hint: None,
                supplier_hint: None,
                priority: Priority::Default,
                idempotent: true,
                streaming: true,
                cancel_on_consumer_gone: true,
            },
        },
    );
    assert!(
        !rejects(&effects)
            .iter()
            .any(|(_, code, _)| code == "capability_unsatisfied"),
        "a request the joined worker can serve is not a capability reject: {:?}",
        rejects(&effects)
    );
    let effects = a.handle(11, claim_msg("w-basalt", 1, 1));
    assert_eq!(
        assigned(&effects),
        vec![("w-basalt".to_string(), "job-1".to_string(), 1)],
        "the consumer's localhost/basalt request dispatches to the supplier that advertised it"
    );
}

// --------------------------------------------------------------------------- slot-release dispatch
//
// Regression tests for the stall diagnosed in
// `tasks/issues/task-fixing-nexus-slot-release-dispatch-stall.md`: a supplier
// slot that frees up never re-triggered the dispatch pass, so a saturated pool
// parked with queued demand in front of it.
//
// **The heartbeat pacing below is load-bearing — do not "simplify" `ready(0)` /
// `ready(1)` into `ready_msg(worker, conn, capacity)`.** A real supplier reports
// `free_slots = capacity - running`
// (`services/molab/deploy/basalt/molab-basalt-prod-nexus.py:717`), so a
// capacity-1 supplier reports **0 for the entire duration of its job** and only
// raises that number again on the next `Ready` heartbeat. That heartbeat's period
// is the cadence the hub advertises, not a client constant: `Welcome.heartbeat_ms`
// is halved by the loop wait (`molab-basalt-prod-nexus.py:703`, reading
// `heartbeat_ms` at `:725`) and clamped to `[HEARTBEAT_MIN_S, HEARTBEAT_MAX_S]` =
// [5, 60] s (`:759-766`), so this suite's `limits.heartbeat_ms = 20_000`
// (`acceptance.rs:72`) yields 10 s. `HEARTBEAT_DEFAULT_S = 20.0` (`:27`) is only
// the fallback for a missing or non-positive `heartbeat_ms`. Hub-side dispatch is
// gated on exactly that client-reported number: `WorkerRecord::available_slots()`
// is `min(capacity - in_flight, free_slots)` and `dispatchable()` requires it to
// be non-zero (`model.rs:204-215`), and `free_slots` is written only by a `Ready`
// frame (`actor.rs:1923`). So:
//   * the **busy heartbeat** `ready(0)` is what puts a worker into the state a
//     real supplier is actually in while it runs a job, and
//   * the **idle/restoring heartbeat** `ready(1)` is the only frame that lifts it
//     back out — it is therefore the frame that has to re-check parked claim
//     authority.
// The control that hides the bug is the shipped suite's own pacing: every
// pre-existing test drives `ready_msg(worker, conn, capacity)` — never fewer
// slots than capacity — so no test ever reached the busy-heartbeat state
// (issue §2 point 3, control F3).

#[test]
fn a_ready_heartbeat_that_frees_a_slot_dispatches_the_queued_job() {
    let mut a = actor();
    // One capacity-1 supplier, started exactly like the basalt client: it holds
    // live claim authority from the first frame.
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    // job-1 takes the only slot; job-2 is accepted (the hub never answers 429 for
    // capacity) and must wait in the queue.
    let first = a.handle(100, submit_msg("job-1", "k1", None));
    assert_eq!(
        assigned(&first),
        vec![("w-a".to_string(), "job-1".to_string(), 1)],
        "job-1 dispatches on the supplier's standing authority"
    );
    let second = a.handle(101, submit_msg("job-2", "k2", None));
    assert!(
        assigned(&second).is_empty(),
        "job-2 cannot dispatch: the pool's one slot is busy — {:?}",
        assigned(&second)
    );
    assert_eq!(a.queue_depth(), 1, "job-2 awaits the slot that job-1 holds");

    // Busy heartbeat: `free_slots = capacity - running = 0`, engine still Ready.
    a.handle(110, ready_msg("w-a", 1, 0));
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].free_slots,
        0,
        "a capacity-1 supplier mid-job reports zero free slots"
    );

    let done = a.handle(200, done_msg("w-a", 1, "job-1", 0, None));
    assert_eq!(
        terminals(&done),
        vec![("job-1".to_string(), TerminalReason::Completed, false)],
        "the Done is accepted and closes job-1"
    );
    assert!(
        assigned(&done).is_empty(),
        "`Done` does run a dispatch pass, but it has no authority to serve with: \
         w-a's single `Claim` row was consumed by job-1's dispatch, which drops \
         the row outright when `slots == 1` (`actor.rs:2139`, `:2147-:2149`) — so \
         `fulfil_claims` has nothing to park, and `free_slots` is irrelevant here \
         — {:?}",
        assigned(&done)
    );

    // The supplier's post-completion `claim(1)` (`_finish_job` sends it without a
    // preceding `Ready`). It is granted — the grant formula uses
    // `capacity - (in_flight + outstanding)` — and then parked, because at that
    // instant `free_slots` is still 0 and `dispatchable()` says no. Parked
    // authority is kept, never dropped (issue §2 point 1).
    let claim = a.handle(201, claim_msg("w-a", 1, 1));
    assert!(
        assigned(&claim).is_empty(),
        "the fresh authority parks rather than dispatching job-2 — {:?}",
        assigned(&claim)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "the parked authority survives the no-op pass"
    );

    // The restoring heartbeat — **and no further `Submit`.** `free_slots` goes
    // 0 → 1 and the engine was already Ready, so the transition branch in
    // `on_ready` does not fire; the pass has to run because the *gate datum*
    // changed, not because the engine did.
    let heartbeat = a.handle(210, ready_msg("w-a", 1, 1));
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].free_slots,
        1,
        "the idle heartbeat reports the slot free again"
    );
    assert_eq!(
        assigned(&heartbeat),
        vec![("w-a".to_string(), "job-2".to_string(), 1)],
        "a `Ready` that frees a slot must dispatch the queued job — the pool may \
         not wait for unrelated traffic to notice"
    );
    assert_eq!(
        a.queue_depth(),
        0,
        "the queue is drained by the restoring heartbeat alone"
    );
    assert_eq!(
        a.leases_len(),
        1,
        "exactly one lease: job-2, and only job-2"
    );

    // Post-drain idempotency (invariant 1, and §5's "a no-op pass is harmless"):
    // the widened gate fires on **every** accepted `Ready` while work is parked,
    // so its whole risk surface is "a later heartbeat re-dispatches". One more
    // ordinary heartbeat after the drain must be inert — job-2 already has its
    // owner, and the row that served it was consumed, not re-armed.
    let after = a.handle(220, ready_msg("w-a", 1, 1));
    assert!(
        assigned(&after).is_empty(),
        "a later heartbeat may not re-dispatch job-2 — {:?}",
        assigned(&after)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        0,
        "the row served by the drain is consumed exactly once"
    );
    assert_eq!(
        a.leases_len(),
        1,
        "job-2 still has exactly one owner after the extra heartbeat"
    );
}

#[test]
fn two_same_model_suppliers_finishing_together_drain_the_queue_without_new_submits() {
    let mut a = actor();
    // Issue §3's table verbatim: two capacity-1 suppliers of the same model, both
    // finishing in the same breath. This is the reported topology — demand >
    // concurrency is what puts consumers in the queue behind wave 1, after which
    // both suppliers sit inside their stale-`free_slots` window.
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    warm_and_claim(&mut a, "w-b", 2, 1, 0);
    // Proximity to the tenant cap is deliberate but tight: `config()` sets
    // `tenant_max_concurrent = 4` (`acceptance.rs:83`), which is exactly the
    // number of non-terminal jobs admitted here (job-1..job-4), and
    // `tenant_over_cap` (`actor.rs:2198-:2201`) fires only on **strictly** greater,
    // so that second dispatch gate stays inert by one unit and this test measures
    // the slot gate alone. A 5th `Submit` would flip `pick_job_for`'s skip
    // (`actor.rs:2187`) on and silently make the tenant cap a second participant.
    let s1 = a.handle(100, submit_msg("job-1", "k1", None));
    assert_eq!(
        assigned(&s1),
        vec![("w-a".to_string(), "job-1".to_string(), 1)],
        "mailbox order honours w-a's authority first"
    );
    let s2 = a.handle(101, submit_msg("job-2", "k2", None));
    assert_eq!(
        assigned(&s2),
        vec![("w-b".to_string(), "job-2".to_string(), 1)],
        "…then w-b's"
    );
    let s3 = a.handle(102, submit_msg("job-3", "k3", None));
    assert!(
        assigned(&s3).is_empty(),
        "job-3 awaits: both slots are busy — {:?}",
        assigned(&s3)
    );
    let s4 = a.handle(103, submit_msg("job-4", "k4", None));
    assert!(
        assigned(&s4).is_empty(),
        "job-4 awaits too — {:?}",
        assigned(&s4)
    );
    assert_eq!(
        a.queue_depth(),
        2,
        "both job-3 and job-4 await in the queue"
    );
    assert_eq!(a.leases_len(), 2, "wave 1 holds two leases");

    // Busy heartbeats: both suppliers now report zero free slots.
    a.handle(120, ready_msg("w-a", 1, 0));
    a.handle(120, ready_msg("w-b", 2, 0));

    // Both finish together, and neither `Done` can dispatch — but not for the
    // reason the pacing suggests. At that instant **no claim authority is
    // outstanding at all**: both wave-1 rows were consumed by the wave-1
    // dispatches, which drop the row outright when `slots == 1`
    // (`actor.rs:2139`, `:2147-:2149`). `fulfil_claims` therefore has no row to
    // park, whatever `free_slots` says. The stale `free_slots = 0` datum only
    // becomes the blocker at the post-completion `Claim`s below, which is where
    // the stall actually starts.
    let da = a.handle(200, done_msg("w-a", 1, "job-1", 0, None));
    assert_eq!(
        terminals(&da),
        vec![("job-1".to_string(), TerminalReason::Completed, false)]
    );
    let db = a.handle(200, done_msg("w-b", 2, "job-2", 0, None));
    assert_eq!(
        terminals(&db),
        vec![("job-2".to_string(), TerminalReason::Completed, false)]
    );
    assert!(
        assigned(&da).is_empty(),
        "w-a's Done has no authority to dispatch with — {:?}",
        assigned(&da)
    );
    assert!(
        assigned(&db).is_empty(),
        "…and neither has w-b's — {:?}",
        assigned(&db)
    );
    assert_eq!(
        a.queue_depth(),
        2,
        "both jobs are still queued after the Dones"
    );

    // Both post-completion `Claim`s land inside the stale window, so both park.
    let ca = a.handle(201, claim_msg("w-a", 1, 1));
    assert!(
        assigned(&ca).is_empty(),
        "w-a's fresh authority parks — {:?}",
        assigned(&ca)
    );
    let cb = a.handle(201, claim_msg("w-b", 2, 1));
    assert!(
        assigned(&cb).is_empty(),
        "…and so does w-b's — {:?}",
        assigned(&cb)
    );

    // The two restoring heartbeats are the only remaining traffic. Each one
    // raises one supplier's available slots, so each one owes exactly one
    // assignment — with no new `Submit` anywhere.
    let ha = a.handle(210, ready_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&ha),
        vec![("w-a".to_string(), "job-3".to_string(), 1)],
        "w-a's restoring heartbeat must dispatch the first queued job"
    );
    let hb = a.handle(215, ready_msg("w-b", 2, 1));
    assert_eq!(
        assigned(&hb),
        vec![("w-b".to_string(), "job-4".to_string(), 1)],
        "…and w-b's must dispatch the second"
    );
    assert_eq!(
        a.queue_depth(),
        0,
        "the queue must be empty before any further frame: a healthy, connected, \
         Ready pool with queued demand may not sit idle"
    );
    assert_eq!(
        a.leases_len(),
        2,
        "exactly the two queued jobs are now leased"
    );

    // Post-drain idempotency, same risk as test 1 and doubled here because two
    // rows were drained by two separate heartbeats: each supplier's next ordinary
    // `Ready` must be inert. Neither row may be re-armed, and no job may gain a
    // second owner (invariant 1; §5's "a no-op pass is harmless").
    let ia = a.handle(225, ready_msg("w-a", 1, 1));
    assert!(
        assigned(&ia).is_empty(),
        "w-a's next heartbeat may not re-dispatch — {:?}",
        assigned(&ia)
    );
    let ib = a.handle(225, ready_msg("w-b", 2, 1));
    assert!(
        assigned(&ib).is_empty(),
        "…and neither may w-b's — {:?}",
        assigned(&ib)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        0,
        "w-a's drained row stayed consumed"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        0,
        "w-b's drained row stayed consumed"
    );
    assert_eq!(
        a.leases_len(),
        2,
        "still two leases after the extra heartbeats: one owner per job"
    );
}

/// The fairness half of the same shape, narrowed to exactly what this
/// interleaving can prove. Both suppliers re-claim before either one raises its
/// slot count again, so two rows sit parked in the claim mailbox, and each row's
/// restoring heartbeat must feed **its own owner**. What is pinned here:
/// parked authority survives the no-op pass (`outstanding_claims` stays 1 for
/// both), it is consumed exactly once (the fed worker drops to 0 and gains no
/// second job), it is never spent on a peer's row, and the jobs handed out follow
/// the queue's class+FIFO order (`pick_job_for`, `actor.rs:2172-:2190`) under the
/// widened pass.
///
/// What this interleaving does **not** discriminate is the mailbox *order* of the
/// two rows: when w-a's heartbeat runs, w-b still reports `free_slots = 0`, so
/// w-b's row defers regardless of its position in the deque and the expected
/// effects would be identical under a LIFO pass. Deque order is discriminated by
/// `parked_claim_authority_is_granted_in_mailbox_order_when_both_suppliers_are_free`
/// below, where both suppliers are dispatchable at the same instant.
#[test]
fn a_parked_claim_survives_and_is_consumed_once_by_its_own_owner() {
    let mut a = actor();
    // Fairness half of the same shape: both suppliers re-claim before either one
    // raises its slot count again, so two rows sit parked in the claim mailbox.
    // w-a's restoring heartbeat must feed w-a and must not consume w-b's row.
    // Proximity to the tenant cap: `tenant_max_concurrent = 4`
    // (`acceptance.rs:83`) is exactly the number of non-terminal jobs admitted
    // here, and `tenant_over_cap` (`actor.rs:2198-:2201`) is strictly-greater, so
    // that gate is inert by one unit and only the slot gate is under test — a 5th
    // `Submit` would turn on the `pick_job_for` skip (`actor.rs:2187`).
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    warm_and_claim(&mut a, "w-b", 2, 1, 0);
    for n in 1..=4 {
        a.handle(
            100 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    assert_eq!(a.queue_depth(), 2, "job-3 and job-4 await a free slot");
    a.handle(120, ready_msg("w-a", 1, 0));
    a.handle(120, ready_msg("w-b", 2, 0));
    a.handle(200, done_msg("w-a", 1, "job-1", 0, None));
    a.handle(200, done_msg("w-b", 2, "job-2", 0, None));
    // w-a claims first, so `(w-a, 1)` is in front of `(w-b, 1)` in the deque.
    a.handle(201, claim_msg("w-a", 1, 1));
    a.handle(201, claim_msg("w-b", 2, 1));
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "w-a's authority is parked, not lost"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "…and so is w-b's"
    );

    let ha = a.handle(210, ready_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&ha),
        vec![("w-a".to_string(), "job-3".to_string(), 1)],
        "the front of the mailbox is served first, and served to its own owner"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].available_slots(),
        0,
        "w-a is fed, so it cannot also steal w-b's row"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "w-a's pass must leave w-b's parked row intact"
    );
    assert_eq!(a.queue_depth(), 1, "only job-4 is left queued");

    let hb = a.handle(215, ready_msg("w-b", 2, 1));
    assert_eq!(
        assigned(&hb),
        vec![("w-b".to_string(), "job-4".to_string(), 1)],
        "w-b is served by its own restoring heartbeat, in order"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        0,
        "serving a parked row consumes it"
    );
    assert_eq!(a.queue_depth(), 0, "no queued work is left stranded");
    assert_eq!(a.leases_len(), 2, "one owner per queued job");
}

/// Deque-order half of the same fairness rule (§4: authority is honoured in
/// mailbox order), in the one interleaving that can actually see it — **both**
/// suppliers hold parked rows *and* are dispatchable at the instant a single
/// `Submit` arrives, so the pass has two live rows to choose between and its
/// front/back order is what decides who is fed. Every earlier test in this group
/// parks a row while its owner reports `free_slots = 0`, where the slot gate alone
/// explains the outcome and a LIFO pass would look identical.
///
/// The ordering under test is set up explicitly, and it is load-bearing:
/// `warm_and_claim` for w-a runs **before** w-b's, so `(w-a, 1)` is the first row
/// ever pushed and therefore the front of the mailbox deque — `on_claim` appends
/// at the back (`actor.rs:2030`) and the pass pops from the front
/// (`actor.rs:2066`). The queue stays empty while both rows are parked, which is
/// also why the two heartbeats below are legitimate no-ops rather than misses:
/// `work_parked()` requires queued demand (`actor.rs:2044-:2046`), so the widened
/// `Ready` gate is off and must not fire.
#[test]
fn parked_claim_authority_is_granted_in_mailbox_order_when_both_suppliers_are_free() {
    let mut a = actor();
    // w-a's `Claim` is the first row in the mailbox; w-b's sits behind it.
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    warm_and_claim(&mut a, "w-b", 2, 1, 0);
    assert_eq!(
        a.queue_depth(),
        0,
        "no demand yet, so both rows are parked with nothing to spend"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "w-a holds live authority"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "…and so does w-b, one place behind it in the deque"
    );
    assert!(
        a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable()
            && a.workers()[&WorkerId::new_unchecked("w-b")].dispatchable(),
        "both suppliers report their slot free: the pass has a real choice to make"
    );

    // Heartbeats while the queue is empty. The widened gate's own predicate is
    // false, so neither frame may dispatch — and neither may consume a row.
    let ha = a.handle(5, ready_msg("w-a", 1, 1));
    assert!(
        assigned(&ha).is_empty(),
        "an idle heartbeat may not invent work for w-a — {:?}",
        assigned(&ha)
    );
    let hb = a.handle(6, ready_msg("w-b", 2, 1));
    assert!(
        assigned(&hb).is_empty(),
        "…nor for w-b — {:?}",
        assigned(&hb)
    );

    // One job, two dispatchable rows: mailbox order serves the **front** row. A
    // LIFO pass would answer `("w-b", "job-1", 1)`, which is the discrimination
    // test 3's interleaving cannot provide.
    let s1 = a.handle(100, submit_msg("job-1", "k1", None));
    assert_eq!(
        assigned(&s1),
        vec![("w-a".to_string(), "job-1".to_string(), 1)],
        "the front of the mailbox is granted the job first"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        0,
        "the row that was served is consumed"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "the row behind it is untouched and still parked"
    );

    // w-b's own heartbeat next: its authority is still live, but with nothing
    // queued it has nothing to spend it on.
    let hb2 = a.handle(105, ready_msg("w-b", 2, 1));
    assert!(
        assigned(&hb2).is_empty(),
        "authority without demand must not invent a job — {:?}",
        assigned(&hb2)
    );

    // The next demand goes to w-b on that parked row: `Submit` merely runs the
    // pass, the authority is w-b's, and no second `Claim` round trip is needed
    // (§5.2's rule that parked authority is honoured without another ask).
    let s2 = a.handle(110, submit_msg("job-2", "k2", None));
    assert_eq!(
        assigned(&s2),
        vec![("w-b".to_string(), "job-2".to_string(), 1)],
        "w-b's parked row feeds it the next job"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        0,
        "…and consumes the row while doing it"
    );
    assert_eq!(a.queue_depth(), 0, "nothing is left awaiting authority");
    assert_eq!(a.leases_len(), 2, "one owner per job");
}

/// Invariant 6 — `free_slots` stays a **client-reported upper bound**, and the
/// hub must never trust it in the other direction either: §6's `on_tick` drain
/// really does run the dispatch pass, and the pass must still refuse to hand out a
/// slot that no supplier ever reported. The parked state below is reached exactly
/// as in test 2 (two capacity-1 suppliers, both finishing in the same breath, both
/// re-claiming inside their stale-`free_slots` window); the difference is that the
/// only frame sent next is `ActorMsg::Tick`.
///
/// The expected outcome is **no dispatch**, which is easy to guess wrong:
/// `work_parked()` is true, so `on_tick` calls `fulfil_claims`
/// (`actor.rs:3530-:3531`), but `dispatchable()` fails on `available_slots() > 0`
/// (`model.rs:211-:215`) for both rows, so each one is re-parked untouched. The
/// sweep can only act on a *sweep-local* unblocking event (a tenant slot freed by
/// step 3/4 terminating a job in the same tick), and there is none here. The very
/// next `Ready` does dispatch — that frame is the one carrying the information,
/// which is why §5 is the primary fix and §6 only the backstop.
#[test]
fn a_tick_cannot_invent_a_slot_the_supplier_never_reported() {
    let mut a = actor();
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    warm_and_claim(&mut a, "w-b", 2, 1, 0);
    // Four admits, i.e. exactly `tenant_max_concurrent = 4` (`acceptance.rs:83`).
    // `tenant_over_cap` is strictly-greater (`actor.rs:2198-:2201`), so that second
    // gate is inert by one unit and cannot be what the Tick is reacting to; a 5th
    // `Submit` would switch on the `pick_job_for` skip (`actor.rs:2187`).
    for n in 1..=4 {
        a.handle(
            100 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    a.handle(120, ready_msg("w-a", 1, 0));
    a.handle(120, ready_msg("w-b", 2, 0));
    a.handle(200, done_msg("w-a", 1, "job-1", 0, None));
    a.handle(200, done_msg("w-b", 2, "job-2", 0, None));
    a.handle(201, claim_msg("w-a", 1, 1));
    a.handle(201, claim_msg("w-b", 2, 1));
    // The parked point: authority outstanding, demand queued, nobody dispatchable.
    assert_eq!(a.queue_depth(), 2, "job-3 and job-4 await a slot");
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].free_slots,
        0,
        "w-a's last heartbeat still says zero"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].free_slots,
        0,
        "…and so does w-b's"
    );

    // The sweep, alone. `now_ms` stays well inside the queue deadlines
    // (`claim_deadline_ms` defaults to 600 s) and the heartbeat window, so nothing
    // here is about an expiry firing.
    let tick = a.handle(211, ActorMsg::Tick { now_ms: 211 });
    assert!(
        assigned(&tick).is_empty(),
        "a Tick may not hand out a slot the supplier never reported — {:?}",
        assigned(&tick)
    );
    assert_eq!(
        a.queue_depth(),
        2,
        "the queue is exactly where the Tick left it"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "w-a's parked row survives the no-op pass"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "…and so does w-b's"
    );
    assert!(
        terminals(&tick).is_empty(),
        "nothing is close to a deadline: the Tick must not end either queued job — \
         {:?}",
        terminals(&tick)
    );

    // The restoring `Ready` is the frame that carries the slot information, and
    // the queued job moves on it immediately.
    let ready = a.handle(215, ready_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&ready),
        vec![("w-a".to_string(), "job-3".to_string(), 1)],
        "the `Ready` re-check of §5 dispatches what the sweep could not"
    );
    assert_eq!(
        a.queue_depth(),
        1,
        "w-b stays parked until it reports a slot itself"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "w-b's authority is still outstanding"
    );
}

/// Invariant 7 while the pool is parked — the state where a refused frame is also
/// a *lost* dispatch, so the guard's position in `on_ready` actually matters:
/// `is_stale()` returns before any mutation (`actor.rs:1908-:1911`, ahead of the
/// `w.free_slots` write at `:1923`), so a heartbeat from a **retired** connection
/// may not even update the slot datum, let alone raise it and wake the parked row.
///
/// The retirement is genuine rather than an invented conn id: w-a re-declares on a
/// new connection mid-test, which is the eviction path (`actor.rs:1715-:1730`) and
/// voids the old socket's claim authority. The row parked afterwards therefore
/// belongs to the live generation (conn 5), and conn 1 is a dead socket's — the
/// exact shape the `is_stale()` guard exists for.
#[test]
fn a_frame_from_a_retired_connection_cannot_wake_a_parked_claim() {
    let mut a = actor();
    warm_and_claim(&mut a, "w-a", 1, 1, 0);
    let first = a.handle(100, submit_msg("job-1", "k1", None));
    assert_eq!(
        assigned(&first),
        vec![("w-a".to_string(), "job-1".to_string(), 1)],
        "job-1 takes the only slot"
    );
    a.handle(101, submit_msg("job-2", "k2", None));
    assert_eq!(a.queue_depth(), 1, "job-2 awaits the slot");
    a.handle(110, ready_msg("w-a", 1, 0));
    a.handle(200, done_msg("w-a", 1, "job-1", 0, None));
    // Park one row on the live socket (conn 1), inside its stale-`free_slots`
    // window, so there is something for a stale frame to (wrongly) wake.
    let claim = a.handle(201, claim_msg("w-a", 1, 1));
    assert!(
        assigned(&claim).is_empty(),
        "the fresh authority parks — {:?}",
        assigned(&claim)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "w-a's row is parked, not lost"
    );

    // The socket is replaced mid-stall. Only the authority dies with the old
    // socket: engine state, capacity and the `free_slots = 0` datum carry over.
    a.handle(205, hello_msg("w-a", 5, 1));
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        0,
        "a replaced socket voids the old generation's authority (§4)"
    );
    // Fresh authority on the live generation, parked for the same slot reason.
    let reclaim = a.handle(206, claim_msg("w-a", 5, 1));
    assert!(
        assigned(&reclaim).is_empty(),
        "the new row parks too — {:?}",
        assigned(&reclaim)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "the live generation holds exactly one parked row"
    );

    // The retired socket sends the restoring heartbeat. Without the guard's
    // position this is the frame that would raise `free_slots` 0 → 1 and dispatch
    // job-2 on a socket the hub has already evicted.
    let stale = a.handle(210, ready_msg("w-a", 1, 1));
    assert!(
        assigned(&stale).is_empty(),
        "a stale connection's frame may not dispatch — {:?}",
        assigned(&stale)
    );
    assert!(
        stale.is_empty(),
        "the guard returns before any effect at all (§9 invariant 7): {:?}",
        stale
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].free_slots,
        0,
        "the stale frame may not even write the slot datum"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "the live row is untouched by the stale frame"
    );
    assert_eq!(a.queue_depth(), 1, "job-2 is still queued");

    // The live generation's own heartbeat does what the retired one could not.
    let live = a.handle(215, ready_msg("w-a", 5, 1));
    assert_eq!(
        assigned(&live),
        vec![("w-a".to_string(), "job-2".to_string(), 1)],
        "the live socket's restoring heartbeat dispatches the queued job"
    );
    assert_eq!(
        a.queue_depth(),
        0,
        "the queue drains on the live frame alone"
    );
    assert_eq!(a.leases_len(), 1, "job-2 has exactly one owner");
}

// -------------------------------------------------- consumer-directed pinning
//
// The `x-supplier-id` contract (HTTP header / `submit.supplier` on the WS face): a
// consumer may **ask** for the supplier it was connected to before. The hub queues the
// job to that supplier while its connection is live, serves it first on that supplier's
// own claim, falls back to any supplier the moment it is gone, and always answers with
// the supplier it actually connected. Nothing here is an error path: a hint that names
// nobody live is a hint the hub declines by routing normally.

#[test]
fn t20a_a_pin_runs_only_on_the_named_live_supplier() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);

    // Pinned submit while nobody holds claim authority: accepted, queued for `w-b`, and
    // with no lease yet the verdict has no supplier to name.
    let accept = a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-b")));
    assert_eq!(
        accepted(&accept),
        vec![("job-1".to_string(), false, Vec::new())]
    );
    assert_eq!(
        accepted_workers(&accept),
        vec![("job-1".to_string(), false, None)],
        "not connected yet is reported as nothing, never as a guess"
    );
    assert!(assigned(&accept).is_empty());

    // `w-a` asks for work and has the model: still nothing, because the reservation is
    // not a wall a peer can walk through.
    let peer = a.handle(20, claim_msg("w-a", 1, 2));
    assert_eq!(
        assigned(&peer),
        Vec::new(),
        "a peer never serves another supplier's pinned work"
    );
    assert_eq!(a.queue_depth(), 1, "the job waits, it is not refused");
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("record").phase,
        JobPhase::Queued
    );

    // The named supplier asks: its own claim sees the reservation first and takes it.
    let owner = a.handle(21, claim_msg("w-b", 2, 2));
    assert_eq!(
        assigned(&owner),
        vec![("w-b".to_string(), "job-1".to_string(), 1u32)],
        "the pin routes to the supplier the consumer named"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-b")
    );

    // Unpinned work after the pin is served is unaffected by it: it goes to whichever
    // supplier holds the next authority row, and each verdict names the one that actually
    // got the job. The order below is mailbox order, i.e. the same determinism t01/t02
    // pin, not a property of the pin: when `w-b`'s leftover row found no work at t = 21 it
    // went to the deferred side of the mailbox, behind `w-a`'s parked row, so `w-a` is
    // next; once `w-a` has spent a slot its own remainder re-queues behind `w-b`'s.
    let second = a.handle(30, submit_msg("job-2", "k2", None));
    assert_eq!(
        assigned(&second),
        vec![("w-a".to_string(), "job-2".to_string(), 1u32)],
        "the pin did not gate `w-a` out of the pool afterwards"
    );
    assert_eq!(
        accepted_workers(&second),
        vec![("job-2".to_string(), false, Some("w-a".to_string()))],
        "the answer names the supplier the task was connected to"
    );
    let third = a.handle(31, submit_msg("job-3", "k3", None));
    assert_eq!(
        assigned(&third),
        vec![("w-b".to_string(), "job-3".to_string(), 1u32)]
    );
    assert_eq!(
        accepted_workers(&third),
        vec![("job-3".to_string(), false, Some("w-b".to_string()))]
    );
}

#[test]
fn t20b_a_pin_waits_for_a_loading_supplier_and_runs_the_moment_it_is_ready() {
    // "Queue me to that supplier" is a wait, not a re-routing: a named supplier that is
    // connected but still warming its engine keeps the job, and a warm peer with spare
    // slots and the right model is gated out until the named one vanishes.
    let mut a = actor();
    a.handle(0, hello_msg("w-b", 2, 4));
    a.handle(1, claim_msg("w-b", 2, 4));
    warm(&mut a, "w-a", 1, 4, 2);

    a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-b")));
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].engine,
        EngineState::Loading,
        "the named supplier is live but cannot serve yet"
    );

    // The warm peer cannot steal it.
    let peer = a.handle(11, claim_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&peer),
        Vec::new(),
        "a live pin holds the job out of a peer's reach even while the owner is loading"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("record").phase,
        JobPhase::Queued
    );

    // The engine reports ready and drains the authority `w-b` granted at t = 1: the wait
    // ends on that heartbeat, with no new claim and no new submit.
    let ready = a.handle(20, ready_msg("w-b", 2, 4));
    assert_eq!(
        assigned(&ready),
        vec![("w-b".to_string(), "job-1".to_string(), 1u32)],
        "the dispatch is caused by the named supplier becoming able"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-b")
    );
}

#[test]
fn t20c_an_unknown_or_malformed_supplier_hint_routes_normally_and_is_never_refused() {
    // The optional contract from the consumer's side: an id this hub never issued — a
    // typo, another replica's id, a traversal-shaped string a hostile client sent — is
    // worth an answer, never an error, and the answer says who really served it.
    let mut a = actor();
    warm_and_claim(&mut a, "w-a", 1, 4, 0);

    let unknown = a.handle(
        10,
        submit_pinned_msg("job-unknown", "k-u", Some("wkr-never-minted")),
    );
    assert!(
        rejects(&unknown).is_empty(),
        "a bad hint can never reject the submission"
    );
    assert_eq!(
        assigned(&unknown),
        vec![("w-a".to_string(), "job-unknown".to_string(), 1u32)],
        "no live supplier means no pin: ordinary model routing"
    );
    assert_eq!(
        accepted_workers(&unknown),
        vec![("job-unknown".to_string(), false, Some("w-a".to_string()))],
        "the consumer is answered with the supplier it actually got"
    );

    let malformed = a.handle(
        11,
        submit_pinned_msg("job-malformed", "k-m", Some("../etc")),
    );
    assert!(
        rejects(&malformed).is_empty(),
        "a malformed id is not a malformed frame"
    );
    assert_eq!(
        assigned(&malformed),
        vec![("w-a".to_string(), "job-malformed".to_string(), 1u32)]
    );
    assert_eq!(
        accepted_workers(&malformed),
        vec![("job-malformed".to_string(), false, Some("w-a".to_string()))]
    );
    // Neither submission cost the pool a worker or a queue entry.
    assert_eq!(
        a.workers().len(),
        1,
        "an unknown id does not materialise a supplier"
    );
    assert_eq!(a.queue_depth(), 0, "both were served");
}

#[test]
fn t20d_losing_the_pinned_supplier_dispatches_the_job_on_that_very_event() {
    // The relaxation must be an **event**, not a poll: a disappearing supplier takes its
    // pins with it, and the peers' parked claim authority is not re-examined by the loss
    // handling on its own. Without the pass on this transition the job would sit until an
    // unrelated message woke the actor.
    let mut a = actor();
    warm_and_claim(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 1);

    a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-b")));
    assert_eq!(
        a.queue_depth(),
        1,
        "`w-a`'s parked authority cannot take it while `w-b` is live"
    );

    let gone = a.handle(20, gone_msg("w-b", 2));
    assert_eq!(
        assigned(&gone),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)],
        "the socket close is itself the dispatch attempt"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-a")
    );
    // Relaxation is a gate, not a rewrite: the record keeps the (now lapsed) hint, so a
    // later join or observe still shows what the consumer asked for.
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("record")
            .supplier_hint
            .as_ref()
            .map(|w| w.to_string()),
        Some("w-b".to_string()),
        "the reservation lapses at the gate; the consumer's ask is never erased"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].link,
        LinkState::Orphaned,
        "inside `resume_grace_ms` the supplier is still known, just not live"
    );
}

#[test]
fn t20e_an_unpinned_submit_is_the_pre_pin_verdict_and_keeps_mailbox_order() {
    // The regression guard for the whole feature: with no hint, the verdict is the same
    // effect, byte for byte, including the new field staying absent (`worker: None`), and
    // the two-pass scan cannot reorder a single claim.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    let effects = a.handle(10, submit_msg("job-1", "k1", None));
    assert_eq!(
        effects,
        vec![Effect::Accepted {
            job: JobId::new_unchecked("job-1"),
            attached: false,
            replayed: Vec::new(),
            worker: None,
        }],
        "an absent hint changes nothing on the wire or in the effect stream"
    );

    for (first, first_conn, second, second_conn) in
        [("w-a", 1u64, "w-b", 2u64), ("w-b", 2, "w-a", 1)]
    {
        let mut b = actor();
        warm(&mut b, "w-a", 1, 4, 0);
        warm(&mut b, "w-b", 2, 4, 0);
        // Built by the pin helper with `None`, so the only difference from the pinned
        // tests is the hint itself.
        b.handle(10, submit_pinned_msg("job-1", "k1", None));
        let first_effects = b.handle(20, claim_msg(first, first_conn, 1));
        let second_effects = b.handle(20, claim_msg(second, second_conn, 1));
        assert_eq!(
            assigned(&first_effects),
            vec![(first.to_string(), "job-1".to_string(), 1u32)],
            "{first} asked first and won"
        );
        assert_eq!(
            assigned(&second_effects),
            Vec::new(),
            "the reservation pass must not hand the same job out twice"
        );
    }
}

#[test]
fn t20f_pinned_work_is_served_ahead_of_an_earlier_generic_job_by_one_claim() {
    // Requirement 4, and the reason pass 1 exists at all. `job-old` is first in line and
    // every supplier may run it; `job-pin` arrived later but only `w-a` may run it. Under
    // a plain FIFO scan, `w-a`'s first slot goes to work anyone else could have run and
    // the reserved job starves behind it — with a 4-slot claim that is four dispatches
    // before the promise is kept.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-old", "k-old", None));
    a.handle(11, submit_pinned_msg("job-pin", "k-pin", Some("w-a")));
    let effects = a.handle(20, claim_msg("w-a", 1, 2));
    assert_eq!(
        assigned(&effects),
        vec![
            ("w-a".to_string(), "job-pin".to_string(), 1u32),
            ("w-a".to_string(), "job-old".to_string(), 1u32),
        ],
        "the reservation first, then the ordinary class-then-FIFO head of line, in one pass"
    );
    assert_eq!(a.leases_len(), 2);

    // The control: the same two submissions with no hint come out in plain arrival
    // order, so the reorder above is the pin and not the two-pass scan being greedy.
    let mut b = actor();
    warm(&mut b, "w-a", 1, 4, 0);
    b.handle(10, submit_msg("job-old", "kb-old", None));
    b.handle(11, submit_pinned_msg("job-pin", "kb-pin", None));
    let plain = b.handle(20, claim_msg("w-a", 1, 2));
    assert_eq!(
        assigned(&plain),
        vec![
            ("w-a".to_string(), "job-old".to_string(), 1u32),
            ("w-a".to_string(), "job-pin".to_string(), 1u32),
        ],
        "an absent hint keeps the pre-feature queue order"
    );
}

#[test]
fn t20g_a_pin_on_a_supplier_that_cannot_serve_the_model_waits_rather_than_moving() {
    // Composition is by AND, never by override: a live pin on a supplier that does not
    // offer the requested model does not force a mismatched dispatch, and it does not
    // quietly fall back to a supplier that could. The job waits, and the ordinary
    // `dispatch_deadline_ms` answers it — no new reason code, no new effect.
    let mut a = actor();
    warm_model(&mut a, "w-b", 2, 4, "llama4", 0);
    warm(&mut a, "w-a", 1, 4, 0);

    let accept = a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-b")));
    assert_eq!(
        accepted(&accept),
        vec![("job-1".to_string(), false, Vec::new())]
    );
    assert!(
        assigned(&accept).is_empty(),
        "a pin never forces a mismatched dispatch"
    );

    // Neither the supplier that could serve it nor the pinned one can move it.
    let right_model = a.handle(11, claim_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&right_model),
        Vec::new(),
        "the capable peer is gated by the live pin, not by the model"
    );
    let pinned = a.handle(12, claim_msg("w-b", 2, 1));
    assert_eq!(
        assigned(&pinned),
        Vec::new(),
        "the pinned supplier is gated by the model, not by the pin"
    );
    assert_eq!(a.queue_depth(), 1);
    assert_eq!(a.leases_len(), 0, "nothing is leased to anyone");
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("record").phase,
        JobPhase::Queued,
        "the job is already `Accepted`, so the body is open and the answer must be in band"
    );

    // The wait is bounded by the deadline the job already had (600 s default).
    let late = a.handle(
        10 + 600_001,
        ActorMsg::Tick {
            now_ms: 10 + 600_001,
        },
    );
    assert_eq!(
        relay_events(&late),
        vec![ConsumerEvent::Error {
            code: "queue_timeout".to_string(),
            message: "no capable worker claimed this job in time; the response is complete as far as it goes".to_string(),
            retryable: true,
            through_seq: Some(0),
        }],
        "the stream ends in band with the pre-existing reason code"
    );
    assert_eq!(
        terminals(&late),
        vec![("job-1".to_string(), TerminalReason::QueueTimeout, true)]
    );
    assert!(
        rejects(&late).is_empty(),
        "the status was spent when `Accepted` was emitted, so no 503 can follow"
    );
}

#[test]
fn t20h_a_dead_pinned_supplier_releases_the_job_to_the_supplier_that_can_serve_it() {
    // Same world as t20g, and the resolution the consumer actually wants: the pinned
    // supplier disappears, the model-eligible peer picks the job up on that event, and it
    // is the peer that then answers the consumer.
    let mut a = actor();
    warm_model(&mut a, "w-b", 2, 4, "llama4", 0);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-b")));
    a.handle(11, claim_msg("w-a", 1, 1));
    assert_eq!(a.queue_depth(), 1, "the live pin holds it back");

    let gone = a.handle(20, gone_msg("w-b", 2));
    assert_eq!(
        assigned(&gone),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)],
        "with the pin lapsed the capable supplier is unblocked immediately"
    );
    assert_eq!(a.queue_depth(), 0);
}

#[test]
fn t20i_the_verdict_precedes_the_body_and_names_the_same_supplier() {
    // Both planes answer from one actor turn: the consumer's `Accepted` (HTTP
    // `x-supplier-id` header, WS `accepted.supplier`) and the supplier's `Assigned` name
    // the same connection, and the verdict still comes first (NEX-3.3).
    let mut a = actor();
    warm_and_claim(&mut a, "w-a", 1, 4, 0);
    let effects = a.handle(10, submit_pinned_msg("job-1", "k1", Some("w-a")));
    assert_eq!(
        accepted_workers(&effects),
        vec![("job-1".to_string(), false, Some("w-a".to_string()))]
    );
    match &effects[..] {
        [Effect::Accepted { job, worker, .. }, Effect::SendWorker {
            worker: sent,
            frame,
        }] => {
            assert_eq!(job.as_ref(), "job-1");
            assert_eq!(worker.as_ref(), Some(&WorkerId::new_unchecked("w-a")));
            assert_eq!(
                sent.as_ref(),
                "w-a",
                "the verdict and the body name one supplier, so a client that pins the \
                 id it was answered with reaches the connection that is running its job"
            );
            assert!(matches!(
                frame.as_ref(),
                HubFrame::Assigned { job, .. } if job.as_ref() == "job-1"
            ));
        }
        other => panic!("expected [Accepted, SendWorker(Assigned)], got {other:?}"),
    }
}

/// A session string whose rendezvous target is **exactly** `want`, over the pool `a` holds
/// right now.
///
/// t20j needs the session hint and the supplier hint to name *different* workers, and
/// `affinity_target` is a rendezvous hash of the session against the whole dispatchable
/// pool — no fixture can state "the hash owner is this one" without knowing the hash. So
/// the test searches short candidates until one lands where it wants, the same probe
/// `scheduler.rs:409-422` uses. It is deterministic rather than lucky: the hash is a pure
/// function of `(session, worker set)`, so the candidate found here is the same candidate
/// on every run, and the assertion inside the helper is what stops a pool change from
/// quietly turning t20j into a test of two hints that happen to agree.
fn session_hashing_to(a: &PoolActor, want: &str) -> String {
    let target = WorkerId::new_unchecked(want);
    (0u32..1_024)
        .map(|i| format!("sess-pin-{i}"))
        .find(|session| {
            nexus_core::scheduler::affinity_target(session, a.workers()).as_ref() == Some(&target)
        })
        .unwrap_or_else(|| {
            panic!(
                "no candidate session hashes to {want} over {:?}",
                a.workers().keys().collect::<Vec<_>>()
            )
        })
}

#[test]
fn t20j_a_supplier_pin_outranks_a_session_hint_that_disagrees_with_it() {
    // Both hints, pointing apart, on one record — the state every other pin test avoids.
    //
    // THE DEADLOCK THIS PINS. Before `affinity_allows` consulted `live_pin`, a record with
    // `session_hint = Some(S)` hashing to `V` and a live `supplier_hint = Some(W)`, `V != W`,
    // was leasable by **nobody**: `V` passed affinity and was refused by the pin; `W` was
    // refused by affinity, which short-circuits before the pin is consulted; every third
    // supplier was refused by affinity. `pick_job_for` returned `None` for every claimant on
    // every event, and neither relaxation pass could help, because both links stayed healthy
    // — so the job sat until `dispatch_deadline_ms` answered it `QueueTimeout` (the
    // arithmetic t20g pins) instead of being served.
    //
    // The fix ranks the two signals: an explicit, live pin outranks the derived hash **for
    // the pin's own owner only**. Legs below: (i) the pin owner serves it while both gates
    // are live and the hash owner is still refused; (ii) control — when the pin lapses, the
    // job goes to the session's hash owner, not to an arbitrary peer; (iii) `serves_model`
    // is never outranked by the new arm.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    // The premise, stated rather than assumed: the hash wants `w-a`, the consumer pinned
    // `w-b`. Both are live, warm and serve the model, so nothing but the two gates is
    // holding the job back.
    let session = session_hashing_to(&a, "w-a");

    // ---- (i) the pin owner leases it while the hash owner is still refused ----
    let accept = a.handle(
        10,
        submit_pinned_session_msg("job-1", "k1", Some("w-b"), Some(&session)),
    );
    assert_eq!(
        accepted(&accept),
        vec![("job-1".to_string(), false, Vec::new())],
        "a both-hints submission is admitted like any other"
    );
    assert_eq!(
        accepted_workers(&accept),
        vec![("job-1".to_string(), false, None)],
        "nobody leased it on the submit turn, so the verdict names nobody"
    );

    // The rendezvous owner asks first. Affinity alone would hand it the job; the live pin
    // still refuses it, which is the half that must NOT be relaxed by the new arm.
    let hash_owner = a.handle(20, claim_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&hash_owner),
        Vec::new(),
        "the new arm releases only the pin's own owner; the hash owner is still gated out \
         by the pin, so a pin keeps being a reservation and not a preference"
    );
    assert_eq!(a.queue_depth(), 1, "the job waits, it is not refused");

    // The pin owner asks. This is the arm: affinity used to refuse it before the pin was
    // even consulted, and with it the whole both-hints record was unreachable.
    let owner = a.handle(21, claim_msg("w-b", 2, 1));
    assert_eq!(
        assigned(&owner),
        vec![("w-b".to_string(), "job-1".to_string(), 1u32)],
        "the supplier the consumer named serves the job, however the hash ranks the pool"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-b")
    );
    assert_eq!(a.queue_depth(), 0);

    // ---- (ii) control: the pin lapses, and affinity re-asserts on that event ----
    //
    // The same two hints, but `w-b` disappears instead of being asked. `live_pin` goes `None`
    // with the link flip, so the record is ordinary affinity work again, and the relaxation
    // pass `pins_held_by` arms on that transition hands it to `w-a` — the worker the hash
    // names, which is what keeps the prompt cache the session hint exists for. An arbitrary
    // third peer taking it would be a different (and wrong) story, and t20d already shows the
    // same handover with a single hint.
    let mut b = actor();
    warm_and_claim(&mut b, "w-a", 1, 4, 0);
    warm(&mut b, "w-b", 2, 4, 1);
    let session_b = session_hashing_to(&b, "w-a");
    b.handle(
        10,
        submit_pinned_session_msg("job-1", "k1", Some("w-b"), Some(&session_b)),
    );
    assert_eq!(
        b.queue_depth(),
        1,
        "`w-a` holds parked claim authority and still cannot take it while the pin is live"
    );

    let gone = b.handle(20, gone_msg("w-b", 2));
    assert_eq!(
        b.workers()[&WorkerId::new_unchecked("w-b")].link,
        LinkState::Orphaned,
        "the pin lapses on a worker the pool still knows, so this leg exercises the \
         liveness test rather than an id lookup miss"
    );
    assert_eq!(
        assigned(&gone),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)],
        "the session's rendezvous owner leases it on the very event that lapsed the pin"
    );
    assert_eq!(
        b.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-a"),
        "the hash owner, not an arbitrary peer"
    );
    assert_eq!(
        b.job(&JobId::new_unchecked("job-1"))
            .expect("record")
            .supplier_hint
            .as_ref()
            .map(|w| w.to_string()),
        Some("w-b".to_string()),
        "lapsed is not erased: the relaxation is a gate, and the consumer's ask is still \
         what a later observe reports"
    );

    // ---- (iii) the new arm never outranks `serves_model` ----
    //
    // Same disagreement, but the pinned supplier does not offer the requested model. The arm
    // does let `w-b` past `affinity_allows` here — and the model gate refuses the mismatch one
    // term later in `worker_may_lease`, so the job is leased to nobody rather than dispatched
    // to a supplier that cannot serve it. `w-a` is the capable supplier and the affinity owner,
    // and the live pin still reserves the job from it: composition stays by `AND`.
    let mut c = actor();
    warm(&mut c, "w-a", 1, 4, 0);
    warm_model(&mut c, "w-b", 2, 4, "llama4", 0);
    let session_c = session_hashing_to(&c, "w-a");
    c.handle(
        10,
        submit_pinned_session_msg("job-1", "k1", Some("w-b"), Some(&session_c)),
    );
    let pinned = c.handle(11, claim_msg("w-b", 2, 1));
    assert_eq!(
        assigned(&pinned),
        Vec::new(),
        "the pin owner passed affinity and is still refused by the model: a pin cannot \
         force a mismatched dispatch (t20g's rule, now proven with both hints live)"
    );
    let capable = c.handle(12, claim_msg("w-a", 1, 1));
    assert_eq!(
        assigned(&capable),
        Vec::new(),
        "the affinity owner is refused by the live pin, exactly as in leg (i)"
    );
    assert_eq!(c.queue_depth(), 1, "the job waits for its supplier");
    assert_eq!(
        c.leases_len(),
        0,
        "and it is leased to nobody in the meantime"
    );

    // The wait is still the pre-existing bounded one: no new clock, no new reason code.
    let late = c.handle(
        10 + 600_001,
        ActorMsg::Tick {
            now_ms: 10 + 600_001,
        },
    );
    assert_eq!(
        terminals(&late),
        vec![("job-1".to_string(), TerminalReason::QueueTimeout, true)],
        "a both-hints refusal is answered by the deadline the job already had"
    );
}

/// The tenant concurrency cap is one of the `AND` terms in `worker_may_lease`
/// (`actor.rs:2306-2320`) and the one a **pin owner** can be refused by while every other
/// term is in its favour: same model, `Live` link, ready engine, free slots, live claim
/// authority, and a reservation that names it. No shipped test paired the pin with the cap,
/// so "the cap still binds the pin owner" was resting on a reading of that one line.
///
/// WHY IT MUST BIND. A pin is a routing promise — it says **which** supplier may serve, never
/// **how many** of the tenant's tasks may run at once. Lifting the cap for the owner would
/// let any consumer that pins itself to one supplier outrank the tenant's own concurrency
/// budget, and the cap is the hub's admission bound: the "await, never `429`" contract
/// (`tenant_over_cap`'s note, `actor.rs:2409-2420`) is a capacity rule, not a hint.
///
/// THE CONTROL LEG IS WHAT MAKES THIS EVIDENCE. The *same* `Claim` from the *same* supplier,
/// one unit below the cap, does lease — so the only fact that flips between the two turns is
/// the tenant's running count, and the refusal cannot be a capacity, model, affinity or
/// liveness story. Every expectation below is an exact effect list or an exact count, never
/// "eventually".
#[test]
fn t20k_the_tenant_concurrency_cap_still_binds_the_supplier_a_pin_reserved() {
    let mut a = actor();
    // One task at a time for the tenant. Every `submit_*_msg` record is built on the default
    // tenant ("acme"), so this is the number `tenant_over_cap` compares against.
    a.limits_mut().tenant_max_concurrent = 1;
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);

    // ---- control: one slot under the cap, and the owner's claim takes its own task ----
    a.handle(5, claim_msg("w-b", 2, 1));
    let control = a.handle(10, submit_pinned_msg("job-run", "k-run", Some("w-b")));
    assert_eq!(
        assigned(&control),
        vec![("w-b".to_string(), "job-run".to_string(), 1u32)],
        "under the cap the pin is served exactly as t20a serves it: the named supplier leases"
    );
    assert_eq!(
        accepted_workers(&control),
        vec![("job-run".to_string(), false, Some("w-b".to_string()))],
        "and the verdict names the supplier that got it"
    );
    assert_eq!(
        a.leases_len(),
        1,
        "the tenant now holds exactly `tenant_max_concurrent` non-terminal tasks, which is \
         not yet over it — `tenant_over_cap` is strictly greater"
    );

    // ---- the cap binds: live authority on both suppliers, one pinned task, zero leases ----
    //
    // `w-b` is the pin's owner (so affinity and the pin are satisfied) and `w-a` is a capable
    // peer; both hold a parked claim and a free slot. `tenant_over_cap` is the only term left
    // in the `AND`, and it must refuse on all three paths that can dispatch: the submit turn,
    // the `Claim`, and the widened `Ready` re-check.
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(21, claim_msg("w-b", 2, 1));
    assert!(
        a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable()
            && a.workers()[&WorkerId::new_unchecked("w-b")].dispatchable(),
        "both suppliers are dispatchable before the verdict, so the silence below is not \
         the slot gate: the owner holds three free slots of its own"
    );

    let over = a.handle(22, submit_pinned_msg("job-wait", "k-wait", Some("w-b")));
    assert!(
        assigned(&over).is_empty(),
        "the cap outranks the pin: neither the owner nor the peer leases a second task for \
         a tenant already at its cap — {:?}",
        assigned(&over)
    );
    assert_eq!(
        accepted(&over),
        vec![("job-wait".to_string(), false, Vec::new())],
        "the answer is the same await-it verdict the cap gives unpinned work: accepted, \
         never `429` — the pin buys no special status at admission either"
    );
    assert_eq!(
        accepted_workers(&over),
        vec![("job-wait".to_string(), false, None)],
        "and a task nobody is running has no supplier to name, pin or no pin"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-wait"))
            .expect("record")
            .phase,
        JobPhase::Queued,
        "it waits in the queue rather than being refused"
    );
    assert_eq!(
        a.queue_depth(),
        1,
        "exactly one queued row, and it is that one"
    );
    assert_eq!(
        a.leases_len(),
        1,
        "no second lease was created — the pin did not lift the cap for its own owner"
    );

    // The owner's own heartbeat cannot buy it the task either. `work_parked()` holds here
    // (authority **and** queued demand), so the §5 `Ready` pass really runs: the emptiness
    // below is that pass meeting the cap, not the pass being skipped.
    let ready = a.handle(23, ready_msg("w-b", 2, 4));
    assert_eq!(
        assigned(&ready),
        Vec::new(),
        "the `Ready` re-check is gated by the same term — {:?}",
        assigned(&ready)
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        1,
        "the owner's authority is parked, not spent, while the cap binds"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "…and so is the peer's; neither turned into a lease and neither was voided"
    );

    // ---- the cap lifts, and the parked row goes to the owner ----
    //
    // One unit of room. `w-a`'s row still sits in front of the mailbox, so this leg also
    // shows the two gates composing by `AND` rather than cancelling: the cap no longer
    // refuses anybody, and the pin still reserves the task from the first claimant.
    let release = a.handle(30, done_msg("w-b", 2, "job-run", 0, None));
    assert_eq!(
        terminals(&release),
        vec![("job-run".to_string(), TerminalReason::Completed, false)],
        "the task that held the tenant's only slot finished"
    );
    assert_eq!(
        assigned(&release),
        vec![("w-b".to_string(), "job-wait".to_string(), 1u32)],
        "the moment the count is back at the cap, the owner's parked claim serves the task \
         it waited for — while `w-a`, whose claim is ahead of it in mailbox order, is still \
         refused by the pin"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-wait"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-b"),
        "the cap never rerouted the task away from the supplier the consumer pinned"
    );
    assert_eq!(a.queue_depth(), 0, "the queue is drained");
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].outstanding_claims,
        1,
        "the peer is still gated by the pin alone, with its authority intact"
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].outstanding_claims,
        0,
        "the owner's parked row was consumed by exactly one dispatch"
    );
    assert_eq!(
        a.leases_len(),
        1,
        "one running task for the tenant — the cap held for the whole run"
    );
}
