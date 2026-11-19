//! Every bound the broker enforces, in one place, with the task's numbers as
//! defaults. "Explicit limits/TTLs/deadlines" is a requirement (task §4, §8,
//! §10): every queue entry, lease, ring and spilled buffer here states why its
//! bytes exist and when they leave. One deliberate deviation, settled by the
//! operator: the **dispatch queue itself is unbounded** — the hub refuses no
//! work for capacity reasons (no admission rate limit), so submissions beyond
//! the tenant budget are accepted and awaited in the queue rather than failed.
//! The bytes story stays bounded where it can be: each prompt by the payload
//! and token caps below, each spilled body by the spill budget, each retained
//! answer by its TTL.

use crate::protocol::WireCaps;
// Derives below are serde ones: the import is what makes `#[derive(Serialize,
// Deserialize)]` resolve (it is not in the std prelude).
use serde::Deserialize;
use serde::Serialize;

/// All numeric policy for one pool's actor plus the shared wire/store bounds.
///
/// `#[serde(default)]` means a config file may override *only* the numbers an
/// operator cares about; `deny_unknown_fields` means a typo in a limit name is a
/// startup error instead of a silently retained default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    // ---- wire / framing (task §9, §10) ----
    /// Max bytes of one JSON line on a worker socket.
    pub worker_line_cap_bytes: usize,
    /// Max bytes of one `Frame.text` body.
    pub worker_frame_text_cap_bytes: usize,
    /// Inline payload cap; above it the payload travels by `payload_ref`.
    pub inline_payload_cap_bytes: usize,
    /// Largest `jobs` a worker may ask for in one `Claim`.
    pub max_claim_batch: u32,
    /// Admission control on worker fan-in; a runaway host cannot DoS the actor.
    pub max_workers_per_pool: usize,

    // ---- admission (task §8) ----
    /// Concurrent non-terminal jobs per tenant across all its pools. This is a
    /// concurrency bound on **running** jobs, not a request rate and not an
    /// admission limit: a submission that arrives while every slot is busy is
    /// accepted and **awaited** in the queue (the hub has no admission rate
    /// limit — it never answers 429 for capacity), and dispatch picks it up as
    /// soon as one of the tenant's slots frees.
    pub tenant_max_concurrent: usize,
    /// Prompt body cap per submission, the cheap KV-mitigation of task §8.
    pub tenant_max_payload_bytes: u64,
    /// Prompt token estimate cap per submission.
    pub tenant_max_prompt_tokens: u64,
    /// LRU-ish bound on remembered idempotency keys.
    pub max_idempotency_entries: usize,
    /// How long an idempotency key keeps attaching to its job.
    pub idempotency_ttl_ms: u64,

    // ---- scheduling, leases, retries (task §5.1, §5.3, §17.5) ----
    /// Expected gap between `Ready` heartbeats (reference: 15–30 s).
    pub heartbeat_ms: u64,
    /// Lease lifetime granted per dispatch; renewed by heartbeats.
    pub lease_ms: u64,
    /// Window after a supplier socket closes during which its leases are held
    /// while the loss is decided. A reconnect is a new connection with a new
    /// hub-assigned id and cannot re-take them; `0` applies the expiry table
    /// immediately.
    pub resume_grace_ms: u64,
    /// Pre-body grace for "no ready worker" → then `503 + Retry-After`.
    pub no_worker_grace_ms: u64,
    /// Max time a job may sit undispatched (Pending or Queued) → `503`.
    pub claim_deadline_ms: u64,
    /// Hard per-job cap from dispatch to terminal.
    pub job_hard_cap_ms: u64,
    /// How long a worker may take to answer `Cancel` before its slot is freed.
    pub cancel_ack_ms: u64,
    /// Max attempts (1-based) for an idempotent kind before failing.
    pub max_attempts: u32,
    /// Max resume-by-continuation re-dispatches per job (task §7.4). `0` — the
    /// default — leaves a mid-stream death terminal; `1` re-runs `prompt +
    /// partial text` on another worker, which costs a prefill and may show the
    /// consumer a seam.
    pub max_continuations: u32,
    /// Whether a continuation may be attempted when the relayed output was **not**
    /// reconstructible assistant text — tool-call chunks, or a stream interleaved
    /// with `event`/`log`/`artifact_chunk` frames. Default `false`: with
    /// continuation enabled, an opaque attempt still ends in band rather than being
    /// re-run on a half-written tool argument (task §7.4). Only set this on a pool
    /// whose workers restart cleanly from arbitrary prefixes.
    pub continuation_allows_opaque_output: bool,

    // ---- relay (task §7.2, §7.5, §8) ----
    /// Flush an outbound SSE batch after this long even if under-sized.
    pub flush_interval_ms: u64,
    /// Flush an outbound SSE batch once this many bytes accumulate.
    pub flush_bytes: usize,
    /// Replay ring: frames retained per job.
    pub replay_frames: usize,
    /// Replay ring: bytes retained per job (the RSS bound).
    pub replay_bytes: usize,
    /// Cap on retained partial text for continuation (memory-only, task §9).
    pub partial_text_cap_bytes: usize,
    /// Per-job consumer backlog before the worker is back-pressured.
    pub consumer_backpressure_frames: usize,

    // ---- consumer face (spec §5.4, NEX-3/NEX-6) ----
    /// Ruler for the hub's liveness probe on a **consumer** socket (`GET
    /// /v1/consumer`): the probe period is a quarter of this, clamped into
    /// `[250 ms, 5 s]`, and the verdict lands after `max(3 × probe, this + probe)`
    /// of hearing **nothing at all** — not one frame, not one `Pong`. `0` turns the
    /// probe off and reproduces the old behaviour (a consumer socket lives exactly
    /// as long as its client says it does).
    ///
    /// The arithmetic at each setting, spelled out because it is what decides how much
    /// risk a false positive carries. Which term of `max(3 × probe, ruler + probe)`
    /// binds is not obvious: `ruler + probe` dominates for every ruler at or above the
    /// 5 s probe clamp, and the `3 × probe` term only binds at the bottom, where the
    /// 250 ms probe floor takes over:
    ///   * `60_000` (default) → probe `clamp(60000 / 4, 250, 5000) = 5_000`, verdict
    ///     after `max(3 × 5000, 60000 + 5000) = 65_000` ms of silence;
    ///   * `20_000` → probe `5_000`, verdict after `max(15000, 25000) = 25_000` ms —
    ///     the verdict a reviewer measured, and the one that was too tight to ship;
    ///   * `250` (what the reap tests set) → probe at the `250` floor, verdict after
    ///     `max(750, 500) = 750` ms, the one case where `3 × probe` is the binding term.
    ///
    /// It exists because a consumer socket is the one long-lived connection on this
    /// face, and the read loop had no way to notice a peer that vanished without a
    /// close: a browser tab killed behind a proxy, a laptop that slept, an edge that
    /// dropped the path quietly. Until now the session parked in `stream.next()`
    /// forever and kept its whole subscription graph — writer task, per-task pumps,
    /// relay subscribers — for the life of the process. The supplier face has had
    /// this mechanism since its first pass (`worker_ws.rs`'s keepalive); the
    /// consumer face simply never got one.
    ///
    /// Why the ruler is 60 s and not 20 s: the consumer verdict is **not** symmetric
    /// with the supplier one. A supplier verdict costs a connection; a consumer
    /// verdict applies NEX-6.1/6.3 to whatever that socket is attached to, which
    /// means the task is **cancelled and its record forgotten** — an alive job
    /// cancelled *and* deleted, answer and idempotency entry gone, so the client's
    /// retry pays for the GPU a second time. A stalled event loop, a notebook cell
    /// held open in a debugger, or a proxy that defers control frames can each go
    /// quiet for more than 25 s, while the leak this probe exists for is a socket
    /// that stays silent for **hours**. Nothing in the hub's own correctness needs a
    /// fast consumer reap, so the window is set where a false verdict is rare enough
    /// to be worth the trade.
    ///
    /// Two rules this window must never break, and the reason it is expressed in
    /// pings rather than in job progress: a **slow but live stream is not
    /// silence** (any inbound frame at all — a `submit`, a `Pong`, anything — resets
    /// the clock), and the verdict is about the *path*, never about how busy the
    /// consumer's application is. A live WebSocket answers a ping on its runtime's
    /// behalf without any application code involved, which is what makes the probe
    /// safe for a client that never sends a frame of its own.
    pub consumer_keepalive_ms: u64,

    // ---- store (task §7.3, §9, §10) ----
    /// TTL for a spilled request payload. Prompts are memory-only: the TTL *is*
    /// the retention policy, and nothing here ever writes prompt text to disk.
    pub payload_ttl_ms: u64,
    /// Max concurrently spilled payloads.
    pub max_spilled_payloads: usize,
    /// Total spilled-bytes budget; over it, admission fails with `429`.
    pub max_spill_bytes: u64,
    /// TTL for produced artifacts on disk.
    pub artifact_ttl_ms: u64,
    /// Max artifact size the hub will accept.
    pub max_artifact_bytes: u64,
    /// Max bytes in one artifact `PUT` chunk.
    pub artifact_chunk_bytes: usize,
    /// How long a terminal job record (and its replay ring) stays queryable.
    pub job_retention_ms: u64,
    /// How long a **detached** submission (`cancel_on_consumer_gone == false`) may
    /// stay unclaimed with no consumer attached at all, before the actor deletes the
    /// record and its idempotency entry rather than keeping a task nobody asked for.
    ///
    /// This is the second half of spec NEX-6.6's permitted exception (§9.1 item 3):
    /// "cap its lifetime while it has no subscriber". A detached job is already an
    /// exception to NEX-6.1's "no consumer, no task", so it needs its own bound;
    /// `job_retention_ms` only covers records that reached a terminal state, and the
    /// queue/claim deadlines only cover jobs someone is still waiting on.
    ///
    /// Only an **unclaimed** (`Pending`/`Queued`) detached job is swept by this clock:
    /// GPU work a worker has already committed to is bounded by `job_hard_cap_ms`, not
    /// by a subscriber count (see `PoolActor::on_tick` step 9).
    pub detached_orphan_ttl_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        // Reference anchors: services/molab/molab-strata-runtime.py:111-120
        // (PARALLEL=4, CONTEXT=524288, loopback :8090) and task §10 (~8 MB
        // inline cap), §7.2 (50–100 ms / 1–2 KiB flush), §4 (15–30 s heartbeat).
        Self {
            worker_line_cap_bytes: 1 << 20,
            worker_frame_text_cap_bytes: 256 << 10,
            inline_payload_cap_bytes: 8 << 20,
            max_claim_batch: 8,
            max_workers_per_pool: 32,

            tenant_max_concurrent: 16,
            tenant_max_payload_bytes: 32 << 20,
            tenant_max_prompt_tokens: 524_288,
            max_idempotency_entries: 4_096,
            idempotency_ttl_ms: 600_000,

            heartbeat_ms: 20_000,
            lease_ms: 60_000,
            resume_grace_ms: 1_500,
            no_worker_grace_ms: 2_000,
            claim_deadline_ms: 600_000,
            job_hard_cap_ms: 1_800_000,
            cancel_ack_ms: 10_000,
            max_attempts: 3,
            // Resume-by-continuation (task §7.4) is **opt-in**: a mid-stream
            // death is terminal unless the operator asks for a re-run. It costs
            // a full prefill on the second worker (quota on a free-tier host),
            // the hub cannot tell whether that worker's prompt cache is cold as
            // §7.4 asks it to gate on, and the consumer may see a seam. Set
            // `NEXUS_MAX_CONTINUATIONS=1` to turn exactly one attempt on.
            max_continuations: 0,
            // Tool-call chunks are the exact case §7.4 warns about: the hub cannot
            // turn `{"function":{"arguments":"{\"ci"` into a prompt prefix, so an
            // opaque attempt stays terminal even when continuation is on.
            continuation_allows_opaque_output: false,

            flush_interval_ms: 75,
            flush_bytes: 1_536,
            replay_frames: 256,
            replay_bytes: 1 << 20,
            partial_text_cap_bytes: 1 << 20,
            consumer_backpressure_frames: 256,
            // NEX-6 on the consumer face: the ruler for the hub's liveness probe on
            // `GET /v1/consumer` — 5 s probes, and a verdict after 65 s of total
            // silence. Ships on; `NEXUS_CONSUMER_KEEPALIVE_MS=0` is the documented way
            // to turn it off, which is the only case where a zero is not a mistake here
            // (the retention TTLs above have no "off" reading). The ruler is deliberately
            // wide: a consumer verdict cancels **and deletes** an attached task, and the
            // leak it exists for is a socket that is silent for hours.
            consumer_keepalive_ms: 60_000,

            payload_ttl_ms: 120_000,
            max_spilled_payloads: 64,
            max_spill_bytes: 512 << 20,
            artifact_ttl_ms: 600_000,
            max_artifact_bytes: 512 << 20,
            artifact_chunk_bytes: 4 << 20,
            job_retention_ms: 300_000,
            // Same order of magnitude as terminal retention: a detached job nobody
            // watches and nobody claims is forgotten after 5 minutes, which is long
            // enough for a worker to come up and claim it on a cold pool, and short
            // enough that a client that never comes back cannot leave the hub holding
            // a queue entry forever (NEX-6.6 / §9.1 item 3).
            detached_orphan_ttl_ms: 300_000,
        }
    }
}

impl Limits {
    /// Wire caps for the worker→hub direction.
    #[must_use]
    pub fn worker_caps(&self) -> WireCaps {
        WireCaps {
            max_line_bytes: self.worker_line_cap_bytes,
            max_frame_text_bytes: self.worker_frame_text_cap_bytes,
            max_inline_payload_bytes: self.inline_payload_cap_bytes,
        }
    }

    /// Wire caps for the hub→worker direction: the line cap must accommodate a
    /// full inline payload, otherwise `Assigned` could never carry one.
    #[must_use]
    pub fn hub_caps(&self) -> WireCaps {
        WireCaps {
            max_line_bytes: self.inline_payload_cap_bytes + (self.worker_line_cap_bytes / 2),
            max_frame_text_bytes: self.worker_frame_text_cap_bytes,
            max_inline_payload_bytes: self.inline_payload_cap_bytes,
        }
    }

    /// Reject nonsense combinations before they can misbehave at runtime.
    ///
    /// Returns the list of violations so an operator fixing a config sees every
    /// problem at once, not one per restart.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.lease_ms < self.heartbeat_ms * 2 {
            problems.push(format!(
                "lease_ms ({}) must be at least 2x heartbeat_ms ({}) or leases expire between heartbeats",
                self.lease_ms, self.heartbeat_ms
            ));
        }
        if self.flush_bytes == 0 {
            problems.push("flush_bytes must be > 0".to_string());
        }
        if self.flush_interval_ms == 0 {
            problems.push("flush_interval_ms must be > 0".to_string());
        }
        if self.replay_frames == 0 || self.replay_bytes < 1024 {
            problems.push("replay ring must hold at least 1 frame and 1 KiB".to_string());
        }
        // NEX-5.3 / NEX-7.2 cross-check. A flush batch is drained out of the replay
        // ring (`relay_into` re-reads `replay.since(last_flushed_seq)`), so a window
        // larger than the whole ring can never be emitted in one piece: the oldest
        // deltas are evicted before the gate ever opens, stranding bytes behind a seq
        // the consumer never saw. Ring bytes are therefore an upper bound on the
        // batch the actor promises to relay.
        if self.flush_bytes > self.replay_bytes {
            problems.push(format!(
                "flush_bytes ({}) must not exceed replay_bytes ({}) or a full flush batch can be evicted from the ring before it is relayed",
                self.flush_bytes, self.replay_bytes
            ));
        }
        if self.inline_payload_cap_bytes > self.worker_line_cap_bytes * 32 {
            problems.push(format!(
                "inline_payload_cap_bytes ({}) is absurdly large next to worker_line_cap_bytes ({}); use payload_ref or raise the line cap deliberately",
                self.inline_payload_cap_bytes, self.worker_line_cap_bytes
            ));
        }
        if self.max_attempts == 0 {
            problems.push("max_attempts must be >= 1".to_string());
        }
        if self.resume_grace_ms > self.lease_ms {
            problems.push("resume_grace_ms must not exceed lease_ms".to_string());
        }
        if self.claim_deadline_ms <= self.no_worker_grace_ms {
            problems.push("claim_deadline_ms must exceed no_worker_grace_ms".to_string());
        }

        // NEX-7.2 says every retained byte needs a stated reason; the corollary is that
        // a TTL is only a bound if it is non-zero. `expires = now + ttl` is *saturating*,
        // and every sweep fires on `expiry <= now`, so `ttl = 0` is not "no retention" —
        // it is "expired before anything could read it", which silently deletes a
        // behaviour the rest of the design depends on. Each message names the thing that
        // breaks, in the same style as the lease/heartbeat trap above.
        let zero_ttl_problems: [(&str, u64, &str); 5] = [
            (
                "payload_ttl_ms",
                self.payload_ttl_ms,
                "a spilled prompt leaves memory before any worker can fetch it",
            ),
            (
                "job_retention_ms",
                self.job_retention_ms,
                "a terminal record is dropped before a reconnect, a poll or an idempotent retry can read the answer"
            ),
            (
                "idempotency_ttl_ms",
                self.idempotency_ttl_ms,
                "a duplicate submission re-runs work that is already in flight"
            ),
            (
                "artifact_ttl_ms",
                self.artifact_ttl_ms,
                "an artifact is deleted before its consumer can download it"
            ),
            (
                "detached_orphan_ttl_ms",
                self.detached_orphan_ttl_ms,
                "an unclaimed detached job is deleted the moment it is submitted (§9.1 item 3)"
            ),
        ];
        for (name, value, consequence) in zero_ttl_problems {
            if value == 0 {
                problems.push(format!("{name} must be > 0, or {consequence}"));
            }
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_task_numbers() {
        let l = Limits::default();
        assert_eq!(l.inline_payload_cap_bytes, 8 << 20);
        // The queue itself has no bound (no admission rate limit); the tenant
        // cap is a bound on running jobs, 16 by default.
        assert_eq!(l.tenant_max_concurrent, 16);
        assert_eq!(l.tenant_max_prompt_tokens, 524_288);
        assert_eq!((l.heartbeat_ms, l.lease_ms), (20_000, 60_000));
        assert_eq!((l.flush_interval_ms, l.flush_bytes), (75, 1_536));
        assert_eq!(
            l.max_continuations, 0,
            "resume-by-continuation is opt-in (§7.4)"
        );
        assert!(
            !l.continuation_allows_opaque_output,
            "opaque (tool-call / non-delta) output is never auto-continued by default (§7.4)"
        );
        // NEX-6.6's cap on a detached job with no subscriber: 5 minutes, the same
        // order as terminal retention, and non-zero so `validate()` accepts it.
        assert_eq!(l.detached_orphan_ttl_ms, 300_000);
    }

    #[test]
    fn validate_catches_the_lease_heartbeat_trap() {
        // Struct-update instead of `default()` then assignment: the two fields this
        // fixture changes are the whole point of the test.
        let l = Limits {
            heartbeat_ms: 30_000,
            lease_ms: 45_000, // < 2x heartbeat
            ..Limits::default()
        };
        assert_eq!(
            l.validate(),
            vec![
                "lease_ms (45000) must be at least 2x heartbeat_ms (30000) or leases expire between heartbeats"
                    .to_string()
            ]
        );
        assert!(Limits::default().validate().is_empty());
    }

    /// NEX-5.3 / NEX-7.2: a flush window bigger than the ring that has to hold it is a
    /// stranding bug waiting to happen, so it is a startup error, not a runtime surprise.
    #[test]
    fn validate_rejects_a_flush_window_larger_than_the_replay_ring() {
        let l = Limits {
            flush_bytes: 4_096,
            replay_bytes: 2_048,
            ..Limits::default()
        };
        assert_eq!(
            l.validate(),
            vec![
                "flush_bytes (4096) must not exceed replay_bytes (2048) or a full flush batch can be evicted from the ring before it is relayed"
                    .to_string()
            ]
        );
        // The boundary is allowed: a batch exactly as large as the ring is still
        // drainable in one piece.
        let equal = Limits {
            flush_bytes: 2_048,
            replay_bytes: 2_048,
            ..Limits::default()
        };
        assert!(equal.validate().is_empty(), "{:?}", equal.validate());
    }

    /// NEX-7.2: every retention TTL is a bound, and `now + 0` is already expired, so a
    /// zero anywhere in this list silently removes a behaviour instead of bounding it.
    #[test]
    fn validate_rejects_zero_retention_ttls() {
        let cases: [(&str, Limits, &str); 5] = [
            (
                "payload_ttl_ms",
                Limits {
                    payload_ttl_ms: 0,
                    ..Limits::default()
                },
                "payload_ttl_ms must be > 0, or a spilled prompt leaves memory before any worker can fetch it",
            ),
            (
                "job_retention_ms",
                Limits {
                    job_retention_ms: 0,
                    ..Limits::default()
                },
                "job_retention_ms must be > 0, or a terminal record is dropped before a reconnect, a poll or an idempotent retry can read the answer",
            ),
            (
                "idempotency_ttl_ms",
                Limits {
                    idempotency_ttl_ms: 0,
                    ..Limits::default()
                },
                "idempotency_ttl_ms must be > 0, or a duplicate submission re-runs work that is already in flight",
            ),
            (
                "artifact_ttl_ms",
                Limits {
                    artifact_ttl_ms: 0,
                    ..Limits::default()
                },
                "artifact_ttl_ms must be > 0, or an artifact is deleted before its consumer can download it",
            ),
            (
                "detached_orphan_ttl_ms",
                Limits {
                    detached_orphan_ttl_ms: 0,
                    ..Limits::default()
                },
                "detached_orphan_ttl_ms must be > 0, or an unclaimed detached job is deleted the moment it is submitted (§9.1 item 3)",
            ),
        ];
        for (field, limits, expected) in cases {
            assert_eq!(limits.validate(), vec![expected.to_string()], "{field} = 0");
        }
    }

    /// NEX-6 on the consumer face: the keepalive ruler ships **on**, because a
    /// consumer socket is the one connection whose lifetime nothing else bounded —
    /// a hub that retains a vanished consumer retains every stream, subscriber and
    /// pump that consumer was holding. `0` is the deliberate "off" sentinel, so
    /// `validate()` must not group it with the retention TTLs, where a zero silently
    /// deletes a behaviour instead of turning it off.
    #[test]
    fn the_consumer_keepalive_ships_on_and_has_a_zero_off_sentinel() {
        assert_eq!(Limits::default().consumer_keepalive_ms, 60_000);
        let off = Limits {
            consumer_keepalive_ms: 0,
            ..Limits::default()
        };
        assert!(off.validate().is_empty(), "{:?}", off.validate());
    }

    #[test]
    fn hub_line_cap_fits_a_full_inline_assigned() {
        let l = Limits::default();
        assert!(l.hub_caps().max_line_bytes > l.inline_payload_cap_bytes);
        assert_eq!(l.worker_caps().max_frame_text_bytes, 256 << 10);
    }
}
