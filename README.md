# @typecrate/nexus — outbound-dial inference broker

Rust implementation of the retired outbound-dial inference-broker brief (originally
`tasks/task-rust-hub-inference-broker.md`; the normative statement is now
[`documents/specifications/hub-specification.md`](documents/specifications/hub-specification.md)):
a hub that inference hosts reach by **dialing out**, so a worker behind NAT, inside a
marimo/molab container, or in a Kaggle notebook can serve consumer requests without
a public URL. The hub is the only public surface; it owns the queue, claims each job
to exactly one worker, relays the worker's stream to the consumer, and propagates
cancellation back.

This package is **standalone** (`[workspace]` declared here on purpose) and contains
no JS/TS application logic — the only JavaScript/TypeScript in it is the Daytona/Modal
deploy scripts, which no hub request path ever touches.

```text
consumer ──POST /v1/chat/completions (SSE)──▶┌───────────────┐◀──one persistent WS──▶ worker A (Strata, 4 slots)
consumer ──GET /v1/consumer (WS)          ──▶│  nexus hub    │◀──one persistent WS──▶ worker B (ComfyUI, 1 slot)
consumer ──POST /v1/jobs (artifact)       ──▶│ axum + tokio  │
                                             └───────────────┘
                          one actor per pool owns the queue, leases and jobs
```

## Layout

| Path                                                       | What it is                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/nexus-core`                                        | The spine: `protocol`, `actor` (pool_actor), `queue` (trait + in-memory), `relay`, `adapters`, `store`, `scheduler`, `auth`, `seq`, `metrics`, `limits`, `config`. Synchronous and IO-free — every scheduling rule lives here and nowhere else.                                                                                                                                                                                                                                                                       |
| `crates/nexus-server`                                      | Native runtime: axum + tokio, worker WebSocket, **consumer WebSocket (`/v1/consumer`)** and consumer SSE, artifact spill to disk, metrics, graceful drain. The only thing in the package that touches a socket, a timer or `std::env`.                                                                                                                                                                                                                                                                                |
| `crates/nexus-server/src/observe.rs`                       | `GET /v1/observe`: the one read-only document the monitoring UI is built against — readiness, pools, workers, active jobs with leases, live consumer sockets, unresponsive pools. Open by the same rule as `/healthz`, always `200`, and the only route that _relies_ on CORS — the layer itself wraps every route. `crates/nexus-server/src/main.rs` (`observe_cors`) is the policy.                                                                                                                                 |
| `crates/nexus-server/src/runtime_e2e.rs`                   | Runtime tests: a real listener, a real worker WebSocket, real SSE/JSON responses and real consumer-WebSocket frames (`§16.5`, `§16.6`, `§16.7`, `§16.14`, `§9` tenancy, `/metrics` shape), plus the observation plane end to end (the `/v1/observe` contract, and a cross-origin preflight + read). A unit-test module of the bin, because this crate has no library target. These **run and pass** — all 21, against real loopback sockets, real WebSocket handshakes and real SSE bytes; see _Verification status_. |
| `crates/nexus-worker-example`                              | Rust worker client that dials out and fronts a loopback engine (task §12).                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `scripts/start-daytona.ts`                                 | Creates a Daytona sandbox and deploys the hub into it (the only consumer of `@services/daytona/sandbox`).                                                                                                                                                                                                                                                                                                                                                                                                             |
| `scripts/deploy-endpoint.ts` / `scripts/deploy-daytona.ts` | Ships the native binary to any endpoint that speaks the runtime contract v1 (Daytona preview, Modal box/sandbox URL, loopback runner): preflight, teardown, POST, poll, log relay. On a Modal box the slot decides: empty means the container is used as the fresh machine, busy means it is **replaced** first (`DELETE /sandbox` → wait for a new, empty container → deploy into it), so nothing the previous machine held survives the deploy.                                                                     |

## Verification status — read this first

**Compiled and executed on this host, on the pinned toolchain, 2026-11-10.** What is
actually installed: active toolchain `1.99.1` (the pin in the root `rust-toolchain.toml`),
`rustc 1.99.1`, `cargo 1.99.1`, with `rustfmt` and `clippy` as declared components.
There is no toolchain gap left: the one this section used to record — `wasm-bindgen` /
`wasm-bindgen-cli` / `wasm-pack` / `wasm-opt` absent from `PATH` — stopped being a gap
when the wasm leg was removed (see _No WASM boundary_, below).

**2026-11-14 — the wasm leg was removed from this package.** `crates/nexus-wasm`,
`scripts/build-wasm.ts` and `examples/node-smoke.ts` are deleted, along with the
`wasm` / `wasm:check` / `wasm:glue` / `build:wasm` / `smoke:wasm` verbs and the two
`verify` legs that used them. Reason: the package publishes nothing to npm and no other
package imports it — it ships one native binary onto a sandbox — so the Node/browser
re-export of the actor had no consumer, no test of its own, and a toolchain
(`wasm-bindgen-cli` pinned to the exact `Cargo.lock` library version) that nothing here
could run. **The rows dated 2026-11-10 to 2026-11-13 below are left exactly as they were
measured, and several of them cite commands that no longer exist**; they are records, not
instructions. What removing the crate did _not_ do is relax `nexus-core`'s IO-free
contract — no tokio, injected clocks, `BTreeMap` for observable order — which is what
makes its tests exact-value assertions rather than wall-clock ones. (An even older row
set, dated 2026-11-03, was measured on Windows/MSVC with the same pinned toolchain and
is superseded by the 2026-11-10 set; the environment notes under the table describe it.)

Everything below was run from `packages/underload/nexus` with the repo-root
`.cargo/config.toml` in force (`target-dir = .rust/target`). Exit codes are the
command's own. The whole set also reproduces from a **pristine copy with a fresh target
directory and `--locked`**, so nothing here depends on an incremental build.

| Command                                                                                                                                                                                                                                                                                                                            | Result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cargo fmt --all -- --check`                                                                                                                                                                                                                                                                                                       | **clean** (exit 0). **Re-run 2026-11-14: clean (exit 0)** on the memory pass, from the repo-root `.rust/target`. **Re-run 2026-11-15: clean (exit 0)** — after one `cargo fmt --all` over the package, whose only diffs were in the five files the relay-lifetime pass had just edited; the tree was clean before them — the `cargo fmt --all` behind that row moved ~1 900 lines, all of them inside the files that pass had just edited (its additions were written un-formatted), while a detached-HEAD worktree checked with `-- --check` was already clean, so no committed line moved. **Re-run again 2026-11-15 after the test-closure pass below: clean (exit 0)**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cargo check --workspace --all-targets`                                                                                                                                                                                                                                                                                            | **ok**, 0 errors, 0 warnings. **Re-run 2026-11-15: ok**, three members, 0 errors, 0 warnings; re-measured after the test-closure pass below with the same result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cargo test --workspace`                                                                                                                                                                                                                                                                                                           | **exit 0 — 177 passed / 0 failed**: `nexus-core` lib **86/86**, `crates/nexus-core/tests/acceptance.rs` **48/48**, `nexus-server` bin **43/43** (21 `runtime_e2e` + 10 `observe::tests` + 7 `io::tests` + 5 `state::tests`), `nexus-wasm` 0 tests, `nexus-worker-example` 0 tests, doc-tests 0. **Re-run 2026-11-11 at 181/181** after the supplier-admission and credential fixes below (89 + 49 + 43). **Re-run 2026-11-14 at 203/203** — `nexus-core` lib **89**, `crates/nexus-core/tests/acceptance.rs` **60**, `nexus-server` bin **54**, `nexus-worker-example` 0, doc-tests 0; `cargo test -p nexus-server` alone is **54/54**, and the new `state.rs` test was revert-checked (it fails on the pre-fix line, passes on the fix) from the same target dir. **Re-run 2026-11-15 at 223/223** — `nexus-core` lib **91**, `crates/nexus-core/tests/acceptance.rs` **60**, `nexus-server` bin **72** (33 `runtime_e2e` + 11 `state::tests` + 10 `observe::tests` + 7 `io::tests` + 5 `worker_ws::tests` + 5 `consumer::tests` + 1 `consumer_ws::tests`), doc-tests 0, no warnings during compilation; `cargo test -p nexus-core -p nexus-server` alone is the same 91 + 60 + 72. The new tests are named under _Relay-lifetime pass_ below. **Re-run 2026-11-15 again at 225/225** after the test-closure pass below — `nexus-core` lib **91**, `acceptance.rs` **60**, `nexus-server` bin **74** (35 `runtime_e2e` + 11 `state::tests` + 10 `observe::tests` + 7 `io::tests` + 5 `worker_ws::tests` + 5 `consumer::tests` + 1 `consumer_ws::tests`), doc-tests 0 — and the two additions are revert-checked by **mutation**, not by reading: neutering `JobRelay::retire`'s body (`state.rs:282`, the `Subscriber::Drop` unregister) turns both of them red, and no other change was needed to get there                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cargo clippy --workspace --all-targets`                                                                                                                                                                                                                                                                                           | **exit 0**, every crate linted. **Re-run 2026-11-14: 1 finding** — `clippy::new_without_default` at `crates/nexus-core/src/queue.rs:125` (`InMemoryQueue::new`), a file the memory pass never touched; it predates that pass. **Re-run 2026-11-15: 1 finding**, that same `queue.rs:125` and nothing else; re-measured after the test-closure pass below, same single finding                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cargo clippy --workspace --all-targets -- -D warnings` (what `yarn clippy` runs)                                                                                                                                                                                                                                                  | **exit 0 — zero findings**. The 17 pre-existing `nexus-core` lints this row used to record are gone, so `yarn verify` no longer aborts before `yarn test`. **Re-run 2026-11-14: red on that one lint and no other** — the `queue.rs:125` row above is the whole finding. **Re-run 2026-11-15: red on that one lint and no other**, and `cargo clippy --workspace --all-targets -- -D warnings --allow clippy::new_without_default` is **exit 0, zero findings** — the form the relay-lifetime pass measured its own cleanliness with, and stated here because the allow is part of the command. **Re-run 2026-11-15 after the test-closure pass: `-D warnings` is red on `queue.rs:125` and nothing else (exit 101), and the same command with `--allow clippy::new_without_default` is exit 0, zero findings**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `cargo build --release -p nexus-server`                                                                                                                                                                                                                                                                                            | **ok** → `.rust/target/release/nexus-hub`, 3 403 096 bytes (3.25 MiB), a stripped ELF executable (fat LTO, `panic = abort`, symbols stripped). **Re-run 2026-11-15: ok**, and it now describes two builds, because the RSS work below needed both sides of the relay-lifetime pass as binaries: the working tree gives `.rust/target/release/nexus-hub` at **3 557 704 bytes**, and a detached-HEAD worktree of `d3c0ed66` built into its own target dir gives **3 520 496 bytes** — byte-identical to the same HEAD built twice, which is what makes the pair comparable                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cargo check -p nexus-wasm --target wasm32-unknown-unknown`                                                                                                                                                                                                                                                                        | **ok** — the spine still compiles for the wasm target                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `yarn vite-node scripts/build-wasm.ts`, `yarn vite-node examples/node-smoke.ts`                                                                                                                                                                                                                                                    | **ok** — both load and execute under vite-node (re-measured 2026-11-12); the old `node --check` gate is gone because raw node cannot load a `.ts` at all                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `yarn wasm:glue` (`vite-node scripts/build-wasm.ts`)                                                                                                                                                                                                                                                                               | **blocked**: exits 1 — `wasm-bindgen is not on PATH … cargo install wasm-bindgen-cli --locked --version 0.2.130` (the version is read from `Cargo.lock`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `yarn smoke:wasm` (`vite-node examples/node-smoke.ts`)                                                                                                                                                                                                                                                                             | **blocked**: exits 2 — `[smoke] missing …\pkg\nexus_wasm.js`, because the glue step above cannot run, so its assertion blocks have still never executed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cargo test --offline --workspace` · `cargo fmt --all -- --check` · `cargo check --offline --workspace --all-targets` · `cargo clippy --offline -p nexus-server --all-targets -- -D warnings` — **2026-11-18 supplier-pinning re-run, re-run once more on that date after the pin-versus-affinity deadlock fix**                   | **exit 0 — 262 passed / 0 failed**: `nexus-core` lib **105**, `crates/nexus-core/tests/acceptance.rs` **70**, `nexus-server` bin **87**, `nexus-worker-example` 0 tests, doc-tests 0. Against the identity pass's 227 (`nexus-core` **92**, `acceptance` **60**, `nexus-server` **75**) that is 13 core units, 10 acceptance cases and 12 bin tests, and 262 is their sum, not a recount. Against the 256 this row carried before the deadlock fix it is **+6**, and the six are named rather than counted: three `actor.rs` units on the new precedence (`a_live_pin_releases_affinity_for_its_owner_and_nobody_else` `:4392`, `a_lapsed_pin_hands_the_job_back_to_the_affinity_owner` `:4427`, `the_affinity_arm_never_outranks_the_model_gate` `:4471`), `t20j_a_supplier_pin_outranks_a_session_hint_that_disagrees_with_it` (`tests/acceptance.rs:5024`), and the two server tests `a_hangup_inside_the_head_hold_cancels_the_queued_task_it_never_leased` (`runtime_e2e.rs:6163`) and `a_pinned_job_post_names_the_supplier_it_leased_and_holds_the_task_from_the_peer` (`:6348`). `cargo fmt --all -- --check` **clean** (exit 0). `cargo check --offline --workspace --all-targets` **ok** — three members, 0 errors, 0 warnings. `cargo clippy --offline -p nexus-server --all-targets -- -D warnings` stops on **one lint and nothing else**: `clippy::new_without_default` at `crates/nexus-core/src/queue.rs:125`, the same pre-existing finding every `clippy` row above records, in a file this change never touched; the identical scan with `--allow clippy::new_without_default` is **exit 0, zero findings**, which is how the new code's own cleanliness is stated, in the form the command can be re-run in. **No performance or latency number is recorded on this row** — none was measured, and §14's 30-60 ms budget stays unmeasured                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cargo test --offline --workspace` · `cargo fmt --all -- --check` · `cargo check --offline --workspace --all-targets` · `cargo clippy --offline --workspace --all-targets -- -D warnings --allow clippy::new_without_default` — **2026-11-19 bounded-cleanup re-run** (a racy runtime test, a drop-guard panic, one coverage hole) | **exit 0 — 265 passed / 0 failed**: `nexus-core` lib **105**, `crates/nexus-core/tests/acceptance.rs` **71**, `nexus-server` bin **89** (47 `runtime_e2e` + 12 `state::tests` + 10 `observe::tests` + 7 `io::tests` + 5 `worker_ws::tests` + 7 `consumer::tests` + 1 `consumer_ws::tests`), `nexus-worker-example` 0 tests, doc-tests 0. Against 2026-11-18's **262** that is **+3**, and every number on that row — including its 227 and 256 comparisons — is left exactly as that pass measured it. The three additions are named, not counted: `t20k_the_tenant_concurrency_cap_still_binds_the_supplier_a_pin_reserved` (`crates/nexus-core/tests/acceptance.rs:5211`), which pairs a supplier pin with the tenant concurrency cap and shows the cap still binding the pin's own owner, and `an_overflowed_hold_detach_is_still_delivered_by_the_fallback` (`crates/nexus-server/src/consumer.rs:2630`) with `an_overflowed_hold_detach_does_not_panic_without_a_runtime` (`:2670`) — the two halves of `SupplierHoldDetach`'s mailbox-overflow fallback. **No behaviour changed.** The product edit is one guard inside that fallback: `tokio::runtime::Handle::try_current()` around the one-shot drain, so a `Drop` that lands with no runtime current loses the detach instead of panicking — and with `panic = "abort"` (`Cargo.toml:101`) a panic there is a whole-process abort; the held lease then falls back to the already-documented claim-deadline release. Both tests are **mutation-checked**: restoring bare `tokio::spawn` turns the no-runtime one red with `there is no reactor running`, and `t20k` goes red at its own assertion when the `tenant_over_cap` term is taken out of `worker_may_lease` (`actor.rs:2319`). The remaining changes are a test settle and two stale sentences. `a_pinned_job_post_names_the_supplier_it_leased_and_holds_the_task_from_the_peer` (`runtime_e2e.rs:6355`) was order-dependent in its parked-supplier leg — three parked claim slots plus a park frame on a different socket than the `POST` — and now waits for the park to reach the actor's own mailbox before posting; measured after the change at **10/10** full-target runs and **10/10** focused runs of that test, against **1 failure in 20** full-target runs of the same tree with the settle deleted and **4/4** red runs with the park deliberately sent after the `POST` (`left: Some(<pinned id>)`, the hub having legitimately leased it). One source-comment coordinate was corrected with no code change: `actor.rs:2058`'s pointer to the `free_slots` write, `:1923` → **`:2009`**. `cargo fmt --all -- --check` **clean** (exit 0); `cargo check --offline --workspace --all-targets` **ok** — three members, 0 errors, 0 warnings; plain `cargo clippy --offline --workspace --all-targets -- -D warnings` stops on the pre-existing `clippy::new_without_default` at `crates/nexus-core/src/queue.rs:125` and on nothing else — that file is byte-identical to `HEAD` on this pass — and the `--allow` form named above is **exit 0, zero findings**. **No performance or latency number is recorded on this row** — none was measured |

**2026-11-17 — connection-scoped identities (this host, Linux).** `cargo test
--offline --workspace` re-run after the identity pass: **227 passed / 0 failed** —
`nexus-core` lib **92**, `acceptance` **60**, `nexus-server` bin **75** (36
`runtime_e2e` + 11 `state::tests` + 10 `observe` + 7 `io` + 5 `worker_ws::tests` + 5
`consumer::tests` + 1 `consumer_ws::tests`), doc-tests 0. The new
`two_suppliers_get_distinct_hub_assigned_ids_and_both_stay_connected` opens two
supplier sockets and asserts both live with distinct hub-minted UUIDv4 ids;
`hello_ignores_a_legacy_supplier_declared_id` pins that an old client's extra field
still decodes (and is ignored).
`cargo fmt --all -- --check` is clean; `cargo clippy --workspace --all-targets -- -D
warnings --allow clippy::new_without_default` is exit 0, zero findings (without the
allow it still stops on the pre-existing `queue.rs:125` lint alone).

That minting rule is what the supplier-pinning pass built on: the id a consumer pins with
`x-supplier-id` **is** one supplier socket, so a reconnect is a new id and the id the consumer
is holding is permanently gone — see _Consumer-directed supplier pinning_ below, and spec
**NEX-4.7** for the rule.

**2026-11-11 — the reliable relay and unique-request passes (this host, Linux).** Two field
failures and one design error, all three reproduced locally before the fix and re-run
green after it (`cargo fmt --all`, `cargo test --workspace` **186/186** — `nexus-core` lib
89, `acceptance.rs` 52, `nexus-server` bin 45, the two new runtime tests below included;
`cargo clippy --workspace --all-targets -- -D warnings` exit 0; `cargo check -p
nexus-wasm --target wasm32-unknown-unknown` ok):

- **A fast producer corrupted the consumer's answer, then the job died mid-generation.**
  The per-job fan-out was a 256-slot broadcast ring that silently dropped whatever a pump
  had not yet read. One burst — a supplier streaming thousands of deltas faster than the
  SSE pump drained them, which is what a long prefill followed by a decode run looks
  like — answered the consumer with a `consumer_lagged` in-band error and ended its
  response early ("Upstream stream terminated" past any proxy that killed it later). The
  pump's `Detach` then cancelled the job, and the supplier finished its generation into a
  lease that no longer existed: its remaining frames and its `Done` were refused with
  "no lease for that job", and the consumer's stream never carried the last piece.
  Reproduced with a stub supplier streaming 5 000 deltas back to back: the consumer saw
  1 370 chunks, no finish, no `[DONE]`, and two refusals in the hub log. The fan-out is
  now a **queue per consumer** (`JobRelay::subscribe` / `push`, `state.rs`): every item
  is delivered to every live consumer, a queue that stays full past a 25 ms grace is
  dropped explicitly (its stream ends with an in-band error, never a silent frame loss),
  and `Effect::Terminal` ends every still-attached body so a terminal job can never leave
  a consumer riding keepalives. Pinned by `runtime_e2e::a_burst_of_frames_reaches_the_consumer_whole`
  (3 000 frames + `Done`, every seq present, `[DONE]`, no lag error).
- **Two identical requests shared one execution.** Without an explicit `Idempotency-Key`
  the submission's key was `sha256(tenant|kind|body)` (`store::content_idempotency_key`),
  so a benchmark's second request joined the first job and both consumers received the
  same response — and a later identical request inside the TTL was answered from the
  earlier one's replay, which is response caching keyed on the request body. For an LLM
  that is wrong twice over: every request is unique, and a supplier with free slots must
  serve concurrent conversations from its own queue. The content-hash fallback is gone —
  a request that carries no key gets a fresh one per submission
  (`store::unique_idempotency_key`), the tables stay dedupe pointers for retries that
  DECLARE a key, and the WS face's `ws:{job}` fallback is unchanged. Pinned by
  `runtime_e2e::identical_bodies_without_a_key_run_two_executions` and
  `adapters::tests`'s unique-key assertions, and verified live: two byte-identical
  concurrent bodies now get two job ids and two complete independent streams.

**2026-11-11 — supplier admission and consumer credentials (this host, Linux).** Re-executed
after the fixes below: `cargo fmt --all`, `cargo test --workspace` (**183/183** —
`nexus-core` lib 89, `acceptance.rs` 51, `nexus-server` bin 43) and
`cargo clippy --workspace --all-targets -- -D warnings` (exit 0, zero findings). The
release-build, wasm and Node rows above are the 2026-11-10 pass and were not re-run here.
Five defects, all on the two paths a live supplier/consumer pair actually walks:

- **A hub with an empty allowlist rejected every worker that attested anything.**
  `AttestationAllowlist::verify` checked list _membership_ unconditionally, while
  `validate()` treated an empty list as legal for an anonymous hub — so the daytona
  profile's default boot (`allow_anonymous`, no `model_shas`) accepted the socket upgrade
  and then closed every supplier with `attestation_rejected`, a refusal no supplier can
  repair from its own side. Membership is now gated by `required`, which is what
  `require_attestation`'s own doc comment already claimed; a malformed digest stays a
  refusal either way, because that is a broken supplier rather than an unjudged one.
- **`attestation_required()` ignored the operator's actual signal** (`unwrap_or(true)`),
  so the allowlist itself carried no meaning. The list now implies the rule: digests
  listed ⇒ enforced, list empty ⇒ recorded as unenforced. Anonymity is the _credential_
  switch, not the identity switch, so an anonymous hub that lists digests still enforces
  them (`t13`, and scenario B below). A credential-gated hub with no anchors now refuses
  to boot instead of booting into that dead end.
- **A supplier had to clear a capability floor to join a pool.** The pool's and the
  kind's configured `requires` (model name, `min_context_tokens`, capabilities) were
  checked against the `Hello`'s offer (`actor.rs::resolve_pools`) and merged into every
  OpenAI submission (`adapters::ChatRequest::to_submission`), so a supplier honestly
  advertising its trained 262 144 under its own model name was answered
  `pool_unavailable`, and a consumer's request inherited a context floor it never asked
  for. Neither check exists any more: a supplier joins **by pool name alone**, its
  `requires` is the OFFER jobs are routed by, and a submission's requirements are
  exactly what its own body implies (model, body-derived capabilities, `max_tokens`,
  prompt estimate). A supplier may subscribe under any model — including one other
  workers of the same pool already serve; per-job matching (`scheduler::eligible`,
  `PoolActor::can_satisfy`) load-balances over the workers that actually satisfy the
  request (`t13c`).
- **A consumer was `401`d for a credential the hub could not check.** `ApiKeyRegistry::verify`
  and `PoolTokenRegistry::verify` denied any _presented_ credential that was not on their
  list — including on an anonymous hub whose list is empty, where there is nothing to
  compare it against. Every OpenAI-shaped client sends `Authorization` whether or not the
  endpoint asked, so `POST /v1/chat/completions` with a placeholder key died in ~1 s with
  `{"error":{"code":"unauthorized","message":"unauthorized: unknown api key"}}` and **never
  queued a job** — from the client's side indistinguishable from a dead hub. An anonymous
  hub with an empty set now ignores a credential it cannot check (the request lands in the
  `anonymous` tenant, exactly where a keyless one went); listing a credential restores the
  denial for every other value, so a gated hub still fails closed
  (`an_anonymous_hub_ignores_a_credential_it_cannot_check`).
- **A terminal `Done` whose seq lagged the accepted frames was refused, and the
  refusal stranded the job forever.** The reference supplier reports an engine
  failure with `Done{seq: start_seq}` (`molab-basalt-runtime.py:_run_job`'s error
  paths), which is always below the deltas it already streamed. `on_done` answered
  that with the `refuse_foreign_frame` treatment: a `Cancel` the worker could only
  ack, a dropped lease, and a record left `phase: streaming` **forever** — no reaper
  looks at a lease-less streaming record, heartbeats keep the socket alive, and every
  consumer hung on the SSE `: ping` keepalives until it gave up (observed live: three
  jobs in a row — `hi`, `ping`, and a `max_tokens: 24` probe whose stream went silent
  ~128 s and then died — three refusals, three hung consumers, `nexus_jobs_terminal`
  never moving). The current lease owner's terminal declaration is now accepted even
  when its seq lags: the consumer gets the in-band `error`/`finish` and the record
  closes; the lag is a Warn log with both seqs. The refusal survives where it does
  real work: a job that was **requeued** refuses a previous attempt's late clean
  `Done`, so a zombie attempt cannot end the newer run (`t19`, `t19b`). Also fixed
  while there: a `CancelAck` from a worker that does not own the lease no longer
  releases someone else's lease.

Verified end to end against the real binary with the failing supplier's own declaration
(262 144, capacity 8, `36fdc507…`, tier `ephemeral`; scratch harness in `Temp/nexus/`):
**A** anonymous hub, no allowlist — the daytona shape → `welcome`; `/v1/observe` reports
`ready_workers: 1`, `capacity_slots: 8`, `engine: "ready"`; a
`POST /v1/chat/completions` was dispatched to it and the relayed chunk reached the
consumer's SSE verbatim. **B** same hub with a _different_ digest listed → still
`attestation_rejected`, so nothing opened up. **C** the pack's digest listed → joins.
**D** credential-gated (`NEXUS_POOL_TOKENS` + `NEXUS_API_KEYS` + allowlist) → joins, and
the keyed consumer is tenant `default`. **E** credential-gated with no allowlist → **the
hub refuses to boot**, with the remedies named in the message. **F** anonymous hub, client
sends a made-up consumer key _and_ a stale pool token → supplier `welcome`, consumer
`status 200`, answer relayed (this is the pair that was `401` in the field). **G** same
shape with credentials actually listed and the wrong ones presented → supplier socket `401`,
so the rule still bites. **H** a local hub whose worker streams three deltas and then
reports its engine failure with `Done{seq: 0}` — the field failure verbatim → the
consumer's SSE now carries the in-band `engine_error` and **closes at 0.10 s** (it used to
ping forever), the job goes terminal, and no `Cancel` chases the worker.

Two environment notes, because they will bite the next person:

- **cargo cannot open a direct connection to `index.crates.io` on this host** — every
  fetch dies with `curl error 28, "Connection timed out"` (or "transfer too slow:
  transferred 0 bytes"), while `curl.exe`, PowerShell and Node reach the same URL in
  ~200 ms. Builds in this pass ran with `HTTPS_PROXY=http://127.0.0.1:18080` pointing
  at a throwaway loopback CONNECT proxy (`Temp/opencode/proxy.mjs`, IPv4-pinned
  upstream). Nothing about the package requires it; the per-session env is the fix.
- the registry cache was empty, so `Cargo.lock` is **new in this pass** (the repo-root
  `.gitignore` says Cargo.lock is committed on purpose). Nothing else was added to the
  dependency graph.

The suite is green, so "covered" means _a test asserts it and that test passed on this
host_. What is still unproven is listed in _Known gaps_ rather than averaged away:
every §14 latency number (nothing has been benchmarked).

The compile pass itself fixed roughly forty defects that a read-only review cannot
catch. The classes, so the shape of the work is visible without re-reading the diff:

- **vocabulary.** `PoolId::parse` rejected `llm.chat/qwen38` because the id alphabet
  had no `/`. Pool ids are validated per segment now (`util::is_safe_pool_id` +
  `ids.rs`); with the strict rule in force every `Hello` came back as
  `pool_unavailable` and nothing anywhere dispatched.
- **traits that were never derived.** `Effect` and `LogLevel` gained `Serialize` (the
  WASM host is handed `Effect[]`), `JobView` gained `PartialEq` (`Effect::Snapshot`
  compares), `TerminalReason` gained `Ord` (the §16 tests sort tuples of it), and
  `limits.rs` was missing its `serde` import, which took `BrokerConfig` down with it.
- **object safety across the async boundary.** `QueueBackend: Send + Sync` —
  `Box<dyn QueueBackend>` inside `PoolActor` made `run_actor`'s future `!Send`, and
  `tokio::spawn` refused the only actor loop the hub has.
- **one real scheduling defect.** `PoolActor::fulfil_claims` parked a claim's
  remaining slots in a side list that was merged back only _after_ the loop, so the
  next `pop_front` saw an empty mailbox and broke: a 4-slot worker with four queued
  jobs took exactly one lease (§16 test 12). The remainder is re-queued into the live
  mailbox, still behind the other claimants.
- **dependency API drift.** `axum 0.8` (`Message::Text`/`Utf8Bytes`, no
  `header::LAST_EVENT_ID`), `tungstenite 0.26` (`Message::Frame` in the match),
  `Result` is not an iterator (`timeout(..).ok().ok().flatten()`), a `MutexGuard`
  temporary outliving the `JobRelay` it borrowed (a tail-position `match` without `;`),
  an orphan doc comment at the end of `worker_ws.rs`, and `JsonLines::push`'s nested
  `split_last` whose arms disagreed on `&[u8]` vs `[u8]`.
- **tests that encoded arithmetic the code never had.** The metrics `# HELP` line, a
  13-vs-14-byte delta length, a replay ring sized below its own 1 KiB floor, a payload
  TTL boundary read from the wrong origin, an api key naming an undeclared tenant,
  `Claim`s stamped with another socket's connection id, ten `drain(..)` calls that were
  never awaited, a tenant-concurrency cap that contradicted the test it lived in, and a
  WASM smoke config with no pools, kinds or tenants — which
  `BrokerConfig::from_json_str` refuses, so the smoke would have died in the
  constructor.
- **`FlushGate::force` was a no-op**: it moved a timestamp that `should_flush` never
  read. A `forced` flag now carries the "a terminal or cancelled job never holds bytes
  behind a batching window" rule the tests document.
- **the runtime test hub now runs with `allow_anonymous = false`**, which is what makes
  `job_reads_require_an_api_key_and_operational_routes_do_not` mean anything; every
  request in that file already carries `API_KEY` or `?token=`.

### Fixed in this pass — all fourteen failures, with their real causes

The previous draft listed fourteen failing tests and left several of them as open
semantic questions. Every one is now resolved, and the diagnosis in a few rows
corrects what this README previously guessed. Read it as: _which side was wrong_, and
_what the rule now is_.

| Test                                                                         | Which side was wrong                                          | Resolution                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `t02_concurrent_claims_resolve_deterministically_in_mailbox_order`           | test                                                          | Each `Claim` now carries the connection it arrived on. The `("w-b","w-a")` permutation had stamped `w-b`'s claim with `w-a`'s generation, so the actor correctly dropped it as a stale frame and the "lost dispatch" was the §11 rule working.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere`    | test                                                          | The expected length was `2 400`; the two attempt-2 deltas are 1 800 + 1 200 chars. The test now asserts the delta **texts** verbatim as well as the 3 000-char total, so "one coherent stream" is pinned by content and not only by length.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `t09_consumer_reconnect_with_last_event_id_replays_without_a_gap`            | test                                                          | §7.2's age flush is a timer, so the third delta leaves on the next `Tick` (the same rule `deltas_are_batched_by_size_and_age_not_per_token` pins). The test now advances the clock one flush interval before asserting the three live deltas, and the attach/replay assertions keep their original values — the ring held all three frames the whole time.                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `t12_declared_capacity_is_respected_per_worker`                              | test fixture                                                  | Five jobs have to be admitted for the fifth to stay queued; the shared fixture's 4-job tenant cap answered the fifth `Submit` with `429`. The test raises its own cap (the tenant-limit test keeps the shared number).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume`          | test fixture                                                  | The ComfyUI worker's `Hello` named `llm.chat/qwen38`, a pool this actor does not own, so it joined nothing, the pool had zero capacity, and the image job was refused `400 capability_unsatisfied` before anything was dispatched. It now declares `image.render/comfyui`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `t15` (digest-mismatch half)                                                 | test sequencing                                               | The mismatched declaration was uploaded **after** the artifact had completed, and `ArtifactBook::record_chunk` refuses chunk writes to a completed artifact, so the mismatch path was never reached. It is now the first completion attempt: the refusal is surfaced in band, the slot stays open (`complete` is still false), and the honest retry completes normally.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `t15` (foreign-writer half)                                                  | test                                                          | `w-other` had no live socket at all, so the upload was refused `401 stale_connection` by the §11 generation rule one step before the ownership rule under test. `w-other` now connects, and the refusal is the `409 stale_attempt` the row is about.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `session_affinity_keeps_a_conversation_on_one_worker`                        | test, twice                                                   | Only three of the six turns were ever dispatched, and two of the six were refused outright: every turn stayed in flight, so the fourth filled the rendezvous target's 4 slots and turns five and six hit the shared fixture's 4-job tenant cap (`429 tenant_limit`) — neither of which is what the row tests. A conversation is also sequential, so each turn is now answered with `Done` before the next is submitted. And `seen` only collected the `Submit` effects, which missed the turn the first `Claim` dispatched (no authority exists at that instant); all three messages are counted now, and each turn asserts exactly one dispatch of its own job at attempt 1.                                                                                                        |
| `stale_connection_frames_and_closes_are_ignored`                             | **implementation**                                            | An evicting `Hello` replaced the `WorkerRecord` wholesale: `in_flight` (the actor's own slot accounting) and the engine state vanished, the held leases kept the retired `epoch`, heartbeat-driven lease renewal silently stopped renewing them, and the resume-grace timer `orphan_worker` had armed then deleted a **live** worker at `now + resume_grace_ms`. `on_hello` re-attaches in place now: only the replaced socket's claim authority is voided, the leases move to the new generation, the orphan timer is cancelled, and the engine state carries over only when the previous socket was still live (after a death, readiness must be re-declared — §8).                                                                                                                |
| `resume_for_a_moved_or_finished_lease_is_nacked`                             | test                                                          | The stale `Resume` was sent on the connection the hub had already retired, and stale frames are dropped without a reply. The worker now reconnects first and the nack lands on the live generation, which is the race the row is about.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only`      | test, twice                                                   | The lease deadline is granted at dispatch (`10 + lease_ms` = 60 010); the expected 60 001 anchored it on the worker's last heartbeat, which would shorten a lease by however long the worker had been idle — and the same test's renewal assertion already agreed with the dispatch anchor. Separately, 3 × `heartbeat_ms` equals `lease_ms` at the reference 20 s cadence, so "dispatch paused, still leased" and "lease expired" land on the same tick and the state is unobservable: the test now runs a 5 s cadence, which `Limits::validate`'s `lease_ms >= 2 × heartbeat_ms` rule accepts. §5.1's state is now asserted rather than deferred.                                                                                                                                  |
| `hard_deadline_terminates_a_stalled_job_and_cancels_the_worker`              | test                                                          | `job_hard_cap_ms` is measured from accept (10 + 5 000 = 5 010), the same origin `Limits` documents for the queue and grace windows. The deadline `Cancel` + `DeadlineExceeded` terminal fire on the tick at 5 010 exactly as asserted, and the 60 s lease still does not double-fire.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map`   | **implementation** + test timing                              | A requeued job with no _ready_ worker was parked as `Pending`, and `Pending` is the phase §7.2 still allows a `503 + Retry-After` for — so a job whose SSE body had already been opened came out of the queue-timeout path with an HTTP status nobody could deliver, ending the stream with no explanation. A requeue is now always `Queued`. Two timing expectations also moved onto the events that actually carry them: the dispatch failure lands on the `Ready` heartbeat that drains the worker's retained claim authority (exactly what `t11` asserts), and retention is measured from the terminal (20 + 30 000), so the sweep is asserted at the 30 020 boundary. The test now also pins the in-band retryable `Error` and that the later `Claim` does not re-fail the job. |
| `io::tests::full_object_read_streams_bounded_chunks_and_verifies_the_digest` | **implementation** — and the earlier diagnosis here was wrong | This README blamed "`sha2 0.10.9` incremental hashing" and advised re-testing on another host. The streaming task was feeding the _already-final_ digest into `store::sha256_hex`, which digests its input, so it compared `sha256(sha256(body))` against the recorded `sha256(body)` and aborted **every** correct whole-object download (the mismatch test passed only because any disagreement aborts). It now hex-encodes the 32 digest bytes; `Get-FileHash`, `store::sha256_hex` and the streaming chain now all agree, and the mismatch-aborts test still proves the check bites. No dependency fault, no host caveat.                                                                                                                                                        |
| `runtime_e2e::loading_worker_answers_503_with_retry_after_before_any_body`   | **implementation** (runtime)                                  | The actor emits `Accepted` for a job that is still `Pending` inside the no-worker grace, and the dispatcher resolved the first-answer gate on it, so the handler wrote SSE headers and answered `200` before the grace could expire. The gate now stays unresolved while the job is `Pending`, and the `Assigned` frame is what opens the body — which is §7.2's "the 503 is still free until the first byte", with the core's `t06` unchanged.                                                                                                                                                                                                                                                                                                                                      |
| `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse`           | test                                                          | An OpenAI stream is **three** chunks plus the sentinel: the role delta, the relayed frame, and the finish chunk carrying `usage`. The expected 2 had forgotten the terminal chunk. The count is kept and every chunk is now also asserted field by field (role, the half-written tool argument, `chatcmpl-x`, `finish_reason`, `usage`, the `id:` replay cursors).                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `runtime_e2e::consumer_disconnect_reaches_the_worker_as_cancel`              | **implementation** (runtime)                                  | `pump_sse` drains the submission's private replay queue before it starts watching the fan-out, and for a fresh (non-attaching) submission nothing ever closed that queue: the pump sat in a receive loop that would not end until the job reached a terminal, so deltas were withheld until then **and** a hung-up consumer was never noticed, so §6's `Detach` → `Cancel` never fired. A non-attached `Accepted` now closes the queue, the same way the `Reject` path already did.                                                                                                                                                                                                                                                                                                  |

Outstanding work, in order: take §14's latency numbers by running the hub, which nobody
has done yet. (This list used to start with installing `wasm-bindgen-cli` at the locked
version so `yarn wasm:glue`, `yarn build:wasm` and `yarn smoke:wasm` could run; the wasm
leg those three verbs built was removed on 2026-11-14, so the item is gone with it.) The
`clippy -D warnings` step that used to sit between those two is green and no longer
blocks `yarn verify`.

No performance claim is made anywhere in this README that has not been measured by
a human running the binary; §14's 30–60 ms is the task's own budget, restated, and
**no measurement of it has been taken** (the hub binary exists now, so `yarn start` +
`yarn worker` can produce one).

## Running it

```bash
# native hub (loopback by default)
export NEXUS_ALLOW_ANONYMOUS=1        # local smoke only; unset = credentials required
yarn start                            # cargo run -p nexus-server --bin nexus-hub

# a worker fronting the reference Strata engine on 127.0.0.1:8090
export NEXUS_POOL_TOKEN=pool-token-a
yarn worker                            # the hub assigns this connection a UUIDv4 id

# consumer
curl -N localhost:8085/v1/chat/completions \
  -H 'Authorization: Bearer sk-1' -H 'Idempotency-Key: demo-1' \
  -d '{"model":"qwen38","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

### Routes

Everything the hub listens for, as registered in `crates/nexus-server/src/main.rs:118-164`.
The frame vocabulary and the body shapes are the specification's §7
(`documents/specifications/hub-specification.md`).

| Route                                                        | Who                          | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------------------------------ | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /v1/worker` (WS)                                        | supplier                     | The one dial-out socket: `Hello`/`Ready`/`Claim`/`Frame`/`Done`/`Resume` in, `Welcome`/`Assigned`/`Ack`/`Cancel`/`Reject`/`ResumeNack`/`Bye` out. Pool token.                                                                                                                                                                                                                                                                                                    |
| **`GET /v1/consumer` (WS)**                                  | consumer                     | **New.** NEX-3's face: `submit`/`attach`/`detach`/`cancel` as JSON text messages, and `accepted`/`rejected` then tagged `meta`/`delta`/`event`/`log`/`artifact`/`finish`/`error`/`gap`/`replay` plus exactly one `end {job, reason}` per task out — several tasks multiplexed on one socket. The API key is checked **before** the upgrade: a bad key is a plain `401` with `www-authenticate: Bearer realm="nexus"`, never a socket that opens and then closes. |
| `POST /v1/chat/completions`, `POST /v1/completions`          | consumer                     | OpenAI-shaped; the SSE response body **is** the stream, on the connection it was posted on. Unchanged, and no `detach` field: the response body is the subscription.                                                                                                                                                                                                                                                                                             |
| `POST /v1/jobs`                                              | consumer                     | **Changed, breaking.** `202` + `Location`. The body must now carry `"detach": true`; absent or `false` is `400 detach_required` (`consumer.rs:907-914`), because this route answers and hangs up, so it cannot honour the coupled contract.                                                                                                                                                                                                                      |
| **`GET /v1/jobs`**                                           | consumer                     | **New.** Lists the jobs this hub currently holds a live relay for, filtered to the calling tenant.                                                                                                                                                                                                                                                                                                                                                               |
| **`GET /v1/jobs/{id}`**                                      | consumer                     | **New.** One job's status, tenant-checked (a foreign job is `404`).                                                                                                                                                                                                                                                                                                                                                                                              |
| `GET /v1/jobs/{id}/events` (SSE), `GET /v1/jobs/{id}/result` | consumer                     | Follow a detached job's stream with `Last-Event-ID` replay; fetch its result (`?from=&to=` ranges).                                                                                                                                                                                                                                                                                                                                                              |
| `DELETE /v1/jobs/{id}`                                       | consumer                     | Explicit cancel; the record keeps its normal terminal retention, unlike a lost connection.                                                                                                                                                                                                                                                                                                                                                                       |
| `GET /v1/models`, `GET /healthz`, `GET /metrics`             | consumer / ops               | Model list, health, Prometheus exposition. The last two are open on purpose: a load balancer has no API key.                                                                                                                                                                                                                                                                                                                                                     |
| **`GET /v1/observe`**                                        | consumer / ops / **browser** | **New.** One read-only document for the monitoring UI: readiness, one row per pool, worker liveness and load, the active jobs with their leases, and the live consumer sockets with the jobs each is attached to. Open like `/healthz`, and **always `200`** while the hub is serving. The only route that _relies_ on CORS (the layer wraps every route). See the section below.                                                                                |
| `GET /v1/payloads/{id}`, `GET\|PUT /v1/artifacts/{id}`       | supplier / both              | Bulk payload fetch, and artifact upload/download with query-parameter ranges and a measured digest.                                                                                                                                                                                                                                                                                                                                                              |

### Headers

`x-supplier-id` points both ways, and it is the only header the pinning contract adds. The
normative rule is spec **NEX-4.7**, the spelling is spec §7's **Supplier pinning (`x-supplier-id`)**
block; what follows is what a client relies on.

- **Request — optional.** `POST /v1/chat/completions`, `POST /v1/completions` and `POST /v1/jobs`
  read `x-supplier-id`: the hub-minted id of the supplier this request should run on, which is the
  id the hub **answered with last time**, never one the client invents. It names a **connection**,
  not a machine and not a model: a reconnect mints a fresh UUIDv4, so the id in the client's hand is
  gone for good the moment that supplier's socket drops, and it is never reissued for the machine
  behind it. While that supplier's connection is live, only it takes the job — it waits even when
  that supplier is still loading or momentarily has no free slot. Once it is gone, the hub chooses
  any supplier that serves the requested model, exactly as it does when the header is absent. A bad,
  unknown or malformed value is never an error: it costs the pin, not the request. If the request
  also carries `x-session-id` and the two hints point at different suppliers, **the pin wins for the
  supplier it names** — that supplier takes the job even though the session hash pointed elsewhere,
  and no other supplier is released (spec NEX-4.7's precedence rule); without it, that combination
  was leasable by nobody and died at the claim deadline. The consumer WebSocket has no headers, so
  that face carries the same value as `submit.supplier`.
- **Response — the supplier actually connected,** read from the job's lease, not from what was
  asked for. **It names the _first_ supplier the task was leased to, and it does not move after
  that.** On a requeue — the first supplier leases, dies, a second one produces the answer — a head
  that flushes late still names the first one, because the header exists so a client can ask again
  for the connection it was already handed; an id that drifted between two claims would report a
  migrated conversation to a client whose own request never moved. Who holds the lease **now** is a
  different fact with a different home: `lease.worker` on `GET /v1/jobs/{id}`, and after a takeover
  that is the field to read. Present on a streamed SSE body and on a non-streaming aggregate.
  **Absent** on a refusal, where no lease ever existed. A `POST /v1/jobs` `202` names the supplier
  when the submit turn itself leased one; while the job is still queued it names nobody, and a
  `202` that **joins** a job that has already finished names nobody either, because the lease that
  carried the id has been released — a join carries an id exactly when a lease exists, on this
  route and on the completion routes. A queued or terminal job's lease is on `GET /v1/jobs/{id}`,
  which is where a client reads it instead. Absent means "no supplier was connected", never "this
  hub does not know the header". On the WS face the same value arrives as `accepted.supplier`,
  because a WebSocket cannot carry a header after the upgrade.
- **One timing consequence, on the streaming path only: a queued request holds its response head
  open until the lease it will name.** A submission the hub admitted into the queue with nobody
  connected yet does not get its headers at admission — the face waits for the relay to learn the
  lease (`consumer::supplier_before_body`, reading `JobRelay::supplier_watcher`, which the
  dispatcher writes in `JobRelay::note_supplier` when the `Assigned` goes out) and then writes the
  head with the id in it. The wait is bounded by the kind's own `claim_deadline_ms` — `Limits`'
  default `600_000` ms, a kind's override honoured — which is the same interval that task was already
  going to sit queued for, so there is no new timer and no new knob. **A client that hangs up
  inside that window is not left holding the position.** The face arms a drop guard around the wait
  (`consumer.rs`'s `SupplierHoldDetach`), and hyper drops the request handler the instant the
  connection dies, so the guard posts the very detach the SSE pump would have posted: the queued
  task is cancelled and its row deleted then rather than at the deadline, and a task the client
  pinned stops refusing every other consumer's claim for it. The pump could not have noticed — its
  body channel and watcher are built only after the hold returns, so nothing polls them while the
  head is pending (`a_hangup_inside_the_head_hold_cancels_the_queued_task_it_never_leased`).
  **While it is open, no bytes of any kind reach the client:** an SSE keepalive is a body byte, so
  it cannot precede the head, and an intermediary with an aggressive idle timeout can therefore
  close a request that is merely queued behind busy capacity — that stays the cost of naming the
  supplier in the head rather than at admission. `HTTP status before bytes` for refusals —
  `503 + Retry-After` included — is unchanged, because every refusal arm returns before the wait
  begins; only the accepted-but-not-yet-leased window moved.
- **The aggregate and detached paths never hold.** `stream: false` writes its head when the answer is
  assembled and reads the lease at that moment, and the `POST /v1/jobs` `202` answers on its own
  verdict: neither parks a receipt on a busy slot until the claim deadline lapses.
- **CORS.** The hub now **exposes** `x-supplier-id` beside `x-nexus-job` and `x-nexus-chunks` on
  `Access-Control-Expose-Headers`, so a browser can read the value back. That list goes on
  **non-preflight responses only** — tower-http's `CorsLayer` does not write it on the `OPTIONS`
  answer, and a preflight is not a response a page reads a header off, so that is the sufficient
  place for it (`main.rs`'s `observe_cors`, pinned by
  `a_cross_origin_dashboard_can_read_observe`). Sending the header needed no change: the layer has
  always allowed any request **header** — but its **method** list is `GET`, `HEAD` and `OPTIONS`,
  so a cross-origin page still cannot replay a pinned `POST` at all. The exposed list is for
  readers of an answer, not posters of a request.

### `GET /v1/observe` — the monitoring plane

`crates/nexus-server/src/observe.rs`. One request answers every question a dashboard
asks, so a static bundle hosted on GitHub Pages does not have to combine `/healthz`
(counts only), `/metrics` (a text exposition the browser must parse) and one
`GET /v1/jobs/{id}` probe per job.

```jsonc
GET /v1/observe            // no Authorization header
{
  "protocol": "nexus-hub-observe/v1",
  "now_ms": 1762760000000,                 // epoch ms, hub clock
  "readiness": { "status": "ok", "ready_workers": 1, "loading_workers": 0,
                 "queue_depth": 0, "mailbox_pending": 0, "warm_models": ["qwen38"],
                 "capacity_signal": "warm", "connected_workers": 1, "active_jobs": 1,
                 "pool_mailbox_depth": [ … ] },   // field names mirror `/healthz`
  "pools":  [ { "id": "llm.chat/qwen38", "queue_depth": 0, "ready_workers": 1,
                "loading_workers": 0, "connected_workers": 1, "leases": 1,
                "mailbox_pending": 0 } ],
  "workers":[ { "id": "w-…", "pools": ["llm.chat/qwen38"], "link": "live",
                "engine": "ready", "epoch": 1, "capacity_slots": 4, "free_slots": 2,
                "free_kv_tokens": 8192, "in_flight": ["job-…"], "outstanding_claims": 3,
                "last_seen_ms": 1762759999000, "telemetry": null,
                "resumable": false, "available_slots": 3 } ],
  "jobs":   [ { "id": "job-…", "pool": "llm.chat/qwen38", "tenant": "default",
                "phase": "assigned", "running": true, "created_ms": 1762759990000,
                "consumers": 2,
                "lease": { "worker": "w-…", "epoch": 1, "attempt": 1,
                           "deadline_ms": 1762760005000 } } ],   // null while queued
  "leases": [ { "job": "job-…", "worker": "w-…", "state": "leased", … } ],
  "consumer_connections": [ { "id": "c-…", "tenant": "default",
                              "since_ms": 1762759980000, "peer": null,
                              "attached_jobs": ["job-…"] } ],
  "unresponsive_pools": []
}
```

Rules worth stating, because a dashboard is written against them:

- **Open, and always `200`.** No key, same argument as `/healthz` (`main.rs`'s
  "operations: open on purpose" group). Unlike `/healthz`, zero ready workers is a
  field (`readiness.status: "degraded"`), never a `503`: a monitor should treat only a
  transport failure as downtime.
- **The actor is asked, not the mirror.** Workers, leases and job phases have exactly
  one owner (`nexus-core/src/actor.rs`), so `Hub::pool_snapshot` sends one
  `ActorMsg::Observe` per pool and reads the answer off the same mailbox that mutates
  the state — the identical serialized read `GET /v1/jobs/{id}` already relies on. The
  mirrored `PoolGauges` appear only where the actor could not answer (per-pool mailbox
  depth) or where the actor is missing.
- **A wedged pool cannot hang the dashboard.** The fan-out is concurrent and each
  probe is bounded by the same 2 s budget every read-through probe uses
  (`state.rs`, `PROBE_TIMEOUT_MS`). A pool that does not answer keeps its mirrored
  gauges in `pools[]` and is named in `unresponsive_pools`; the response is still
  `200`. `None` never means "empty pool".
- **Clocks are translated, not mixed.** The actor's clock is process-relative
  (`std::time::Instant`), so `last_seen_ms`, `created_ms` and a lease `deadline_ms`
  are translated to epoch ms by one function (`state::actor_to_wall_ms`), with signed
  arithmetic so a deadline in the future stays in the future.
- **`leases` is null while a job is queued**, `telemetry` is arbitrary JSON or `null`,
  and `jobs[].phase` is the actor's own vocabulary (`pending|queued|assigned|streaming`
  for active work) rather than a single `"running"` — the queued-vs-leased distinction
  is the whole point of the view. `jobs[].running` is the boolean for a client that
  only wants "in flight". Extra fields are additive; renaming or dropping a documented
  one is breaking.
- **CORS** (`main.rs`, `observe_cors`): any origin, `GET`/`HEAD`/`OPTIONS`, any request
  headers, **no credentials**, preflight cached 600 s. `Allow-Origin: *` is legal only
  because nothing is sent that could identify the caller — a cross-site read cannot be
  forged into a mutation. The layer is outermost, so a preflight is answered without
  reaching the router. It wraps every route; see the privacy note below.
- **Consumer sockets are hub state, not actor state.** The actor keys its consumer sets
  by a per-(connection, task) id and cannot answer "which sockets are up", so
  `consumer_ws` registers one record per accepted socket in the hub (`Hub::
consumer_connections`) and updates it on attach, on end-of-stream, and on close via
  a `Drop` guard — which is what makes an abnormal disconnect disappear from this
  document instead of lingering as a live connection.

### Configuration

Load order: built-in defaults → `NEXUS_CONFIG_JSON` (replaces **everything** — every
`NEXUS_*` variable stops applying, credentials included, `config.rs:334-347`) → individual
`NEXUS_*` overrides. Secrets are never hardcoded: `validate()` **refuses to boot** with an
empty pool-token set or an empty API-key set unless `NEXUS_ALLOW_ANONYMOUS=1` says
otherwise, and refuses an empty attestation allowlist unless the hub is anonymous or has
turned attestation off explicitly with `NEXUS_REQUIRE_ATTESTATION=0` — the "no fail-open
auth" requirement. `requires` (per pool and per kind) has **no** environment variable and
floors nothing: a supplier joins by pool name and a submission routes on the model its own
body carries, so the built-in sample topology needs no per-deployment tuning. The field is
still read as the DEFAULT `requires` of a generic submission that declares none of its own
(`consumer.rs::build_submission`), and a deployment whose default binding should differ
from the built-in sample (qwen38) is a `NEXUS_CONFIG_JSON` deployment, and that document
must restate every pool, kind and credential it wants to keep.

| Variable                                                     | Default                | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------ | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `NEXUS_LISTEN`                                               | `127.0.0.1:8085`       | Consumer + worker surface. Keep it loopback unless you terminate TLS elsewhere.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `NEXUS_POOL_TOKENS`                                          | —                      | Comma-separated worker tokens; stored as SHA-256 digests.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `NEXUS_API_KEYS`                                             | —                      | `secret=tenant` pairs; a bare secret maps to `default`. The tenant must also exist in the config's `tenants` map (only `default` ships in the built-in defaults): `validate()` refuses a key that names an undeclared tenant, so a multi-tenant deployment supplies `NEXUS_CONFIG_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `NEXUS_MODEL_SHAS`                                           | —                      | Attestation allowlist. Seed it from `SHARD_TABLE` in `services/molab/molab-strata-runtime.py:94-99`. **Listing any digest makes the list the rule**; listing none means the hub has no trust anchor, so it refuses malformed digests but draws no conclusion from a well-formed one.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `NEXUS_REQUIRE_ATTESTATION`                                  | implied by the list    | `1` demands attestation even with an empty allowlist (which then makes the hub refuse to boot rather than reject every supplier), `0` turns the check off even with digests listed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `NEXUS_ALLOW_ANONYMOUS`                                      | `0`                    | Local smoke only. It is the **credential** switch, not the identity switch: an anonymous hub that lists `NEXUS_MODEL_SHAS` still enforces the allowlist. With no credentials configured it also **ignores** a credential a client presents anyway — every OpenAI client sends `Authorization`, and a key this hub cannot check cannot be wrong (`auth.rs:94-122`, `:169-199`). Once any credential is listed, an unknown one is refused again.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `NEXUS_ARTIFACT_ROOT`                                        | `Temp/nexus/artifacts` | Artifact spill directory (repo-root `Temp/`, per root `AGENTS.md`). **Set it to the empty string for memory-only mode**: the hub starts and creates no directory, and a kind whose result must be an artifact is refused at admission with `400 artifact_unavailable` instead of half-running (spec §8.2).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `NEXUS_INLINE_PAYLOAD_CAP_BYTES`                             | `8 MiB`                | Above it the prompt travels by `payload_ref`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `NEXUS_LEASE_MS` / `NEXUS_HEARTBEAT_MS`                      | `60000` / `20000`      | `validate()` requires lease ≥ 2× heartbeat. `NEXUS_HEARTBEAT_MS` also sets the supplier socket keepalive: a probe every quarter of it (clamped 250 ms-5 s), a dead-path verdict after a whole heartbeat plus one probe of total silence.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `NEXUS_CONSUMER_KEEPALIVE_MS`                                | `60000`                | The **consumer** socket keepalive, the consumer-direction twin of `NEXUS_HEARTBEAT_MS` above: a ping every quarter of the ruler (clamped 250 ms–5 s), and a dead-path verdict only after `max(3 × probe, ruler + probe)` of total silence — no consumer frame and no `Pong`. At the default that is a **5 s probe and a 65 s verdict: thirteen unanswered pings**. The ruler moved from `20000` on 2026-11-15 because the consumer verdict is **not** symmetric with the supplier's: a supplier verdict pauses dispatch, while a consumer verdict applies NEX-6.1/6.3 to whatever that socket is attached to and so **cancels and deletes** the task — record, answer and idempotency entry — making the client's retry pay for the GPU twice. Nothing in the hub's correctness needs a fast consumer reap, and the state this probe exists to reclaim belongs to a socket that has been silent for **hours**; a false reap costs a rerun, a missed one costs only bytes until the client eventually hangs up. `0` turns the probe off, and the socket then lives exactly as long as its client says it does, which is what this face used to do: a consumer that vanished without a `Close` (a tab killed behind a proxy, a laptop that slept, an edge that dropped the path quietly) kept its session, its pumps, one fan-out queue per task it had ever watched, and its actor-side consumer slot for the life of the process. The ruler is measured in pings and never in job progress: a task producing nothing for ten minutes is an alive stream, and both survive it. Pinned as arithmetic by `consumer_ws::tests::the_probe_windows_are_the_arithmetic_the_comments_claim`. |
| `NEXUS_RESUME_GRACE_MS`                                      | `1500`                 | `0` = strict "expire immediately" (see _Open decisions_).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `NEXUS_NO_WORKER_GRACE_MS`                                   | `2000`                 | Pre-body grace; then `503 + Retry-After`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `NEXUS_JOB_HARD_CAP_MS`                                      | `1800000`              | Per-job deadline.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `NEXUS_DETACHED_ORPHAN_TTL_MS`                               | `300000`               | **New.** How long a detached (`"detach": true`) job that **nobody has ever watched and nobody has claimed** is kept before it is forgotten with its queue entry, tenant slot and idempotency key. Must be `> 0` — a zero would delete a detached job the instant it was submitted. A job already running is not covered by it; `job_hard_cap_ms` and the lease rules are.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `NEXUS_IDEMPOTENCY_MAX_ENTRIES` / `NEXUS_IDEMPOTENCY_TTL_MS` | `4096` / `600000`      | Bounds of the idempotency mirror (both hub-side and actor-side); the TTL must be `> 0` or a duplicate submission would re-run work that is already in flight. Entries also go the moment the job they name is forgotten.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `NEXUS_MAX_CONTINUATIONS`                                    | `0`                    | Resume-by-continuation (§7.4) is **opt-in**: `0` leaves a mid-stream death terminal, `1` re-runs it once on another worker.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `NEXUS_CONTINUATION_ALLOW_OPAQUE`                            | `0`                    | With continuation on, an attempt whose relayed output was **not** reconstructible assistant text (tool-call chunks, or frames interleaved with `event`/`log`/`artifact_chunk`) is still terminal. Set `1` only for a pool whose workers restart cleanly from an arbitrary prefix.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `NEXUS_FLUSH_INTERVAL_MS` / `NEXUS_FLUSH_BYTES`              | `75` / `1536`          | Outbound SSE batching window (task §7.2).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `NEXUS_MAX_BODY_BYTES`                                       | `64 MiB`               | HTTP request body cap (tower-http `RequestBodyLimitLayer`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

Every name is handled in `nexus-core/src/config.rs`, and a typo inside
`NEXUS_CONFIG_JSON` is a hard startup error (`deny_unknown_fields`), not a silently
retained default.

## No WASM boundary

There is no `wasm32-unknown-unknown` build in this package, and there never was a WASM
_hub_: a WASM module has no listener to accept into, and tokio's IO/time drivers do not
exist on that target, so the axum side could only ever run native. What used to exist
alongside the server was `crates/nexus-wasm`, a `wasm-bindgen` re-export of the same
`PoolActor` for a JS host that supplied the clock and performed the effects.

It was removed on **2026-11-14**, because nothing consumed it. The package is
`"private": true`, publishes nothing, and no other workspace package imports it — the
hub is deployed as one native binary onto a sandbox (`scripts/deploy-*.ts`), and the
monitoring UI (`@underload/nexus`) reads the hub over HTTP. The re-export had no test of
its own, and generating its Node glue needed `wasm-bindgen-cli` installed at the exact
version `Cargo.lock` pins the library at, which this host never had — so its
decision-parity claim was an argument in a header comment, never an executed check.

What still holds, deliberately unchanged:

```text
native : sockets, HTTP/SSE, timers, disk artifacts   →  ActorMsg
core   : ActorMsg in, Effect[] out                  →  nexus-server performs the I/O
```

The scheduling/ownership brain is one state machine with one implementation. Admission,
the atomic claim, leases, requeue-vs-terminal, seq de-duplication, replay, flush
batching, spill decisions and artifact TTLs live in `nexus-core`, which is synchronous
and IO-free by rule — no tokio, no threads, no `SystemTime` (every clock is an injected
`now_ms`), `BTreeMap`/`BTreeSet` wherever order is observable. That rule was written down
as a wasm requirement; it survives as the determinism requirement behind the core's
exact-value tests, and removing the crate did not relax one line of it.

## Module boundaries (task §13) and how promises are kept

- **`protocol`** — frame vocabulary, both directions: supplier
  (`Hello/Ready/Claim/Frame/Done/Cancel/Resume` in,
  `Welcome/Assigned/Ack/Reject/ResumeNack/Bye` out) and consumer
  (`ConsumerFrame::Submit/Attach/Detach/Cancel` in, `HubConsumerFrame`'s
  `Accepted/Rejected/meta/delta/event/log/artifact/finish/error/gap/replay/End` out, with
  `EndReason` naming why a stream closed), integer version negotiation
  (mismatch = clean reject + close), the wire codec — JSON lines for the worker direction,
  one JSON object per WebSocket text message for the consumer direction — with
  per-direction size caps (`decode_consumer_frame`, `protocol.rs:1015`),
  and semantic validation of untrusted input (id alphabet, capacity ≥ 1, artifact
  frames must carry a manifest).
- **`actor` (`pool_actor`)** — the only mutator. Claim semantics are defined as
  **authority granted in mailbox order**: `Claim` appends `(worker, slots)` to a FIFO,
  fulfilment consumes the oldest authority first, and a job leaves the queue the moment
  it is leased. Two claims in the same tick therefore have exactly one winner, and the
  winner is a function of message order alone.
- **`queue`** — a trait, with `enqueue/claim/requeue/remove/ordered/purge_expired/drain`
  mapped in the module docs to Redis Streams (`XADD`/`XREADGROUP`/`XAUTOCLAIM`),
  Postgres (`SELECT … FOR UPDATE SKIP LOCKED`) and NATS JetStream (pull, `AckExplicit`,
  ack-wait) without touching the actor. The trait is synchronous and non-blocking on
  purpose: an async backend satisfies it with one round trip per call off the actor
  thread.
- **`relay`** — the neutral consumer event vocabulary, SSE framing primitives, and the
  flush gate. Dialect bodies belong to `adapters`.
- **`adapters`** — OpenAI request parsing (tools/vision/logprobs/structured-output
  inference, prompt token estimate), chunk shapes, delta merge and the non-streaming
  assembly; the generic job API bodies; `/v1/models` and `/healthz`.
- **`store`** — spill decision (`size > inline cap`), in-memory payload book bounded by
  entry count **and** total bytes, artifact book with chunk accounting, TTLs, traversal-
  checked paths and declared-vs-measured digest/size verification.

## Open decisions (task §17), resolved

1. **Wire encoding for M0** — JSON lines, one frame per line, `{"type": …}`
   discriminator; a length-prefixed binary encoding is a later optimization. Chosen for
   debuggability from a notebook cell while the protocol is unstable.
2. **Non-LLM consumer contract** — job API only (`202` + `/events` + `/result` +
   `DELETE`). No synchronous "block until artifact" endpoint: a 400/503 for "no capable
   worker" is worth more than a request that dies at 80 % progress, and blocking would
   hold a socket open for minutes.
3. **Queue depth / admission** — **unbounded queue, no admission rate limit**. The
   hub never answers `429` for capacity: a submission beyond the tenant budget is
   accepted and _awaited_ in the queue, and the tenant concurrency cap (16 by
   default, per-tenant override) gates **dispatch**, not admission — the job runs
   the moment a slot frees. A job that is dispatchable (tenant slot free) still
   gets `claim_deadline_ms` before it is answered `503` (streaming, before body
   bytes) or marked retryable-failed (job API), so a hub that can never run the
   work still says so instead of hanging. The queue is bounded in bytes where it
   matters: per-prompt payload/token caps plus the spill budget, not an entry
   count.
4. **Conversation history** — the hub stores **no** history. It trusts a
   client-supplied `messages` array plus a session hint header (`X-Session-Id`), and uses
   the hint only for rendezvous-hash affinity. Cheaper, and it keeps prompt text out of
   long-lived hub state. A second affinity now sits beside it and is state-free the same way:
   `x-supplier-id` names **one supplier** directly, rather than hashing a conversation onto
   whichever connection the hash lands on (spec **NEX-4.7**). Both are hints, and where they point
   apart the explicit one wins — for the supplier it names and for nobody else — so the hash cannot
   veto a promise the consumer made by hand. Neither hint
   outlives a supplier reconnect, because a supplier id is minted per connection — so a pin is
   how a conversation stays on the supplier that answered it _while that link lives_, not how it
   re-finds the machine after a restart.
5. **Per-kind timeouts** — `claim_deadline_ms` (wait for dispatch), `job_hard_cap_ms`
   (hard cap), `no_worker_grace_ms` (pre-body grace), all per-kind overrides of global
   defaults. `lease_ms` is renewed only by heartbeats.
6. **Single replica** — yes for the first deployment, so the queue stays in-process.
   The `QueueBackend` trait is the upgrade path; nothing else changes.
7. **Multi-consumer fan-out** — in scope, read-only: several consumers may attach to the
   same job (each gets the live stream, joiners get a replay), but `n>1` sampling (one
   submission executed N times) is **deferred**: it would need a fan-out/selection policy
   the task does not specify. Two joins no longer race each other: a joiner **borrows** the
   relay that belongs to the job rather than owning one, so its request ending must not
   prune the map entry — removing it while the original submission's pump is still attached
   would move the fan-out to a fresh relay under a live stream and silence the first
   consumer's deltas (`prune_relay`, `consumer.rs:739-758`, honoured at `:835`; pinned by
   `a_non_streaming_retry_on_a_key_assembles_the_running_answer`).
8. **Worker client location** — in-repo reference is `crates/nexus-worker-example` (Rust,
   because this package keeps application logic in Rust). A per-host client beside the
   engine it fronts (`services/molab/`-style, one Python cell) is the deployment shape for
   notebook hosts; the protocol is identical, and `protocol::encode_line` /
   `protocol::decode_worker_frame` in `nexus-core` are the one codec any such client has
   to match, caps included. (Until 2026-11-14 the WASM build re-exported these as
   `encode_hub_frame`/`decode_worker_line` for a JS client; those wrappers went with the
   crate, the codec did not move.)

Two documented deviations from a literal reading of the task:

- **Connection identity vs `Resume`** (§5.1 vs §4). Identity is
  **connection-scoped**: the hub mints a fresh UUIDv4 for every supplier socket
  and sends it in `Welcome`; the supplier never declares one. A dropped
  connection's id is retired and never reused, so a reconnect is a new worker
  with no leases and a `Resume` cannot re-take the old attempt. Dispatchability
  still expires **immediately** (the worker leaves the ready set, counts as zero
  capacity, gets no new work, cannot renew), while **ownership** is retained for
  `resume_grace_ms` (1500 ms) only so the loss is decided without a second owner
  appearing: at grace expiry the §5.1 table is applied (zero-output+idempotent →
  requeue preferring a different worker; zero-output+non-idempotent → fail
  loudly; bytes delivered → in-band terminal, or continuation). A `Resume` on a
  fresh connection is refused with `ResumeNack`; stale-connection frames are
  dropped by connection generation. A duplicate id handed to the actor by an
  embedder (the native server never does) still takes the defensive eviction
  path; it is not part of the wire contract.
- **`payload_ref` byte ownership.** Task §10 says the _worker_ `GET`s the payload; the hub
  must therefore hold the prompt bytes somewhere. They are held in **memory only**
  (`payload_ttl_ms`, bounded by entry count and total bytes), never on disk, per §9's
  retention rule. If the TTL lapses before dispatch, the attempt is refused with a
  retryable `503`-class terminal (`queue_timeout`) rather than handing the worker a
  reference to bytes that are gone.
- **Resume-by-continuation is opt-in, and it refuses opaque output** (§7.4).
  `max_continuations = 0` by default: a mid-stream death is terminal, because a
  continuation costs a full prefill on a second worker, may show the consumer a seam, and
  §7.4 asks the hub to gate on prompt-cache warmth the hub cannot observe. With
  `NEXUS_MAX_CONTINUATIONS=1` the attempt is re-run elsewhere carrying `start_seq` plus
  the partial text — but only when that text is reconstructible assistant prose. A stream
  that carried tool-call chunks (`"tool_calls"` / `"function_call"`) or interleaved
  `event`/`log`/`artifact_chunk` frames is marked `output_opaque` at relay time
  (`relay_into`) and stays in-band-terminal, with a `Warn` log naming the reason;
  `NEXUS_CONTINUATION_ALLOW_OPAQUE=1` is the explicit opt-in for a pool whose workers
  restart cleanly from an arbitrary prefix. Neither half is automatic: the budget **and**
  the content class both have to be turned on.

## Performance bounds, stated rather than hidden

- Added proxy overhead ≈ **30–60 ms** (dispatch over a live WS 5–20 ms, loopback hop
  ~0.1 ms, delta → consumer 10–30 ms plus the flush window). TTFT is dominated by queue
  wait and prefill, not by the middleman (task §14).
- Outbound SSE is batched per job: flush at **1536 bytes or 75 ms**, whichever first, and
  a timer flush so a small tail never waits for the next token. Per-token frames would be
  ~4× the syscalls and JSON parsing for no perceptible gain.
- Worker in-flight is capped at **declared** capacity (reference Strata `PARALLEL = 4` →
  4; ComfyUI → 1), clamped at 64 against typos, and reconciled against heartbeats so a
  stale `free_slots` cannot over-dispatch.
- Bounded everywhere: mailbox 512, per-worker outbound 256, per-consumer live queue
  `consumer_backpressure_frames` deep **per subscriber** (a stuck consumer is dropped
  past a 25 ms grace, never silently — a body that keeps up is never missing a frame),
  replay ring (256 frames / 1 MiB), queue depth 128, spilled payloads 64 entries /
  512 MiB, idempotency map 4096 entries, terminal-job retention 300 s, worker fan-in 32
  per pool. Two bounds were added in the second pass:
  an artifact download holds at most 5 × 64 KiB between the file and the socket
  (`io::STREAM_CHUNK_BYTES` × `io::STREAM_INFLIGHT_CHUNKS`), and a frame pushed to a
  worker gets 25 ms (`state::SEND_GRACE_MS`) before the connection is declared dead
  rather than the actor parking on it.
- Release profile: `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`,
  `panic = "abort"` (fail-stop for a single-replica broker whose parse paths all return
  `Result`; tests keep `unwind`), `strip = "symbols"`, `debug = "line-tables-only"`.
- Metrics that exist because debugging is otherwise blind (task §13): `nexus_queue_depth`,
  `nexus_ready_workers`, `nexus_loading_workers`, `nexus_leases`, `nexus_ttft_ms`,
  `nexus_dispatch_latency_ms`, `nexus_job_duration_ms`, `nexus_frames_total{kind}`,
  `nexus_frames_duplicate_total`, `nexus_seq_gaps_total`, `nexus_lease_expiries_total{kind}`,
  `nexus_inband_errors_total`, `nexus_artifact_bytes_total`, `nexus_spill_bytes_total`,
  `nexus_tokens_total`, `nexus_cancels_sent_total`, `nexus_resume_accepted_total`.

## Security and retention

- **Distinct credentials per direction**: pool token for workers, API key for consumers,
  never one secret for both. Both are compared by SHA-256 digest with constant-time
  equality; the plaintext is never retained in the registry. On a hub that configures no
  credential for a direction (`allow_anonymous`, empty set), a credential that arrives
  anyway is ignored rather than refused — the hub has declared nobody is authenticated,
  so a presented-but-uncheckable secret buys no security and its 401 was indistinguishable
  from "the endpoint is broken" on the client side. Listing a credential restores the
  denial for every other value.
- **Attestation**: a self-declared model name is not identity — **while the hub has
  anything to compare against**. A worker reports the GGUF blob SHA-256 it loaded, and
  every digest must be on the allowlist whenever one is populated; the reference
  `SHARD_TABLE` digests are the intended values, so the hub reuses the identifier that
  already anchors the worker's own download. With no allowlist the hub keeps refusing
  malformed digests but admits the digests it cannot judge — an unenforced identity rule
  stated as an operator choice (`config.rs:138-156`, spec §8.1) rather than a hub that
  closes every supplier socket it ever meets.
- **Scoped, expiring capability tokens** for payload fetch and artifact upload, bound to
  `(job, attempt, worker)`; the book stores only the token digest, and an old attempt
  cannot refetch after a requeue.
- **Size limits**: 1 MiB worker line, 256 KiB frame body, 8 MiB inline payload, 64 MiB
  HTTP body, 4 MiB artifact chunk, 512 MiB per artifact.
- **Retention, explicitly**: prompt text (including spilled `payload_ref` bodies) is
  memory-only, released at job terminal or `payload_ttl_ms` (120 s), whichever first, and
  never written to disk. Deltas live in a bounded ring per job. Artifacts go to disk
  streamed, TTL `artifact_ttl_ms` (600 s), digest and size measured by the hub — a worker's
  declared digest that disagrees with the measurement is surfaced in band, not trusted.
  A deployment with no scratch disk sets `NEXUS_ARTIFACT_ROOT=""` and runs memory-only: no
  directory is created and a kind that would have to write is refused at admission with
  `400 artifact_unavailable` (spec §8.2). The idempotency tables are the only other
  keyed state on the hub — 4 096 entries / 600 s, and dropped the moment the job they name
  is forgotten, so an entry is a dedupe pointer rather than a cached answer. And a dedupe
  pointer only exists where the CLIENT declared one: without an explicit `Idempotency-Key`
  a request is unique — its key is minted fresh per submission (`store::
unique_idempotency_key`), so two byte-identical bodies are two conversations, never one
  execution replayed as a cached answer.
- **Path safety**: job/artifact ids are restricted to `[A-Za-z0-9._-]{1,64}` with no
  leading/trailing dot, and artifact paths are built through `artifact_path_checked`, so a
  `../` cannot escape the root.
- **What `GET /v1/observe` publishes, to anyone.** The snapshot is open by the same
  rule as `/healthz` and `/metrics`, and from any origin, so whoever can reach the
  port can read **worker ids, pool and kind names, job ids, tenant ids, connection ids,
  byte counters, lease deadlines and per-worker telemetry**. That is a deliberate
  disclosure of topology and activity — which tenant is running what, and how loaded a
  worker is. It is not a disclosure of content: no prompt text, no streamed answer, no
  artifact bytes, no credentials, and no peer addresses (the transport does not expose
  them). A deployment that must hide tenant names or job ids from the network should
  not expose this route — front it with a proxy, or keep the dashboard on the same
  origin — since the route itself carries no authentication to revoke.
- A worker on hardware someone else administers is a worker, not a trusted peer: unknown
  fields are tolerated (forward compatibility) but every value is validated, and a bad
  frame closes the socket rather than desynchronising it.

## §16 acceptance matrix

Two words carry precise meaning in this table:

- **core** — `crates/nexus-core/tests/acceptance.rs` asserts the actor's decision
  with injected time. That file has been **run and is green** (71/71, re-counted on
  2026-11-19), so "core" now means "a test states the exact expected value _and that
  value was observed_";
- **runtime** — the behaviour needs the native handlers (`crates/nexus-server`)
  and a live socket. These are asserted by
  `crates/nexus-server/src/runtime_e2e.rs` (real listener, real worker WebSocket, real
  SSE bytes and real consumer-WebSocket frames) — **also run and green**, as is the rest
  of the bin's **89** on that same 2026-11-19 run: 47 `runtime_e2e`, 12 `state`, 10
  `observe`, 7 `io` against real files, 5 `worker_ws`, 7 `consumer`, 1 `consumer_ws`.
  The same `cargo test --offline --workspace` run puts the `nexus-core` library at **105**
  unit tests, and 105 + 71 + 89 = **265** — that sum, not a recount, is what the counts
  elsewhere in this file refer to (the full row is at the top). The run this sentence
  carried before was 2026-11-18's **262** (105 + 70 + 87, itself 13 core units, 10
  acceptance cases and 12 bin tests over the identity pass's 227); the three additions on
  2026-11-19 are one acceptance case
  (`t20k_the_tenant_concurrency_cap_still_binds_the_supplier_a_pin_reserved`,
  `tests/acceptance.rs:5211`) and two `consumer::tests` units on the supplier-hold drop
  guard, and nothing was removed or re-tagged.

| #   | Requirement                                                                            | Status                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Where                                                                                                                                                                                                                                                                                                                                                                                                    |
| --- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Two workers, one job: exactly one `Assigned`, the other gets nothing                   | **core**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t01_two_workers_one_job_yields_exactly_one_assigned`                                                                                                                                                                                                                                                                                                                                                    |
| 2   | Concurrent claims: one winner, deterministically                                       | **core** (mailbox order; both arrival orders asserted)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `t02_concurrent_claims_resolve_deterministically_in_mailbox_order`, `the_actor_never_reports_two_owners_for_one_job`                                                                                                                                                                                                                                                                                     |
| 3   | Worker killed before any byte: requeued, completes elsewhere, no error to the consumer | **core**, with a caveat: a delta that is still inside the flush window counts as "delivered", so a kill in that window takes the in-band-terminal branch instead of the transparent requeue                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere`                                                                                                                                                                                                                                                                                                                                |
| 4   | Worker killed after bytes: in-band error, one finish, no duplicate deltas              | **core**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t04_worker_killed_after_output_terminates_in_band_with_one_finish`                                                                                                                                                                                                                                                                                                                                      |
| 5   | Consumer disconnect → worker receives `Cancel`, slot freed on ack                      | **core**; at runtime the decision existed but nothing ever sent `Detach`, so the chat surface could not satisfy this at all. Fixed: both pumps send `Detach` when the body ends, and `runtime_e2e::consumer_disconnect_reaches_the_worker_as_cancel` drives it over a real socket. Ownership is now the **subscription's** flag, not the wire format: `JobRecord::cancel_on_consumer_gone` (copied from the submission) decides in `on_detach`, so a poll-later job survives its `GET /v1/jobs/{id}/events` watcher hanging up                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `t05_consumer_disconnect_cancels_the_worker_immediately`, `an_async_job_survives_its_events_watcher_disconnecting` + runtime e2e                                                                                                                                                                                                                                                                         |
| 6   | Zero ready workers at submit: `503 + Retry-After` before any body bytes                | **core**; **runtime asserted** (a connected-but-loading worker, real 503, JSON content type, `Retry-After` present)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `t06_no_ready_worker_answers_503_with_retry_after_before_any_body`, `t06b`, `t06c` + `runtime_e2e::loading_worker_answers_503_with_retry_after_before_any_body`                                                                                                                                                                                                                                          |
| 7   | Duplicate idempotency key while in flight: one execution, second client joins/replays  | **core** and **runtime**. The key is now resolved **before** any id is minted: both consumer faces look it up in the hub's own mirror (`Hub::idempotency`, `state.rs:237-259`, read by `idempotency_target` `:943`) and, on a hit, **follow the job that already exists** — `join_existing_job` (`consumer.rs:1814`) on HTTP, `handle_attach` (`consumer_ws.rs:533`) on the socket — rather than submitting a second execution and hoping the actor's duplicate branch reaches them. What this row used to claim ("fixed in `submit`, which installs the queue before the actor is told") described a mechanism that did not hold at runtime: the actor replays under the **original** job id, which is not the id a fresh submission would have registered its own relay and verdict gate under, so the second chat client saw only the later deltas and its retry could time out as `503 hub_unavailable` while the original's replay was being dropped. Deciding on the face removes the id mismatch instead of racing it; the mirror is bounded and TTL'd, and is pruned the moment the actor forgets the job (`state.rs:1065-1082`), so it is a dedupe table and not a result cache (spec §8.1.6) | `t07_duplicate_idempotency_key_attaches_and_replays_never_reruns`, plus `a_retry_on_an_idempotency_key_replays_the_original_completion` (`runtime_e2e.rs:2857`), `a_retry_on_an_idempotency_key_gets_the_task_that_already_exists` (`:2977`), `a_resubmitted_key_on_the_consumer_socket_follows_the_original_task` (`:3070`) and `a_non_streaming_retry_on_a_key_assembles_the_running_answer` (`:3198`) |
| 8   | Reconnecting worker resumes from the last acked `seq`, no re-run, no duplicate deltas  | **core**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates`, `resume_for_a_moved_or_finished_lease_is_nacked`                                                                                                                                                                                                                                                                                |
| 9   | Consumer reconnect with `Last-Event-ID`: replay with no gap                            | **core**; runtime: the job API had the plumbing, the chat surface had none (a chat reconnect always allocates a new job, so `Last-Event-ID` was ignored) — the join case is now covered by row 7's fix, an in-place chat resume is still not implemented                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t09_consumer_reconnect_with_last_event_id_replays_without_a_gap`                                                                                                                                                                                                                                                                                                                                        |
| 10  | Unsatisfiable capability → immediate `400`                                             | **core**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t10_unsatisfiable_capability_is_an_immediate_400`, `t06c`                                                                                                                                                                                                                                                                                                                                               |
| 11  | Only `ready` workers are dispatched to                                                 | **core**, **runtime asserted** (a loading worker's consumer gets the 503 rather than a dispatch)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `t11_a_loading_worker_never_receives_assigned` + runtime e2e                                                                                                                                                                                                                                                                                                                                             |
| 12  | Declared capacity respected (4 and 1 slot)                                             | **core**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `t12_declared_capacity_is_respected_per_worker`, `t12b_a_one_slot_worker_takes_exactly_one_job`                                                                                                                                                                                                                                                                                                          |
| 13  | Off-allowlist SHA cannot join                                                          | **core** (+ version rejects; `t13c` pins that joining is by pool name — any offer, any model, including one already served — while an unknown pool is still refused, and `t13e` pins that a request for the foreign model label then dispatches to the supplier that advertised it)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `t13_attestation_outside_the_allowlist_cannot_join_the_pool`, `t13b`, `t13c`, `t13e`, and its counterpart `t13d_a_smoke_hub_with_no_trust_anchor_admits_an_honest_supplier_it_cannot_judge` (no anchors ⇒ an unlisted digest and an honest 256K offer both join; a malformed digest still bounces)                                                                                                       |
| 14  | Tool-call/reasoning deltas and `usage` survive verbatim                                | **core**; **runtime asserted** for byte-for-byte relay over real SSE (one chunk in, the same chunk out, one `[DONE]`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `t14_tool_call_reasoning_and_usage_survive_the_relay_verbatim` + `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse`                                                                                                                                                                                                                                                                      |
| 15  | Tens-of-MB artifact: RSS flat, resumable download with valid sha256                    | **partly core**: chunk accounting, measured-digest verification, size mismatch and foreign-writer refusal. **Runtime**: the download is a **streamed** range — seek + bounded 64 KiB reads through a 4-deep channel, never a whole-object buffer — with `416` for unsatisfiable ranges, an on-disk length check before the status is committed, and a whole-object digest verification that aborts the body on mismatch. Chunk-cap enforcement and attempt-scoped parts came from the review pass. Asserted by `io::tests` (7 tests, real files under the repo-root `Temp/nexus-tests/` scratch directory, body drained chunk by chunk; the ingest half pins the append-only staging file, the streaming digest and the publish-by-rename); **RSS itself has never been measured**                                                                                                                                                                                                                                                                                                                                                                                                                     | `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume` + `io::tests::full_object_read_streams_bounded_chunks_and_verifies_the_digest`, `partial_range_streams_only_the_requested_slice`, `unsatisfiable_ranges_answer_416_before_any_byte_is_committed`, `a_recorded_size_beyond_the_stored_object_is_refused`, `a_digest_mismatch_aborts_the_body`                                         |
| 16  | Above-cap payload travels by `payload_ref` and is fetched                              | **core** (ref descriptor, in-memory bytes, owner/attempt/token/TTL authorization, release on terminal). The review pass fixed the byte accounting: `PayloadBook::register` double-counted every re-dispatch, which both inflated the spill budget and made the entry cap reject an ordinary renewal                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `t16_large_payload_travels_by_reference_and_is_fetchable_by_its_owner`, `prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map`                                                                                                                                                                                                                                                       |
| 17  | `stream: false` returns the same content as streamed                                   | **core** (assembly equals concatenation); runtime: the aggregate path used to subscribe _after_ the verdict, so early deltas could be missed — fixed by subscribing before `Submit`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `t17_non_streaming_response_carries_the_same_content_as_the_stream`                                                                                                                                                                                                                                                                                                                                      |
| 18  | Hub restart: clean retryable failure; work resumes with workers reconnecting           | **core** for the actor contract. The review pass added the in-band retryable `Error` relay on `Shutdown` and on queue/dispatch timeout: a `Terminal` effect is invisible to the SSE pumps, so before that a draining hub left every open stream hanging                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `t18_hub_shutdown_fails_clients_retryably_and_a_fresh_hub_resumes`                                                                                                                                                                                                                                                                                                                                       |
| 19  | A terminal `Done` whose seq lags the accepted frames still ends the job                | **core** — the current lease owner's terminal declaration is accepted even when its seq is below the last accepted delta (the reference supplier reports an engine failure with `Done{start_seq}`); a requeued job still refuses a previous attempt's late clean `Done`, and a `CancelAck` from a worker that does not own the lease no longer releases someone else's lease                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `t19_an_engine_failure_after_streaming_still_ends_the_job`, `t19b_a_requeued_attempts_late_done_cannot_end_the_newer_run`                                                                                                                                                                                                                                                                                |

Extra coverage beyond §16: flush batching by size and by age, seq gap reporting, the
unbounded queue (over-budget submissions accepted and awaited), the tenant cap gating
dispatch, payload/prompt `413`, unknown-kind `400`,
stale-connection frames and closes, refused foreign-owner frames, non-idempotent
never-requeue, lease renewal and missed-heartbeat degradation, hard deadlines, warm-model
reporting, keepalive pings, the supplier socket's own probe and the vanished-supplier verdict
it produces, session affinity (and its invalidation on worker death), consumer-directed supplier
pinning (a pin binds only its owner while that connection is live, relaxes on its loss, outranks a
session hint that disagrees with it for that owner alone, and never
trades the requested model for the name),
resume-by-continuation on/off **and its refusal of opaque tool-call output**, detached
jobs surviving an events-watcher disconnect, and TTL/retention sweeps.

The review passes also added the runtime and I/O coverage the package shipped without, and
all of it has been executed since: `nexus-server`'s `runtime_e2e` module (**21** tests at
the time — real listener, real worker socket, real SSE, and now real consumer-WebSocket
traffic for the NEX-3 face and §16.7), `io.rs`'s download-and-ingest module (**7** tests:
chunked streaming, exact slices, `416`, length/digest refusal, and staged ingest finalising
by rename), and `state.rs`'s **5** unit tests (label merging, the stalled-connection report,
the `JobForgotten` pruning of `job_index` and the relays, the idempotency mirror's bound
and expiry, and the routing index holding only the jobs the actor still knows).
Every one of them is written against the real types, and **177/177 passed** on this host at
that point (the 163 that existed before the observation plane are unchanged and still green;
the 14 more are the `/v1/observe` actor snapshot, its unit tests, and the two runtime tests
for the route and its CORS). Those modules have kept growing since, and the count is stated
as the command states it rather than carried forward: on 2026-11-18,
`cargo test --offline --workspace` is **262/262**, of which `runtime_e2e` is **47** (the
supplier-header, pinning, hangup-inside-the-hold and detached-`POST /v1/jobs` cases are all
there), `io` is still **7**, and `state` is **12** — seven more than the five above, the
newest of them the first-write-wins supplier latch
(`a_known_supplier_survives_every_later_verdict`, `state.rs:2239`) that §7's "which moment
the header names" rule depends on.

## Commands

| Command                          | What it runs                                                                                                                                                                                         |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `yarn fmt` / `yarn fmt:check`    | `cargo fmt --all [-- --check]`                                                                                                                                                                       |
| `yarn check`                     | `cargo check --workspace --all-targets` — **but yarn 1.x shadows this script** with its own built-in lockfile-integrity verb, so run the cargo command directly (see the spec's 2026-11-14 addendum) |
| `yarn clippy`                    | `cargo clippy --workspace --all-targets -- -D warnings`                                                                                                                                              |
| `yarn test` / `yarn test:accept` | core + server tests / the §16 matrix only                                                                                                                                                            |
| `yarn build`                     | release native server (LTO, `panic=abort`)                                                                                                                                                           |
| `yarn verify`                    | fmt-check + clippy + tests + release build                                                                                                                                                           |

The repo-root `.cargo/config.toml` still governs this workspace on purpose:
`target-dir = .rust/target`, and `TMP`/`TEMP`/`TMPDIR` point at `Temp/rust` for every
child process cargo spawns, which is the root `AGENTS.md` scratch rule.

## Known gaps — what this build does _not_ do

**Blocking, and not fixable from here:** there is nothing in this category any more.
Until 2026-11-14 the first bullet here was "the Node glue cannot be generated on this
host" — `wasm-bindgen-cli` was absent, so `yarn wasm:glue` and `yarn smoke:wasm` had never
run and `pkg/` had never existed. That blocker was removed with the wasm leg itself rather
than by installing the tool (see _No WASM boundary_).

**Not measured, and nothing in this README claims it was:**

- **no §14 number exists.** The hub is exercised on loopback — every `runtime_e2e` test
  dials a real listener with a real WebSocket client and reads real SSE bytes — but no
  §14 latency budget, no RSS figure during an artifact download and no long-run soak has
  ever been recorded on this host.
- **the suite is green on this host, and on a clean copy of it.** **163/163** with the
  pinned `1.99.1` toolchain at the time of that reproduction, reproduced from a pristine
  copy with a fresh target directory and `--locked` — so it is no longer a single-host,
  single-incremental-build result. The observation plane has since taken that to
  **177/177** on the same pinned toolchain (`cargo test --workspace`, 2026-11-10), with
  the 14 additions listed in the table above; the pristine-copy `--locked` reproduction
  has not been re-run since. §14's latency budgets remain unmeasured, so nothing in this
  README claims them.

**Resolved since the first draft of this package:**

- the dependency graph is resolved and locked: `Cargo.lock` is committed in this
  package (the repo-root `.gitignore` says Cargo.lock is committed on purpose), and
  every version stated in `Cargo.toml` resolved — `axum 0.8.9`, `tokio 1.54.1`,
  `tower-http 0.6.11`, `tokio-tungstenite 0.26.2`, `reqwest 0.12.28`, `sha2 0.10.9`,
  `serde 1.0.229`. (`wasm-bindgen 0.2.130` used to head that list: it was a direct
  dependency of `crates/nexus-wasm`. It is gone from `Cargo.toml`; the lockfile still
  carries it, `js-sys`, `web-sys` and `wasm-streams` because `Cargo.lock` resolves
  `reqwest`'s optional per-target dependencies for **every** target, wasm included, which
  is cargo behaviour and not a wasm build in this package.)
- `cargo check --workspace --all-targets` and a release build now pass; the sources
  have been through `rustfmt`.
- **`yarn verify` no longer stops before the tests.** It used to abort on `yarn clippy`,
  because 17 pre-existing `nexus-core` findings were denied by `-D warnings`. All 17 are
  settled. **Corrected 2026-11-19, because the last sentence of that bullet stopped being
  true:** `cargo clippy --workspace --all-targets -- -D warnings` — the command `yarn clippy`
  runs — is **not** exit 0 today. It stops on one finding and on nothing else,
  `clippy::new_without_default` at `crates/nexus-core/src/queue.rs:125`
  (`InMemoryQueue::new`), which the verification table above has recorded red on every re-run
  since 2026-11-13; `-D warnings` denies it, so `yarn verify` stops at `yarn clippy` again
  rather than reaching `yarn test`. Re-measured on 2026-11-19: red on `queue.rs:125` alone,
  and the identical scan with `--allow clippy::new_without_default` is **exit 0, zero
  findings** — that allow is what the zero-findings claim is true in, and it is the form every
  dated row in this file has been using since 2026-11-14. The lint is left open on purpose:
  closing it means editing `queue.rs`, which this pass was told not to touch.

**Resolved by the conformance pass (2026-11-10), item by item against the specification's
§9.1** (`documents/specifications/hub-specification.md`):

- **NEX-3 — the consumer WebSocket face exists.** `GET /v1/consumer`
  (`crates/nexus-server/src/consumer_ws.rs`, 1 005 lines) carries post _and_ stream on one
  socket: `submit`/`attach`/`detach`/`cancel` in, `accepted`/`rejected` first, then
  `job`-tagged `meta`/`delta`/`event`/`log`/`artifact`/`finish`/`error`/`gap`/`replay`, and
  exactly one `end {job, reason}` per task with `EndReason {finished, errored, lagged,
detached, closed}`. Several tasks multiplex on one connection, the verdict can never be
  preceded by an event byte (one writer owns every write), and the API key is checked
  **before** the upgrade (`401` + `www-authenticate: Bearer realm="nexus"`). The SSE
  surface is unchanged for OpenAI clients, and both faces share one admission path, so the
  same task is refused the same way (`a_refusal_reaches_the_consumer_before_any_event_bytes`,
  `one_consumer_socket_carries_two_tasks_and_answers_both_in_order`,
  `the_consumer_socket_refuses_an_unauthenticated_upgrade`).
- **orphan ⇒ delete, not orphan ⇒ retain** (NEX-6.3). A record that was still _running_
  when its last consumer left is cancelled **and dropped in the same mailbox step**:
  `forget_job` (`actor.rs:1552`) removes the record, the replay ring, the lease, the
  payload reference and the idempotency entry, so a later same-key request executes fresh
  instead of attaching to a dead stranger's answer. Only an already-**terminal** record is
  still retained, which is exactly the reason NEX-7.2 allows (`dropping_a_consumer_socket_cancels_and_deletes_its_task`).
- **hub-side mirror pruning.** `Effect::JobForgotten` now removes the routing entry, the
  relay and the idempotency row (`state.rs:1065-1082`). That also closed a slow leak this
  README did not used to mention: `job_index` was otherwise one entry per submission for
  the life of the process (`job_index_and_relays_are_pruned_when_the_actor_forgets_a_job`,
  `the_routing_index_holds_only_the_jobs_the_actor_still_knows`).
- **detached is now an explicit request field** (NEX-6.6), not a side effect of which route
  was used: `submit{detach}` defaults to `false` on the socket, `POST /v1/jobs` requires
  `"detach": true` (`400 detach_required` otherwise), and a detached job nobody ever
  watched and nobody claimed is forgotten after `NEXUS_DETACHED_ORPHAN_TTL_MS`
  (`a_task_posted_without_a_consumer_is_deleted_when_its_window_closes`,
  `the_async_job_route_requires_detach_to_be_stated`).
- **disconnection is noticed by the connection, not by the first failed write** (NEX-6.1):
  the socket face breaks on FIN/broken frame, and the SSE pump races the event source
  against `tx.closed()` (`consumer.rs:1358`) — `an_idle_sse_hangup_cancels_the_worker_without_a_failed_write`
  is the case that used to sit on a GPU slot until the TCP stack gave up.
- **`ArtifactBook` is bounded by count as well as bytes**: `MAX_ARTIFACT_ENTRIES = 128`
  (`store.rs:394`), deliberately `queue_depth`'s number, with evictions drained through the
  TTL sweep's deletion path rather than a second effect. Bytes alone were the wrong bound:
  a thousand 1 KiB uploads sit far below the byte budget while still growing the map, the
  directory count and the downloadable set without limit.
- **artifact ingest is bounded and atomic**: one append-only staging file per attempt,
  O(chunk) memory, a digest the hub measures as it goes, and a **rename** to publish
  (`io.rs:759`, `:809`, `:918`) — no second copy at completion, no half-written object for
  a downloader to find. `staged_ingest_appends_then_finalizes_by_rename` pins it at the
  file level.
- **flush/replay coherence and zero-TTL config are startup errors, not runtime surprises**:
  `Limits::validate` (`limits.rs:213`) refuses `flush_bytes > replay_bytes` (a full flush
  batch would otherwise be evictable from the ring before it was relayed) and refuses a
  zero `payload_ttl_ms`, `job_retention_ms`, `idempotency_ttl_ms`, `artifact_ttl_ms` or
  `detached_orphan_ttl_ms`, each with the consequence in the message
  (`limits.rs:351`, `:377`).
- **memory-only mode exists** for deployments with no scratch disk: an empty
  `NEXUS_ARTIFACT_ROOT` makes a kind that must write an artifact a clean
  `400 artifact_unavailable` at admission instead of a failure after the response body has
  opened (`memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest`).

**Real limitations of the current implementation:**

- `scheduler::{eligible, rank, pick, Candidate}` is a complete, unit-tested ranking
  function that **the actor does not call**. Dispatch is pull-driven (§5): a worker
  claims, and `pick_job_for` scans the queue for the first job that worker may take
  (the requested model, the record's phase, session affinity and the tenant cap — §5.2's gates,
  with NEX-4.7's pin ranking above affinity for its own owner). Consequence: §8's "prefer
  dedicated workers for
  long-context and latency-sensitive traffic" and "fall back to least-loaded" are
  only partially live — tier preference and ephemeral-as-burst-spill are not applied
  at dispatch, so two claiming workers are served in mailbox order. `affinity_target`
  is the one scheduler function the actor uses.
- the reference worker (`nexus-worker-example`) reads the loopback engine's response
  to end before relaying, so **the example client does not stream**: end-to-end TTFT
  through it equals the engine's full generation time. The hub's own consumer path
  does stream. A production worker parses `data:` blocks incrementally.
- §13's "tokens/s and frames/s **per job**" are not exported: frames and tokens are
  hub-wide counters, and the latency summaries (`ttft/dispatch/duration`) are
  `_sum`/`_count` pairs without per-job series.
- §9's "rate limits per consumer" is a **concurrency cap**, not a request rate: a
  tenant may issue unlimited `POST`s — jobs beyond the
  `tenant_max_concurrent` cap are accepted and await a free slot in the unbounded
  queue, and the hub itself has no admission rate limit (`max_idempotency_entries`
  and the spill budget are the only backstops).
- §16.3's "worker killed before any byte reaches the consumer" has a window: a delta
  that is accepted but still inside the flush gate sets `bytes_delivered`, so a kill
  in that window answers in-band-terminal rather than requeueing transparently.
  Narrow (≤ `flush_interval_ms`), and the consumer still gets the buffered text plus
  an error, but it is not the literal acceptance.
- the actor loop performs effect delivery inline, so a worker whose socket stops
  draining used to park **its whole pool** indefinitely. The push is now bounded: the
  frame gets 25 ms (`SEND_GRACE_MS`) to enter the worker's 256-deep outbound queue, and
  on expiry the connection is reported to the actor as `ActorMsg::WorkerGone`, which runs
  the ordinary resume-grace / lease-expiry rules. Stated plainly, the cost is: that one
  frame (`Assigned`, `Cancel`) is not delivered, and a worker whose queue has been full
  for 25 ms straight — merely slow, not dead — is treated as gone and its jobs take the
  resume path. Task §8 asks for per-job backpressure; this is still coarser.
- artifact downloads are **streamed** ranges (seek + 64 KiB reads behind a 4-deep
  channel), so a `GET /v1/artifacts/{id}` of a 40 MB range costs the hub a few hundred
  KiB, not 40 MB. Two honest limits: a **partial** read cannot be digest-checked without
  reading the whole object, so `x-nexus-sha256` is the _object_ digest and the consumer
  verifies the reassembled bytes itself; and the bound is by construction — RSS during a
  download has still never been measured. Writes are chunked with the cap enforced
  before the disk is touched.
- **`nexus_idempotent_attach_total` counts less than it used to, deliberately.** A
  same-key retry the hub can answer from its own mirror never reaches the actor, so the
  actor's counter does not move for it; what it counts now is the duplicates **the actor
  itself replays** — a key the hub's mirror has already dropped or expired on, or a race
  the mirror lost. The hub cannot compensate: `PoolGauges::from_actor` (`state.rs:213`)
  rebuilds the mirrored registry from the actor on every dispatch, so a hub-side increment
  would be erased a tick later (`consumer.rs:200-206`, `consumer_ws.rs:427-432`). Read the
  series as "actor-side joins", not "duplicate submissions observed"; the hub-answered
  retries are visible as the absence of a second `nexus_jobs_submitted_total` instead.
- **the artifact ingest layout changed, and the README's old wording did not.** The
  per-attempt _part files_ are gone. One attempt now appends to a single staging file,
  `{object}.parts/a{attempt:04}/staging.bin` (`io.rs:759`), while a running SHA-256 covers
  exactly the bytes it has appended (`io::stage_chunk`, `io.rs:809`), and completion
  publishes by **rename** (`io.rs:918`) — no second copy at completion, and no half-written
  object for a downloader to meet. Chunk accounting still lives in the actor's
  `ArtifactBook`; what changed is that an abandoned upload leaves one disposable file
  instead of a directory of part files, and the TTL sweep's single `io::delete_artifact`
  (`io.rs:944-951`) removes the finished object **and** the `{path}.parts` directory under
  it in one call — which is why staging lives _inside_ that directory.
- **ranges are query parameters, not the `Range:` header.** `GET /v1/artifacts/{id}` and
  `GET /v1/jobs/{id}/result` read `?from=&to=` (exclusive end) while _answering_ with
  `Accept-Ranges` / `Content-Range` as a proper ranged endpoint would; a client that sends
  a standard `Range: bytes=1000-2000` header still gets the whole object. The worker's
  resumable upload is query-parameter driven too (`?chunk=&complete=`). Adding header
  parsing is a handler-level step (`io::get_artifact`), not a storage change.
- the `/metrics` exposition carries `# TYPE` but no `# HELP` lines, and the actor
  registers no metric help strings.
- **an output frame's `content_type` overwrites the job's payload `content_type`**
  (`relay_into`, the assignment at `actor.rs:3909`): `JobRecord::content_type` is documented as the
  _payload's_ type and is sent to the next attempt as such, so a worker that labels its
  deltas changes what a re-queued attempt is told the prompt was. Left as found — the
  fix needs a separate output-type field, which no test in the suite pins today.
- the eviction asymmetry between the two hosts is deliberate but real: the native
  registry closes the stale socket itself and suppresses `Bye{Evicted}` on the live one
  (`state::send_frame`), while a WASM host **does** receive that frame, because it owns
  its sockets and cannot be reached from inside the actor. A JS host that reconnects on
  every `Bye` will therefore loop; it must compare `Welcome.epoch` first.
- the native runtime does not relay the engine's _prompt-cache_ telemetry into
  scheduling; affinity uses the session hint alone.
- multi-replica durability is deliberately **not** implemented (task §18): the `queue`
  trait is the boundary, and a hub restart relies on client retry.
- `n>1` sampling fan-out, KV-replica/cache sharing, autoscaling, a web UI, billing and
  sandboxing claims are out of scope (task §18).

## Review pass — corrected defects

A second, independent read of the whole package against the task found the
following. Each was a defect in code that had already been written; none of them had
been observed by a compiler or a test run, and each fix is local to `packages/underload/nexus`.

Compile-shape (these three would have stopped a build):

- `nexus-worker-example/src/main.rs`: `struct EngineStatus` was defined twice
  (E0428);
- `nexus-wasm/src/lib.rs`: `NexusBroker::hello` held a `&mut` borrow of `self`
  (`actor_mut()`) across another `&mut self` call (`next_conn`) (E0499);
- `nexus-server/src/state.rs`: `status.as_u16()` on `Effect::Reject.status`, which is
  already a `u16`; plus `HelloInput` needed `Default` for `worker_ws`'s
  `unwrap_or_default()`, and two imports (`relay::sse`, `protocol::CancelReason`)
  were dead under `clippy -- -D warnings`.

Correctness against the task:

- **cancellation was not wired at runtime** (`§6`, the task's own "wire this before
  anything optional"): no handler ever sent `ActorMsg::Detach`, so a consumer that
  hung up left the job leased and the worker generating. Both SSE pumps now send
  `Detach` when their body ends, and the aggregate (`stream: false`) path does too;
- **an idempotent join lost its replay** on the OpenAI surface: the actor returns
  `Accepted { attached: true, replayed }` and the dispatcher pushed it into a private
  queue that only `/v1/jobs/{id}/events` had subscribed to (§16.7's second client
  silently got a truncated stream). The fix that actually holds is **upstream of the
  queue**: the consumer faces resolve the idempotency key against the hub's own mirror
  (`Hub::idempotency`, `state.rs:237-259`) **before minting an id**, and on a hit follow
  the job the hub already routed — `join_existing_job` (`consumer.rs:1251`) on HTTP,
  `handle_attach` (`consumer_ws.rs:533`) on the socket — so the replay is delivered on the
  relay that joiner registered under, and there is no second execution to reconcile. (What
  this bullet used to credit — `submit` installing the queue before the actor is told — was
  the wrong mechanism: the actor replays under the _original_ id, which is not the id a
  fresh submission registers its relay and verdict gate under, so the retry could still
  time out as `503 hub_unavailable` while the original's replay was dropped.) Subscribing
  to the fan-out before the actor is told still stands, and still closes the
  lost-first-frames race in the same place (§16.14/§17). Pinned by the four idempotency
  retries in `runtime_e2e.rs` named in §16 row 7;
- **terminal transitions could hang a stream**: `Shutdown`, queue-timeout and
  dispatch-failure emitted `Terminal`/`Reject` only, and the pumps end on a terminal
  _event_, so a draining or timing-out hub left consumers attached to a stream that
  never closed. All three now relay an in-band `Error` (§7.2's "once bytes are on the
  wire, failures are in band"), and the pre-body `Pending` case still answers a clean
  `503`;
- **continuation could not work**: a re-dispatched attempt restarts `seq` at 1, and
  every frame at or below the acked seq is dropped as a duplicate, so the consumer
  would have received nothing after the seam. `Assigned` now carries `start_seq`, the
  worker example honours it, and §7.4 is **opt-in** (`max_continuations` default `0`,
  was `1`);
- **`X-Accel-Buffering: no` was never set** anywhere, which is §7.2's explicit
  requirement for a stream that intermediaries do not buffer;
- **tenant isolation was absent on job reads**: `GET /v1/jobs/{id}`, `/events`,
  `/result`, `GET /v1/artifacts/{id}` and `DELETE /v1/jobs/{id}` did not check the
  caller's tenant (two of them did not authenticate at all), so any anonymous client
  could read, stream or cancel anyone's job. They now require a key and treat a
  foreign job as `404`; explicit `Idempotency-Key` headers are namespaced per tenant
  so a guessed key cannot attach to another tenant's execution;
- **memory-accounting leaks**: `PayloadBook::register` re-added the same prompt's
  bytes on every dispatch (acceptance test 16 asserts the exact figure and would have
  failed), and `ArtifactBook::register` left the dead attempt's bytes in the shared
  budget;
- **artifact uploads were not attempt-isolated**: the parts directory was shared
  across attempts, so a requeue's digest was measured over the previous attempt's
  leftover chunks. Parts are now per attempt, and an oversized chunk is refused
  before it reaches the disk;
- **`/metrics` was not scrapeable**: labelled counters got a second brace group
  (`nexus_frames_total{kind="delta"}{pool="…"}`) and every gauge
  (`nexus_queue_depth`, `nexus_ready_workers`, `nexus_leases`) was missing from the
  response. Labels are now merged into one set and gauges are exported;
- **eviction did not evict** (task §11): the actor's `Bye{Evicted}` is addressed by
  worker id, which by then pointed at the _new_ socket, so the stale socket stayed
  open and the new one was told it had been evicted (a client honouring `Bye`
  reconnects forever). The registry now closes the previous generation explicitly,
  and `send_frame` no longer forwards that frame to the live socket;
- **`send_frame` encoded outbound frames against `WireCaps::default()`** (1 MiB line)
  instead of the configured hub caps, so an inline payload above ~1 MiB failed to
  encode and the dispatch was dropped without a word;
- **a bad `NEXUS_CONFIG_JSON` was silently ignored** (the warning hook was an inert
  no-op) while the README promised a hard startup error; `apply_values` now returns
  the error and `main` refuses to boot on it — the hub will not come up on sample
  pools with no credentials and look configured;
- the worker example's single shared cancellation flag stopped _every_ in-flight job
  and was cleared by the next assignment; flags are now per job;
- `examples/node-smoke.ts` referenced `chunk` outside its block (a
  `ReferenceError` before the first assertion), read frames with the wrong tagging
  (`frame.Assigned` instead of `frame.type === "assigned"`), and asserted a flush
  behaviour the gate does not implement. It has been rewritten against the real
  effect/frame shapes.

## Second pass — the six bounded corrections

A follow-up pass took the six correctness items that the first pass had found but left
open. Same rules: only this package, nothing compiled, and at the time every claim below
was from reading code and tests that had never been executed. The verification pass above
has since executed them: the claims held (the suite stood at 128/128 in that pass's
own count, and at **163/163** when the verification pass executed it — **177/177**
today, with the observation plane), with the fourteen exceptions itemised in _Fixed
in this pass_ — three of which were this document being confidently wrong, notably
the `sha2` diagnosis for the streamed artifact digest.

1. **The contradictory phase assertion was fixed in the test, not in the actor**
   (`tests/acceptance.rs:1982`, in `heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only`).
   `JobPhase::Streaming` is set in exactly one place — `PoolActor::relay_into`
   (`actor.rs:2312`), on the first relayed frame — and `JobPhase::Assigned`'s own doc
   says "leased to a worker, waiting for the first frame". Two sibling tests
   (`:393`, `:1239`) already assert `Assigned` for a dispatched, frame-less job, so the
   lone `Streaming` expectation was the defect. Nothing in `nexus-core` was changed to
   chase it.
2. **Artifact result serving is now bounded streaming** (`io::serve_artifact`).
   The file is opened, `stat`ed, seeked to `from` and read in `STREAM_CHUNK_BYTES`
   (64 KiB) pieces through a 4-deep `mpsc` into `Body::from_stream` — the old
   `read_range` helper allocated `to - from` up front, i.e. 40 MB for a 40 MB range.
   Checks before any byte is committed: over-long `to` clamps (RFC 9110 §14.21.1),
   `from > to` and `from >= size` answer `416` with `Content-Range: bytes */size`, a
   stored object shorter than its recorded size answers `500 artifact_incomplete`, and a
   missing file answers `404`. A whole-object read hashes the bytes as they stream and
   aborts the body on a digest mismatch; a partial read cannot be digest-checked without
   reading the whole object, so `x-nexus-sha256` stays the _object_ digest — documented
   in the function. Tenant checks were already on both readers (`io::get_artifact`,
   `consumer::job_result`). Seven tests in `io::tests` drive real files and read the body
   frame by frame, so "streamed" is asserted by chunk sizes, not by trust.
3. **Opaque output can no longer be auto-continued** (`model::JobRecord::output_opaque`,
   set in `relay_into`; consulted in `resolve_lost_job`). `max_continuations` was already
   `0` by default, but with an operator who turned it on, a tool-call stream would have
   been re-run on `prompt + a half-written JSON argument`. Now a non-`delta` frame or a
   chunk carrying `"tool_calls"` / `"function_call"` marks the attempt un-reconstructible,
   the death stays in-band-terminal, and a `Warn` log says why. The escape hatch is
   `NEXUS_CONTINUATION_ALLOW_OPAQUE` / `Limits::continuation_allows_opaque_output`
   (default `false`). Tests: `opaque_tool_output_is_not_continued_even_when_continuation_is_on`,
   plus the new default in `limits::tests::defaults_match_the_task_numbers`.
4. **`cancel_on_consumer_gone` stopped being a dead field**
   (`JobRecord::cancel_on_consumer_gone`, honoured in `on_detach`). The actor used to key
   last-watcher cancellation off `streaming`, i.e. off the wire format: an async job
   submitted through `POST /v1/jobs` with a streaming kind was cancelled the moment a
   transient `GET /v1/jobs/{id}/events` watcher hung up, which is the opposite of
   "poll-later". The flag now travels from the submission onto the record and is the only
   thing `on_detach` consults. Idempotency fan-out is untouched: joins still attach
   consumers and replay. Test: `an_async_job_survives_its_events_watcher_disconnecting`.
5. **The actor loop can no longer park on a stalled worker**
   (`state::send_frame`, `state::report_dead_connection`). The push to a worker's
   outbound queue gets `SEND_GRACE_MS` (25 ms); on expiry — or on a closed queue — the
   connection is reported to the actor as `ActorMsg::WorkerGone{worker, conn}` with the
   generation the actor believes is live, so `on_worker_gone` runs the existing resume
   grace / lease rules. The frame for that one effect is still lost (honest: the peer is
   not draining), but it is never _silently_ lost, and the pool stops paying for it.
   Test: `state::tests::a_stalled_worker_queue_is_reported_instead_of_parking_the_actor_loop`
   (no actor task, so the mailbox read cannot race).
6. **The runtime tests are actually in the build, and the spill they write is not
   litter.** `main.rs:8-9` hold `#[cfg(test)] mod runtime_e2e;`, which is the only way a
   binary-only crate gets `cargo test -p nexus-server` coverage; the module is 19 tests
   over a real listener (4 at that pass; the conformance pass added the consumer-WebSocket,
   orphan-deletion, detach, idempotency-retry and route-contract cases). Each test gets its own artifact root under the **repo-root**
   scratch directory (`Temp/nexus-tests/e2e-<test>`, via `io::test_scratch`, resolved from
   `CARGO_MANIFEST_DIR` so it does not depend on cargo's working directory), cleared before
   the run and after it (`TestHub::drop`), so parallel tests cannot collide on per-attempt
   part files, nothing disposable lands in the package or in OS temp (root `AGENTS.md`
   rule), and the hub's own configured artifact paths are unchanged.
   `.gitignore` also gained `Temp/`, because the hub's built-in default artifact root is
   CWD-relative and a `yarn start` from inside this package would otherwise dirty the
   nested repository.

## Third pass — the supplier keepalive, and the model that would not disappear

Reported against a running hub: `GET /v1/models` went on listing a model whose only
supplier had already disconnected. The document is built from the actor's `warm_models`
(`actor.rs:709`), which is correct — it counts a worker only while `engine == Ready` **and**
`link == Live` — and `Hub::readiness` re-derives it from the mirrored gauges on every actor
message, so `/v1/models` is as fresh as the last thing the actor was _told_. The defect was
that nobody ever told it anything. A supplier whose path vanished without a FIN — a Daytona
preview link, a laptop that slept, a NAT that dropped its mapping — left the hub parked in
`stream.next()` on a socket it could still write to, and the actor kept a `WorkerRecord` with
`link: live` forever. Measured with a TCP middleman that never ends the hub-side socket: the
model lingered for three heartbeats (the actor's missed-heartbeat rule _pauses dispatch_,
§5.1, it does not drop the link), the row was never deleted, and the socket was never closed.

1. **The hub now probes its own supplier sockets** (`worker_ws.rs:251-304`). The read loop is
   a `tokio::select!` between `stream.next()` and a probe tick. Every inbound frame refreshes
   `last_inbound` — data or control, a `Pong` included, because a `Pong` is what every
   WebSocket runtime sends on the supplier's behalf without the engine being involved at
   all, so it is evidence about the _path_. When the silence outlasts the verdict window the
   hub logs a `Warn`, queues `Bye{reason: "heartbeat_timeout"}` and breaks the read loop,
   which is exactly the path a clean close takes: teardown reports `ActorMsg::WorkerGone`
   and the actor runs its existing resume-grace and lease rules. `ByeReason::HeartbeatTimeout`
   (`protocol.rs:305`, close code `1000` — normal, reconnect expected) has existed since the
   first pass and had never been sent by anything; this is the mechanism it was named for.
2. **Both windows derive from the heartbeat the hub itself announces in `Welcome`**
   (`keepalive_windows`, `worker_ws.rs:55-96`): a probe every `heartbeat_ms / 4`, clamped into
   `[MIN_PING_MS, MAX_PING_MS]` = `[250 ms, 5 s]` because a hub with 500 sockets cannot poll
   more often than that, and a verdict only after `max(3 × probe, heartbeat + probe)` of
   hearing **nothing at all** — not one `Ready`, not one `Pong`, at least four unanswered
   probes. At the reference 20 s cadence: a 5 s probe and a 25 s verdict, against the 60 s the
   actor already needed to pause dispatch and the infinity anyone needed to close the socket.
   A socket that joined several pools is probed on the tightest of their expectations
   (`heartbeat_ms_for` per pool), and a socket on a pool the config does not know falls back
   to `limits.heartbeat_ms`. The design is safe for a supplier that never pongs at all: it is
   judged by the cadence it actually promised, which is why the verdict can never land before
   a whole heartbeat has passed unheard. `NEXUS_HEARTBEAT_MS`, or a pool's `heartbeat_ms`, is
   the knob; nothing else moves with it.
3. **What did not move.** The 3 × `heartbeat_ms` rule still pauses dispatch without touching
   the link, a quiet engine is still the actor's verdict and not the socket's, and no lease,
   resume-grace or requeue rule changed. The keepalive contributes exactly one fact: this
   path is gone. That is the one fact the actor was already wired to act on, which is why
   `models_document_loses_a_model_when_its_last_supplier_leaves` passes with and without the
   fix — a supplier that _does_ say goodbye has always disappeared from the list in well under
   a second.
4. **Tests: 5 unit, 3 end-to-end.** `worker_ws::tests` pin the windows without a socket: both
   defaults, the band's floor and ceiling, the two floors the design rests on (a verdict needs
   at least four probes, and never arrives before one missed heartbeat), the tightest-pool
   rule and the unconfigured-pool fallback. `runtime_e2e` adds the three behavioural cases
   against a real listener: the baseline honesty above; `a_supplier_that_vanishes_without_closing_stops_being_advertised`, which `std::mem::forget`s the supplier socket so the hub
   has a live connection and no bytes, and asserts that at a 2 s heartbeat the model is gone
   in about 3 s **and** the worker row itself is deleted once the resume grace lapses, which
   no amount of waiting achieves without the probe; and `a_supplier_that_only_heartbeats_and_pongs_stays_advertised`, seven seconds of idleness during which the hub's pings are counted
   from the supplier's own socket, the model stays advertised, and a real close then retires
   it. With `keepalive_windows` forced to an hour, the two keepalive cases fail at 4.5 s and
   7.0 s while the baseline stays green.
5. **Boundary, stated rather than assumed.** `nexus-wasm` has no listener and no socket: its
   host performs what `worker_ws.rs` does (`worker_ws.rs:5-8`). Whoever owns that socket owes
   the same probe — a WASM hub whose JS host never pings has the same blind spot this pass
   closed on the native side, and the fix there is the host's, not the actor's.

## Memory pass — the prompt that outlived its job, and the relay row that outlived the prompt (2026-11-14)

Reported against the Modal deployment of the compiled Rust hub: "Memory used" stepped up with
traffic and plateaued near 340 MB. Two job-keyed structures are retained past the job they
describe; both were real, and each is now released at the moment it stops having a reader.
What was **measured** is the A/B on this host — same harness (`Temp/coding/rss-probe/`), same
2 req/s, 840 + 180 jobs, 2 s sampling: plateau VmRSS **49.51 → 32.08 MB (−17.43 MB, −35 %)**,
anon-heap term **42.05 → 22.20 MB (−47 %)**, idle RSS unchanged (5.62 → 5.72 MB),
**53.5 kB → 32.1 kB per retained job**, and a second wave adding **+0 kB (baseline) vs +4 kB
(fixed)** — the plateau that is left is a glibc high-water mark rather than retained job
state, and it is never returned to the OS. Nothing beyond those numbers is claimed as
causation: which resident term dominates depends on **prompt size**, because
`inline_payload_cap_bytes` (8 MiB, `limits.rs:32` `:140`) decides whether a prompt is resident
inline at all, and the replay ring (`replay_bytes` = 1 MiB per job, `limits.rs:173`), the
retained records themselves and the spill bookkeeping (`PayloadBook`, `store.rs:127-136`:
the map and its capacity live for the life of the process, its entries are capped by
`max_spilled_payloads` / `max_spill_bytes` and swept at `payload_ttl_ms`,
`limits.rs:177-179`) are all still resident across that 300 s window and are untouched by
this pass.

1. **A terminal record kept its prompt for the whole retention window** (`nexus-core`).
   `finish_job` released the **spilled** copy — `record.payload_ref.take()` →
   `self.payloads.release` → `Effect::ReleasePayload` (`actor.rs:3160-3167`) — armed
   `job_retention_ms` (300 s by default, `limits.rs:183`), and dropped the **inline** copy
   only when `forget_job` deleted the record 300 s later (`actor.rs:1479`). That copy is
   `JobRecord::payload_inline` (`model.rs:383`), populated at `model.rs:780`, and its one
   reader anywhere is `PoolActor::dispatch` (`actor.rs:2283`) — the **pre-dispatch** path,
   gated twice against a terminal record (`pick_job_for` admits only `Queued`/`Pending`,
   `actor.rs:2179`; `dispatch` nacks, `actor.rs:2228`). So for those 300 seconds a hub that
   had answered N prompts was holding N prompts with **zero** readers — the term the old spill
   branch alone never covered, and on this profile the only term the RSS movement above can
   be attributed to: that run never emitted `Effect::Reject` (0 `stale_attempt`), so the
   −17.43 MB plateau and −47 % anon-heap delta belong to this byte release, while the relay
   fix in item 2 is pinned by unit test, not by RSS. The §7.4 continuation draft is the same
   case (`JobRecord::partial_text`, `model.rs:410`, appended per delta at `actor.rs:3684-3686`,
   capped at 1 MiB per job by `partial_text_cap_bytes`, `limits.rs:174`): its two readers are
   `dispatch` (`actor.rs:2310`) and the continuation decision in `resolve_lost_job`, which
   returns for a terminal record at `actor.rs:2942` **before** reading it. `finish_job` now
   drops both beside the release that already existed (`actor.rs:3179`, `:3187`) —
   `payload_inline = None`, and `partial_text = String::new()` rather than `clear()`, because
   `clear` keeps the 1 MiB capacity and the capacity is the thing being returned.
   What did **not** move: the replay ring (its retention is the point — reconnect replay
   reads `record.replay`/`finish`/`usage` through `collect_replay`, `actor.rs:1361-1390`),
   the published `JobView` (`model.rs:519-570`), which carries no prompt **body** field: it
   publishes only `payload_bytes` (`model.rs:537`) and `payload_sha256` (`model.rs:539`), the
   two fields this section's second test asserts stay exactly as they were; the retention
   window itself, the queue, `Metrics`, `limits.rs` and the allocator. `JobRecord` derives no
   `Serialize`, so no wire shape changed, and `nexus_payload_bytes` counts the spill book only
   (`actor.rs:3624`), so no gauge moved. Pinned by two tests at `acceptance.rs:2624`
   (`a_terminal_job_releases_its_prompt_bytes_while_it_is_still_retained_for_replay`: the
   record's retained bytes — inline body plus any spilled copy, measured in the same unit the
   hub charges its spill budget with — go 86 → 0 and the draft 11 → 0, while `has_job` and
   `retention_len()` stay where they were) and `acceptance.rs:2713`
   (`a_terminal_job_still_answers_replay_join_and_status_after_its_prompt_is_dropped`: with
   the bytes already gone, a `from_seq: 0` re-attach replays the exact `[Delta, Delta, Finish]`
   the live stream relayed, `assemble_completion` still builds `Hello world` with the same
   `usage`, a same-key submit joins rather than re-runs, and the whole `JobView` — including
   `payload_bytes: 86` and the digest — is unchanged). Measured on this tree with the repo-root
   target dir (`.rust/target`): `cargo fmt --all -- --check` **clean**; `cargo test
-p nexus-core` **149 passed / 0 failed** (89 lib + 60 acceptance); `cargo test
-p nexus-server` **54 passed / 0 failed**; `cargo test --workspace` **203 passed / 0
   failed**.
   Those are the 2026-11-14 numbers, recorded as measured and left standing. **Re-measured
   2026-11-15**, after the relay-lifetime pass, from the same target dir, both tests above still
   green: `cargo fmt --all -- --check` **clean**; `cargo test -p nexus-core` **151 passed / 0
   failed** (91 lib + 60 acceptance); `cargo test -p nexus-server` **72 passed / 0 failed**;
   `cargo test --workspace` **223 passed / 0 failed**.
2. **A `Reject` naming an already-forgotten job resurrected its relay row** (`nexus-server`).
   The `Effect::Reject` arm published the verdict through `handle.relay(job, capacity)`, which
   is **get-or-create** (`state.rs:1233-1241`), so a rejection that named a job the actor had
   already forgotten inserted a fresh row into `PoolHandle.relays`. Nothing could remove it
   afterwards: ids are never reused, no later effect names a dropped id, and `JobForgotten`
   had already run — one permanent row per occurrence for the life of the process, invisible to
   the pruning tests because the row looks exactly like a live fan-out. The arm now **looks the
   relay up** instead (`state.rs:1624-1651`), which is sound for the half that matters: a
   verdict can only be answered by a row that already holds a **gate**, and only the submit
   faces install one — `consumer::submit` behind `POST /v1/jobs` and the OpenAI surface
   (`consumer.rs:1470-1474`), reached from the WS submit at `consumer_ws.rs:720`. The attach
   faces create the row with a `replay_target` and **no gate** (`consumer.rs:1109-1111` `:1115`,
   `consumer_ws.rs:840-842` `:850`), and a gateless row discards the verdict either way:
   `JobRelay::new` leaves `gate: None` (`state.rs:240-249`) and `JobRelay::resolve` no-ops on
   `None` (`state.rs:252-256`). So looking the row up rather than inventing one changes no
   answer anybody receives. What this closes is the last unbounded row **the dispatcher itself
   created**; it does not claim the map can no longer grow. One residual case is untouched by
   the fix and is now at least observable: an `/events` row created by a face that races
   `Effect::JobForgotten` (`consumer.rs:1086-1132` creates the row after the prune at
   `state.rs:1773` has already run) is still never pruned, and each such refusal now logs
   `rejection names a job with no relay` (`state.rs:1661-1667`).
   **Amended 2026-11-15:** that residual is closed, along with two neighbours of the same shape.
   What made such a row unprunable was not the race itself but `JobRelay::is_idle`'s third
   condition — an installed `replay_target` — outliving every waiter that could clear it. A pool
   tick now sweeps any idle row the actor holds no record for once `ORPHAN_RELAY_GRACE_MS` (10 s)
   has passed (`state.rs`'s `prune_orphaned_relays`); an attach abandoned **after** it installed
   its target takes the target back out (`JobRelay::abandon_replay_target`, called from the
   `/events` `503` path and from the WS `attach` rejection); and a slot registered on a relay that
   has already finished is retired on the spot (`JobRelay::add_slot`), which is what a join of a
   task that went terminal mid-attach used to leave parked. Pinned by
   `the_tick_sweep_takes_relay_rows_that_outlived_their_job` and
   `an_abandoned_attach_leaves_a_sweepable_row` (`state::tests`), and the surviving rows are now
   countable from outside the process, which is what `nexus_relay_rows` and
   `nexus_relay_subscribers` on `/metrics` are for. Reachable path and the
   pinning test: `a_rejection_for_a_forgotten_job_resurrects_no_relay` (`state.rs:2079`),
   driven by the late `409 stale_attempt` an artifact upload emits after its job was forgotten
   (`io.rs:229-283`, `actor.rs:2759`). Revert-checked rather than merely green: on the pre-fix
   get-or-create line that test fails with `assertion left == right failed: a refusal nobody is
waiting on creates no relay — left: "job-neighbour,job-stranded", right: "job-neighbour"`,
   and it passes on the fix, after which `state.rs` was restored byte-exact by sha256.
   `cargo clippy --workspace --all-targets` on this tree reports **one finding**,
   `clippy::new_without_default` at `queue.rs:125` — a file this pass never touched; it predates
   this pass and is the only thing that turns `cargo clippy --workspace --all-targets -- -D
warnings` red.

## Relay-lifetime pass — the stream that ended, and the row that outlived it (2026-11-15)

The deployed hub climbed at **0.76 MB per minute** and never came back down: **48 kB retained per
accepted task**, against ~15.6 tasks a minute and 165 kB relayed per task, on **one** consumer
socket that stayed open for 117 minutes across some 596 tasks. The curve was the bug. A hub whose
consumer sockets are long-lived keeps one set of per-task bookkeeping per **task**, not per
**stream**, and nothing about it was visible: `/metrics` carried nine series, none of them about
the hub's own follow bookkeeping, so the process looked healthy the whole way up.

Nothing here is a release build, a benchmark, or a re-measured RSS curve — this pass ran in the
**dev profile** on this host, on instruction, and `cargo build --release -p nexus-server` was not
re-run, so the byte counts in the ledger above still describe their own builds and nothing newer.
What is claimed is narrower and testable: the structures that curve was attributed to are now
bounded by **live streams plus consumer-socket liveness** instead of by the task's lifetime. The
deployed curve has **not** been re-measured after the change.

1. **A streamed answer no longer depends on another task's cleanup to end** (`consumer.rs`'s
   `StreamPump`). The three streamed-HTTP pumps — the aggregate `POST /v1/jobs` answer, the OpenAI
   SSE body, and `GET /v1/jobs/{id}/events` — drained their private replay queue strictly before
   they looked at the live fan-out, the response body, or their own deadline. One interleaving
   breaks that ordering: a join resolves the task's relay row, the actor serves the terminal and
   the dispatcher takes that row away, and only then does the join install its replay target and
   post its `Attach`. The `ReplayTo` that follows is lookup-only (`state.rs:1685-1691`), so the
   queue is never written and never closed, and `replay.recv()` parks forever — inside a row the
   join's own `Subscriber` keeps alive, which is why the orphan sweep could not take it either.
   The pumps now poll **both queues, plus `tx.closed()`, plus the caller's clock** in one biased
   `select!`, holding any live item taken before the backlog drained (`held`, FIFO) until the
   backlog closes, so what a live consumer sees and in what order is **byte-identical** to what the
   sequential pumps printed; a live `End` taken early is deferred and becomes the _exit_ after the
   backlog, not a truncation of it. The negative proof is in §9.2's addendum: with only the two
   pump bodies reverted, the two hangup tests fail with `Elapsed(())` while the three ordering
   tests stay green. Exit needs **both** queues done, and only `pump.ended()` emits the in-band
   frame — so `detach`-after-end still answers `error{not_attached}`, as it did before.
2. **An attach abandoned after installing its replay target takes the target back out**
   (`JobRelay::abandon_replay_target`, called from the `/events` `503` path and the WS `attach`
   rejection; the third attach site already called `handle.forget_relay`). The receiver half of
   that queue dies with the handler while the sender stays in the row, and `JobRelay::is_idle`
   reads an installed target as "somebody is waiting" — which the sweep is built never to delete.
   That is one permanent row per abandoned attach, invisible to every test that existed, and it is
   the closed residual the 2026-11-14 entry above records.
3. **The bookkeeping is published**: `nexus_relay_rows` and `nexus_relay_subscribers`, sampled in
   the same locked pass that sweeps (`state::prune_orphaned_relays`) and merged per pool at render
   time (`Hub::render_metrics`). `pool` stays the only label; no existing name, label set or value
   moved. `GET /metrics` on a hub booted with the built-in defaults now ends its pool-shaped
   families with:

    ```
    # TYPE nexus_relay_rows gauge
    nexus_relay_rows{pool="image.render/comfyui"} 0
    nexus_relay_rows{pool="llm.chat/qwen38"} 0
    # TYPE nexus_relay_subscribers gauge
    nexus_relay_subscribers{pool="image.render/comfyui"} 0
    nexus_relay_subscribers{pool="llm.chat/qwen38"} 0
    ```

    `rows` follows the **actor's record** (a row lives while the task can still be re-followed),
    `subscribers` follows the **live stream** (a queue exists only while a pump reads it). That
    split is the whole point, and `runtime_e2e::the_relay_bookkeeping_shows_up_on_the_metrics_page`
    walks it end to end: nothing followed → a `202` submission (row 1, queues 0) → a watcher joins
    (queues 1) → the watcher hangs up (queues 0, **row still 1**, the task is still running) → the
    task finishes (both 0).

4. **A consumer that attaches after its task went terminal is closed at once**: `JobRelay::add_slot`
   retires a slot registered on a `finished` relay, so the receiver sees the same end of stream it
   would have seen one moment earlier, instead of parking on a queue nothing will ever write.
5. **The consumer ruler is 60 s, not 20 s** — see the `NEXUS_CONSUMER_KEEPALIVE_MS` row above for
   the arithmetic and for why a consumer verdict costs a rerun rather than a connection.
6. **A WS session's watch bookkeeping is bounded by its streams**, not by its socket (round one of
   this same pass, recorded nowhere else in this file): the per-task `watches` entry is posted back
   through the session's reap postbox when its pump ends, the fan-out slot is retired by
   `state::Subscriber`'s `Drop` — synchronously, so an aborted pump cannot strand a queue — the
   pump task is aborted after `PUMP_DRAIN`, and the observe mirror leaves with the stream.

**Twenty new tests**, all in this package: `state::tests` gained
`an_abandoned_attach_leaves_a_sweepable_row` and `the_sweep_publishes_the_relay_gauges`;
`consumer::tests` is new, with `an_sse_join_with_an_unclosable_backlog_ends_when_the_client_hangs_up`,
`an_events_join_with_an_unclosable_backlog_ends_when_the_client_hangs_up`,
`a_live_delta_waits_behind_the_backlog_it_arrived_before`,
`a_live_end_does_not_cut_off_a_backlog_that_is_still_open` and
`an_unclosable_backlog_still_answers_when_the_clock_fires`; `consumer_ws::tests` gained
`the_probe_windows_are_the_arithmetic_the_comments_claim`; and `runtime_e2e` gained
`the_relay_bookkeeping_shows_up_on_the_metrics_page`,
`a_hangup_on_a_silent_running_task_releases_its_watcher` (a watcher that vanishes on a **silent**
running task — the case a failed-write-only detector is blind to — pinned against a second consumer
that stays, so "the detach was recorded" cannot be confused with "any hangup cancels the task"), and
`an_events_join_that_races_the_terminal_leaves_nothing_behind` (that race arriving through real
sockets forty times, `job_retention_ms` at 1 s, every join answered and the pool empty afterwards).
Round one added `one_long_lived_consumer_socket_leaves_no_per_task_state_behind`,
`a_finished_sse_body_leaves_no_subscriber_on_its_relay`,
`a_silent_consumer_socket_is_reaped_and_releases_what_it_held` and
`a_consumer_that_only_answers_pings_stays_attached`. No test was weakened or deleted to get here.

Two things this pass deliberately left alone, so the absence is a decision and not an oversight:
**no allocator change** (`mimalloc`/`jemalloc` is a separate question with a separate risk surface;
every item above removes bookkeeping rather than re-laying it out), and **no `Default` for
`InMemoryQueue`** — `clippy::new_without_default` at `crates/nexus-core/src/queue.rs:125` stays
exactly where the tables above record it, in a file this pass never touched. Cleanliness was
therefore measured with the allow stated in the command:
`cargo clippy --workspace --all-targets -- -D warnings --allow clippy::new_without_default` →
**exit 0, zero findings**.

## Test-closure and measurement pass — the numbers the relay-lifetime pass said it did not have (2026-11-15)

The pass above ends on a sentence that is hard to beat for honesty: _nothing here is a release build,
a benchmark, or a re-measured RSS curve … the deployed curve has not been re-measured after the
change._ This pass is that measurement, plus the two end-to-end soaks the pass still lacked. **No
production line of `packages/typecrate/nexus` was changed to get it**: the code measured here is the code that
pass left behind, and the gates at the top of this file were re-run over it afterwards.

**Two new `runtime_e2e` soaks** (35 in that module, 74 in the binary, 225 across the workspace):

- `the_relay_gauges_stay_flat_as_streams_keep_arriving` — streams opened and closed on one
  long-lived socket, round after round, with `nexus_relay_rows`, `nexus_relay_subscribers` and the
  per-relay queue series required to be back on their starting lines after every round.
- `abandoned_streams_on_both_http_faces_leave_no_row_and_no_queue` — the deployed shape: a stream
  opened and **never read**, on the WS face and on `GET /v1/jobs/{id}/events`, then hung up without a
  `Last-Event-ID` and never revisited. Every round must end with `subscriber_count() == 0` on both
  relays, `followed_jobs()` empty, and both gauges exactly at the no-follow line.

They are pinned by **mutation**, not by observation. Neutering `JobRelay::retire`'s body
(`state.rs:282`, the unregister `Subscriber::Drop` performs) turns **both red**; restoring it turns
both green again. Removing the tick's `prune_orphaned_relays` call (`state.rs:1421`) leaves both
green, which is the correct reading: the sweep is the _backstop_, and
`state::tests::the_tick_sweep_takes_relay_rows_that_outlived_their_job` is what holds that leg.
Removing both at once is red again. `state.rs` went back byte-for-byte afterwards (numstat 1221/96
before the probes and after them).

**The RSS numbers.** Every row below is a **release** ELF — the two binaries in the build row above —
booted the way deployment boots it: uploaded to the Modal sandbox container as a `profile:"nexus"`
bundle, started with the container-derived `NEXUS_LISTEN` (`modal-sandbox-container.py:1370`), probed
on `/v1/observe`, torn down with `DELETE /` — sampled from `/proc/<pid>/status` by a process that is
not the process under test. One supplier link claims and answers; 5 tasks/s on a ruler; 180 s of
load, 90 s of idle; least-squares slope per window. `head` is `d3c0ed66`, `postfix` is the working
tree.

| Load shape                                                                                                                   | Rev     | Load Δ VmRSS | Per accepted task | Idle, 90 s    | Hub's own ledger afterwards                      |
| ---------------------------------------------------------------------------------------------------------------------------- | ------- | ------------ | ----------------- | ------------- | ------------------------------------------------ |
| One long-lived `/v1/consumer` socket, 1 125 tasks, none abandoned — **the deployed shape**                                   | head    | +6 596 kB    | **6 004 B**       | +28 kB (flat) | no such series (the gauge arrives with the pass) |
| The same                                                                                                                     | postfix | +4 656 kB    | **4 238 B**       | +72 kB (flat) | rows 0, subscribers 0                            |
| 1 125 tasks over `POST /v1/chat/completions` (read to `[DONE]`) plus `POST /v1/jobs` with `/events` opened and **abandoned** | head    | +4 892 kB    | 4 453 B           | 0 kB          | no series                                        |
| The same                                                                                                                     | postfix | +4 728 kB    | 4 304 B           | 0 kB          | rows 0, subscribers 0                            |

Three findings, none of them flattering:

1. **In the deployed shape the pass moves the number: 6 004 → 4 238 bytes per accepted task, ~30 %
   less — 1.94 MB less across 1 125 tasks on one socket.** In the one-stream-per-task HTTP shape the
   two revisions are indistinguishable (4 453 vs 4 304 B), which is what the pass's own scope
   predicts: it bounds per-_task_ state on a _long-lived_ socket, and that shape has no long-lived
   socket. A change that only fixed the abandoned-stream path would have shown up in the last row
   too; it did not, and it was not what the shape exercised.
2. **Neither revision comes back down in the idle window** — and the fixed hub's ledger reads zero at
   both sample points: 1 125 accepted tasks, `nexus_relay_rows 0`, `nexus_relay_subscribers 0`, no
   consumer-queue series left, RSS still ~5 MB above where it started. What is left on this host is
   therefore **not follow bookkeeping**; it is the allocator holding what it was handed. That is the
   allocator question this file has deliberately kept out of scope, and it is why the deployment's
   `MALLOC_TRIM_THRESHOLD_` / `MALLOC_ARENA_MAX` pins — which a host run does not apply — belong to
   the rest of the story rather than to this section.
3. **The container's own process is not where the deployed growth lives.** The same A/B against the
   sandbox as a _runner_, four legs: at the deployed rate (0.075 tasks/s) its RSS was flat-to-falling
   (load +0.34 kB/s working, +0.35 kB/s HEAD; idle +0.05 and −0.01 kB/s, 43 tasks in the window), and
   at 8 tasks/s both revisions climbed (3.5 and 4.6 kB/s) during load and then handed it back during
   idle (−0.68 and −0.89 kB/s). The "340 B per job" the runner was suspected of sits **below this
   harness's noise floor at the deployed rate** (43 tasks, ±40 kB), while the deployed 0.76 MB/min
   would have been +2.3 MB across that same window. Nothing in the runner explains the curve, and
   both revisions of the runner behave identically — so the runner needed no fix and got none.

**Supplier churn costs nothing.** Two runs that hammered the supplier face with a relay that dropped
the link every ~1 s left the hub with **24 and 29 re-handshakes** and `/v1/observe` still listing
**one** worker (`outstanding_claims: 0`, `available_slots: 0`, `link: orphaned` while the link was
down), `jobs: []`, and both pools at `queue_depth: 0`, `leases: 0`, `mailbox_pending: 0`. Reconnect
hygiene and detached-task abort needed no change, and none was made.

Two things the harness learned, written down because they will bite the next person who drives this
protocol from a hand-rolled client: **a pong must echo the ping's payload** — tungstenite rejects a
mismatch with a frame-protocol error and the hub ends the socket with _no close reason_, ~1.04 s in,
which is exactly the pool's `heartbeat_ms / 4` probe landing for the first time; and **the hub's
handshake read consumes one frame**, so a `ready` pushed in the same flight as `hello` is lost and
the link dies on the "worker never became ready" path. Worth recording next to them: `/healthz` is
`503` until a worker is ready and `200` after it, and `/v1/models` names the model only once the pool
can serve it — both are the hub's readiness contract, and a smoke that asserted them the other way
round was wrong about the contract, not about the build.

**Not claimed.** The deployed cell has not been re-measured. These are host runs of the same binaries
in the same container, with the deployment's allocator pins absent and at a task size (~900 B in,
~200 B out) far below the deployed 165 kB relayed per task, so the absolute per-task bytes above are
not the deployed 48 kB; what that residual is made of is attributed by the allocator pass below, at
production payload sizes and with the deployment's pins in force. Gates, re-run over the tree after
all of this: `cargo fmt --all -- --check` exit 0, `cargo check --workspace --all-targets` ok,
`cargo test --workspace` **225/225**, `cargo clippy --workspace --all-targets -- -D warnings` red on
`queue.rs:125` alone and clean with `--allow clippy::new_without_default` (exit 0), `python3
-m py_compile` clean on `services/modal/deploy/sandbox/modal-sandbox-container.py`, and a host smoke
of the container hosting the hub — bundle accepted, slot `ready`, `/metrics` `/healthz` `/v1/models`
on both faces, a supplier joining and becoming ready, one `stream: true` completion answered end to
end, `DELETE /`, slot `empty`, hub process gone, runtime directory removed, tunnel ledger at `active:
0` — **24 checks, 0 failures**.

## Allocator attribution — the residual is arena retention, measured (2026-11-15)

The section above ends one clause as an assertion rather than a measurement: what stays resident
after the hub's own ledger goes to zero is "the allocator holding what it was handed". It is measured
now, and on the topology where the deployment's pins are actually in force — the release hub started
inside `modal-sandbox-container.py` as a `profile:"nexus"` runtime, its child environment read back
from `/proc/<pid>/environ` to confirm `MALLOC_ARENA_MAX=2`, `MALLOC_MMAP_THRESHOLD_=1048576` and
`MALLOC_TRIM_THRESHOLD_=1048576` were in it. The traffic is production-shaped: **165 kB in and 165 kB
answered per task** (the hub's own `nexus_relay_bytes_total` books 163 kB of inbound payload per
accepted job), ~**15.4 accepted jobs/min**, 24.8 % of submissions abandoned mid-SSE, **16 min of load
followed by 6 min of idle** — **ten legs, five pin configurations, two reps each**. Every idle sample
is taken past `job_retention_ms` (300 s, `limits.rs:241`) with `nexus_relay_rows` and
`nexus_relay_subscribers` both reading **0**, so the registry is provably empty at the point the
memory is measured. Driver and captures: `Temp/allocator/alloc-soak.py`, summarised in
`Temp/allocator/summary-final.txt`.

1. **The residual is glibc arena retention, measured.** At idle end glibc's own `malloc_stats()`
   reports **1.35–1.55 MB in use in every leg** — the legitimately live heap, unchanged by any pin —
   while `system bytes` sits **18.8–26.1 MB above it**: mapped, and owned by nothing. An in-process
   `malloc_trim(0)` (a `gdb -p` probe into the hub; nothing external was touched) handed
   **15.1–19.7 MB** of it back, return value `1`, in **0.19–0.24 s**, leaving **4.12–5.06 MB** mapped
   beyond the live heap. The plateau is memory the allocator is holding after being handed it, which
   is exactly what the section above asserted and did not measure.
2. **The arena count is where it hides, and the shipped cap is the lever that pays.** With
   `MALLOC_ARENA_MAX` unset the hub ends idle holding **twelve arenas**, with 24.0–24.8 MB of the
   plateau resident inside mappings of 1 MiB and over instead of inside the two arenas the cap keeps it
   in — the same freed memory, parked somewhere else. What the cap does reclaim is mapped-but-free:
   **25.9–27.4 MB mapped with the cap absent against 22.3–22.7 MB as shipped**, at `in use` 1.35–1.37
   MB against 1.37 MB either way, and every capped configuration finished below both un-capped legs in
   **both** reps.
3. **The hypothesised ~48 kB retained per job is not reproduced at production payload sizes.** Over
   the retention window — the 76–77 records still inside `job_retention_ms` at load end, counted
   against the `nexus_jobs_accepted_total` diff — the hub hands back **0–19 kB per record, median 9
   kB**, two legs giving back slightly more than they took. The residual per **accepted job** is
   80–100 kB, and it does not scale with job count: the plateau is the allocator holding the **peak
   working set**, and the plateau is **flat, not creeping** — the idle-tail slope, least squares
   over each leg's full idle window, is **0 to −4.5 kB/s across all ten legs, never positive**.
4. **The three pins stay, on the gate this file states elsewhere.** Idle-end anonymous memory moves
   by **0.19–3.35 MB run-to-run under identical pins**, so a candidate has to beat the shipped values
   by three times that spread to count as a finding; the best alternative on offer — both ceilings at
   glibc's 128 KiB — came in at **−1.16 MB against a 3×-noise gate of 2.78 MB**, inside noise, and it
   is not free: with the trim threshold at 128 KiB, glibc shrinks the heap top on every free past it
   and lets it grow back, which this project measured at **18× the CPU** on its most operator-hot
   route (`GET /logs`: 9.2 ms against 0.5 ms for the same 200 × 512 KiB alloc/touch/free loop,
   `modal-sandbox-container.py:795-811`). So `MALLOC_ARENA_MAX=2`, `MALLOC_MMAP_THRESHOLD_=1048576`
   and `MALLOC_TRIM_THRESHOLD_=1048576` stay exactly where they are (`:730`, `:814-815`), still
   operator-overridable through `RUNNER_MALLOC_*`, and `malloc_trim(0)` stays out of the runner for the
   reason already recorded there: in CPython it would run inside a hot request path while holding the
   GIL across the FFI call, to win back ~1 MB.

**What the runner's `GET /stats` exposes about memory, field by field.** Five keys inside the
payload's `relay` block, **additive** to what it already carried (`selfRssKb`, `selfHwmKb`,
`activeThreads`, `mallocArenaMax`) and **camelCase as the code emits them**, not the snake*case a
consumer may reach for. `selfRssAnonKb`, `selfRssFileKb` and `selfVmDataKb` are the runner's own
private heap, mapped files plus page cache, and allocator segment size — `RssAnon`, `RssFile`
and `VmData` off `/proc/self/status`, each `null` rather than absent where the kernel stops short
(`:1234-1236`, `:3530-3532`). `mallocMmapThresholdBytes` and `mallocTrimThresholdBytes` are the
byte counts the two `MALLOC*`ceilings actually impose,`null` for an operator form glibc refuses
rather than the raw string echoed back (`:3544-3545`). Together they let an operator confirm from
outside which pins a hosted process was handed — `MALLOC*ARENA_MAX=2`,
`MALLOC_MMAP_THRESHOLD*=1048576`and`MALLOC*TRIM_THRESHOLD*=1048576`, each operator-overridable
through `RUNNER*MALLOC*\*`, the arena cap reporting as the older `mallocArenaMax`. What they cannot
show is stated where the fields are defined: whether Modal's own edge worker inherited those pins
is **not observable** from `/stats`, because the runner reads its own environment, not its
parent's (`:4154-4163`).

**What this does not claim.** The deployed cell has **not** been re-measured after the fix, and these
legs are host runs: hosting the same release binaries, at the deployed pins and at production-shaped
payloads, is as far as the claim goes. A host cannot stand in for the deployed **0.125-core** cell,
where allocator contention is priced by a queue rather than by cores, nor for the **4-hour** production
horizon the 0.76 MB/min curve was read over. The attribution moved; the deployment did not.

**Addendum — the image-wide variant is reverted; the pins keep their scope (2026-11-16).** Row 4 of
this section says the three values "stay exactly where they are", and after one day deployed that
sentence needs its scope written down. They were, briefly, in two places: `_sandbox_env` — and on
2026-11-15 also `IMAGE_ENV`, so the envelope would reach every process in the container including
Modal's own worker (`modal-sandbox-container.py:4132-4174` at that revision). On 2026-11-16 the three
keys were taken back out of `IMAGE_ENV` and the constants stayed put (`:730`, `:814-815`), so what is
true now is the narrower sentence: the pins are delivered by **`_sandbox_env` (`:4530`, keys at
`:4549` / `:4564` / `:4565`)** into the driver's `Popen` of the runner (`_start_box_runner`, `:5190`
/ `:5199`), and every hosted child — a nexus hub included — inherits them through **`_child_env`
(`:1632`, `:1636`, spawned at `:1708`)**, which is literally `{**os.environ, …}`. Nothing about the
measured values, the `RUNNER_MALLOC_*` operator overrides, or the `/stats` caveat changes.

Why it came back out: Modal bills `max(requested, actual)` at roughly **$0.047 per physical
core-hour**, and its own Function runtime is `exec`'d in the same process chain, so an image `ENV`
lands on the billed worker too — a Function running `@modal.concurrent(max_inputs=32)`
(`:4071`, applied `:5244`) with one input thread per concurrent input, i.e. up to **32** threads.
Arena cap 2 across 32 threads in that process was an unmeasured allocator-contention risk,
**unobservable from `/stats` either way** for the reason this section already states (`:1534-1536`),
and ~4 MB of idle-end mapped-but-free RSS (25.9–27.4 → 22.3–22.7 MB, row 2) is not the price of a
variable that big on the thing being billed. Reverting it also **does not attribute the deployed
CPU**: local measurement on 2026-11-16 put the runner's idle floor at **0.000143 core**, its
marginal cost at **0.000872 CPU-s per request** and the rate needed to reach one core at
**~1 147 req/s** against the **~0.67 req/s** the box sees, i.e. the billed ~1.0 core is ~7 000×
the idle floor and ~1 365× the measured traffic-leg cost (`Temp/runner-cpu/out/analysis.txt`); the
CPU rise is still somebody else's line item, and this change removes one unmeasured variable from
the picture rather than naming a cause. And it is not in effect until the **next image rebuild**:
`IMAGE_ENV` is an image `ENV` directive, and removing it changes the toolchain layer, so every
layer under it rebuilds on the next deploy; nothing about the running box changes before that.

## Consumer-directed supplier pinning — the header that names a connection (2026-11-18)

A consumer that liked an answer wanted the same machine for the next one. The hub had already
minted the name for that wish and never used it: every supplier socket is stamped with a hub-minted
UUIDv4 and reported in `Welcome`, so "the same machine" is an id the hub can check rather than one
the supplier declares and the consumer repeats blindly. The uplift puts that name on a request and
gives it back on the answer. The normative rule is spec **NEX-4.7**, both spellings are spec §7, and
what belongs in this file is what a client may rely on and where the caveats bite.

1. **The pin is a reservation enforced at pick time, not a placement made at submit time.** The
   hub is pull-driven and stays pull-driven: nothing is pushed at the named supplier and no table
   holds a slot for it. The name rides on the submission (`Submission::supplier_hint`) and is
   re-read on every claim by one predicate, `supplier_pin_allows`, which refuses _every other_
   supplier while that connection is `Live` — including while the named supplier is still `Loading`
   or momentarily has no free slot, which is the only meaning "queue the consumer to that supplier"
   can have in an architecture where the supplier asks for work. A pinned task is preferred ahead of
   older unpinned tasks **on its owner's own claim**, so the promise cannot starve behind FIFO
   traffic; the two-pass scan that does it (`pick_job_for`) changes nothing else — priority class,
   arrival order and the tenant cap are the same gates, combined by `AND`. **Session affinity is the
   one gate that does not stay ANDed, and that is a defect closed, not a preference added.** A
   request carrying both `x-session-id` and `x-supplier-id`, whose targets differ, was leasable by
   **nobody**: the pin refused the hash owner, affinity refused the pin owner and every peer, and
   neither relaxation pass could help because both links stayed healthy — so the task sat until
   `claim_deadline_ms` (600 s by default) answered it with in-band `queue_timeout` on the streaming
   face. Controls with either hint alone dispatched normally; with both, zero leases and zero
   dispatch. One shared liveness reader (`actor.rs`'s `live_pin`) now lets a live pin past
   `affinity_allows` **for its own owner alone**: the hash owner and every peer are still gated, and
   the phase term, `serves_model` and the tenant cap still bind the pin owner, so a pin stays a
   reservation rather than a suggestion and still cannot force a mismatched dispatch. The rationale
   is that the hash is an optimisation over a pool the consumer never chose, while the pin is the
   consumer's own promise naming a connection this hub handed it, so honouring the more specific
   signal is the only reading under which both headers can be satisfied. On the event that lapses
   the pin, the session's hash owner takes the task **in that same step**. Pinned by
   `t20j_a_supplier_pin_outranks_a_session_hint_that_disagrees_with_it`, with the three-arm version in
   `actor.rs`'s own test module. What that ranking does **not** fix, and does not claim to: a session
   hint whose rendezvous target does not serve the requested model still leaves the task leasable by
   nobody — affinity refuses the peers, the model gate refuses the owner — and it terminates at the
   claim deadline; that state predates the pin and this build leaves it as it found it.
2. **The model is never traded for the pin.** A live pin on a supplier that does not serve the
   requested model neither forces a mismatched dispatch nor quietly falls back to a supplier that
   does: the task waits, and the deadline it already had answers it with the codes the hub already
   had — `no_ready_worker` while the record is still `Pending`, in-band `queue_timeout` once
   `Accepted` has opened the body. No new reason code, no new effect. This is why the feature is not
   the scheduling sophistication spec §6.7 declines: a pin ranks nothing, it declines every other
   supplier for one connection's lifetime because the consumer asked for that.
3. **Relaxation is an event, not a poll.** The pin lasts exactly as long as that connection is live.
   On disconnect, on orphan-grace expiry, on eviction, the hub re-runs its own dispatch pass **in
   that step** (`pins_held_by` gates the pass, so a close with nothing pinned stays byte-identical
   to what it was before the feature), and the task goes to a model-eligible peer without waiting
   for an unrelated frame. A hub-minted id is never reused, so a lapsed pin cannot silently re-arm
   behind the consumer's back.
4. **The caveat that decides how a client should use this: a supplier id is a connection, not a
   machine.** A reconnect mints a fresh UUIDv4, so the id in the consumer's hand is permanently gone
   the moment the socket drops. Pinning keeps a conversation on the warm supplier that answered it,
   for the life of that link; it does not re-find the machine after a restart. A client should send
   back the id it was last answered with, read the id it is answered with now, and treat a pin it
   cannot honour as ordinary routing — which is all the hub does with it.
5. **The answer names who was connected — the _first_ one, not the current one —** read from
   the job's lease in the same actor turn that produced the verdict, so the header and the `Assigned`
   the supplier receives cannot disagree. The hub-side latch is first-write-wins, so a requeue does
   not move the value: A leases, A dies, B leases and produces the answer, and a head that flushes
   late still names **A**. That is the moment the header is about — the connection this hub handed
   this consumer — and who holds the lease **now** is the actor's `lease.worker`, which
   `GET /v1/jobs/{id}` reports and which is what a client reads after a takeover. HTTP answers
   `x-supplier-id`, the consumer socket answers `accepted.supplier` —
   not redundancy, but the only plane a WebSocket has: nothing can be added to a response head after
   the upgrade. Absence means "no supplier was connected yet" and nothing else. On the streaming path
   the price of the header is that the response head **waits for the lease it will name** instead of
   flushing at admission, and **nothing at all reaches the client while it waits**: an SSE keepalive
   is a body byte, and a body byte cannot precede the head. The wait is bounded by the kind's own
   `claim_deadline_ms` (600 s by default), the interval that task was already going to sit queued
   for, so no new timer and no new knob — but an intermediary on an aggressive idle timeout can now
   close a request that is merely queued behind busy capacity. A client that **does** hang up
   inside that window is not left to the timer: the face arms a drop guard around the wait
   (`consumer.rs`'s `SupplierHoldDetach`), hyper drops the request handler with the connection, and
   the guard posts the detach the pump would have posted, so the queued task is cancelled and its row
   deleted on the spot instead of holding a queue position — and, when the request was pinned, a
   whole claim window of refusing every other consumer's claim for a task nobody wants. It needed a
   guard rather than a `select!` because the body channel and its watcher are created only _after_
   the hold returns, so nothing polls them while the head is pending
   (`a_hangup_inside_the_head_hold_cancels_the_queued_task_it_never_leased`). Refusals still answer
   status-before-bytes, `503 + Retry-After` included, because every refusal arm returns before the
   wait begins, and neither `stream: false` nor the `POST /v1/jobs` `202` ever holds its head open.
   A `4xx`/`5xx` refusal carries no header, and neither does a `202` whose verdict leased nobody —
   a join of a finished task included, whose lease has been released — while a `202` whose turn did
   connect one carries it, and the detached route's own answer — naming a supplier while an idle
   peer is refused the task — is pinned by
   `a_pinned_job_post_names_the_supplier_it_leased_and_holds_the_task_from_the_peer`. A queued job's
   lease is already readable on `GET /v1/jobs/{id}`.
6. **What moved, and what did not.** One routing rule moved: **session affinity**, in the narrow way
   item 1 states it — a live pin releases its own owner from the affinity gate, and nothing else is
   released. Everything else on that list is exactly as it was: credentials, pool membership, the
   queue, lease lifetime and renewal, priority class, FIFO order and the tenant cap. The supplier face
   gained no field and the wire version did not move, because a supplier cannot tell pinned work from
   ordinary work — that property is what keeps every deployed supplier speaking the same dialect.
   Both new consumer-frame fields are optional and skipped when absent, so a pre-feature
   client's frames still decode and a hub with no pin to honour encodes byte-identical frames
   (`a_supplier_pin_is_wire_compatible_in_both_directions`, `protocol.rs:1501`). The rule lives in
   the actor, so a host that embeds the actor inherits it with the same code: the
   `consumer-directed pinning` group in `crates/nexus-core/tests/acceptance.rs` (banner at `:4561`,
   `t20a`–`t20j`) and the worker-state table in `actor.rs`'s own test module — including the three
   that pin the precedence, `a_live_pin_releases_affinity_for_its_owner_and_nobody_else` (`:4392`),
   `a_lapsed_pin_hands_the_job_back_to_the_affinity_owner` (`:4427`) and
   `the_affinity_arm_never_outranks_the_model_gate` (`:4471`) — pin it state by state;
   the header and the frame field themselves belong to the two consumer faces in
   `crates/nexus-server`. The gates are the ones in the table at the top of this file, and they have
   run over this tree, once more after the deadlock fix: `cargo test --offline --workspace` is
   **262 passed / 0 failed** — `nexus-core` lib **105**, `tests/acceptance` **70**, `nexus-server` bin
   **87**, worker-example 0, doc-tests 0 —
   against the identity pass's 227, which is 13 core units, 10 acceptance cases and 12 bin tests this
   change added. `cargo fmt --all -- --check` is clean and `cargo check --offline --workspace
--all-targets` reports 0 errors and 0 warnings; `cargo clippy --offline -p nexus-server
--all-targets -- -D warnings` stops on the pre-existing `clippy::new_without_default` at
   `crates/nexus-core/src/queue.rs:125` and on nothing else, and the same scan with that lint allowed
   is exit 0 with zero findings. What this section still does not claim is a measurement: no latency,
   throughput or allocation figure was taken over this tree.
