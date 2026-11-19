//! Runtime end-to-end tests for the native hub: a real listener, a real worker
//! WebSocket, and a real consumer HTTP request whose SSE bytes are read off the
//! wire. The core acceptance tests (`crates/nexus-core/tests/acceptance.rs`) drive
//! the actor with injected time and prove the *decisions*; only this file can prove
//! the runtime performs them — that effect bytes reach a socket, that a 503 is
//! answered before a body opens, that an unauthenticated read is refused.
//!
//! ## Status: executed
//!
//! These run on every `cargo test --workspace` (the `--bins` target), and they are
//! the only place the **runtime** is observed. The acceptance suite drives the actor
//! with injected time, so anything that lives in the wire layer is invisible there:
//! an SSE body that never ends, a detach that never reaches the actor, a verdict
//! that arrives after the first event byte, a 503 that opens a body anyway. When one
//! of these fails, suspect the runtime wiring before the actor's decisions — the
//! decision half has its own green suite.
//!
//! ## Why the assertions wait rather than sleep
//!
//! The hub batches outgoing deltas (`flush_interval_ms`), the actor ticks on an
//! interval, and the no-worker grace is a wall-clock deadline. Tests therefore wait
//! for the next wire frame or read the response body to completion, and the config
//! shrinks the windows so a broken expectation fails fast instead of hanging.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::protocol::decode_hub_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::Attestation;
use nexus_core::protocol::FinishKind;
use nexus_core::protocol::FrameKind;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::Requires;
use nexus_core::protocol::WireCaps;
use nexus_core::protocol::WorkerFrame;
use nexus_core::BrokerConfig;
use nexus_core::JobId;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Reference digest from `SHARD_TABLE` (services/molab/molab-strata-runtime.py:95).
const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
/// The pool the reference worker joins, matching `BrokerConfig::default()`.
const POOL: &str = "llm.chat/qwen38";
/// The other pool of the sample topology: declared, never submitted to. It is named because the
/// per-pool metric families render a line for it, and a test that ignored that line would be
/// asserting a hub-wide tally rather than a per-pool reading.
const OTHER_POOL: &str = "image.render/comfyui";
/// Consumer key and worker token for the test hub (secrets in both directions,
/// exactly as §9 requires — they are deliberately different strings).
const API_KEY: &str = "sk-e2e-1";
const POOL_TOKEN: &str = "pool-token-e2e";

/// A hub config with the task's shape and the wait windows shrunk to test scale.
fn test_config() -> BrokerConfig {
    let mut cfg = BrokerConfig::default();
    // Anonymous is deliberately **off**: the hub must answer an unauthenticated
    // consumer read with `401` (`job_reads_require_an_api_key_and_operational_routes_do_not`),
    // and every test request carries `API_KEY` / `?token=POOL_TOKEN` anyway.
    cfg.auth.allow_anonymous = false;
    cfg.auth.worker_tokens = vec![POOL_TOKEN.to_string()];
    cfg.auth.api_keys = vec![nexus_core::config::ApiKeyConfig {
        key: API_KEY.to_string(),
        tenant: "default".to_string(),
    }];
    cfg.auth.model_shas = BTreeSet::from([SHARD_1.to_string()]);
    // Short enough to keep the suite quick, long enough that the actor's 1 s tick
    // is guaranteed to see the expiry before the handler's own deadline does.
    cfg.limits.no_worker_grace_ms = 800;
    cfg.limits.flush_interval_ms = 20;
    cfg.limits.flush_bytes = 4_096;
    cfg.validate().expect("test config must validate");
    cfg
}

/// A test hub: its address, the `Hub` behind it (kept alive by the caller so the
/// actor tasks are not dropped mid-test), and the spill directory to clean up.
struct TestHub {
    addr: std::net::SocketAddr,
    /// The `Hub` behind the address. Holding a second `Arc` is what keeps the hub (and
    /// every pool actor it owns) alive for the lifetime of the test, and the tests that
    /// assert on hub-side bookkeeping (the routing index, the relay map, the idempotency
    /// mirror) read it directly rather than inferring it from a response.
    hub: Arc<crate::state::Hub>,
    root: String,
}

impl Drop for TestHub {
    /// Best-effort teardown: these tests write real artifact parts to disk, and a
    /// green run must not leave untracked spill in the working tree.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Serve the **production router** on an ephemeral loopback port.
///
/// `name` isolates the artifact spill: one directory per test, because `cargo test`
/// runs these in parallel and two hubs sharing a spill directory would race on the
/// per-attempt staging file (`io::write_chunk` appends to
/// `{object}.parts/a{attempt:04}/staging.bin`, `io.rs:759`). The directory is the
/// repo-root scratch location (`io::test_scratch`, i.e. `<repo>/Temp/nexus-tests/<name>`)
/// rather than the package's `var/`, per the root `AGENTS.md` rule: this is
/// throwaway test state, not a hub's configured artifact root. The runtime default
/// (`Temp/nexus/artifacts`, `NEXUS_ARTIFACT_ROOT`) is untouched.
async fn spawn_hub(name: &str, cfg: BrokerConfig) -> anyhow::Result<TestHub> {
    let root = crate::io::test_scratch(&format!("e2e-{name}"))
        .display()
        .to_string();
    // Start from an empty tree so a leftover part from an aborted run can never
    // satisfy a digest assertion in this one.
    let _ = std::fs::remove_dir_all(&root);
    crate::io::ensure_root(&root).await?;
    let hub = crate::state::Hub::new(cfg, &root);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // The serving task owns its own handle: cloning before the `async move` keeps
    // `hub` alive for the caller (the tests assert against it after `spawn`).
    let serving = Arc::clone(&hub);
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, crate::app(serving, 1 << 20)).await {
            eprintln!("[e2e] test hub stopped: {err}");
        }
    });
    Ok(TestHub { addr, hub, root })
}

/// A worker that dials out and handshakes. The hub assigns the worker id; the
/// caller learns it from the `Welcome` frame. `ready` additionally sends
/// `Ready{engine_ready:true}` and a `Claim`, i.e. a worker the hub may dispatch to.
async fn dial_worker(
    addr: std::net::SocketAddr,
    ready: bool,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let url = format!("ws://{addr}/v1/worker?token={POOL_TOKEN}");
    let (mut ws, _response) = connect_async(url).await?;
    let caps = WireCaps::default();
    let hello = WorkerFrame::Hello {
        pools: vec![POOL.to_string()],
        capacity: 4,
        requires: Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(524_288),
            capabilities: BTreeSet::from(["tools".to_string()]),
            kv_estimate_tokens: None,
        },
        attestation: Attestation {
            model_shas: BTreeSet::from([SHARD_1.to_string()]),
            weight_bytes: Some(83_863_586_496),
        },
        protocol: nexus_core::PROTOCOL_VERSION,
        engine: Some("strata".to_string()),
        tier: Some(nexus_core::WorkerTier::Dedicated),
    };
    ws.send(Message::Text(encode_line(&hello, &caps)?.into()))
        .await?;
    if ready {
        ws.send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
            .await?;
        ws.send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
        ))
        .await?;
    }
    Ok(ws)
}

/// The heartbeat that makes a worker dispatchable: engine ready, four slots, enough
/// KV for the pool's declared context.
#[must_use]
fn ready_frame() -> WorkerFrame {
    WorkerFrame::Ready {
        free_slots: 4,
        free_kv_tokens: Some(524_288),
        engine_ready: true,
        telemetry: None,
    }
}

/// A worker the hub may admit a task for but never picks one up for: `hello` and
/// `Ready`, no `Claim`. The distinction the caller needs is the job's phase — with a
/// ready worker the record reaches `Queued`, which NEX-6.6's sweep may delete, while a
/// `Claim` would move it to `Assigned`, which the sweep deliberately leaves alone
/// because a lease means GPU work has been committed (`actor.rs:3221-3227`).
async fn dial_ready_but_idle_worker(
    addr: std::net::SocketAddr,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let mut ws = dial_worker(addr, false).await?;
    ws.send(Message::Text(
        encode_line(&ready_frame(), &WireCaps::default())?.into(),
    ))
    .await?;
    Ok(ws)
}

/// The next hub frame of the named kind (`welcome`, `assigned`, `cancel`, …).
///
/// Anything else on the socket is *read and skipped* rather than ignored silently:
/// a hub that sends the wrong thing surfaces as "timed out waiting for X", with the
/// frames it did send visible in the failure of the calling assertion.
async fn next_frame_of(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    kind: &str,
) -> anyhow::Result<HubFrame> {
    let until = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(until);
    loop {
        // The select only *produces* the message: replying to a ping needs another
        // mutable borrow of the socket, which must not overlap the `Next` future.
        let message = tokio::select! {
            _ = &mut until => return Err(anyhow::anyhow!("timed out waiting for a `{kind}` frame")),
            message = ws.next() => message,
        };
        let text = match message {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await?;
                continue;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                return Err(anyhow::anyhow!("the hub closed the worker socket"))
            }
            Some(Ok(_)) => continue,
        };
        // One message may carry several lines; the hub writes one frame per line.
        for line in text.split('\n').filter(|line| !line.trim().is_empty()) {
            let frame = decode_hub_frame(line.trim(), &WireCaps::default())?;
            let name = match &frame {
                HubFrame::Welcome { .. } => "welcome",
                HubFrame::Assigned { .. } => "assigned",
                HubFrame::Cancel { .. } => "cancel",
                HubFrame::Ack { .. } => "ack",
                HubFrame::Reject { .. } => "reject",
                HubFrame::ResumeNack { .. } => "resume_nack",
                HubFrame::Bye { .. } => "bye",
            };
            if name == kind {
                return Ok(frame);
            }
        }
    }
}

/// The consumer body every test posts: an OpenAI chat request with streaming on.
fn chat_body(content: &str) -> serde_json::Value {
    serde_json::json!({"model": "qwen38", "messages": [{"role": "user", "content": content}], "stream": true})
}

/// §16.6 at runtime: a connected-but-loading worker makes the request serviceable
/// in principle, so it is not a `400`; when nobody becomes ready inside the grace
/// window the consumer must get `503 + Retry-After` **and no SSE body at all**,
/// because at that point the status is still free (task §7.2, §11).
#[tokio::test]
async fn loading_worker_answers_503_with_retry_after_before_any_body() -> anyhow::Result<()> {
    let hub = spawn_hub("loading_503", test_config()).await?;
    let mut ws = dial_worker(hub.addr, false).await?;
    // The handshake must be accepted before the consumer arrives, otherwise this
    // test would be measuring "no workers at all" (a 400) instead.
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("hi"))
        .send()
        .await?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "grace expiry must answer 503"
    );
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    assert!(retry_after.is_some(), "a 503 must carry Retry-After");
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        content_type, "application/json",
        "the answer is a plain JSON error, not an opened SSE body"
    );
    let text = response.text().await?;
    assert!(
        text.contains("no_ready_worker"),
        "expected the retryable no-capacity code, got {text}"
    );
    Ok(())
}

/// The M0/M1 pipe end to end: the worker claims, the hub dispatches with a seq
/// floor, the worker relays an OpenAI-shaped `tool_calls` fragment, and the
/// consumer's SSE body carries that fragment **verbatim**, one finish reason, and
/// exactly one `[DONE]` (§16.14; §16.4's no-double-finish half; §7.2's headers).
#[tokio::test]
async fn worker_relay_reaches_the_consumer_verbatim_over_sse() -> anyhow::Result<()> {
    let hub = spawn_hub("relay_sse", test_config()).await?;
    let mut ws = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let chunk = serde_json::json!({
        "id": "chatcmpl-x",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "id": "c1", "function": {"name": "get_weather", "arguments": "{\"ci"}}]}, "finish_reason": null}],
    });
    let client = reqwest::Client::new();
    let mut body = chat_body("weather?");
    body["tools"] = serde_json::json!([{"type": "function"}]);
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&body)
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "a ready worker means the stream opens"
    );
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE, got {content_type}"
    );
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok()),
        Some("no"),
        "§7.2: intermediaries must not buffer"
    );

    let assigned = next_frame_of(&mut ws, "assigned").await?;
    let (job, start_seq) = match assigned {
        HubFrame::Assigned { job, start_seq, .. } => (job, start_seq),
        other => return Err(anyhow::anyhow!("expected an Assigned, got {other:?}")),
    };
    assert_eq!(start_seq, 0, "a first attempt numbers from 1");

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: start_seq + 1,
        kind: FrameKind::Delta,
        content_type: Some("application/json".to_string()),
        text: chunk.to_string(),
        artifact: None,
    };
    ws.send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: start_seq + 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 3})),
        error: None,
    };
    ws.send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    // `text()` resolves once the handler ends its body, which is itself the
    // assertion that the stream terminated instead of hanging.
    let text = tokio::time::timeout(Duration::from_secs(10), response.text()).await??;

    // The OpenAI chunk stream is exactly three chunks plus the sentinel: the hub's
    // own role delta (headers-before-dispatch, §7.2), the worker's frame relayed as
    // its own event, and the terminal chunk that carries `finish_reason` + `usage`.
    // Counting the `object` label is the "no duplicates" half of §16.4/§16.14: a
    // relay that emitted the same delta twice, or a finish chunk emitted twice, lands
    // on 4 here.
    assert_eq!(
        text.matches("chat.completion.chunk").count(),
        3,
        "role delta + the relayed chunk + the finish chunk, no duplicates: {text}"
    );
    assert_eq!(
        text.matches("get_weather").count(),
        1,
        "the tool-call fragment is relayed exactly once: {text}"
    );
    let payloads: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .map(|line| serde_json::from_str(line).unwrap_or_else(|_| panic!("chunk is JSON: {line}")))
        .collect();
    assert_eq!(payloads.len(), 3, "one SSE data payload per chunk: {text}");
    assert!(
        payloads
            .iter()
            .all(|chunk| chunk["object"] == serde_json::json!("chat.completion.chunk")),
        "every event is an OpenAI chunk object: {payloads:?}"
    );
    assert_eq!(
        payloads[0]["choices"][0]["delta"]["role"].as_str(),
        Some("assistant"),
        "§7.2: the role delta opens the stream"
    );
    assert_eq!(
        payloads[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"].as_str(),
        Some("get_weather"),
        "§16.14: the tool-call fragment survives the relay"
    );
    assert_eq!(
        payloads[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"].as_str(),
        Some("{\"ci"),
        "a half-written tool argument is not re-tokenized: {text}"
    );
    assert_eq!(
        payloads[1]["id"].as_str(),
        Some("chatcmpl-x"),
        "the worker's own chunk fields travel untouched"
    );
    assert_eq!(
        payloads[2]["choices"][0]["finish_reason"].as_str(),
        Some("stop"),
        "§16.4: exactly one finish_reason, on the last chunk"
    );
    assert_eq!(
        payloads[2]["usage"],
        serde_json::json!({"completion_tokens": 3}),
        "usage is relayed verbatim"
    );
    assert!(
        text.contains("id: 1\n") && text.contains("id: 2\n"),
        "the two relayed frames keep their seqs as `Last-Event-ID` cursors (§7.5): {text}"
    );
    assert_eq!(
        text.matches("[DONE]").count(),
        1,
        "exactly one [DONE] sentinel: {text}"
    );
    Ok(())
}

/// A producer that bursts must not cost the consumer a single frame. The old
/// fan-out was a 256-slot broadcast ring that silently dropped whatever a pump
/// had not yet read, so one burst ended the consumer's stream with a
/// `consumer_lagged` error mid-generation, the job was cancelled, and the
/// supplier finished into a lease that no longer existed. Delivery is a queue
/// per consumer now: the burst arrives whole.
#[tokio::test]
async fn a_burst_of_frames_reaches_the_consumer_whole() -> anyhow::Result<()> {
    let hub = spawn_hub("burst_relay", test_config()).await?;
    let mut ws = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("count to three thousand"))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "a ready worker means the stream opens"
    );

    let assigned = next_frame_of(&mut ws, "assigned").await?;
    let (job, start_seq) = match assigned {
        HubFrame::Assigned { job, start_seq, .. } => (job, start_seq),
        other => return Err(anyhow::anyhow!("expected an Assigned, got {other:?}")),
    };

    // 3000 deltas, each uniquely numbered, streamed back to back - more than
    // the old ring held, and with the consumer reading nothing until the end,
    // which is exactly when the old code dropped whatever the pump had not
    // drained yet.
    const BURST: u64 = 3000;
    let caps = WireCaps::default();
    for seq in (start_seq + 1)..=(start_seq + BURST) {
        let chunk = serde_json::json!({
            "id": "chatcmpl-burst",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0,
                         "delta": {"content": format!("x{seq}")},
                         "finish_reason": null}],
        });
        let frame = WorkerFrame::Frame {
            job: job.clone(),
            seq,
            kind: FrameKind::Delta,
            content_type: Some("application/json".to_string()),
            text: chunk.to_string(),
            artifact: None,
        };
        ws.send(Message::Text(encode_line(&frame, &caps)?.into()))
            .await?;
    }
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: start_seq + BURST + 1,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": BURST})),
        error: None,
    };
    ws.send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    // Resolves once the handler ends its body - which is itself the assertion
    // that the stream terminated instead of hanging on keepalives.
    let text = tokio::time::timeout(Duration::from_secs(30), response.text()).await??;
    // Every delta is present: the consumer's own numbering is the proof that
    // no frame was dropped by the fan-out.
    for seq in (start_seq + 1)..=(start_seq + BURST) {
        let token = format!("x{seq}");
        assert!(
            text.contains(&token),
            "delta {seq} never reached the consumer: the fan-out dropped frames"
        );
    }
    assert!(
        text.contains("[DONE]"),
        "the stream ends with its sentinel: {text}"
    );
    assert!(
        !text.contains("consumer_lagged"),
        "no frames were dropped in flight: {text}"
    );
    Ok(())
}

/// Two requests whose bodies are byte-identical, neither carrying an
/// `Idempotency-Key`, are two conversations - not one execution answered
/// twice. The old fallback hashed the body, so the second request joined the
/// first job: both consumers saw the same answer, and the joiner's stream
/// could end mid-flight. A completion is never a cached response; only a key
/// the client declared dedupes (the retry contract of task §5.2).
#[tokio::test]
async fn identical_bodies_without_a_key_run_two_executions() -> anyhow::Result<()> {
    let hub = spawn_hub("unique_bodies", test_config()).await?;
    let mut ws = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/chat/completions", hub.addr);
    let body = chat_body("the same question");
    let first = client
        .post(&url)
        .bearer_auth(API_KEY)
        .json(&body)
        .send()
        .await?;
    let second = client
        .post(&url)
        .bearer_auth(API_KEY)
        .json(&body)
        .send()
        .await?;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    let job_of = |r: &reqwest::Response| {
        r.headers()
            .get("x-nexus-job")
            .map(|v| v.to_str().unwrap_or_default().to_string())
    };
    let a = job_of(&first);
    let b = job_of(&second);
    assert_ne!(a, b, "two identical bodies without a key are two jobs");
    Ok(())
}

/// §6 at runtime: when the consumer's socket goes away the hub must tell the
/// worker. The core proves the decision (`t05_...`); this proves the native
/// runtime sends the `Detach` that triggers it — before the review pass it never
/// did, so a client that hung up left the GPU decoding for nobody.
#[tokio::test]
async fn consumer_disconnect_reaches_the_worker_as_cancel() -> anyhow::Result<()> {
    let hub = spawn_hub("disconnect_cancel", test_config()).await?;
    let mut ws = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("long one"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let assigned = next_frame_of(&mut ws, "assigned").await?;
    let job = match assigned {
        HubFrame::Assigned { job, .. } => job,
        other => return Err(anyhow::anyhow!("expected an Assigned, got {other:?}")),
    };

    // Dropping the response without draining the body is an SSE client hanging up.
    drop(response);
    let caps = WireCaps::default();
    // Any inbound frame gives the handler's pump a chance to notice the closed
    // body; the hub's own write is what the assertion waits for.
    let still_working = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Log,
        content_type: None,
        text: "still generating".to_string(),
        artifact: None,
    };
    ws.send(Message::Text(encode_line(&still_working, &caps)?.into()))
        .await?;

    let frame =
        tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut ws, "cancel")).await??;
    match frame {
        HubFrame::Cancel { job: cancelled, .. } => {
            assert_eq!(cancelled, JobId::new_unchecked(job.as_ref()))
        }
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }
    Ok(())
}

/// §9 tenancy at runtime: job-scoped reads are consumer reads, and the operational
/// endpoints stay open for a load balancer. The `/metrics` assertion also pins the
/// exposition shape the review pass fixed (one label set per key, gauges present).
#[tokio::test]
async fn job_reads_require_an_api_key_and_operational_routes_do_not() -> anyhow::Result<()> {
    let hub = spawn_hub("tenancy", test_config()).await?;
    let client = reqwest::Client::new();

    let anonymous = client
        .get(format!("http://{}/v1/jobs", hub.addr))
        .send()
        .await?;
    assert_eq!(
        anonymous.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "a job listing is a consumer read"
    );

    let scoped = client
        .get(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(scoped.status(), reqwest::StatusCode::OK);

    let unknown = client
        .get(format!("http://{}/v1/jobs/job-does-not-exist", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);

    let health = client
        .get(format!("http://{}/healthz", hub.addr))
        .send()
        .await?;
    assert!(
        health.status() == reqwest::StatusCode::OK
            || health.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "healthz answers without a key, degraded or not"
    );

    let metrics = client
        .get(format!("http://{}/metrics", hub.addr))
        .send()
        .await?;
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    let text = metrics.text().await?;
    assert!(
        !text.contains("}{"),
        "each metric key carries exactly one label set: {text}"
    );
    assert!(
        text.contains("nexus_queue_depth"),
        "gauges must be exported, not only counters: {text}"
    );
    Ok(())
}

// ------------------------------------------------------- the advertised model list (§7.1)

/// The `id`s `GET /v1/models` currently advertises, in document order.
async fn advertised_models(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<String>> {
    let response = client.get(url).send().await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "`/v1/models` is operational and open by the same rule as `/healthz`"
    );
    let document: serde_json::Value = response.json().await?;
    Ok(document["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row["id"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default())
}

/// Poll `/v1/models` until it advertises exactly `want`, within `budget`.
///
/// The document is actor-derived and the actor is a mailbox on a tick, so "eventually
/// this exact set" is the honest precondition; what each caller asserts afterwards is
/// exact, and the budget is what turns "later" into a testable claim.
async fn models_until(
    client: &reqwest::Client,
    url: &str,
    want: &[&str],
    budget: Duration,
) -> anyhow::Result<Vec<String>> {
    let until = tokio::time::Instant::now() + budget;
    let mut last = advertised_models(client, url).await?;
    loop {
        if last.iter().map(String::as_str).collect::<Vec<_>>() == want {
            return Ok(last);
        }
        if tokio::time::Instant::now() >= until {
            return Err(anyhow::anyhow!(
                "`/v1/models` never settled on {want:?} within {budget:?}; last list: {last:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = advertised_models(client, url).await?;
    }
}

/// Keep a supplier socket alive without giving it any work: it reads — so its peer's
/// automatic `Pong` replies go out — and it sends its `Ready` heartbeat on the
/// configured cadence. It returns how many `Ping` probes the hub sent while idle.
///
/// Reading matters more than it looks: tungstenite flushes the queued `Pong` while the
/// socket is serviced, so a socket nobody reads is a silent socket, and silence is what
/// the hub's keepalive is entitled to act on.
async fn idle_supplier_answering_the_hub(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    dur: Duration,
    heartbeat_ms: u64,
) -> anyhow::Result<usize> {
    let mut pings = 0_usize;
    let until = tokio::time::Instant::now() + dur;
    let mut since_beat = tokio::time::Instant::now();
    loop {
        let Some(remaining) = until.checked_duration_since(tokio::time::Instant::now()) else {
            return Ok(pings);
        };
        match tokio::time::timeout(Duration::from_millis(100).min(remaining), ws.next()).await {
            Ok(Some(Ok(Message::Ping(_)))) => pings += 1,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                return Err(anyhow::anyhow!("an idle supplier's socket broke: {err}"))
            }
            Ok(None) => return Err(anyhow::anyhow!("the hub closed an idle supplier's socket")),
            Err(_) => {}
        }
        if since_beat.elapsed() >= Duration::from_millis(heartbeat_ms) {
            ws.send(Message::Text(
                encode_line(&ready_frame(), &WireCaps::default())?.into(),
            ))
            .await?;
            since_beat = tokio::time::Instant::now();
        }
    }
}

/// §7.1 at runtime, in both directions: the advertised model list is **live**. A
/// model appears when a ready supplier declares it and disappears when that supplier's
/// socket dies — a consumer must never be handed a `model` the hub can no longer
/// serve, or its first request is a `503` for a model it was told was warm.
#[tokio::test]
async fn models_document_loses_a_model_when_its_last_supplier_leaves() -> anyhow::Result<()> {
    let hub = spawn_hub("models_live", test_config()).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/models", hub.addr);
    let settle = Duration::from_millis(3_000);

    // An idle hub advertises nothing.
    models_until(&client, &url, &[], settle).await?;

    // A ready supplier joining makes its model warm.
    let worker = dial_ready_but_idle_worker(hub.addr).await?;
    models_until(&client, &url, &["qwen38"], settle).await?;

    // The same socket dying takes the model back with it: no warm worker, no
    // advertised model (§7.1, `PoolActor::warm_models`).
    drop(worker);
    models_until(&client, &url, &[], settle).await?;

    // `/healthz` reads the same mirror, so it must agree model-for-model.
    let health = client
        .get(format!("http://{}/healthz", hub.addr))
        .send()
        .await?;
    let body: serde_json::Value = health.json().await?;
    assert_eq!(
        body["warm_models"],
        serde_json::json!([]),
        "`/healthz` and `/v1/models` must never disagree: {body}"
    );
    Ok(())
}

/// Two suppliers on one hub must both live. Identity is minted by the hub per
/// connection (UUIDv4), so a second socket can never take over the first: there is
/// no client-declared id to collide on, and no eviction path on the wire. This is
/// the regression test for the two-server molab case.
#[tokio::test]
async fn two_suppliers_get_distinct_hub_assigned_ids_and_both_stay_connected() -> anyhow::Result<()>
{
    let hub = spawn_hub("two_suppliers", test_config()).await?;

    let mut first = dial_worker(hub.addr, true).await?;
    let first_id = match next_frame_of(&mut first, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id,
        other => panic!("expected a welcome frame, got {other:?}"),
    };
    let mut second = dial_worker(hub.addr, true).await?;
    let second_id = match next_frame_of(&mut second, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id,
        other => panic!("expected a welcome frame, got {other:?}"),
    };

    assert_ne!(
        first_id, second_id,
        "the hub mints a fresh id per connection"
    );
    for id in [&first_id, &second_id] {
        assert_eq!(id.as_ref().len(), 36, "UUIDv4 shape: {id}");
        assert_eq!(id.as_ref().as_bytes()[14], b'4', "version nibble: {id}");
        assert!(
            matches!(id.as_ref().as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
            "RFC 4122 variant nibble: {id}"
        );
    }

    // Both are attached, live and ready at the same time — not one evicting the
    // other with a fresh id on every reconnect.
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/observe", hub.addr);
    let body = observe_until(&client, &url, "two live suppliers", |doc| {
        doc["workers"]
            .as_array()
            .is_some_and(|rows| rows.len() == 2)
    })
    .await?;
    let ids: BTreeSet<&str> = body["workers"]
        .as_array()
        .expect("workers array")
        .iter()
        .filter_map(|row| row["id"].as_str())
        .collect();
    assert_eq!(ids, BTreeSet::from([first_id.as_ref(), second_id.as_ref()]));
    assert_eq!(
        body["readiness"]["connected_workers"], 2,
        "both sockets are connected: {body}"
    );
    assert_eq!(
        body["readiness"]["ready_workers"], 2,
        "both reported Ready: {body}"
    );

    // Dropping one retires only its id; the survivor keeps serving.
    drop(second);
    let body = observe_until(&client, &url, "one remaining supplier", |doc| {
        doc["workers"]
            .as_array()
            .is_some_and(|rows| rows.len() == 1)
    })
    .await?;
    assert_eq!(
        body["workers"][0]["id"],
        first_id.as_ref(),
        "the survivor's id is untouched: {body}"
    );
    Ok(())
}

/// The same rule with the socket's death made invisible: the peer is leaked without
/// a close, so the hub sees a live TCP connection and **no inbound bytes at all** —
/// exactly what a supplier behind a Daytona preview link, a laptop that slept, or a
/// NAT that dropped its mapping looks like.
///
/// Nothing tells the actor this supplier is gone, and the actor's own missed-heartbeat
/// rule only pauses dispatch (`actor.rs`'s tick step 5), so before the hub probed its
/// own sockets this model stayed advertised for three heartbeats and the phantom
/// `WorkerRecord` stayed `link: live` forever after. The keepalive is what converts
/// silence into a disconnect, and a disconnect is what the actor can act on.
#[tokio::test]
async fn a_supplier_that_vanishes_without_closing_stops_being_advertised() -> anyhow::Result<()> {
    // 2 s heartbeat ⇒ a 500 ms probe and a 2.5 s silence verdict, both far inside the
    // 6 s the actor's heartbeat sweep needs, so a green run here can only be the
    // keepalive. The resume grace is shortened for the same reason: the row below has
    // to be *gone*, not merely demoted, and only a real disconnect deletes it.
    let mut cfg = test_config();
    cfg.limits.heartbeat_ms = 2_000;
    cfg.limits.resume_grace_ms = 300;
    cfg.validate().expect("keepalive config must validate");
    let hub = spawn_hub("models_silent", cfg).await?;
    let client = reqwest::Client::new();
    let models = format!("http://{}/v1/models", hub.addr);
    let observe = format!("http://{}/v1/observe", hub.addr);

    let worker = dial_ready_but_idle_worker(hub.addr).await?;
    models_until(&client, &models, &["qwen38"], Duration::from_millis(3_000)).await?;

    // Leak the socket instead of closing it: the hub keeps a connection it can write
    // to and never hears from again.
    std::mem::forget(worker);

    // The model stops being advertised, and `/v1/models` is not the only thing that
    // follows: the worker row itself disappears once the resume grace lapses, which no
    // amount of waiting achieves without the keepalive.
    models_until(&client, &models, &[], Duration::from_millis(4_500)).await?;
    let body = observe_until(
        &client,
        &observe,
        "the vanished supplier's row to go",
        |doc| doc["workers"].as_array().is_some_and(Vec::is_empty),
    )
    .await?;
    assert_eq!(
        body["readiness"]["ready_workers"], 0,
        "a supplier the hub has stopped hearing from is not ready capacity: {body}"
    );
    assert_eq!(
        body["readiness"]["warm_models"],
        serde_json::json!([]),
        "`/v1/observe` reads the same readiness as `/v1/models`: {body}"
    );
    Ok(())
}

/// The other half of the rule: the probe must never *itself* be a reason to drop a
/// supplier. This one does nothing but exist — a `Ready` heartbeat and the automatic
/// `Pong` its WebSocket peer sends — and stays warm across several silence windows.
///
/// It also pins the cadence from the supplier's side of the wire: the frames counted
/// below are the hub's probes, which is how a real runtime learns the hub is on the
/// other end of the socket at all.
#[tokio::test]
async fn a_supplier_that_only_heartbeats_and_pongs_stays_advertised() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.limits.heartbeat_ms = 2_000;
    cfg.validate().expect("keepalive config must validate");
    let hub = spawn_hub("models_idle", cfg).await?;
    let client = reqwest::Client::new();
    let models = format!("http://{}/v1/models", hub.addr);
    let observe = format!("http://{}/v1/observe", hub.addr);

    let mut worker = dial_ready_but_idle_worker(hub.addr).await?;
    models_until(&client, &models, &["qwen38"], Duration::from_millis(3_000)).await?;

    // Seven seconds: nearly three times the 2.5 s silence verdict this configuration
    // produces (a 500 ms probe), and long enough that a probe answered by a pong — and
    // nothing else — has had every chance to be mistaken for a dead peer.
    let pings =
        idle_supplier_answering_the_hub(&mut worker, Duration::from_millis(7_000), 2_000).await?;
    assert!(
        pings >= 5,
        "the hub probed an idle supplier only {pings} times in 7 s; \
         a 2 s heartbeat means a probe every 500 ms: {pings}"
    );

    let listed = models_until(&client, &models, &["qwen38"], Duration::from_millis(500)).await?;
    assert_eq!(
        listed,
        vec!["qwen38".to_string()],
        "a supplier that answers probes is warm capacity however idle it is"
    );

    // Same on `/v1/observe`: the row is still there and still `live`, so nothing about
    // the keepalive has half-killed the connection.
    let body = observe_until(
        &client,
        &observe,
        "the idle supplier to still hold its row",
        |doc| {
            doc["workers"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|row| row["link"] == serde_json::json!("live"))
            })
        },
    )
    .await?;
    assert_eq!(
        body["readiness"]["warm_models"],
        serde_json::json!(["qwen38"]),
        "an idle supplier the hub keeps probing is still advertised capacity: {body}"
    );

    // And it is still the *live* socket: a real close now retires it like any other.
    drop(worker);
    models_until(&client, &models, &[], Duration::from_millis(3_000)).await?;
    Ok(())
}

// -------------------------------------------------- the consumer WebSocket face (NEX-3)

/// The kind the consumer socket posts: a streaming LLM kind on the pool the test
/// worker offers, so the capability pre-check passes and the deltas are real.
const WS_KIND: &str = "llm.completion";

/// The `Sec-WebSocket-Key` every consumer dial in this file sends: the RFC 6455
/// sample value, fixed so the handshake (and the `accept` the client verifies) is
/// the same on every run.
const WS_HANDSHAKE_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

/// A consumer on `GET /v1/consumer`, authenticated with the test API key.
///
/// `connect_async` needs a request, not a URL, because the consumer face is keyed
/// on the `Authorization` header exactly as the HTTP surfaces are (spec §7: the
/// consumer direction presents the API key, never the pool token).
async fn dial_consumer(
    addr: std::net::SocketAddr,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    // `connect_async` takes a *request* here rather than a URL because the face is
    // keyed on the `Authorization` header exactly as the HTTP surfaces are (spec
    // §7: the consumer direction presents the API key, never the pool token).
    // It also means the handshake headers are ours to supply — tungstenite validates
    // that a caller-built request carries `Host`/`Connection`/`Upgrade`/
    // `Sec-WebSocket-Version`/`Sec-WebSocket-Key` and derives the expected `accept`
    // from the key, so a missing one fails client-side before the hub is reached.
    // The key is the RFC 6455 sample value, which keeps the handshake deterministic.
    let request = axum::http::Request::builder()
        .method("GET")
        .header("host", addr.to_string())
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", WS_HANDSHAKE_KEY)
        .header("authorization", format!("Bearer {API_KEY}"))
        .uri(format!("ws://{addr}/v1/consumer"))
        .body(())?;
    let (ws, response) = connect_async(request).await?;
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SWITCHING_PROTOCOLS,
        "a valid API key gets the upgrade, not a closed socket"
    );
    // Nothing is read here on purpose: the consumer dialect has no handshake frame
    // (NEX-2.2 keeps the two vocabularies disjoint — a consumer has no version or
    // attestation to offer), so the first message on the socket is always the
    // client's own, and the first reply is always a verdict.
    Ok(ws)
}

/// Sends one consumer frame (a JSON object per text message, NEX-2.2).
async fn send_consumer(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    frame: &serde_json::Value,
) -> anyhow::Result<()> {
    ws.send(Message::Text(frame.to_string().into())).await?;
    Ok(())
}

/// Posts one task on the consumer socket. `prompt` doubles as the task's identity:
/// it travels to the worker verbatim, which is how the assertions below can tell
/// one task's `Assigned` from another's.
async fn consumer_submit(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    prompt: &str,
    detach: bool,
) -> anyhow::Result<()> {
    send_consumer(
        ws,
        &serde_json::json!({
            "type": "submit",
            "kind": WS_KIND,
            "payload": {"prompt": prompt},
            "detach": detach,
        }),
    )
    .await
}

/// The next frame on the consumer socket, decoded into the hub→consumer dialect.
///
/// The five-second budget matches `next_frame_of`'s: a broken expectation should
/// fail with the frames it *did* get, not hang until the test harness gives up.
async fn next_consumer_frame(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> anyhow::Result<nexus_core::protocol::HubConsumerFrame> {
    let until = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(until);
    loop {
        let message = tokio::select! {
            _ = &mut until => return Err(anyhow::anyhow!("timed out waiting for a consumer frame")),
            message = ws.next() => message,
        };
        let text = match message {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await?;
                continue;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                return Err(anyhow::anyhow!("the hub closed the consumer socket"))
            }
            Some(Ok(_)) => continue,
        };
        // One JSON object per text message, so the first non-empty line *is* the frame.
        // A message with nothing in it (a bare pong payload would be one) is not a
        // frame, and the read just goes around again.
        let Some(line) = text.split('\n').find(|line| !line.trim().is_empty()) else {
            continue;
        };
        return Ok(nexus_core::protocol::decode_hub_consumer_frame(
            line.trim(),
            &WireCaps::default(),
        )?);
    }
}

/// NEX-3.1/3.2/3.3/3.4 at runtime: two tasks on one socket, each verdict followed
/// by the snapshot that opens its stream, each task's stream tagged with its own id,
/// and exactly one `end` per task.
#[tokio::test]
async fn one_consumer_socket_carries_two_tasks_and_answers_both_in_order() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_two_tasks", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    // Both submits are written before a single verdict is read: NEX-3.3 promises
    // verdicts in submission order for a client that does not wait, which is the
    // only ordering guarantee a pipelining client can rely on.
    consumer_submit(&mut consumer, "first", false).await?;
    consumer_submit(&mut consumer, "second", false).await?;

    let mut jobs = Vec::new();
    for expected in 0..2 {
        match next_consumer_frame(&mut consumer).await? {
            nexus_core::protocol::HubConsumerFrame::Accepted {
                job,
                kind,
                pool,
                detached,
                // The supplier pin is echoed back on this face as `accepted.supplier`.
                // Read here, not asserted: this test is about two tasks on one socket and
                // their ordering, and which of the two suppliers took which task is the
                // routing suite's business (`supplier_*` tests below).
                supplier: _,
            } => {
                assert_eq!(kind, WS_KIND, "the verdict echoes the kind it accepted");
                assert_eq!(pool.as_ref(), POOL);
                assert!(!detached, "a plain submit keeps the NEX-6.1 contract");
                jobs.push(job);
            }
            other => {
                return Err(anyhow::anyhow!(
                    "expected verdict #{expected}, got {other:?}"
                ))
            }
        }
        // The pair the hub owes for each accepted task is `accepted` then `meta`
        // (§5.4 item 5: the snapshot opens the stream). Reading it here rather than
        // skipping it is what proves `meta` sits ahead of every event byte of its own
        // task, and that the next task's verdict cannot jump the queue.
        match next_consumer_frame(&mut consumer).await? {
            nexus_core::protocol::HubConsumerFrame::Meta {
                job,
                kind,
                pool,
                state,
                attached_ms,
                ..
            } => {
                assert_eq!(job, jobs[expected], "the snapshot names the task it opens");
                assert_eq!(kind, WS_KIND);
                assert_eq!(pool, POOL);
                // The phase is whatever the actor's record held at attach time, which
                // races with the dispatch below, so this asserts presence, not value.
                assert!(state.is_some(), "the snapshot carries the task's phase");
                assert!(
                    attached_ms.is_some(),
                    "the snapshot carries the attach time"
                );
            }
            other => {
                return Err(anyhow::anyhow!(
                    "expected the snapshot that opens task #{expected}, got {other:?}"
                ));
            }
        }
    }
    assert_ne!(jobs[0], jobs[1], "two submissions are two tasks");

    // The worker sees the tasks in arrival order and answers the first one to
    // completion before touching the second, so the per-task grouping the consumer
    // is about to observe is forced by the wire, not incidental.
    let mut slots = Vec::new();
    for index in 0..2 {
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned { job, .. } = assigned.clone() else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        let marker = if index == 0 { "first" } else { "second" };
        assert!(
            serde_json::to_string(&assigned)?.contains(marker),
            "task {} must carry its own payload: {assigned:?}",
            index + 1
        );
        slots.push(job);
    }
    assert_eq!(slots, jobs, "the hub dispatches in the order it accepted");

    let caps = WireCaps::default();
    for (slot, (a, b)) in slots.iter().zip([("one", "two"), ("three", "four")]) {
        for (seq, text) in [(1_u64, a), (2, b)] {
            let delta = WorkerFrame::Frame {
                job: slot.clone(),
                seq,
                kind: FrameKind::Delta,
                content_type: None,
                text: text.to_string(),
                artifact: None,
            };
            worker
                .send(Message::Text(encode_line(&delta, &caps)?.into()))
                .await?;
        }
        let done = WorkerFrame::Done {
            job: slot.clone(),
            seq: 3,
            finish: FinishKind::Stop,
            usage: Some(serde_json::json!({"completion_tokens": 2})),
            error: None,
        };
        worker
            .send(Message::Text(encode_line(&done, &caps)?.into()))
            .await?;
    }

    // The exact stream the consumer is owed: per task, two deltas, the finish, and
    // one `end` naming the highest seq delivered. Nothing else on the socket, so a
    // cross-streamed delta (NEX-6.5) or a duplicated terminal (NEX-3.4) shows up
    // here as the first mismatching element.
    let mut seen = Vec::new();
    for _ in 0..8 {
        seen.push(next_consumer_frame(&mut consumer).await?);
    }
    let slot_of = |job: &JobId| -> &'static str {
        if job == &slots[0] {
            "a"
        } else {
            "b"
        }
    };
    let mut actual = Vec::new();
    for frame in &seen {
        actual.push(match frame {
            nexus_core::protocol::HubConsumerFrame::Delta { job, seq, text } => {
                format!("{} delta {seq} {text}", slot_of(job))
            }
            nexus_core::protocol::HubConsumerFrame::Finish {
                job,
                seq,
                finish,
                usage,
            } => {
                assert_eq!(*finish, FinishKind::Stop);
                assert_eq!(
                    usage.as_ref(),
                    Some(&serde_json::json!({"completion_tokens": 2}))
                );
                format!("{} finish {seq}", slot_of(job))
            }
            nexus_core::protocol::HubConsumerFrame::End {
                job,
                reason,
                through_seq,
            } => {
                assert_eq!(*reason, nexus_core::protocol::EndReason::Finished);
                assert_eq!(
                    *through_seq,
                    Some(3),
                    "the cursor a reconnecting consumer needs"
                );
                format!("{} end", slot_of(job))
            }
            other => return Err(anyhow::anyhow!("unexpected frame on the socket: {other:?}")),
        });
    }
    assert_eq!(
        actual,
        [
            "a delta 1 one",
            "a delta 2 two",
            "a finish 3",
            "a end",
            "b delta 1 three",
            "b delta 2 four",
            "b finish 3",
            "b end",
        ],
        "one socket, two tagged streams, one end each"
    );
    Ok(())
}

/// NEX-3.3 at runtime, the refusal half: a task refused for an oversized prompt is
/// reported with the HTTP surface's code, tied to its own task id. Nothing that
/// streams precedes it — the only frame ahead of this verdict is the snapshot that
/// opened the accepted task's stream, never an event byte.
#[tokio::test]
async fn a_refusal_reaches_the_consumer_before_any_event_bytes() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // A 64-byte tenant payload cap: the first submission fits, the second is
    // refused in `on_submit` at the payload check (`actor.rs` `payload_too_large`),
    // before the queue and before the capability pre-check — which is what makes
    // the frame order below an exact statement of §5.4 rather than a race.
    cfg.limits.tenant_max_payload_bytes = 64;
    let hub = spawn_hub("ws_refusal", cfg).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "allowed", false).await?;
    // The refused prompt: hundreds of bytes against the 64-byte cap.
    let oversized = "x".repeat(400);
    send_consumer(
        &mut consumer,
        &serde_json::json!({
            "type": "submit",
            "kind": WS_KIND,
            "payload": {"prompt": oversized},
            "detach": false,
        }),
    )
    .await?;
    // The worker has not been given anything to answer yet, so no event frame can
    // exist on this socket at all: whatever arrives in the next two reads is a
    // verdict, and the assertion below is what NEX-3.3 is about.
    let first = next_consumer_frame(&mut consumer).await?;
    let accepted = match &first {
        nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } => job.clone(),
        other => return Err(anyhow::anyhow!("the first task is accepted, got {other:?}")),
    };
    // The snapshot that opens the first task's stream comes next, still ahead of the
    // second task's verdict: §5.4 item 5 puts `meta` first in the stream it opens, and
    // a submit's verdict is the first frame of its own stream.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job, .. } => {
            assert_eq!(job, accepted, "the snapshot belongs to the task it opens");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    let second = next_consumer_frame(&mut consumer).await?;

    // The wire shape: a refusal names a task the client never chose an id for,
    // carries the HTTP surface's code, and — because a size refusal is not
    // retryable — carries no retry hint at all.
    let value = serde_json::to_value(&second)?;
    let mut object = value.as_object().expect("a frame is a JSON object").clone();
    let job = object
        .remove("job")
        .expect("a refusal names the task it refuses");
    assert!(
        job.as_str().is_some_and(|id| !id.is_empty()),
        "the refusal is tied to a task id: {job}"
    );
    assert_eq!(object.get("type"), Some(&serde_json::json!("rejected")));
    assert_eq!(object.get("status"), Some(&serde_json::json!(413)));
    assert_eq!(
        object.get("code"),
        Some(&serde_json::json!("payload_too_large"))
    );
    assert!(
        !object.contains_key("retry_after_secs"),
        "a size refusal is not retryable, so it carries no retry hint: {object:?}"
    );

    // Nothing else has been written on the socket: the refused task produced no
    // stream, and the accepted one has not been answered yet.
    drop(consumer);
    Ok(())
}

/// NEX-6.1/NEX-6.3 at runtime on the consumer socket: dropping the socket must
/// reach the worker as a `Cancel` and delete the task, with no goodbye frame sent.
#[tokio::test]
async fn dropping_a_consumer_socket_cancels_and_deletes_its_task() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_hangup", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let job = {
        let mut consumer = dial_consumer(hub.addr).await?;
        consumer_submit(&mut consumer, "orphan me", false).await?;
        let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
            next_consumer_frame(&mut consumer).await?
        else {
            return Err(anyhow::anyhow!("expected an accepted verdict"));
        };
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned {
            job: assigned_job, ..
        } = assigned
        else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        assert_eq!(assigned_job, job);
        // The hangup itself: no `detach`, no `cancel`, no close frame — the socket
        // is the subscription, and dropping it is the whole event (NEX-6.4).
        drop(consumer);
        job
    };

    let cancel = tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut worker, "cancel"))
        .await
        .map_err(|_| anyhow::anyhow!("the worker never heard about the consumer leaving"))??;
    match cancel {
        HubFrame::Cancel { job: cancelled, .. } => assert_eq!(cancelled, job),
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }

    // NEX-6.3's "deleted", not "retained": the record is gone, so a later read is a
    // `404` rather than an orphan's answer.
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND,
        "an orphaned task is deleted, not kept for a stranger"
    );
    Ok(())
}

/// NEX-6.6 at runtime: `detach:true` is the declared-detached opt-in. The stream
/// closes on request with its own reason, the socket may then go away, and the task
/// keeps running for a consumer that polls later.
#[tokio::test]
async fn an_explicit_detach_ends_the_stream_and_keeps_the_task_alive() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_detach", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "keep running", true).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, detached, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    assert!(
        detached,
        "the accepted verdict echoes the contract it granted"
    );
    // The snapshot that opens this stream (§5.4 item 5), ahead of the first delta.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job: snapshot, .. } => {
            assert_eq!(snapshot, job, "the snapshot names the task it opens");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    // One delta, so the `end` frame has a cursor to report.
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "still working".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(
            encode_line(&delta, &WireCaps::default())?.into(),
        ))
        .await?;
    assert_eq!(
        next_consumer_frame(&mut consumer).await?,
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "still working".to_string(),
        },
        "the delta arrives before the detach is even sent"
    );

    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "detach", "job": job.to_string()}),
    )
    .await?;
    assert_eq!(
        next_consumer_frame(&mut consumer).await?,
        nexus_core::protocol::HubConsumerFrame::End {
            job: job.clone(),
            reason: nexus_core::protocol::EndReason::Detached,
            through_seq: Some(1),
        },
        "§5.4: an explicit detach closes the stream and names its own reason"
    );

    // The socket goes away while the task is still live and now has no consumer.
    drop(consumer);
    let quiet = tokio::time::timeout(
        // Longer than `no_worker_grace_ms` plus the actor's 1 s tick: a hub that
        // treated this as an orphan would have cancelled by now.
        Duration::from_millis(1_400),
        next_frame_of(&mut worker, "cancel"),
    )
    .await;
    // `as_ref` so the value survives into the failure message: what arrived instead of
    // a `Cancel` is the evidence.
    assert!(
        quiet.as_ref().is_err(),
        "a detached task keeps running (NEX-6.6), got {quiet:?}"
    );

    // And the task is still there to be polled — which is the whole point of the
    // opt-in, and what NEX-6.1 would have destroyed.
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let view = response.json::<serde_json::Value>().await?;
    // The job-status body names the task `job_id` (`adapters.rs:550`), which is the
    // same key `POST /v1/jobs` answers with.
    assert_eq!(view["job_id"].as_str(), Some(job.as_ref()));
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "the detached task is watched by nobody, which is what bounds its lifetime"
    );
    Ok(())
}

/// NEX-3's `cancel` at runtime, and the retention half of NEX-6.3: cancelling on
/// purpose tells the worker, closes the stream, and **keeps** the record as a terminal
/// one — the opposite of hanging up, which deletes it outright.
#[tokio::test]
async fn an_explicit_cancel_closes_the_stream_and_keeps_the_record() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_cancel", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "cancel me", false).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    // One delta first, so the stream has a cursor for its `end` to report, and so the
    // task is past the zero-bytes state that would make a retry legal.
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "as far as it goes".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(
            encode_line(&delta, &WireCaps::default())?.into(),
        ))
        .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "as far as it goes".to_string(),
        },
    )
    .await?;

    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "cancel", "job": job.to_string()}),
    )
    .await?;

    // The supplier is told, with the reason the HTTP `DELETE /v1/jobs/{id}` sends: one
    // cause for both surfaces, so a worker cannot tell a socket loss from a decision.
    match next_frame_of(&mut worker, "cancel").await? {
        HubFrame::Cancel {
            job: cancelled,
            reason,
        } => {
            assert_eq!(cancelled, job);
            assert_eq!(
                reason,
                nexus_core::protocol::CancelReason::ConsumerGone,
                "a consumer cancel is the same cause from either face"
            );
        }
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }

    // The stream, closed in band. The actor's `on_cancel` relays no terminal event of
    // its own (`actor.rs:1449-1480`), so without this frame the consumer would sit on an
    // open stream behind a task that already stopped existing.
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Closed,
            Some(1),
        ),
    )
    .await?;

    // And the record is still there to be read: a deliberate cancellation is a consumer's
    // decision about a task, not a lost connection, so it keeps normal terminal retention
    // (NEX-6.3) instead of being deleted like an orphan.
    let client = reqwest::Client::new();
    let status = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .status();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a cancelled task is retained, not deleted"
    );
    Ok(())
}

/// Reads the next consumer frame and requires it to be exactly `want`.
///
/// Whole-frame equality, not a per-field probe: a stream that is one frame short or one
/// frame long is the bug these tests exist to catch.
async fn expect_consumer_frame(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    want: nexus_core::protocol::HubConsumerFrame,
) -> anyhow::Result<()> {
    let got = next_consumer_frame(ws).await?;
    assert_eq!(got, want, "the socket owes this exact frame");
    Ok(())
}

/// Reads the next consumer frame and requires it to be this task's opening snapshot,
/// returning nothing: the tests that are about the snapshot's contents assert its
/// fields, the others only need it out of the way.
async fn expect_consumer_meta(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    job: &JobId,
) -> anyhow::Result<()> {
    match next_consumer_frame(ws).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job: meta, .. } => {
            assert_eq!(meta, *job, "the snapshot names the task it opens");
            Ok(())
        }
        other => Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
}

/// NEX-3.2/3.4's join half at runtime: a task whose answer already ran out can be
/// followed again on the same socket, and the replay is a complete stream of its own —
/// verdict, snapshot, the buffered answer, one `end`.
///
/// This is what a consumer that dropped and re-attached at `end.through_seq` needs. The
/// task is still inside the actor's retention, so the replay ring is the source and the
/// hub adds nothing to it; the second stream being byte-for-byte the first one's answer
/// is the claim.
#[tokio::test]
async fn a_finished_task_can_be_followed_again_on_the_same_socket() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_reattach", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "read me twice", false).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "twice".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let usage = serde_json::json!({"completion_tokens": 1});
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(usage.clone()),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    let answer = [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "twice".to_string(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ];
    for want in answer.clone() {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // The join: same socket, same task, from the beginning of the ring.
    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "attach", "job": job.to_string(), "from_seq": 0}),
    )
    .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: WS_KIND.to_string(),
            pool: nexus_core::ids::PoolId::new_unchecked(POOL),
            // An attach cannot change the contract the task was submitted under.
            detached: false,
            // And no supplier is named: the task is **Finished** (the `meta` below proves
            // the phase), its lease is gone, and a join of a terminal task has nothing to
            // report. `None` here is the "nothing to name" answer, never a refusal.
            supplier: None,
        },
    )
    .await?;
    // The snapshot says what the task is now, which is the only thing a joiner can
    // learn that the replay itself does not already say.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta {
            job: meta,
            state,
            attached_ms,
            ..
        } => {
            assert_eq!(meta, job);
            assert_eq!(
                state.as_deref(),
                Some("Finished"),
                "the phase is the actor's, not a guess"
            );
            assert!(attached_ms.is_some(), "and it carries the attach time");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    // Then the answer, identical to the first time, and its own single `end`.
    for want in answer {
        expect_consumer_frame(&mut consumer, want).await?;
    }
    Ok(())
}

/// Consumer sockets in these tests: the stream type `dial_consumer` hands back, spelled
/// once so the helpers below can take one by mutable reference without repeating the
/// twenty-word type each time.
type ConsumerSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// One whole task lifetime on an already-connected consumer socket and its already-ready
/// worker, and the fan-out row the hub created for it.
///
/// The relay is returned rather than looked up later because the row is removed when the
/// job goes terminal (`Effect::Terminal` hands it back); the `Arc` the caller keeps is a
/// handle on the same object the pump registered its queue in, which is what makes
/// "no subscriber outlived its pump" assertable after the fact instead of inferred.
///
/// The worker is re-armed with a `Ready` and a one-job `Claim` at the end, which is what a
/// real supplier does between jobs and what keeps the next submit dispatchable.
async fn run_whole_task(
    consumer: &mut ConsumerSocket,
    worker: &mut ConsumerSocket,
    handle: &crate::state::PoolHandle,
    prompt: &str,
) -> anyhow::Result<(JobId, Arc<crate::state::JobRelay>)> {
    consumer_submit(consumer, prompt, false).await?;
    let job = match next_consumer_frame(consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job,
            kind,
            pool,
            detached,
            // Read, not asserted: this helper serves the bookkeeping tests, which are
            // about what a task leaves behind, not about which supplier served it. The
            // supplier assertions live in the `supplier_*` tests, which read the id off
            // the relay row this helper hands back.
            supplier: _,
        } => {
            assert_eq!(kind, WS_KIND, "the verdict echoes the kind it accepted");
            assert_eq!(pool.as_ref(), POOL);
            assert!(!detached, "a plain submit keeps the NEX-6.1 contract");
            job
        }
        other => {
            return Err(anyhow::anyhow!(
                "expected an accepted verdict, got {other:?}"
            ))
        }
    };
    // The fan-out row has to exist at the verdict: `submit` registers the relay, the gate and
    // the replay queue *before* the actor hears about the job, so a lookup here cannot race.
    let relay = handle
        .lookup_relay(&job)
        .await
        .expect("an accepted task owns its fan-out row");
    expect_consumer_meta(consumer, &job).await?;

    let assigned = next_frame_of(worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job, "the worker is handed this task");

    let caps = WireCaps::default();
    // The prompt is the delta text, so a delta that arrived on the wrong task's stream shows
    // up below as a mismatched frame rather than passing unnoticed.
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: prompt.to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let usage = serde_json::json!({"completion_tokens": 1});
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(usage.clone()),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    // The exact stream this task owes: its delta, its finish, one `end` naming the highest
    // seq delivered — per task, in order, with nothing from a neighbouring task in between.
    expect_consumer_frame(
        consumer,
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: prompt.to_string(),
        },
    )
    .await?;
    expect_consumer_frame(
        consumer,
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
    )
    .await?;
    expect_consumer_frame(
        consumer,
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    )
    .await?;

    worker
        .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
        .await?;
    worker
        .send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(1) }, &caps)?.into(),
        ))
        .await?;
    Ok((job, relay))
}

/// Waits (bounded) for a hub-side bookkeeping condition to settle.
///
/// The teardowns these tests assert on run on tasks the test does not own: a pump ends when
/// its final frame is written, and its slot in the job's fan-out goes with its `Drop`. "It
/// will be true in a moment" is therefore the honest precondition — the same shape
/// `observe_until` documents for the observer's three independent producers — and what the
/// caller asserts afterwards is an exact value. This helper cannot soften an assertion; it
/// only takes the scheduler's timing out of it.
async fn bookkeeping_until(what: &str, settled: impl Fn() -> bool) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !settled() {
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!("timed out waiting for {what}"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// One long-lived consumer socket, twenty-one task lifetimes, and no per-task state left on
/// the hub when each of those streams ends.
///
/// The production shape this pins is one browser tab that posts a task a minute and never
/// closes its socket. Every task it posts installs three pieces of hub-side bookkeeping: a
/// queue in the job's fan-out, an entry in this session's watch map, and a line in the
/// observe registry. All three used to be released only when the **socket** closed, so a
/// connection whose live stream count was normally zero carried a reservation for every task
/// it had ever watched — measured at roughly 48 kB of retained bytes per accepted task, which
/// is the relay and its queues rather than the frame text. Twenty tasks below is twenty
/// chances to notice, and the twenty-first is the guard against the opposite failure:
/// bookkeeping reclaimed by breaking the stream.
#[tokio::test]
async fn one_long_lived_consumer_socket_leaves_no_per_task_state_behind() -> anyhow::Result<()> {
    const TASKS: usize = 20;
    let hub = spawn_hub("ws_long_lived", test_config()).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    // One socket, `TASKS` complete lifetimes, and no `detach` between them — the documented
    // way a streaming client behaves, and exactly the case the old bookkeeping could not exit.
    let mut streams = Vec::with_capacity(TASKS + 1);
    for index in 0..TASKS {
        let prompt = format!("task-{index:02}");
        streams.push(run_whole_task(&mut consumer, &mut worker, handle, &prompt).await?);
    }
    let (first_job, _) = streams[0].clone();

    // The socket is still a working socket: a twenty-first task runs end to end over the
    // connection that has just finished twenty.
    let (last_job, last_relay) =
        run_whole_task(&mut consumer, &mut worker, handle, "twenty-first").await?;
    streams.push((last_job, last_relay));

    // Every fan-out this socket registered is empty again, and `subscriber_count` is the map
    // the leak lived in: one stranded queue per task was the per-task resident cost, and the
    // pump's `Drop` (abort included) is the only thing that retires it. The wait is bounded
    // and the value asserted after it is exact.
    let relays: Vec<Arc<crate::state::JobRelay>> =
        streams.iter().map(|(_, relay)| Arc::clone(relay)).collect();
    bookkeeping_until(
        "every consumer queue this socket registered to be retired",
        || relays.iter().all(|relay| relay.subscriber_count() == 0),
    )
    .await?;
    for (index, relay) in relays.iter().enumerate() {
        assert_eq!(
            relay.subscriber_count(),
            0,
            "task #{index} left a consumer queue behind on a fan-out that has ended"
        );
    }

    // No relay row survives either. `Effect::Terminal` hands each row back as its job goes
    // terminal, the dispatcher arms cannot put one back, and the actor's tick sweep is the
    // backstop for a row a face registered after that prune — so after twenty-one finished
    // tasks this pool holds none at all.
    assert_eq!(
        handle.followed_jobs().await,
        Vec::<JobId>::new(),
        "twenty-one finished tasks left no relay row on this pool"
    );

    // The observe registry, which is what `/v1/observe` renders as
    // `consumer_connections[].attached_jobs`: this one socket, listed against no task. The
    // pump takes its own line out as its stream ends, which is why a tab that posted twenty
    // tasks is not reported as following twenty jobs.
    let connections = hub.hub.consumer_connections();
    assert_eq!(
        connections.len(),
        1,
        "one live socket, and no ghost entry for a stream that ended"
    );
    assert_eq!(
        connections[0].attached_jobs.len(),
        0,
        "the socket is listed against {} tasks after twenty-one of them",
        connections[0].attached_jobs.len()
    );

    // The protocol witness that the session's own watch map went back with the stream: a
    // `detach` for a task whose answer already ran out is answered `error{not_attached}`,
    // which is what the actor also says — its consumer set for that task has been empty since
    // the pump detached it on the way out. Before the fix this found a stale entry and
    // reported `detached` for a stream that had ended long ago.
    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "detach", "job": first_job.to_string()}),
    )
    .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Error {
            job: Some(first_job.clone()),
            code: "not_attached".to_string(),
            message: "this connection does not watch that job".to_string(),
            retryable: false,
            through_seq: None,
        },
    )
    .await?;
    // And the refusal to detach a finished task is not the socket's last word: the stream that
    // follows it proves the session is still serving this connection normally.
    run_whole_task(&mut consumer, &mut worker, handle, "after-the-refusal").await?;
    Ok(())
}

/// A consumer that vanishes without a `Close` is reaped by the hub's own probe, and
/// everything that socket was holding goes with it.
///
/// `limits.consumer_keepalive_ms` is the knob (and `NEXUS_CONSUMER_KEEPALIVE_MS` the way to
/// turn it off). Before it, the read loop could only end on a client FIN, a broken frame, or
/// a failed write — all three of which need the peer, or something answering for it, to speak
/// first. A tab killed behind a proxy, a laptop that slept, or an edge that dropped the path
/// quietly produced none of those, and the session then held its writer task, one pump per
/// task it had ever watched, one subscriber per relay, and the actor-side consumer slot for
/// the life of the process.
///
/// The ruler is deliberately measured against a **producing** stream: the task below keeps
/// relaying deltas while the socket says nothing at all. Business progress is not liveness,
/// and a hub that confused the two would cancel work a patient client was still waiting on.
#[tokio::test]
async fn a_silent_consumer_socket_is_reaped_and_releases_what_it_held() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // The ruler at test scale: `consumer_keepalive_windows` clamps the probe to its 250 ms
    // floor and then demands `max(3 × probe, ruler + probe)` of total silence, so the verdict
    // lands at 750 ms — three consecutive unanswered probes, not one unlucky tick.
    cfg.limits.consumer_keepalive_ms = 250;
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("ws_silent", cfg).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let mut consumer = dial_consumer(hub.addr).await?;
    consumer_submit(&mut consumer, "quiet from here", false).await?;
    let job = match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } => job,
        other => {
            return Err(anyhow::anyhow!(
                "expected an accepted verdict, got {other:?}"
            ))
        }
    };
    expect_consumer_meta(&mut consumer, &job).await?;
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    // The task is producing while the socket is mute. Five deltas, none of them read: a stream
    // that is delivering is exactly what a deadline on business progress would have killed.
    let caps = WireCaps::default();
    for seq in 1_u64..=5 {
        let delta = WorkerFrame::Frame {
            job: job.clone(),
            seq,
            kind: FrameKind::Delta,
            content_type: None,
            text: format!("delta-{seq}"),
            artifact: None,
        };
        worker
            .send(Message::Text(encode_line(&delta, &caps)?.into()))
            .await?;
    }

    // The silence itself: nothing is read from this socket again, so the peer's own runtime
    // never answers the hub's probes, and the only thing the hub can see is that it has heard
    // nothing at all.
    let client = reqwest::Client::new();
    let document = observe_until(
        &client,
        &format!("http://{}/v1/observe", hub.addr),
        "the silent consumer socket is gone",
        |document| {
            document["consumer_connections"]
                .as_array()
                .is_some_and(Vec::is_empty)
        },
    )
    .await?;
    // The verdict is about one socket, not about the hub: the supplier on the other side of
    // this task is untouched by it.
    assert_eq!(
        document["workers"].as_array().map_or(0, Vec::len),
        1,
        "a consumer liveness verdict must not disturb the worker: {document}"
    );

    // The hub hung up, and the client can read that it did. Ahead of the close sit the frames
    // this socket never got round to reading — the task's own deltas, and the probes its
    // runtime never answered — so the reads below skip them, answering the pings a live peer
    // would answer, and stop at the hang-up itself.
    let hung_up = tokio::time::timeout(Duration::from_secs(5), async {
        let mut read = consumer.next().await;
        loop {
            match read {
                Some(Ok(Message::Ping(payload))) => {
                    let _ = consumer.send(Message::Pong(payload)).await;
                }
                // The task's own stream bytes, still queued from before the verdict: the
                // socket was alive and producing when the hub stopped hearing it, which is the
                // whole point of the ruler being measured in pings.
                Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Frame(_))) => {}
                _ => return read,
            }
            read = consumer.next().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("the hub never closed the socket it had stopped hearing"))?;
    assert!(
        matches!(hung_up, Some(Ok(Message::Close(_))) | Some(Err(_)) | None),
        "the reaped socket reports its own close, got {hung_up:?}"
    );

    // The teardown reported to the actor on the way out, exactly as a client FIN does: an
    // attached task whose last consumer has gone is cancelled (NEX-6.1/NEX-6.3), so the GPU
    // work stops for a client that is not there.
    let cancel = next_frame_of(&mut worker, "cancel").await?;
    match cancel {
        HubFrame::Cancel { job: cancelled, .. } => assert_eq!(cancelled, job),
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }

    // And every byte that socket was holding is gone: its fan-out row, and the task itself,
    // which NEX-6.3 deletes rather than retaining. A `404` is the observable form of
    // "the silent connection released the state it owned".
    assert_eq!(
        handle.followed_jobs().await,
        Vec::<JobId>::new(),
        "the reaped socket left a relay row behind"
    );
    let read = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(
        read.status(),
        reqwest::StatusCode::NOT_FOUND,
        "an abandoned task is deleted, not kept for the hub's whole life"
    );
    assert!(
        hub.hub.job_index.lock().await.is_empty(),
        "the routing index kept an entry for a job nobody has"
    );
    Ok(())
}

/// The other half of the same knob: a probe that is **answered** is not a reason to hang up.
///
/// A consumer that sends no frame of its own — no submit, no detach, nothing but the `Pong`
/// its WebSocket runtime replies with — is an ordinary idle tab, and the supplier face already
/// promises the same thing for a supplier that only heartbeats
/// (`a_supplier_that_only_heartbeats_and_pongs_stays_advertised`). Without that rule the probe
/// would turn an idle client into a cancelled task, which is why the ruler measures pings and
/// never job progress.
#[tokio::test]
async fn a_consumer_that_only_answers_pings_stays_attached() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // Same ruler as the reap test, so 2 s of answered probes is about eight probe periods and
    // roughly 2.7 verdict windows: enough to have been reaped twice over if a `Pong` did not
    // count as liveness.
    cfg.limits.consumer_keepalive_ms = 250;
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("ws_pongs", cfg).await?;
    let mut consumer = dial_consumer(hub.addr).await?;

    // Nothing is submitted. The socket's entire traffic is the hub's probe and this test's
    // answer, which is what a live WebSocket runtime produces on its application's behalf
    // without any application code involved.
    let mut probes = 0_usize;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(2_000);
    while tokio::time::Instant::now() < deadline {
        let message = tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(50)) => continue,
            message = consumer.next() => message,
        };
        match message {
            Some(Ok(Message::Ping(payload))) => {
                probes += 1;
                consumer.send(Message::Pong(payload)).await?;
            }
            Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
            Some(Ok(Message::Text(text))) => {
                return Err(anyhow::anyhow!(
                    "the hub spoke on an idle consumer socket, expected only probes: {text}"
                ))
            }
            Some(Ok(other)) => {
                return Err(anyhow::anyhow!(
                    "unexpected frame on an idle socket: {other:?}"
                ))
            }
            Some(Err(err)) => {
                return Err(anyhow::anyhow!(
                    "the hub hung up on a socket that answered every probe: {err}"
                ))
            }
            None => {
                return Err(anyhow::anyhow!(
                    "the hub closed a socket that answered every probe, with no close frame"
                ))
            }
        }
    }
    // The probe really was firing — otherwise "it stayed attached" would be a vacuous pass. A
    // count of a peer-driven probe rate is the one number in this file that cannot be exact,
    // because it is a rate rather than a value: four is comfortably below the eight periods
    // two seconds of 250 ms probing produces.
    assert!(
        probes >= 4,
        "the hub probed {probes} times in 2 s at a 250 ms ruler; the probe never fired"
    );
    // Exactly one live connection, listed against nothing — the idle tab is still a customer.
    let connections = hub.hub.consumer_connections();
    assert_eq!(
        connections.len(),
        1,
        "a socket that answered every probe was reaped anyway"
    );
    assert_eq!(
        connections[0].attached_jobs.len(),
        0,
        "an idle socket was listed against a task it never posted"
    );
    Ok(())
}

/// NEX-7.2 on the HTTP faces: a finished streaming body takes its consumer queue with it.
///
/// The consumer WebSocket is not the only thing that registers a queue on a job's fan-out.
/// Every `POST /v1/chat/completions` body, every non-streaming aggregate response and every
/// `GET /v1/jobs/{id}/events` follow is built on the same `Follow` and therefore the same
/// `state::Subscriber`. A client making one completion a minute over HTTP is the identical
/// accumulating shape to one socket posting one task a minute, so the guarantee is pinned for
/// both surfaces rather than for whichever one was convenient to test.
#[tokio::test]
async fn a_finished_sse_body_leaves_no_subscriber_on_its_relay() -> anyhow::Result<()> {
    let hub = spawn_hub("sse_retire", test_config()).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("stream me whole"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };

    // Held for the assertion at the end: `submit` registers the fan-out before the actor hears
    // about the job, so this lookup cannot race the verdict that has already been written as
    // the response status. The `Arc` outlives the row's removal, which is the point — the
    // queue in it is what must not outlive the pump.
    let relay = handle
        .lookup_relay(&job)
        .await
        .expect("a streaming completion owns its fan-out row");

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "sse-answer".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    // Draining the body to its end is the precondition, not the claim: what the bytes look
    // like is pinned by `worker_relay_reaches_the_consumer_verbatim_over_sse`, and here the
    // body only has to have finished, because the pump's exit is what the next assertion is
    // about.
    let body = response.text().await?;
    assert!(
        body.contains("sse-answer"),
        "the streamed answer did not arrive whole: {body}"
    );

    bookkeeping_until(
        "the SSE pump's consumer queue to be retired with its body",
        || relay.subscriber_count() == 0,
    )
    .await?;
    assert_eq!(
        relay.subscriber_count(),
        0,
        "a finished SSE body left its subscriber registered on the fan-out"
    );
    assert_eq!(
        handle.followed_jobs().await,
        Vec::<JobId>::new(),
        "and left the relay row itself behind"
    );
    Ok(())
}

/// NEX-6.6's other half at runtime: a task posted on the async job API is watched by
/// nobody from the moment the `202` is written, so its subscriber-less window runs and
/// the record stops existing.
///
/// That is only true because `create_job` hands the actor a `Detach` for the consumer
/// `ActorMsg::Submit` registers on the caller's behalf (`consumer.rs:585-604`). Left in
/// `record.consumers`, that phantom consumer made an async task look watched forever,
/// and neither NEX-6.6's window nor NEX-6.3's deletion could ever fire for it. The test
/// ends by posting the **same idempotency key** again: because the actor's
/// `Effect::JobForgotten` prunes the hub's mirror along with the record, that retry is new
/// work rather than a replay of a task that no longer exists.
#[tokio::test]
async fn a_task_posted_without_a_consumer_is_deleted_when_its_window_closes() -> anyhow::Result<()>
{
    let mut cfg = test_config();
    // The window itself, shortened for the test. Deleting is a tick step
    // (`actor.rs:3232-3257`), so the sweep lands on the actor's 1 s tick rather than
    // at the exact millisecond the TTL lapses.
    cfg.limits.detached_orphan_ttl_ms = 300;
    let hub = spawn_hub("orphan_window", cfg).await?;
    let client = reqwest::Client::new();

    // Ready, so the task is admitted into `Queued` and the `202` is immediate; never
    // claiming, so it stays there. Held open for the whole test (hence `_`): the
    // capability pre-check reads live workers, so hanging up here would make the task
    // unserviceable rather than merely unwatched.
    let _idle_worker = dial_ready_but_idle_worker(hub.addr).await?;
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        // Named so the retry at the end of the test is provably the *same* key: the
        // content-hash fallback would make it a different one by construction.
        .header("Idempotency-Key", "orphan-window-key")
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "nobody is watching"},
            // NEX-6.6: the lifetime has to be stated, and this route only accepts the
            // detached reading (`the_async_job_route_requires_detach_to_be_stated`).
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "a detached post is accepted: {text}"
    );
    let body: serde_json::Value = serde_json::from_str(&text)?;
    let job = body["job_id"]
        .as_str()
        .expect("a 202 names the task it accepted")
        .to_string();

    // The phantom, proved: the record reports nobody watching it, which is the
    // precondition for the window existing at all. A hub that left the submitting
    // consumer registered answers `1` here — and then never deletes anything.
    let view = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "an async job has no consumer, so its window is already running: {view}"
    );

    // The window, closing: the record goes away, and a poll after that is a `404`
    // rather than an orphan kept for `job_retention_ms` that nobody will ever read.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client
            .get(format!("http://{}/v1/jobs/{job}", hub.addr))
            .bearer_auth(API_KEY)
            .send()
            .await?
            .status();
        if status == reqwest::StatusCode::NOT_FOUND {
            break;
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "the subscriber-less window never deleted {job}; last status {status}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // NEX-6.3's second half, and the guard on §5.2's hub-side mirror: a key whose task was
    // deleted must not keep answering with it. The mirror is pruned by the actor's
    // `Effect::JobForgotten` (`state.rs`'s arm for it), so the retry re-runs — the same key
    // now mints a record rather than pointing at one that no longer exists.
    assert!(
        !hub.hub
            .idempotency
            .lock()
            .await
            .contains_key("default:orphan-window-key"),
        "forgetting the task has to forget its key, or a deleted task would come back as a replay"
    );
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .header("Idempotency-Key", "orphan-window-key")
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "nobody is watching"},
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let retry: serde_json::Value = response.json().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "the retry is accepted as new work: {retry}"
    );
    let retry_job = retry["job_id"].as_str().unwrap_or_default();
    assert!(
        !retry_job.is_empty(),
        "the retry names the task it accepted: {retry}"
    );
    assert_ne!(
        retry_job,
        job.as_str(),
        "a deleted task's key runs fresh, it does not resurrect the record: {retry}"
    );
    Ok(())
}
/// Spec §7's credential rule at runtime: consumer credentials are the API key,
/// never the pool token, and they are checked before the upgrade — so a bad key
/// is an HTTP `401` with a challenge. Asserted off the raw socket, because that
/// is the claim: what a plain HTTP client sees, not what a WebSocket library
/// reports.
#[tokio::test]
async fn the_consumer_socket_refuses_an_unauthenticated_upgrade() -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let hub = spawn_hub("ws_auth", test_config()).await?;

    for label in ["absent", "wrong"] {
        let auth = match label {
            "absent" => None,
            _ => Some("sk-not-a-key"),
        };
        // The upgrade headers are what make this a *consumer socket* request: axum's
        // `WebSocketUpgrade` extractor runs before the handler, so a request without
        // them is rejected as a malformed upgrade (`400`) and never reaches the key
        // check. With them, the extractor accepts and the handler's answer is the
        // claim under test — spec §7's consumer credential rule enforced with a
        // `401` before the upgrade.
        //
        // `Connection` is sent twice on purpose: `upgrade` is what the extractor
        // requires, `close` is what makes hyper hang up after the response so the
        // read below terminates without an upgrade that never comes.
        let request = format!(
            "GET /v1/consumer HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\n\
             Connection: upgrade\r\nConnection: close\r\n\
             Sec-WebSocket-Key: {WS_HANDSHAKE_KEY}\r\nSec-WebSocket-Version: 13\r\n{}\r\n",
            hub.addr,
            auth.map(|key| format!("Authorization: Bearer {key}\r\n"))
                .unwrap_or_default(),
        );
        let mut socket = tokio::net::TcpStream::connect(hub.addr).await?;
        socket.write_all(request.as_bytes()).await?;
        let mut bytes = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut bytes))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "the hub never answered an unauthenticated upgrade; got: {}",
                    String::from_utf8_lossy(&bytes)
                )
            })??;
        assert!(read > 0, "the hub answered with an empty response");
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let (status_line, headers) = text.split_once("\r\n").unwrap_or((text.as_str(), ""));
        assert_eq!(
            status_line, "HTTP/1.1 401 Unauthorized",
            "{label} key: {text}"
        );
        let challenge: Vec<&str> = headers
            .lines()
            .filter(|line| line.to_ascii_lowercase().starts_with("www-authenticate:"))
            .collect();
        assert_eq!(
            challenge,
            ["www-authenticate: Bearer realm=\"nexus\""],
            "the 401 says how to authenticate ({label} key)"
        );
    }

    // The good key, for contrast: the upgrade happens (asserted inside
    // `dial_consumer`) and the socket speaks the dialect — a warm worker is needed for
    // the verdict to be an acceptance rather than the no-worker refusal, so the claim
    // under test is the key, not the scheduling.
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;
    consumer_submit(&mut consumer, "authenticated", true).await?;
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted { detached, .. } => {
            assert!(detached, "the frame's own `detach` is echoed back");
        }
        other => {
            return Err(anyhow::anyhow!(
                "an authenticated submit is accepted, got {other:?}"
            ))
        }
    }
    Ok(())
}

/// NEX-6.1's mechanism, measured: the hub must notice a consumer hanging up from
/// the connection, not from the first write that fails.
///
/// The counterfactual is concrete on this surface — the OpenAI SSE body carries a
/// 1 s keepalive (`consumer.rs:308-311`), so a hub that only learned on a failed write
/// would take at least that long, and a body with a longer interval would wait
/// forever. The read-side `select` on the body channel ends the stream in
/// milliseconds whatever the keepalive cadence is.
#[tokio::test]
async fn an_idle_sse_hangup_cancels_the_worker_without_a_failed_write() -> anyhow::Result<()> {
    let hub = spawn_hub("idle_hangup", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("nothing to stream"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };

    // Nothing has been written since the hub's own role delta: the pump is parked
    // on the fan-out, which is the state NEX-6.1 says must not be invisible.
    let started = std::time::Instant::now();
    drop(response);
    let cancel = tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut worker, "cancel"))
        .await
        .map_err(|_| anyhow::anyhow!("the worker never learned the consumer left"))??;
    let elapsed = started.elapsed();
    match cancel {
        HubFrame::Cancel { job: cancelled, .. } => assert_eq!(cancelled, job),
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }
    assert!(
        elapsed < Duration::from_millis(700),
        "the notice must come from the connection, not from the 1 s keepalive: {elapsed:?}"
    );
    Ok(())
}

/// One counter out of `GET /metrics`, read as the hub exposed it.
///
/// Exact value, not a range: the counters here are the cheapest way to pin "this
/// submission did not create a second task", which no response body can show on its own.
/// The exposition labels every series by pool (`nexus_jobs_accepted_total{pool=…} 1`), so
/// the answer is the sum over the hub's pools — a hub-wide total, which is what these
/// assertions are about.
async fn metric_value(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    name: &str,
) -> anyhow::Result<u64> {
    let text = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let mut total: u64 = 0;
    let mut series = 0_usize;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((metric, value)) = line.split_once(' ') else {
            continue;
        };
        // The label set is part of the metric token, not a separate field.
        if metric.split('{').next() != Some(name) {
            continue;
        }
        total += value.trim().parse::<u64>()?;
        series += 1;
    }
    if series == 0 {
        return Err(anyhow::anyhow!(
            "`{name}` is missing from the exposition:\n{text}"
        ));
    }
    Ok(total)
}

/// NEX-6.6's letter at the HTTP boundary: detachment has to be **stated** on the
/// request, so neither an omitted field nor `false` is accepted by the async job route.
///
/// The omitted case is the one this test exists for. Treating absence as "detached" was
/// a route artifact — every plain `POST /v1/jobs` would have bought detached-lifetime
/// semantics without anybody asking, which is exactly what the spec's "it never applies to
/// a plain 'stream me the answer' submission" forbids.
#[tokio::test]
async fn the_async_job_route_requires_detach_to_be_stated() -> anyhow::Result<()> {
    let hub = spawn_hub("detach_required", test_config()).await?;
    // Ready, so the accepted case below is an immediate `202` rather than a grace-window
    // race; it never claims, which is irrelevant here because nothing is dispatched in
    // the two refusals.
    let _idle_worker = dial_ready_but_idle_worker(hub.addr).await?;
    let client = reqwest::Client::new();
    let post = |body: serde_json::Value| {
        let client = client.clone();
        async move {
            let response = client
                .post(format!("http://{}/v1/jobs", hub.addr))
                .bearer_auth(API_KEY)
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            let value = response.json::<serde_json::Value>().await?;
            Ok::<_, anyhow::Error>((status, value))
        }
    };
    // The whole refusal, field for field, so the contract's wording is pinned as well as
    // its code. `retryable: false`: nothing about the hub is wrong here.
    let expected_refusal = serde_json::json!({
        "error": {
            "type": "detach_required",
            "code": "detach_required",
            "message": "this route keeps no subscription; post with \"detach\": true, or use a surface that holds the connection open (the streaming completion routes, or GET /v1/consumer)",
            "param": null,
            "retryable": false,
        }
    });

    // Absent: nothing was stated, so nothing is inferred.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "silent about its lifetime"},
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "got: {body}");
    assert_eq!(body, expected_refusal);

    // `false`: the plain streaming contract, which this route cannot honour. Same
    // answer, because the lesson is the same.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "stream me the answer"},
        "detach": false,
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "got: {body}");
    assert_eq!(body, expected_refusal);

    // Stated: accepted. Also the proof the refusals above are not a blanket rejection of
    // the kind or the tenant.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "post it now, I will poll later"},
        "detach": true,
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "got: {body}");
    assert!(
        body["job_id"].as_str().is_some_and(|id| !id.is_empty()),
        "the `202` names the task it accepted: {body}"
    );
    Ok(())
}

/// §5.2 at the runtime level, on the surface that carries the answer: a second request
/// with the same `Idempotency-Key` gets the **first** task's completion replayed, not a
/// second execution and not a `503`.
///
/// Until the hub could find the original job before minting an id, this path answered
/// `503 hub_unavailable`: the actor attached the duplicate to the original record and
/// replayed it there (`actor.rs:1045-1064`), while the face waited for a verdict on a gate
/// registered under an id the actor never named. README §16.7 row 7 claims this works; the
/// claim was about the replay queue being installed, which was true and not sufficient.
#[tokio::test]
async fn a_retry_on_an_idempotency_key_replays_the_original_completion() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_completion", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let chat = || {
        let client = client.clone();
        async move {
            Ok::<_, anyhow::Error>(
                client
                    .post(format!("http://{}/v1/chat/completions", hub.addr))
                    .bearer_auth(API_KEY)
                    .header("Idempotency-Key", "dup-chat-1")
                    .json(&chat_body("count once"))
                    .send()
                    .await?,
            )
        }
    };

    // First request: one task, one dispatch, one answer.
    let first = chat().await?;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first_job = first
        .headers()
        .get("x-nexus-job")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(job.as_ref(), first_job, "the header names the task");

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: serde_json::json!({
            "id": "chatcmpl-dup",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "forty-two"}, "finish_reason": null}],
        })
        .to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;
    let first_body = tokio::time::timeout(Duration::from_secs(10), first.text()).await??;
    assert_eq!(
        first_body.matches("chat.completion.chunk").count(),
        3,
        "role delta + the relayed chunk + the finish chunk: {first_body}"
    );

    // Second request, same key, after the first answer completed but while the record is
    // still retained: the pre-fix answer was a `503` with a 1.3 s wait.
    let second = chat().await?;
    assert_eq!(
        second.status(),
        reqwest::StatusCode::OK,
        "a duplicate key follows the task that exists; it is not a hub failure"
    );
    assert_eq!(
        second
            .headers()
            .get("x-nexus-job")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        first_job,
        "the retry answers with the original task's id, not a second one"
    );
    let second_body = tokio::time::timeout(Duration::from_secs(10), second.text()).await??;
    // The replay, byte for byte: the same three chunks, the worker's text exactly once.
    assert_eq!(
        second_body.matches("chat.completion.chunk").count(),
        3,
        "the retry replays the whole answer: {second_body}"
    );
    assert_eq!(
        second_body.matches("forty-two").count(),
        1,
        "the buffered answer is replayed once, not re-executed: {second_body}"
    );

    // The proof that nothing ran twice, independent of the bytes: the actor accepted one
    // job for the two requests.
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "two requests, one execution"
    );
    Ok(())
}

/// §5.2 on the async job API: the second `POST /v1/jobs` with the same key is answered
/// with the record that already exists — same id, same state — and the hub keeps one
/// idempotency row rather than two.
///
/// The task is deliberately never dispatched (`dial_ready_but_idle_worker`), so its state
/// cannot move underneath the assertions and the retry's answer is fully determined.
#[tokio::test]
async fn a_retry_on_an_idempotency_key_gets_the_task_that_already_exists() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_job", test_config()).await?;
    let _idle_worker = dial_ready_but_idle_worker(hub.addr).await?;
    let client = reqwest::Client::new();
    let post = || {
        let client = client.clone();
        async move {
            let response = client
                .post(format!("http://{}/v1/jobs", hub.addr))
                .bearer_auth(API_KEY)
                .header("Idempotency-Key", "dup-job-1")
                .json(&serde_json::json!({
                    "kind": WS_KIND,
                    "payload": {"prompt": "post me twice"},
                    "detach": true,
                }))
                .send()
                .await?;
            let status = response.status();
            let value = response.json::<serde_json::Value>().await?;
            Ok::<_, anyhow::Error>((status, value))
        }
    };

    let (status, first) = post().await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "got: {first}");
    let first_job = first["job_id"]
        .as_str()
        .expect("a 202 names the task it accepted")
        .to_string();

    let (status, second) = post().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "a retry on a retained task is accepted, not a 503: {second}"
    );
    assert_eq!(
        second["job_id"].as_str(),
        Some(first_job.as_str()),
        "the retry names the task that is already running"
    );
    assert_eq!(
        second["state"], first["state"],
        "and reports the same state, read from the actor"
    );
    // The retry registered nothing: no consumer, so the task's NEX-6.6 window is still
    // running exactly as it was after the first post.
    let view = client
        .get(format!("http://{}/v1/jobs/{first_job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "following a detached task's key does not invent a consumer: {view}"
    );

    // Hub-side bookkeeping, exact: the key maps to the one job, and the retry added no
    // second row (the same mirror entry the retry answered from).
    let mirrored = hub.hub.idempotency.lock().await;
    assert_eq!(
        mirrored
            .get("default:dup-job-1")
            .map(|(_, job, _)| job.as_ref().to_string()),
        Some(first_job.clone()),
        "one key, one mirrored job"
    );
    assert_eq!(
        mirrored.len(),
        1,
        "the retry wrote no second idempotency row"
    );
    drop(mirrored);
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "two posts, one accepted task"
    );
    Ok(())
}

/// §5.2 on the consumer socket: a `submit` whose idempotency key this hub has already
/// routed is answered by following the task that exists — verdict, snapshot, the whole
/// replayed answer, one `end` — rather than `hub_unavailable`.
///
/// This is the reconnect path the socket exists for: a client that dropped and resends the
/// same submission must not pay for a second execution, and must not be told the hub did
/// not answer.
#[tokio::test]
async fn a_resubmitted_key_on_the_consumer_socket_follows_the_original_task() -> anyhow::Result<()>
{
    let hub = spawn_hub("ws_idempotent_submit", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    // Same payload a client would resend, with the key that makes it a duplicate.
    let submission = serde_json::json!({
        "type": "submit",
        "kind": WS_KIND,
        "payload": {"prompt": "answer me once"},
        "detach": false,
        "idempotency_key": "dup-ws-1",
    });
    send_consumer(&mut consumer, &submission).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "once".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let usage = serde_json::json!({"completion_tokens": 1});
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(usage.clone()),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    let answer = [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "once".to_string(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ];
    for want in answer.clone() {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // The resubmission: same socket, same frame, same key. The verdict names the original
    // task, and the answer that follows is the ring's, not a second run.
    send_consumer(&mut consumer, &submission).await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: WS_KIND.to_string(),
            pool: nexus_core::ids::PoolId::new_unchecked(POOL),
            // A resubmission declares its own contract, unlike a bare attach.
            detached: false,
            // No supplier either: this is the same finished task, so the join reports
            // what the lease says, and the lease is gone. Deliberately exact — a hub that
            // invented an id here would fail this whole-frame assertion.
            supplier: None,
        },
    )
    .await?;
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta {
            job: meta, state, ..
        } => {
            assert_eq!(meta, job, "the snapshot is of the original task");
            assert_eq!(
                state.as_deref(),
                Some("Finished"),
                "the phase is the actor's, not a guess"
            );
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    for want in answer {
        expect_consumer_frame(&mut consumer, want).await?;
    }
    Ok(())
}

/// §5.2 on the non-streaming chat surface: a duplicate key assembles its answer from the
/// task that exists, while that task is still running, and the original consumer's stream
/// keeps running alongside it.
///
/// `stream: false` reaches `aggregate_response`, which is the one follow path that used to
/// drop the relay when its request ended. A joiner borrows that relay — the original
/// submission's pump is reading the same fan-out — so the entry has to survive the joiner
/// (see the `prune_relay` parameter). The last assertion is what pins that: if the joiner
/// had torn the relay down, the live stream would stop delivering here.
#[tokio::test]
async fn a_non_streaming_retry_on_a_key_assembles_the_running_answer() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_aggregate", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    // The original: streaming, and deliberately left open with one delta already in the
    // ring, so the retry joins a task that is running rather than finished.
    let live = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("Idempotency-Key", "dup-aggregate-1")
        .json(&chat_body("answer slowly"))
        .send()
        .await?;
    assert_eq!(live.status(), reqwest::StatusCode::OK);
    let live_job = live
        .headers()
        .get("x-nexus-job")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(job.as_ref(), live_job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: serde_json::json!({
            "id": "chatcmpl-dup-aggregate",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "partly"}, "finish_reason": null}],
        })
        .to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The retry, `stream: false`, in its own task: an aggregated answer arrives as one
    // response, so its headers only come when the handler has collected everything — which
    // needs the `Done` below. Its `Attach` is in the actor's mailbox as soon as the handler
    // has run `join_existing_job`, and the pause below is what makes that happen first.
    let retry_client = client.clone();
    // Copied out: the task cannot borrow the hub, and `SocketAddr` is `Copy`.
    let addr = hub.addr;
    let retry = tokio::spawn(async move {
        let response = retry_client
            .post(format!("http://{addr}/v1/chat/completions"))
            .bearer_auth(API_KEY)
            .header("Idempotency-Key", "dup-aggregate-1")
            .json(&{
                let mut body = chat_body("answer slowly");
                body["stream"] = serde_json::json!(false);
                body
            })
            .send()
            .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "a retry on a running task is an answer, not a hub failure"
        );
        let value = response.json::<serde_json::Value>().await?;
        Ok::<_, anyhow::Error>(value)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The task finishes while the retry is still collecting, and the retry's answer is the
    // whole message: the replayed prefix plus whatever arrived after it joined. (If the
    // `Done` were to land first, the ring carries it instead, and the assembled content
    // below is the same — the assertion does not depend on which of the two orders runs.)
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;
    let retry_body = tokio::time::timeout(Duration::from_secs(10), retry)
        .await
        .expect("the retry answers within 10 s")??;
    assert_eq!(
        retry_body["choices"][0]["message"]["content"].as_str(),
        Some("partly"),
        "the assembled answer is the replayed prefix: {retry_body}"
    );
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "the retry ran nothing"
    );

    // The original stream is still a live stream: its own finish and `[DONE]` arrive after
    // the retry has come and gone.
    let live_body = tokio::time::timeout(Duration::from_secs(10), live.text()).await??;
    assert_eq!(
        live_body.matches("partly").count(),
        1,
        "the consumer that was here first still gets its answer: {live_body}"
    );
    assert!(
        live_body.contains("[DONE]"),
        "and its stream ends normally: {live_body}"
    );

    // Third request, same key, now that the task is finished: the whole answer is in the
    // replayed prefix, so the aggregating path must stop there rather than wait for a second
    // terminal event on the fan-out. The 10 s budget is the assertion — the alternative is
    // the aggregate deadline (300 s) followed by a `503 no_ready_worker` for a completion
    // that was already in hand.
    let finished_retry = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .post(format!("http://{}/v1/chat/completions", hub.addr))
            .bearer_auth(API_KEY)
            .header("Idempotency-Key", "dup-aggregate-1")
            .json(&{
                let mut body = chat_body("answer slowly");
                body["stream"] = serde_json::json!(false);
                body
            })
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a retry on a finished task never answered"))??;
    assert_eq!(finished_retry.status(), reqwest::StatusCode::OK);
    let finished_body = finished_retry.json::<serde_json::Value>().await?;
    assert_eq!(
        finished_body["choices"][0]["message"]["content"].as_str(),
        Some("partly"),
        "the finished task's replay is the whole answer: {finished_body}"
    );
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "three requests, one execution"
    );
    Ok(())
}

/// NEX-7.2 on the HTTP faces: a submission the actor refused leaves no per-task state
/// behind — not a routing entry, not a relay, not an idempotency row.
///
/// `submit` registers all three before the actor is told, so the refusal has to undo them:
/// the actor holds no record, which means no `Effect::Terminal` and no `Effect::JobForgotten`
/// will ever name the id to prune it. The consumer WebSocket already did this for its own
/// refusals; the HTTP surfaces previously did not, so every refused submission (a `413`
/// here, a `429`, a `503`) leaked one entry per attempt for the life of the process.
#[tokio::test]
async fn a_refused_submission_leaves_no_route_or_relay_behind() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // A 1-byte tenant payload cap: every submission is refused in `on_submit`, at the
    // payload check (`actor.rs` `payload_too_large`), before the queue and before the
    // capability pre-check — which is what makes the count below an exact number
    // rather than a race.
    cfg.limits.tenant_max_payload_bytes = 1;
    let hub = spawn_hub("refusal_prune", cfg).await?;
    let _idle_worker = dial_ready_but_idle_worker(hub.addr).await?;
    let client = reqwest::Client::new();

    // Refusal one: the async job API.
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "no room"},
            "detach": true,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["error"]["code"], "payload_too_large", "got: {body}");

    // Refusal two: the OpenAI surface, whose reject arm used to leak the same way.
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("still no room"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["error"]["code"], "payload_too_large", "got: {body}");

    // Nothing left in any of the three job-keyed maps.
    assert!(
        hub.hub.job_index.lock().await.is_empty(),
        "a refused submission owns no route"
    );
    assert!(
        hub.hub.idempotency.lock().await.is_empty(),
        "a refused submission owns no idempotency row"
    );
    for handle in hub.hub.pools.values() {
        assert!(
            handle.followed_jobs().await.is_empty(),
            "a refused submission owns no relay"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The observation plane: `GET /v1/observe` + CORS.
//
// The dashboard is a static bundle on another origin, so these two tests are the
// only place the whole feature is observed: that the endpoint is open and always
// `200`, that its numbers come from the actor through the mailbox, and that a
// browser origin actually gets the CORS headers. `nexus-core`'s acceptance suite
// proves the snapshot's *values*; only a real listener can prove the wire shape.
// ---------------------------------------------------------------------------

/// Anything after this is a plausible epoch millisecond stamp (Nov 2023).
///
/// Every timestamp in the observe document is translated to epoch on the way out
/// (`state::actor_to_wall_ms`), so the assertion that matters is "epoch-scale", not
/// "the actor's process-relative number happened to be N".
const EPOCH_FLOOR_MS: u64 = 1_700_000_000_000;

/// Polls `GET /v1/observe` until `settled` accepts the document, and returns it.
///
/// The three producers of this document run independently — the actor answers a
/// mailbox message, the mirrored gauges refresh on dispatch, and the consumer
/// registry is written by the WS session — so "eventually true" is the honest
/// precondition here. What is asserted after the poll is exact, and the failure
/// message carries the whole last document.
///
/// The status assertion lives inside the helper on purpose: every poll of this
/// endpoint must be a `200`, including a poll of a hub with nothing ready.
async fn observe_until(
    client: &reqwest::Client,
    url: &str,
    what: &str,
    settled: impl Fn(&serde_json::Value) -> bool,
) -> anyhow::Result<serde_json::Value> {
    let mut last = serde_json::Value::Null;
    // 8 s of patience: the surfaces this polls are driven by timers a test cannot
    // shorten past a floor (a socket keepalive, a lease tick), so a 2 s budget would
    // make the slow-settling cases flaky rather than meaningful.
    for _ in 0..160 {
        let response = client.get(url).send().await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "`/v1/observe` must always answer `200` while the hub is serving"
        );
        last = response.json().await?;
        if settled(&last) {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(anyhow::anyhow!(
        "`/v1/observe` never showed {what}; last document: {last}"
    ))
}

/// The whole `nexus-hub-observe/v1` contract, against a hub that has one warm
/// worker, one leased job and one consumer socket attached to it.
#[tokio::test]
async fn observe_documents_workers_jobs_leases_and_consumer_sockets() -> anyhow::Result<()> {
    let hub = spawn_hub("observe_snapshot", test_config()).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/observe", hub.addr);

    // ---- 1. An idle hub is a `200`, and an honest one ----
    //
    // No API key is sent: the endpoint is open by the same argument as `/healthz`
    // (a browser dashboard has no secret, and the payload is operational state). A
    // monitor must be able to treat a transport failure as the only downtime
    // signal, so zero ready capacity is a field, never a status code.
    let cold = client.get(&url).send().await?;
    assert_eq!(cold.status(), reqwest::StatusCode::OK);
    assert_eq!(
        cold.headers()[reqwest::header::CONTENT_TYPE],
        "application/json",
        "the dashboard's client sniffs this to decide the body is JSON at all"
    );
    let cold = cold.json::<serde_json::Value>().await?;
    assert_eq!(
        cold["protocol"], "nexus-hub-observe/v1",
        "the version marker the client checks before parsing"
    );
    assert_eq!(
        cold["readiness"]["status"], "degraded",
        "same rule as `/healthz`"
    );
    assert_eq!(cold["readiness"]["ready_workers"], 0);
    assert_eq!(cold["readiness"]["loading_workers"], 0);
    assert_eq!(cold["readiness"]["queue_depth"], 0);
    assert_eq!(cold["readiness"]["connected_workers"], 0);
    assert_eq!(
        cold["readiness"]["capacity_signal"], "no_workers",
        "the reason, not just the count"
    );
    assert_eq!(cold["readiness"]["warm_models"], serde_json::json!([]));
    // Every collection is present and empty — never `null`, or the UI has to
    // distinguish "no data" from "no rows".
    for key in [
        "pools",
        "workers",
        "jobs",
        "leases",
        "consumer_connections",
        "unresponsive_pools",
    ] {
        assert!(cold[key].is_array(), "`{key}` must be an array: {cold}");
    }
    assert_eq!(cold["workers"], serde_json::json!([]));
    assert_eq!(cold["jobs"], serde_json::json!([]));
    assert_eq!(cold["consumer_connections"], serde_json::json!([]));
    assert_eq!(cold["unresponsive_pools"], serde_json::json!([]));
    // One row per configured pool, id-ordered, including the pool with nothing in
    // it: a dashboard that only sees the busy pool cannot tell a cold one apart.
    let cold_pools: Vec<&str> = cold["pools"]
        .as_array()
        .expect("pools array")
        .iter()
        .map(|row| row["id"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(cold_pools, vec!["image.render/comfyui", POOL]);
    assert!(
        cold["now_ms"].as_u64().unwrap_or_default() > EPOCH_FLOOR_MS,
        "`now_ms` is epoch ms: {cold}"
    );

    // ---- 2. One warm worker, one leased job, one attached socket ----
    let mut worker = dial_worker(hub.addr, true).await?;
    let worker_id = match next_frame_of(&mut worker, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id,
        other => panic!("expected a welcome frame, got {other:?}"),
    };
    assert_eq!(worker_id.as_ref().len(), 36, "the hub assigns a UUIDv4 id");
    assert_eq!(
        worker_id.as_ref().as_bytes()[14],
        b'4',
        "UUID version nibble: {worker_id}"
    );
    let mut consumer = dial_consumer(hub.addr).await?;
    consumer_submit(&mut consumer, "observe me", false).await?;
    let job = match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } => job,
        other => panic!("expected the accept verdict, got {other:?}"),
    };
    // Wait for the hand-over: once the worker holds its `Assigned` frame, the lease,
    // the worker's `in_flight` set and the job's phase are all decided, so every
    // number below is a settled fact rather than a race against the dispatch.
    assert!(matches!(
        next_frame_of(&mut worker, "assigned").await?,
        HubFrame::Assigned { .. }
    ));

    // The consumer registry is written by the WS session (`consumer_ws::spawn_pump`),
    // not by the actor, so it lands on its own schedule; poll for it, then assert the
    // rest of the document exactly.
    let body = observe_until(&client, &url, "the attached consumer socket", |doc| {
        doc["consumer_connections"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
    })
    .await?;

    // ---- worker row: the actor's `WorkerRecord`, projected ----
    let workers = body["workers"].as_array().expect("workers array");
    assert_eq!(workers.len(), 1, "one fake worker is connected: {body}");
    let worker_row = &workers[0];
    assert_eq!(worker_row["id"], worker_id.as_ref());
    assert_eq!(worker_row["pools"], serde_json::json!([POOL]));
    assert_eq!(worker_row["link"], "live");
    assert_eq!(worker_row["engine"], "ready");
    assert_eq!(worker_row["epoch"], 1, "the first connection generation");
    assert_eq!(worker_row["capacity_slots"], 4, "as declared in `Hello`");
    assert_eq!(
        worker_row["free_slots"], 4,
        "the heartbeat's own number: a dispatch does not rewrite it"
    );
    assert_eq!(
        worker_row["available_slots"], 3,
        "the actor's reconciled number, which is what dispatch obeys: 4 minus 1 in flight"
    );
    assert_eq!(worker_row["free_kv_tokens"], 524_288, "from `Ready`");
    assert_eq!(
        worker_row["in_flight"],
        serde_json::json!([job.as_ref()]),
        "job ids, so a monitor sees who owns what without joining `jobs[]`"
    );
    assert_eq!(
        worker_row["outstanding_claims"], 3,
        "`Claim{{jobs:4}}` minus the one job it was handed"
    );
    assert_eq!(
        worker_row["telemetry"],
        serde_json::Value::Null,
        "the fake worker sent no telemetry, and `null` is not an empty object"
    );
    assert!(
        worker_row["last_seen_ms"].as_u64().unwrap_or_default() > EPOCH_FLOOR_MS,
        "`last_seen_ms` is epoch, not the actor's process-relative clock: {worker_row}"
    );

    // ---- job row, with its lease ----
    let jobs = body["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1, "one active job: {body}");
    let job_row = &jobs[0];
    assert_eq!(job_row["id"], job.as_ref());
    assert_eq!(job_row["pool"], POOL);
    assert_eq!(
        job_row["tenant"], "default",
        "the tenant the consumer's API key resolves to"
    );
    assert_eq!(
        job_row["phase"], "assigned",
        "the actor's own vocabulary: leased, and not yet streaming"
    );
    assert_eq!(
        job_row["running"], true,
        "the boolean a simple client reads"
    );
    assert_eq!(
        job_row["consumers"], 1,
        "the actor's own consumer set: the one face that submitted"
    );
    assert!(
        job_row["created_ms"].as_u64().unwrap_or_default() > EPOCH_FLOOR_MS,
        "`created_ms` is translated from the actor's clock like every other stamp"
    );
    let lease = &job_row["lease"];
    assert_eq!(lease["worker"], worker_id.as_ref());
    assert_eq!(
        lease["epoch"], 1,
        "the lease names the connection generation it was made on"
    );
    assert_eq!(lease["attempt"], 1, "a first dispatch, never re-leased");
    assert!(
        lease["deadline_ms"].as_u64().unwrap_or_default()
            > body["now_ms"].as_u64().unwrap_or_default(),
        "a live deadline stays in the future; saturating the clock translation would \
         collapse it onto `now_ms`: {job_row}"
    );
    // The same lease, published as its own row so the connection graph can join it
    // without re-reading the job rows.
    let leases = body["leases"].as_array().expect("leases array");
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0]["job"], job.as_ref());
    assert_eq!(leases[0]["worker"], worker_id.as_ref());

    // ---- consumer connection row: who is attached to whom ----
    let conns = body["consumer_connections"]
        .as_array()
        .expect("consumer_connections array");
    assert_eq!(conns.len(), 1, "one consumer socket: {body}");
    let conn = &conns[0];
    assert_eq!(conn["tenant"], "default");
    assert_eq!(
        conn["attached_jobs"],
        serde_json::json!([job.as_ref()]),
        "the socket's own watch set, mirrored by `spawn_pump`"
    );
    let conn_id = conn["id"].as_str().expect("consumer connection id");
    assert_eq!(conn_id.len(), 36, "the hub assigns a UUIDv4 id: {conn}");
    assert_eq!(conn_id.as_bytes()[14], b'4', "UUID version nibble: {conn}");
    assert_eq!(
        conn_id.matches('-').count(),
        4,
        "a connection id is a different axis from the per-task consumer id: {conn}"
    );
    assert!(
        conn["since_ms"].as_u64().unwrap_or_default() > EPOCH_FLOOR_MS,
        "stamped at accept, on the hub's wall clock"
    );
    assert_eq!(
        conn["peer"],
        serde_json::Value::Null,
        "`axum::serve` installs no `ConnectInfo`, so the hub reports no address"
    );

    // ---- the counts, from the same actor answers ----
    let pool_row = &body["pools"].as_array().expect("pools array")[1];
    assert_eq!(pool_row["id"], POOL);
    assert_eq!(pool_row["ready_workers"], 1);
    assert_eq!(pool_row["loading_workers"], 0);
    assert_eq!(pool_row["connected_workers"], 1);
    assert_eq!(pool_row["leases"], 1);
    assert_eq!(
        pool_row["queue_depth"], 0,
        "the only job is leased, not queued"
    );
    assert_eq!(body["readiness"]["status"], "ok");
    assert_eq!(body["readiness"]["capacity_signal"], "warm");
    assert_eq!(body["readiness"]["ready_workers"], 1);
    assert_eq!(body["readiness"]["connected_workers"], 1);
    assert_eq!(body["readiness"]["queue_depth"], 0);
    assert_eq!(
        body["readiness"]["warm_models"],
        serde_json::json!(["qwen38"]),
        "warm means a ready worker declared that model"
    );
    assert_eq!(body["readiness"]["active_jobs"], 1);
    assert_eq!(
        body["unresponsive_pools"],
        serde_json::json!([]),
        "both pool actors answered inside the probe budget"
    );

    // ---- 3. The socket goes away: the registry follows on its own ----
    //
    // Nothing tells the actor about a dropped consumer before the hub's own session
    // teardown runs, which is exactly why the registry exists and why it is written
    // from a `Drop` guard. The job section is deliberately not re-asserted here:
    // dropping the consumer also cancels the task, so its phase is a race between
    // the cancel and the next poll.
    drop(consumer);
    let gone = observe_until(&client, &url, "the consumer socket to be gone", |doc| {
        doc["consumer_connections"]
            .as_array()
            .is_some_and(Vec::is_empty)
    })
    .await?;
    assert_eq!(gone["consumer_connections"], serde_json::json!([]));
    assert_eq!(
        gone["readiness"]["ready_workers"], 1,
        "a consumer leaving does not take the worker's capacity with it"
    );
    let _ = worker;
    Ok(())
}

/// The GitHub-Pages origin, which is the only reason CORS is enabled at all.
const TEST_ORIGIN: &str = "https://underloadjs.github.io";

/// CORS on the observation plane: a preflight is answered by the layer, a simple
/// cross-origin `GET` carries the header the browser checks, and the gateway's own response
/// headers are exposed so a page can actually read them.
#[tokio::test]
async fn a_cross_origin_dashboard_can_read_observe() -> anyhow::Result<()> {
    let hub = spawn_hub("observe_cors", test_config()).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/observe", hub.addr);

    // Preflight: `OPTIONS` + `Origin` + `Access-Control-Request-Method`. The layer is
    // outermost, so this never reaches the router, the body limit, or a handler — and
    // no API key is involved at any point.
    let preflight = client
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", TEST_ORIGIN)
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "content-type")
        .send()
        .await?;
    assert_eq!(
        preflight.status(),
        reqwest::StatusCode::OK,
        "the preflight is answered by the CORS layer itself"
    );
    let headers = preflight.headers();
    assert_eq!(
        headers["access-control-allow-origin"]
            .to_str()
            .unwrap_or_default(),
        "*",
        "any origin, because the payload carries no secret"
    );
    assert_eq!(
        headers["access-control-allow-methods"]
            .to_str()
            .unwrap_or_default(),
        "GET,HEAD,OPTIONS",
        "the read verbs, and nothing that could mutate (tower-http joins the list \
         with a bare comma, which the CORS list grammar accepts)"
    );
    assert_eq!(
        headers["access-control-allow-headers"]
            .to_str()
            .unwrap_or_default(),
        "*",
        "any request headers"
    );
    assert_eq!(
        headers["access-control-max-age"]
            .to_str()
            .unwrap_or_default(),
        "600",
        "a dashboard polling every second must not pay a preflight per poll"
    );
    assert!(
        headers.get("access-control-allow-credentials").is_none(),
        "`Allow-Origin: *` and credentials are mutually exclusive; sending none is \
         what makes `*` legal, and what keeps an attacker's cookie out of the request"
    );

    // The actual read: a simple `GET` needs no preflight, and the response must carry
    // the origin header for the browser to hand the body to the page.
    let get = client
        .get(&url)
        .header("origin", TEST_ORIGIN)
        .send()
        .await?;
    assert_eq!(get.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get.headers()["access-control-allow-origin"]
            .to_str()
            .unwrap_or_default(),
        "*"
    );
    // The headers a page on this origin is allowed to **read** off the answer. `fetch`
    // hands the body over and hides every non-simple response header unless the response
    // names it here, so this is the difference between a dashboard that can pin its next
    // request to the supplier it just used and one that cannot see the id at all. The list
    // is the gateway's own (`main.rs`'s `observe_cors`): `x-supplier-id` for the pin,
    // `x-nexus-job` for the receipt a completion is polled by, and `x-nexus-chunks`, the
    // tally the aggregate face has always sent. Joined with bare commas because that is
    // how tower-http renders a list (`cors/expose_headers.rs`'s `separated_by_commas`),
    // which the CORS list grammar accepts. Read before the body: `json()` takes the
    // response by value.
    let exposed = get
        .headers()
        .get("access-control-expose-headers")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert_eq!(
        exposed, "x-supplier-id,x-nexus-job,x-nexus-chunks",
        "the three headers the observe dashboard needs, and nothing else"
    );
    assert!(
        headers.get("access-control-expose-headers").is_none(),
        "and not on the preflight: the layer applies `expose_headers` to the actual \
         response only (`tower-http-0.6.11/src/cors/mod.rs:690`, 'applied only to \
         non-preflight requests'), so a client that learned the exposure from the preflight \
         would be reading a header that is never sent there"
    );
    let body = get.json::<serde_json::Value>().await?;
    assert_eq!(
        body["protocol"], "nexus-hub-observe/v1",
        "and still the real document"
    );

    // The honest shape of `AllowOrigin::any()`: the value is static, so tower-http
    // writes `access-control-allow-origin: *` (plus a `Vary`) whether or not the
    // request carried an `Origin`, and the layer wraps every route. What must not
    // change is the status and the body of the routes that were here first —
    // `job_reads_require_an_api_key_and_operational_routes_do_not` pins those for
    // each surface, and `/healthz` is pinned again here.
    let plain = client
        .get(format!("http://{}/healthz", hub.addr))
        .send()
        .await?;
    assert_eq!(
        plain.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an idle hub is still `degraded` on `/healthz`, exactly as before the layer"
    );
    let health = plain.json::<serde_json::Value>().await?;
    assert_eq!(health["status"], "degraded");
    assert_eq!(health["ready_workers"], 0);
    assert_eq!(
        health["protocol"],
        nexus_core::PROTOCOL_VERSION,
        "the body is byte-for-byte the one `/healthz` has always returned"
    );
    Ok(())
}
// -------------------------------------------------------------------------------------------
// C3 + C5: the hub's relay bookkeeping, from outside the process
// -------------------------------------------------------------------------------------------

/// Every exposition line one relay gauge renders, for the pools of the sample topology.
///
/// The family is labelled by pool and the default config declares two of them, so the honest
/// expectation is **both** lines — `image.render/comfyui`, which no test here submits anything
/// to, and the llm pool under test. Asserting the whole family rather than one line is also
/// what pins the label vocabulary down to `pool`: a second label would appear as a third line,
/// and a hub-wide tally that hid one leaking pool behind a healthy neighbour would show up as
/// one line instead of two.
fn relay_gauge(name: &str, llm_value: u64) -> Vec<String> {
    vec![
        format!(r#"{name}{{pool="{OTHER_POOL}"}} 0"#),
        format!(r#"{name}{{pool="{POOL}"}} {llm_value}"#),
    ]
}

/// Every sample line of one metric family, verbatim as `GET /metrics` rendered it.
///
/// The lines come back exactly as a scraper reads them — `nexus_relay_rows{pool="…"} 1`,
/// labels and all — because that line, and not a number recovered from it, is the artefact the
/// assertions below are about: a series that renders with the wrong label set is just as
/// invisible to a dashboard as one that is missing. `# TYPE` and comment lines are skipped, and
/// a family nobody has written yields the empty vec, which is how a missing series shows up as
/// a failed assertion rather than as a plausible zero.
async fn metric_lines(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    name: &str,
) -> anyhow::Result<Vec<String>> {
    let text = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    Ok(text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| line.split([' ', '{']).next() == Some(name))
        .map(ToString::to_string)
        .collect())
}

/// Bounded wait for one family to reach a settled reading; the caller then asserts it exactly.
///
/// The two relay families are published by `state::prune_orphaned_relays`, which runs on the
/// pool actor's **tick** — clamped to at most 1 s and biased behind the actor's mailbox — so
/// this helper waits for the publisher's cadence and nothing else. It cannot soften an
/// assertion: whatever the reading settles on is compared to an exact expectation by the caller,
/// which is the same division of labour `bookkeeping_until` documents for the pump teardowns.
async fn metric_lines_until(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    name: &str,
    what: &str,
    settled: impl Fn(&[String]) -> bool,
) -> anyhow::Result<Vec<String>> {
    let mut last: Vec<String> = Vec::new();
    // Four tick periods at the 1 s clamp: a hub that publishes at all cannot miss this window,
    // and a hub that does not fails here with its last reading in the message.
    for _ in 0..160 {
        last = metric_lines(client, addr, name).await?;
        if settled(&last) {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(anyhow::anyhow!(
        "timed out waiting for {what}; the last reading of `{name}` was {last:?}"
    ))
}

/// C3: the fan-out the hub is holding is visible on `/metrics`, at every point of a task's life.
///
/// Before this change the only way to see a relay row or a consumer queue was to read it out of
/// the process's own memory, which is exactly why the leak survived: `/metrics` carried nine
/// series, none of them about the hub's per-follow bookkeeping, and the deployed hub looked
/// healthy while it climbed. This test is the arc an operator would watch — nothing followed →
/// a task submitted → a watcher attached → the watcher hangs up → the task finishes — and pins
/// the exact exposition line at each step, so a series that went missing, mislabelled, or stuck
/// at the high-water mark fails here rather than in a graph three hours later.
///
/// The two numbers are deliberately different things: rows follow the **actor's record** (a row
/// is kept while the task exists to be re-followed) and subscribers follow the **live stream**
/// (a queue exists only while a pump is reading it). That distinction is the whole point of the
/// change, and the middle two steps of this test are where it shows.
#[tokio::test]
async fn the_relay_bookkeeping_shows_up_on_the_metrics_page() -> anyhow::Result<()> {
    let hub = spawn_hub("relay_gauges", test_config()).await?;
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let client = reqwest::Client::new();
    let pool_label = format!(r#"pool="{POOL}""#);
    let row_line = |value: u64| relay_gauge(nexus_core::metrics::names::RELAY_ROWS, value);
    let subscriber_line =
        |value: u64| relay_gauge(nexus_core::metrics::names::RELAY_SUBSCRIBERS, value);

    // Step 1: nothing is followed, and the families exist anyway. A gauge that is absent until
    // the first non-zero reading is a gauge a dashboard cannot baseline, and the sweep publishes
    // it at zero on the actor's first tick precisely so that a healthy hub reads as healthy.
    let rows = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_ROWS,
        "the relay-row gauge to be published",
        |lines| !lines.is_empty(),
    )
    .await?;
    let subscribers = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        "the relay-subscriber gauge to be published",
        |lines| !lines.is_empty(),
    )
    .await?;
    assert_eq!(
        rows,
        row_line(0),
        "both pools of this hub publish the family, at zero"
    );
    assert_eq!(
        subscribers,
        subscriber_line(0),
        "and no consumer queue registered against them"
    );

    // Step 2: a detached submission. Its face creates the relay row a moment before the actor
    // hears about the task, resolves its gate, and drops the fan-out slot it never pumps — so
    // the row appears without a queue, which is the shape of a task waiting to be followed.
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": { "prompt": "gauge-me" },
            "detach": true,
        }))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::ACCEPTED,
        "a stated `detach` on a pool with a ready worker is an immediate `202`"
    );
    let body: serde_json::Value = response.json().await?;
    let job = JobId::new_unchecked(
        body["job_id"]
            .as_str()
            .expect("a 202 names the job it accepted")
            .to_string(),
    );
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job, "the worker is handed this task");

    let rows = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_ROWS,
        "the submitted task's relay row to be counted",
        |lines| lines == row_line(1).as_slice(),
    )
    .await?;
    assert_eq!(rows, row_line(1), "one followed task, one row");
    assert_eq!(
        metric_lines(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_SUBSCRIBERS
        )
        .await?,
        subscriber_line(0),
        "and no live stream: the `202` face retired the slot it registered"
    );

    // Step 3: a watcher attaches. `GET /events` is the surface that production leaves open for
    // hours, and its queue is the half of the leak this change is about.
    let watcher = client
        .get(format!("http://{}/v1/jobs/{job}/events", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(watcher.status(), reqwest::StatusCode::OK);
    let subscribers = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        "the watcher's consumer queue to be counted",
        |lines| lines == subscriber_line(1).as_slice(),
    )
    .await?;
    assert_eq!(subscribers, subscriber_line(1));
    assert_eq!(
        metric_lines(&client, hub.addr, nexus_core::metrics::names::RELAY_ROWS).await?,
        row_line(1),
        "the watcher joined the row the submission created rather than minting a second one"
    );

    // Nothing about an existing series moved: the new families are added to the exposition, not
    // woven into the ones already there. One submission, one accepted, exactly as before.
    assert_eq!(
        metric_lines(&client, hub.addr, nexus_core::metrics::names::JOBS_ACCEPTED,).await?,
        vec![format!(r#"nexus_jobs_accepted_total{{{pool_label}}} 1"#)],
        "an existing counter keeps its name, its label set, and its value"
    );

    // Step 4: the watcher hangs up mid-stream. The queue goes with the pump; the row stays,
    // because the actor still has the record and another client may still follow it. A gauge
    // that only ever grew is the failure mode this pair of series exists to make visible.
    drop(watcher);
    let subscribers = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        "the hung-up watcher's queue to be released",
        |lines| lines == subscriber_line(0).as_slice(),
    )
    .await?;
    assert_eq!(
        subscribers,
        subscriber_line(0),
        "a stream that ended took its queue with it"
    );
    assert_eq!(
        metric_lines(&client, hub.addr, nexus_core::metrics::names::RELAY_ROWS).await?,
        row_line(1),
        "while the row is still the task's, because the task is still running"
    );

    // Step 5: the task finishes. `Effect::Terminal` hands the row back, so both series return to
    // zero on the next reading — the property the deployed hub lost, one row per task, forever.
    let caps = WireCaps::default();
    worker
        .send(Message::Text(
            encode_line(
                &WorkerFrame::Done {
                    job: job.clone(),
                    seq: 1,
                    finish: FinishKind::Stop,
                    usage: None,
                    error: None,
                },
                &caps,
            )?
            .into(),
        ))
        .await?;
    let rows = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::RELAY_ROWS,
        "the finished task's relay row to be released",
        |lines| lines == row_line(0).as_slice(),
    )
    .await?;
    assert_eq!(
        rows,
        row_line(0),
        "one finished task leaves no relay row on this pool"
    );
    assert_eq!(
        metric_lines(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        )
        .await?,
        subscriber_line(0),
        "and no queue against it"
    );
    Ok(())
}

/// C5: a watcher that hangs up on a silent running task stops being an attached consumer — and
/// stops being a reason for anybody else's stream to be treated as the last one.
///
/// The production shape is a client that opened `GET /v1/jobs/{id}/events` to watch a task, then
/// went away while the task was still generating and producing nothing. A hub that only notices a
/// vanished consumer when a write fails never notices *that*: there is nothing to write, so the
/// write never fails. Every byte of state the watcher held — its queue in the fan-out, its
/// membership in the actor's consumer set — used to stay, and the consumer set is what NEX-6.1
/// and NEX-6.3 act on, so the task also looked watched forever.
///
/// It is pinned with a second consumer on the same task rather than with the watcher alone,
/// because the failure mode is asymmetric: dropping the *last* consumer is supposed to cancel,
/// and dropping one of two is not. Only a test with both can tell "the detach was recorded"
/// apart from "any hangup cancels the task", and the assertion afterwards is exact.
#[tokio::test]
async fn a_hangup_on_a_silent_running_task_releases_its_watcher() -> anyhow::Result<()> {
    let hub = spawn_hub("watcher_hangup", test_config()).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let client = reqwest::Client::new();
    let observe = format!("http://{}/v1/observe", hub.addr);

    // Consumer one: an ordinary streaming completion. The worker never sends it anything, which
    // is the case that matters — an idle stream is where a failed-write-only detector is blind.
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("watch me"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    // The body, not the response, is the connection: holding the stream open is what keeps this
    // consumer attached, and dropping it later is what hangs this one up.
    let mut stream = response.bytes_stream();
    // Reading the accepted role delta proves the stream is live without waiting for the worker.
    let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the accepted answer starts without the worker")
        .expect("and is a body chunk");
    assert!(String::from_utf8_lossy(&chunk?).contains("data:"));

    // The task, and the one consumer the actor knows about at this point.
    let listing = observe_until(
        &client,
        &observe,
        "exactly one task, attached to its streaming client",
        |value| {
            value["jobs"]
                .as_array()
                .is_some_and(|jobs| jobs.len() == 1 && jobs[0]["consumers"] == 1)
        },
    )
    .await?;
    let job = JobId::new_unchecked(
        listing["jobs"][0]["id"]
            .as_str()
            .expect("one task, with an id")
            .to_string(),
    );

    // Consumer two: the watcher that will hang up.
    let relay = handle
        .lookup_relay(&job)
        .await
        .expect("a task with a live stream owns its fan-out row");
    let watcher = client
        .get(format!("http://{}/v1/jobs/{job}/events", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(watcher.status(), reqwest::StatusCode::OK);
    bookkeeping_until("the watcher to register its queue on this fan-out", || {
        relay.subscriber_count() == 2
    })
    .await?;
    observe_until(
        &client,
        &observe,
        "the actor to count both consumers",
        |value| {
            value["jobs"]
                .as_array()
                .is_some_and(|jobs| jobs.len() == 1 && jobs[0]["consumers"] == 2)
        },
    )
    .await?;

    // The hangup. Nothing is arriving on this stream, so the only notice available is the body
    // being dropped — which is what the pump waits on, and what posts the `Detach`.
    drop(watcher);
    bookkeeping_until("the watcher's consumer queue to go with it", || {
        relay.subscriber_count() == 1
    })
    .await?;
    observe_until(
        &client,
        &observe,
        "the actor to forget the watcher",
        |value| {
            value["jobs"]
                .as_array()
                .is_some_and(|jobs| jobs.len() == 1 && jobs[0]["consumers"] == 1)
        },
    )
    .await?;

    // And the asymmetry, exactly: the task survives its watcher. It is still running, it is still
    // followed (the row belongs to the actor's record, not to the stream that just ended — a row
    // for a task that still exists is what lets a second client follow it), and its remaining
    // consumer is untouched.
    assert_eq!(
        handle.followed_jobs().await,
        vec![job.clone()],
        "the watcher took its queue, not the task's fan-out row"
    );
    let read = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(read.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = read.json().await?;
    assert!(
        status["terminal"].is_null(),
        "one of two consumers leaving must not cancel the task: {status}"
    );

    // Now the last one goes: the same notice, this time with nobody left, and the cancel is the
    // proof that the detach reached the actor rather than just the fan-out.
    drop(stream);
    let frame = tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut worker, "cancel"))
        .await??;
    match frame {
        HubFrame::Cancel { job: cancelled, .. } => {
            assert_eq!(cancelled, JobId::new_unchecked(job.as_ref()))
        }
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }
    bookkeeping_until("the last queue on this fan-out to go", || {
        relay.subscriber_count() == 0
    })
    .await?;
    // NEX-6.3 deletes an abandoned task, so its row goes with it and the hub is empty again —
    // the end state the deployed hub never reached. Bounded rather than immediate: the `Cancel`
    // frame the worker just read is one effect of the detach, and the row is handed back by a
    // later one in the same dispatch.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let followed = handle.followed_jobs().await;
        if followed.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "the abandoned task left its relay row behind: {followed:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

/// C1, soaked end to end: an `events` join that races its task's terminal still ends, and leaves
/// nothing parked behind it.
///
/// The interleaving is narrow and real: the joiner resolves the task's relay row, the actor
/// serves the terminal and the dispatcher takes that row away, and only then does the joiner
/// install its replay target and post its `Attach`. The actor answers that attach by replaying
/// onto a row that is no longer in the map, so the joiner's replay queue is never written and
/// never closed — and a pump that drains its replay queue before it looks at anything else has
/// nothing left to end it. `consumer::tests` reproduces that state deterministically; this test
/// is the same race arriving on its own, through a real socket, forty times.
///
/// Whether the race lands on any given round is the scheduler's, so each round asserts what must
/// be true either way — the join was answered, and the hub holds nothing for it afterwards — and
/// the final assertion is the leak statement itself: no relay row outlives the soak. `C1` is what
/// makes that hold; on the pre-C1 tree a landed race parks its pump, whose row then survives the
/// task's own forgetting with a replay target nothing will ever write.
#[tokio::test]
async fn an_events_join_that_races_the_terminal_leaves_nothing_behind() -> anyhow::Result<()> {
    // A terminal record kept for five minutes would keep every relay row with it, and the leak
    // this soak asserts the absence of is measured in seconds. One second of retention is what
    // lets the test wait for the hub to be empty rather than for a production TTL.
    let mut cfg = test_config();
    cfg.limits.job_retention_ms = 1_000;
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("events_terminal_race", cfg).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let client = reqwest::Client::new();

    const ROUNDS: usize = 40;
    let caps = WireCaps::default();
    for round in 0..ROUNDS {
        // A detached submission, so the task outlives the face that posted it and the terminal
        // can genuinely arrive while a separate join is on its way.
        let response = client
            .post(format!("http://{}/v1/jobs", hub.addr))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({
                "kind": WS_KIND,
                "payload": { "prompt": format!("race-{round:02}") },
                "detach": true,
            }))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let body: serde_json::Value = response.json().await?;
        let job = JobId::new_unchecked(
            body["job_id"]
                .as_str()
                .expect("a 202 names the job it accepted")
                .to_string(),
        );
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned {
            job: assigned_job, ..
        } = assigned
        else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        assert_eq!(assigned_job, job);

        // The join and the terminal go in together: the `GET` installs its replay target and
        // posts its `Attach` into the same mailbox the worker's `Done` is about to enter, and the
        // order the actor serves them in is what decides whether this round hit the race.
        let join = {
            let client = client.clone();
            let url = format!("http://{}/v1/jobs/{job}/events", hub.addr);
            tokio::spawn(async move { client.get(url).bearer_auth(API_KEY).send().await })
        };
        worker
            .send(Message::Text(
                encode_line(
                    &WorkerFrame::Done {
                        job: job.clone(),
                        seq: 1,
                        finish: FinishKind::Stop,
                        usage: None,
                        error: None,
                    },
                    &caps,
                )?
                .into(),
            ))
            .await?;
        let response = tokio::time::timeout(Duration::from_secs(5), join)
            .await
            .expect("the join answers: a parked pump never does")??;
        // Both answers are honest here. `200` is a join that reached the task while its record
        // was still kept; `404 unknown_job` is a join that arrived after the record was forgotten,
        // which retention of one second makes routine. What is never correct is a join that is
        // answered with keepalives and nothing else, because that is the pump parking.
        let status = response.status();
        assert!(
            status == reqwest::StatusCode::OK || status == reqwest::StatusCode::NOT_FOUND,
            "round {round}: a join of a finishing task was answered {status}, which is neither \
             the transcript nor a forgotten task"
        );
        // Dropping the body is the client hanging up. On the stranded shape this is the exit the
        // pump must be able to take without help from its replay queue.
        drop(response);

        // Re-arm the worker for the next round, as every other end-to-end test does: its slots
        // are what make the next task dispatchable at all.
        worker
            .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
            .await?;
        worker
            .send(Message::Text(
                encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
            ))
            .await?;
    }

    // The soak's point, exact: with the retention window closed and every stream ended, the pool
    // holds no relay row. Bounded at 15 s because each round's row goes when its record is
    // forgotten (~1 s) and the sweep that catches a late joiner's row needs that row's age.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let followed = handle.followed_jobs().await;
        if followed.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{ROUNDS} joins that raced their terminal left {} relay row(s) behind: {followed:?}",
                followed.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // And the same fact the way an operator reads it: these are the two lines that would have
    // carried the leak into a dashboard, and the soak ends with both of them at zero.
    let row_line = relay_gauge(nexus_core::metrics::names::RELAY_ROWS, 0);
    let subscriber_line = relay_gauge(nexus_core::metrics::names::RELAY_SUBSCRIBERS, 0);
    assert_eq!(
        metric_lines_until(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_ROWS,
            "the relay-row gauge to return to zero",
            |lines| lines == row_line.as_slice()
        )
        .await?,
        row_line,
        "forty joins that raced their terminal, and nothing left to report about them"
    );
    assert_eq!(
        metric_lines(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        )
        .await?,
        subscriber_line,
        "and no consumer queue outliving the stream that registered it"
    );
    Ok(())
}

/// The gauges, watched across **connection count** rather than across one socket's task list.
///
/// `one_long_lived_consumer_socket_leaves_no_per_task_state_behind` pins the one-socket,
/// many-tasks shape; `an_events_join_that_races_the_terminal_leaves_nothing_behind` pins the
/// join-that-races-its-terminal shape. Neither pins the shape the deployed hub actually served:
/// a **new connection per task**, arriving steadily, each one closed cleanly, on the two HTTP
/// faces (`POST /v1/chat/completions` streaming and `GET /v1/jobs/{id}/events`) that the
/// consumer socket shares its fan-out with. That is the shape the leak was attributed to —
/// ~48 kB retained per accepted task on a hub whose clients came and went — and it is the shape
/// a gauge has to be watched in, because a gauge that is right about one socket can still be
/// wrong about a hundred.
///
/// Two things are asserted per round, and both are exact: the queues this round registered are
/// retired (`Subscriber::Drop`), and the round's own relay row goes back to the hub when its
/// task ends. The round count is the point: a per-connection ratchet — a queue, a row, a
/// replay target, an observe line — would survive 24 rounds and fail here on the *aggregate*,
/// which is what the final `/metrics` pair pins at zero. `nexus_jobs_accepted_total` is the
/// anti-vacuity witness: it says 2*ROUNDS tasks really ran, so "zero rows" is a claim about
/// reclamation and not about nothing having happened.
#[tokio::test]
async fn the_relay_gauges_stay_flat_as_streams_keep_arriving() -> anyhow::Result<()> {
    // Retention, not the 5-minute default: a terminal record kept for five minutes keeps its
    // relay row with it, and this soak counts rows in seconds.
    let mut cfg = test_config();
    cfg.limits.job_retention_ms = 1_000;
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("relay_gauge_soak", cfg).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let client = reqwest::Client::new();
    let caps = WireCaps::default();
    const ROUNDS: usize = 12;
    // Held for the round-by-round assertions: `Arc::clone` keeps the fan-out readable after the
    // dispatcher has taken the row out of the map, which is the only way to tell "row gone"
    // apart from "queue stranded in a row nobody can find".
    let mut seen: Vec<Arc<crate::state::JobRelay>> = Vec::with_capacity(2 * ROUNDS);

    for round in 0..ROUNDS {
        // Face one: a streaming completion, read to the end — the ordinary HTTP client.
        let completion = client
            .post(format!("http://{}/v1/chat/completions", hub.addr))
            .bearer_auth(API_KEY)
            .json(&chat_body(&format!("soak-sse-{round:02}")))
            .send()
            .await?;
        assert_eq!(completion.status(), reqwest::StatusCode::OK);
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned { job: sse_job, .. } = assigned else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        let sse_relay = handle
            .lookup_relay(&sse_job)
            .await
            .expect("a streaming completion owns its fan-out row");
        seen.push(Arc::clone(&sse_relay));
        worker
            .send(Message::Text(
                encode_line(
                    &WorkerFrame::Frame {
                        job: sse_job.clone(),
                        seq: 1,
                        kind: FrameKind::Delta,
                        content_type: None,
                        text: format!("answer-{round:02}"),
                        artifact: None,
                    },
                    &caps,
                )?
                .into(),
            ))
            .await?;
        worker
            .send(Message::Text(
                encode_line(
                    &WorkerFrame::Done {
                        job: sse_job.clone(),
                        seq: 2,
                        finish: FinishKind::Stop,
                        usage: None,
                        error: None,
                    },
                    &caps,
                )?
                .into(),
            ))
            .await?;
        // Draining the body IS the client hanging up cleanly: the pump writes its last frame,
        // returns, and its `Subscriber` dies on that stack. Bounded on purpose: a pump that has
        // lost the exit its `Drop` depends on would otherwise park this test rather than fail it.
        let body = match tokio::time::timeout(Duration::from_secs(5), completion.text()).await {
            Ok(text) => text?,
            Err(_) => return Err(anyhow::anyhow!(
                "round {round}: the streamed completion's body never ended, so its pump never                  exited"
            )),
        };
        assert!(
            body.contains(&format!("answer-{round:02}")),
            "round {round}: the streamed answer did not arrive whole: {body}"
        );

        // Face two: a detached task followed on its own fresh `GET /events` connection, also
        // closed cleanly — the terminal ends the body, so the pump exits without a hangup.
        let response = client
            .post(format!("http://{}/v1/jobs", hub.addr))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({
                "kind": WS_KIND,
                "payload": { "prompt": format!("soak-events-{round:02}") },
                "detach": true,
            }))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let submitted: serde_json::Value = response.json().await?;
        let job = JobId::new_unchecked(
            submitted["job_id"]
                .as_str()
                .expect("a 202 names the job it accepted")
                .to_string(),
        );
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned {
            job: assigned_job, ..
        } = assigned
        else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        assert_eq!(assigned_job, job, "the worker is handed this task");
        let events_relay = handle
            .lookup_relay(&job)
            .await
            .expect("a watcher owns the row its submission created");
        seen.push(Arc::clone(&events_relay));
        let watcher = client
            .get(format!("http://{}/v1/jobs/{job}/events", hub.addr))
            .bearer_auth(API_KEY)
            .send()
            .await?;
        assert_eq!(watcher.status(), reqwest::StatusCode::OK);
        worker
            .send(Message::Text(
                encode_line(
                    &WorkerFrame::Done {
                        job: job.clone(),
                        seq: 1,
                        finish: FinishKind::Stop,
                        usage: None,
                        error: None,
                    },
                    &caps,
                )?
                .into(),
            ))
            .await?;
        // Running the follow to its end is the clean close: the terminal becomes `End`, the
        // body finishes, the pump returns. Bounded for the same reason as above, and `finish`
        // (`ConsumerEvent::event_name`, relay.rs:113) is the terminal this surface can end on.
        let transcript = match tokio::time::timeout(Duration::from_secs(5), watcher.text()).await {
            Ok(text) => text?,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "round {round}: the /events follow never ended, so its pump never exited"
                ))
            }
        };
        assert!(
            transcript.contains("finish"),
            "round {round}: the follow did not run to its terminal event: {transcript}"
        );

        // Re-arm the worker, exactly as the other soaks do: its slots are what make the next
        // round's two tasks dispatchable.
        for _ in 0..2 {
            worker
                .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
                .await?;
            worker
                .send(Message::Text(
                    encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
                ))
                .await?;
        }

        // Both of this round's streams are over, so both queues are gone. Across `round+1`
        // rounds that is `2*(round+1)` connections and, cumulatively, zero queues: the number
        // that ratcheted per connection on the deployed hub.
        let round_relays = vec![sse_relay, events_relay];
        bookkeeping_until(
            "this round's consumer queues to be retired with their streams",
            || {
                round_relays
                    .iter()
                    .all(|relay| relay.subscriber_count() == 0)
            },
        )
        .await?;
        for relay in &round_relays {
            assert_eq!(
                relay.subscriber_count(),
                0,
                "a closed stream left its queue"
            );
        }
    }

    // Every task this soak accepted has finished, so with retention at one second the pool ends
    // holding no relay row at all. Bounded, because the row goes on the actor's tick.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let followed = handle.followed_jobs().await;
        if followed.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{} rounds of fresh streams left {} relay row(s) behind: {followed:?}",
                2 * ROUNDS,
                followed.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Anti-vacuity: the soak really put 2*ROUNDS tasks through the faces it claims to have
    // soaked, so the zero below is about reclamation rather than about an empty hub.
    let accepted_line = format!(
        r#"nexus_jobs_accepted_total{{pool="{POOL}"}} {}"#,
        2 * ROUNDS
    );
    let accepted = metric_lines_until(
        &client,
        hub.addr,
        nexus_core::metrics::names::JOBS_ACCEPTED,
        "the accepted-job counter to reach the soak's task count",
        |lines| lines.iter().any(|line| line == &accepted_line),
    )
    .await?;
    assert!(
        accepted.iter().any(|line| line == &accepted_line),
        "the soak accepted {} tasks; the counter read {accepted:?}",
        2 * ROUNDS
    );

    // And the statement the deployed hub failed, read the way a dashboard reads it: after
    // 2*ROUNDS connections, both families are back at zero on every pool — i.e. the value did
    // not grow with the number of connections that ever existed.
    let row_line = relay_gauge(nexus_core::metrics::names::RELAY_ROWS, 0);
    let subscriber_line = relay_gauge(nexus_core::metrics::names::RELAY_SUBSCRIBERS, 0);
    assert_eq!(
        metric_lines_until(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_ROWS,
            "the relay-row gauge to be back at zero after the soak",
            |lines| lines == row_line.as_slice()
        )
        .await?,
        row_line,
        "connections do not accumulate rows"
    );
    assert_eq!(
        metric_lines(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        )
        .await?,
        subscriber_line,
        "and they do not accumulate consumer queues"
    );
    Ok(())
}

/// Streams **opened and abandoned** — never read, never reconnected — on both HTTP faces.
///
/// The two clean-close soaks (`a_finished_sse_body_leaves_no_subscriber_on_its_relay`,
/// `the_relay_gauges_stay_flat_as_streams_keep_arriving`) end their bodies, which lets a pump
/// exit through its normal return. The production shape is the other one: a client that opens
/// the stream and goes away without reading a byte — a tab closed, a fetch aborted, a caller
/// with a timeout — and does not come back. Nothing on the hub then ever writes to that queue
/// again *and* nothing errors in a way the pump can see while the task is silent, so the exit
/// has to come from the body channel closing (`tx.closed()` in the pumps) and the queue's
/// `Drop`, not from an event.
///
/// There is deliberately **no reconnect churn** here: no second join, no resume, no `detach`
/// frame. That isolation matters, because the fix under test is per-stream reclamation, and a
/// test that reconnects cannot tell "the abandoned stream released its state" apart from "the
/// next stream happened to clean it up".
#[tokio::test]
async fn abandoned_streams_on_both_http_faces_leave_no_row_and_no_queue() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.limits.job_retention_ms = 1_000;
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("abandoned_streams", cfg).await?;
    let pid = nexus_core::ids::PoolId::new_unchecked(POOL);
    let handle = hub
        .hub
        .pools
        .get(&pid)
        .expect("the sample topology owns the llm pool");
    let mut worker = dial_worker(hub.addr, true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let client = reqwest::Client::new();
    let caps = WireCaps::default();
    const ROUNDS: usize = 8;

    for round in 0..ROUNDS {
        // Abandonment one: a streaming completion dropped before a single SSE byte is read. The
        // task is dispatched and stays dispatched; the client is simply gone.
        let completion = client
            .post(format!("http://{}/v1/chat/completions", hub.addr))
            .bearer_auth(API_KEY)
            .json(&chat_body(&format!("abandon-sse-{round:02}")))
            .send()
            .await?;
        assert_eq!(completion.status(), reqwest::StatusCode::OK);
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned { job: sse_job, .. } = assigned else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        let sse_relay = handle
            .lookup_relay(&sse_job)
            .await
            .expect("the abandoned completion had its fan-out row while it was running");
        drop(completion); // hangs up mid-stream, having read nothing
                          // The hub noticed: a streaming completion is the `cancel_on_consumer_gone` shape, so the
                          // worker is told to stop. This is the witness that the abandonment was *recorded* by the
                          // actor and not merely survived by the transport.
        let cancel = next_frame_of(&mut worker, "cancel").await?;
        let HubFrame::Cancel { job: cancelled, .. } = cancel else {
            return Err(anyhow::anyhow!("expected a Cancel, got {cancel:?}"));
        };
        assert_eq!(cancelled, sse_job, "the cancel names the abandoned task");

        // Abandonment two: a detached task — which therefore keeps running — followed by a
        // `GET /events` that is opened and dropped without reading.
        let response = client
            .post(format!("http://{}/v1/jobs", hub.addr))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({
                "kind": WS_KIND,
                "payload": { "prompt": format!("abandon-events-{round:02}") },
                "detach": true,
            }))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let submitted: serde_json::Value = response.json().await?;
        let job = JobId::new_unchecked(
            submitted["job_id"]
                .as_str()
                .expect("a 202 names the job it accepted")
                .to_string(),
        );
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned {
            job: assigned_job, ..
        } = assigned
        else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        assert_eq!(assigned_job, job);
        let events_relay = handle
            .lookup_relay(&job)
            .await
            .expect("the abandoned follow had its fan-out row");
        let watcher = client
            .get(format!("http://{}/v1/jobs/{job}/events", hub.addr))
            .bearer_auth(API_KEY)
            .send()
            .await?;
        assert_eq!(watcher.status(), reqwest::StatusCode::OK);
        drop(watcher); // no bytes read, no second join

        // The detached task finishes normally; the abandoned completion does not, and needs no
        // `Done` because NEX-6.3 already deleted it.
        worker
            .send(Message::Text(
                encode_line(
                    &WorkerFrame::Done {
                        job: job.clone(),
                        seq: 1,
                        finish: FinishKind::Stop,
                        usage: None,
                        error: None,
                    },
                    &caps,
                )?
                .into(),
            ))
            .await?;
        worker
            .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
            .await?;
        worker
            .send(Message::Text(
                encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
            ))
            .await?;

        let abandoned = vec![sse_relay, events_relay];
        bookkeeping_until(
            "the abandoned streams' consumer queues to be retired",
            || abandoned.iter().all(|relay| relay.subscriber_count() == 0),
        )
        .await?;
        for relay in &abandoned {
            assert_eq!(
                relay.subscriber_count(),
                0,
                "round {round}: a stream that was opened and abandoned left its queue \
                 registered on a fan-out nobody will ever push to again"
            );
        }
    }

    // No relay row survives any of the abandoned streams: the completed task's row goes with its
    // terminal, the cancelled one's with the deletion NEX-6.3 makes when its last consumer
    // vanishes, and the actor's sweep is the backstop for a row a face registered late.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let followed = handle.followed_jobs().await;
        if followed.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{} abandoned streams left {} relay row(s) behind: {followed:?}",
                2 * ROUNDS,
                followed.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let row_line = relay_gauge(nexus_core::metrics::names::RELAY_ROWS, 0);
    let subscriber_line = relay_gauge(nexus_core::metrics::names::RELAY_SUBSCRIBERS, 0);
    assert_eq!(
        metric_lines_until(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_ROWS,
            "the relay-row gauge to be back at zero after the abandonments",
            |lines| lines == row_line.as_slice()
        )
        .await?,
        row_line,
        "a stream nobody ever read left its row on the pool"
    );
    assert_eq!(
        metric_lines(
            &client,
            hub.addr,
            nexus_core::metrics::names::RELAY_SUBSCRIBERS,
        )
        .await?,
        subscriber_line,
        "and a queue on it"
    );
    Ok(())
}

// =============================================================================
// Supplier pin: the consumer faces name the supplier they connected to
// =============================================================================
//
// A supplier's identity is **hub-minted** (`ids.rs`: one UUIDv4 per worker WebSocket,
// reported in `Welcome`, never declared by the supplier and never reused). A consumer
// may ask for one it has been given before — the optional `x-supplier-id` request
// header on the HTTP faces (`consumer.rs`'s `SUPPLIER_HEADER`) and the optional
// `submit.supplier` field on the consumer WebSocket (`protocol.rs`'s `ConsumerFrame`).
// The routing decision itself is core's (`Submission::supplier_hint` plus
// `PoolActor::supplier_pin_allows`, `crates/nexus-core/src/actor.rs:2302-2345`); what
// this section owns is the **wire contract** around it:
//
//   1. every response that connected a supplier names it, under exactly `x-supplier-id`,
//      whether or not the caller asked for that supplier by id;
//   2. a response that connected nobody names nobody — the header is **omitted**, never
//      sent empty, because `x-supplier-id: ""` reads as a supplier the client may pin
//      next time and will never see again;
//   3. the id is the hub's own lease record, not the caller's guess: a pin the hub has
//      never issued, a malformed one, and a blank one are all answered by whichever
//      supplier the router actually chose, and the header reports *that* one;
//   4. the pin never contradicts the lease, and the lease never clobbers a known id
//      (the mirror is first-write-wins — `state.rs`'s `JobRelay::note_supplier` and its
//      `a_known_supplier_survives_every_later_verdict` unit test).
//
// Unlike the core acceptance suite, these dial real sockets, so they are the only place
// the header's *timing* is observable: the SSE head cannot carry a supplier id until the
// lease exists, and `consumer.rs`'s `supplier_before_body` is what makes that ordering
// hold without inventing a new timer.

/// A minted-shaped id (`uuid::Uuid::new_v4` form: version nibble `4`, RFC 4122 variant
/// nibble `8`) that no supplier on a test hub has ever been issued. Pinning it is the
/// "an id this hub has never seen is a hint, not an error" case.
const NEVER_ISSUED: &str = "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f";

/// The `x-supplier-id` value a response carries, as a browser would read it.
///
/// An absent header is `None` and that is the whole contract: refusals, terminal joins,
/// and streams the hub flushed before any supplier claimed them all answer with no
/// value at all (`consumer.rs`'s `set_supplier`), and an empty string would be a lie.
fn supplier_of(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get("x-supplier-id")
        .map(|value| value.to_str().unwrap_or_default().to_owned())
}

/// The `x-nexus-job` value a response carries. Paired with [`supplier_of`], this is what
/// lets an assertion tie a *reported* supplier to a *leased* task: the id here is the id
/// the worker's `Assigned` frame carries, and the supplier behind it is the one that sent
/// that frame.
fn job_of(response: &reqwest::Response) -> String {
    response
        .headers()
        .get("x-nexus-job")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// A supplier the hub may dispatch to, plus the id the hub minted for it.
///
/// The id is read from `Welcome` (`two_suppliers_get_distinct_hub_assigned_ids_...` is
/// the suite's owner of that shape) because it is the only form the pin accepts: a
/// supplier does not choose its own id, so the id a consumer was handed last time is a
/// hub-minted UUID and nothing else. `dial_worker(ready:true)` has already sent `Ready`
/// and a four-job `Claim`, so this socket is warm capacity from the first frame.
async fn dial_supplier(
    addr: std::net::SocketAddr,
) -> anyhow::Result<(
    WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
)> {
    let mut ws = dial_worker(addr, true).await?;
    let id = match next_frame_of(&mut ws, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id.as_ref().to_owned(),
        other => {
            return Err(anyhow::anyhow!(
                "a supplier is owed a `welcome` naming its minted id, got {other:?}"
            ))
        }
    };
    Ok((ws, id))
}

/// Answer one claim the way the suite's baseline worker does: one delta frame carrying an
/// OpenAI-shaped chunk (the hub relays it verbatim, which is what puts `content` on the
/// consumer's wire), then `Done`.
///
/// The socket is re-armed with `Ready` + `Claim` afterwards — the same courtesy
/// `run_whole_task` documents: claim authority is consumed per dispatch, and a supplier
/// that stays warm between jobs is what keeps the *next* submission in the same test
/// dispatchable instead of silently queued.
async fn answer_claim(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    job: &JobId,
    content: &str,
) -> anyhow::Result<()> {
    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: serde_json::json!({
            "id": "chatcmpl-pin",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": null}],
        })
        .to_string(),
        artifact: None,
    };
    ws.send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    ws.send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;
    ws.send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
        .await?;
    ws.send(Message::Text(
        encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
    ))
    .await?;
    Ok(())
}

/// Read a streamed body to completion and pin the shape the supplier header depends on:
/// exactly three `chat.completion.chunk` payloads (role delta, the relayed frame, the
/// finish chunk) and one `[DONE]`.
///
/// This is not decoration. The header's contract is "the id rides on the head, and the
/// head may not flush before the lease" — so a test that only read the head could pass on
/// a hub that answered a stream with a head and no body. Reading to `[DONE]` is what makes
/// the hold observable as a *delay of the head*, never of the answer.
async fn streamed_body_of(response: reqwest::Response) -> anyhow::Result<String> {
    let body = tokio::time::timeout(Duration::from_secs(10), response.text()).await??;
    assert_eq!(
        body.matches("chat.completion.chunk").count(),
        3,
        "role delta + the relayed chunk + the finish chunk, no duplicates and none \
         swallowed by the supplier hold: {body}"
    );
    assert!(body.contains("[DONE]"), "the stream ends normally: {body}");
    Ok(body)
}

/// Read a `stream: false` response whole: one JSON body, the deltas assembled into a
/// `chat.completion`.
///
/// The body is returned rather than unwrapped, because the tests need two things from it:
/// the answer, and the `id` the aggregate face carries in place of the `x-nexus-job` header
/// it has never sent (`consumer.rs`'s `aggregate_response` builds that response by hand —
/// content type, `x-nexus-chunks`, and now `x-supplier-id` — and never calls
/// `response_headers`, which is where the job header is written on the streaming faces).
async fn assembled_of(response: reqwest::Response) -> anyhow::Result<serde_json::Value> {
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "the aggregate answers with a 200"
    );
    let body = tokio::time::timeout(
        Duration::from_secs(10),
        response.json::<serde_json::Value>(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the aggregate never answered"))??;
    Ok(body)
}

/// Make a completion request in its own task, so the caller can act on the hub *before*
/// the response object exists.
///
/// Both faces need this, for two different reasons: an aggregate writes its head only once
/// the terminal event arrives, so the claim has to be answered while the request is still
/// in flight (`a_non_streaming_retry_on_a_key_assembles_the_running_answer` is the model);
/// and a streamed request whose task is queued writes its head only when the lease arrives,
/// which is exactly the delay `consumer.rs`'s `supplier_before_body` accepts in exchange
/// for a supplier id it can put on that head.
fn spawn_completion(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    prompt: &str,
    pin: Option<&str>,
    key: Option<&str>,
    stream: bool,
) -> tokio::task::JoinHandle<anyhow::Result<reqwest::Response>> {
    let client = client.clone();
    let prompt = prompt.to_owned();
    let pin = pin.map(str::to_owned);
    let key = key.map(str::to_owned);
    tokio::spawn(async move {
        let mut request = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .bearer_auth(API_KEY)
            .json(&{
                let mut body = chat_body(&prompt);
                body["stream"] = serde_json::json!(stream);
                body
            });
        if let Some(value) = pin {
            request = request.header("x-supplier-id", value);
        }
        if let Some(value) = key {
            request = request.header("Idempotency-Key", value);
        }
        Ok(request.send().await?)
    })
}

/// Wait out a bounded window for any `Assigned` on this supplier and report the jobs it
/// was handed.
///
/// A pin that excludes a supplier is a statement about this socket's *silence*, and
/// silence is only evidence when the helper cannot mistake "nothing happened" for
/// "something happened late" — hence the bounded drain and the returned list rather than
/// a bare `timeout(...).is_err()`.
async fn drain_assigned(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    window: Duration,
) -> anyhow::Result<Vec<String>> {
    let mut leased = Vec::new();
    loop {
        let next = tokio::time::timeout(window, next_frame_of(ws, "assigned")).await;
        match next {
            Ok(assigned) => {
                let HubFrame::Assigned { job, .. } = assigned? else {
                    return Err(anyhow::anyhow!("only an `assigned` can arrive here"));
                };
                leased.push(job.as_ref().to_owned());
            }
            Err(_) => return Ok(leased),
        }
    }
}

/// The streamed completion names the supplier it was connected to — without being asked.
///
/// The caller pins nobody here. That is deliberate: the header's primary job is not to
/// echo a preference back, it is to report the lease, and the only way to tell those two
/// implementations apart is to ask for nothing and still be told who served the request.
/// The ordering claim lives here too: the head goes out before the worker has even seen
/// the task (`send()` returns before the `Assigned` is read below), and the id it carries
/// is the one the dispatcher reported on the same submit turn (`state.rs`'s
/// `Effect::SendWorker` arm mirrors the lease into the relay and resolves the gate).
#[tokio::test]
async fn a_streamed_completion_names_the_supplier_it_connected() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_stream_lease", test_config()).await?;
    let (mut worker, supplier) = dial_supplier(hub.addr).await?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("who served me?"))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "a warm pool opens the stream"
    );
    assert_eq!(
        supplier_of(&response),
        Some(supplier.clone()),
        "the head names the supplier this request was connected to, on a request that \
         pinned nothing"
    );

    // The id is not free-floating: it is the id of the task the supplier is about to be
    // handed, so "the header matches the lease" is a statement about a real dispatch.
    let job = job_of(&response);
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        job,
        "the task the header belongs to is the task the hub just leased"
    );
    assert!(
        serde_json::to_string(&assigned)?.contains("who served me?"),
        "and it is this request's task: {assigned:?}"
    );

    answer_claim(&mut worker, &leased, "the hub names its supplier").await?;
    let body = streamed_body_of(response).await?;
    assert_eq!(
        body.matches("the hub names its supplier").count(),
        1,
        "the supplier hold changed when the head flushed, not what the consumer was \
         answered: {body}"
    );
    Ok(())
}

/// The aggregated (`stream: false`) completion names its supplier too — and does not need
/// the hold at all.
///
/// An aggregate writes its head exactly once, when the answer is assembled, and an answer
/// presupposes a lease. So `consumer.rs`'s `aggregate_response` just reads the relay at
/// assembly time rather than waiting: `x-nexus-chunks` and `x-supplier-id` land on the
/// same head, and a request that never got a supplier never reaches that line (it answers
/// the `no_ready_worker` refusal, which names nobody).
#[tokio::test]
async fn an_aggregated_completion_names_the_supplier_it_connected() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_aggregate_lease", test_config()).await?;
    let (mut worker, supplier) = dial_supplier(hub.addr).await?;

    let client = reqwest::Client::new();
    let request = spawn_completion(&client, hub.addr, "aggregate me", None, None, false);
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    answer_claim(&mut worker, &job, "the aggregate names its supplier").await?;

    let response = request
        .await
        .map_err(|err| anyhow::anyhow!("the aggregate request task panicked: {err}"))??;
    // The header first, then the body: `json()` takes the response by value. There is no
    // `x-nexus-job` to pair with here — the aggregate face has never sent one, and T2 does
    // not add one — so the pairing is the answer itself: `job` is the task that was leased
    // on the only supplier's socket, and this is the text that supplier relayed for it.
    assert_eq!(
        supplier_of(&response),
        Some(supplier.clone()),
        "the id is on the one head an aggregate writes, and it is the id of the lease \
         that produced the answer"
    );
    assert_eq!(
        response.headers()["x-nexus-chunks"]
            .to_str()
            .unwrap_or_default(),
        "1",
        "the aggregate's own chunk accounting is untouched by the supplier header"
    );
    let body = assembled_of(response).await?;
    assert_eq!(
        body["choices"][0]["message"]["content"].as_str(),
        Some("the aggregate names its supplier"),
        "and the body is still the assembled answer: {body}"
    );
    Ok(())
}

/// A refusal names no supplier, however confident the header on the request is.
///
/// Both refusals here are made *with* a satisfiable or plausible pin, because that is the
/// case a careless implementation gets wrong: a hub that echoes the request's header back
/// on every response would pass a naive round-trip test and still tell the caller about a
/// supplier that was never connected.
///
/// * `413 payload_too_large` — refused in the actor's payload check, before the queue,
///   before any dispatch, with the named supplier live and idle on the other socket.
/// * `503 no_ready_worker` — the grace window lapsing with the supplier connected but
///   never ready (`dial_worker(false)`: `hello`, no `Ready`), which is NEX-3.3's
///   "no body at all" case, and no body means no supplier either.
#[tokio::test]
async fn a_refusal_names_no_supplier_however_confident_the_header_is() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.limits.tenant_max_payload_bytes = 1;
    let hub = spawn_hub("pin_refusal_413", cfg).await?;
    let (_worker, supplier) = dial_supplier(hub.addr).await?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &supplier)
        .json(&chat_body("refuse me"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        supplier_of(&response),
        None,
        "the named supplier is live and idle on the other socket, and the hub still names \
         nobody: the request was refused before the queue, so it leased nothing"
    );
    assert_eq!(
        response.json::<serde_json::Value>().await?["error"]["code"],
        "payload_too_large",
        "the refusal is the one the payload check gives"
    );

    // Connected, never ready: the grace window has to lapse.
    let hub = spawn_hub("pin_refusal_503", test_config()).await?;
    let mut idle = dial_worker(hub.addr, false).await?;
    // The handshake has to be accepted before the consumer arrives, or this would measure
    // "no workers at all" instead of "a worker that is not ready" — the ordering
    // `loading_worker_answers_503_with_retry_after_before_any_body` pins for its own case.
    assert!(
        matches!(
            next_frame_of(&mut idle, "welcome").await?,
            HubFrame::Welcome { .. }
        ),
        "the not-ready supplier is attached before the request is made"
    );
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", NEVER_ISSUED)
        .json(&chat_body("nobody is ready"))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "the grace window answers with the retryable refusal"
    );
    assert_eq!(
        supplier_of(&response),
        None,
        "a request that was never leased names no supplier — the client must not read a \
         pin it can reuse out of a refusal"
    );
    let _ = idle;
    Ok(())
}

/// The same refusals, asserted as the *absence* of the header rather than as a value.
///
/// Kept separate from the status assertions above only because the two refusals share one
/// test and the absence needs to be stated once, plainly, for both.
#[tokio::test]
async fn a_refusal_that_leased_nobody_carries_no_supplier_header() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_refusal_absence", test_config()).await?;
    let (_worker, supplier) = dial_supplier(hub.addr).await?;
    let client = reqwest::Client::new();

    // The one refusal this hub can produce deterministically with a live supplier: the
    // async job route's `detach` rule (`400`), which refuses before the task exists.
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &supplier)
        .json(&serde_json::json!({"kind": WS_KIND, "payload": {"prompt": "no detach"}, "detach": false}))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "this route requires `detach: true`"
    );
    // Read off the head before the body is consumed: `json()` takes the response by value,
    // and the header this is about travels on the head.
    let named = supplier_of(&response);
    let code = response.json::<serde_json::Value>().await?["error"]["code"].clone();
    assert_eq!(
        code, "detach_required",
        "the refusal is the route's own rule, not a routing failure"
    );
    assert_eq!(
        named, None,
        "a refusal carries no supplier, and the id the caller sent does not become one"
    );
    Ok(())
}

/// A join of a completion that already finished names no supplier.
///
/// The task's lease is gone by then — the status view reports `"lease": null`, which is
/// the same fact the header is built from, and the hub does not resurrect the id of a
/// supplier that is no longer attached to this task. The first response on the key keeps
/// the id; the second one, which is a receipt of an answer that already ran, does not.
#[tokio::test]
async fn a_join_of_a_finished_completion_names_no_supplier() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_terminal_join", test_config()).await?;
    let (mut worker, supplier) = dial_supplier(hub.addr).await?;

    let client = reqwest::Client::new();
    let key = "pin-terminal-join";
    let first = spawn_completion(
        &client,
        hub.addr,
        "answer once",
        Some(&supplier),
        Some(key),
        false,
    );
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    answer_claim(&mut worker, &job, "one answer, one execution").await?;

    let first = first
        .await
        .map_err(|err| anyhow::anyhow!("the first request task panicked: {err}"))??;
    assert_eq!(
        supplier_of(&first),
        Some(supplier.clone()),
        "the request that ran the task is told who ran it"
    );
    let first_body = assembled_of(first).await?;
    assert_eq!(
        first_body["choices"][0]["message"]["content"].as_str(),
        Some("one answer, one execution"),
        "and is answered normally: {first_body}"
    );

    // The join: same key, same body, now that the task is Finished.
    let join = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", NEVER_ISSUED)
        .header("Idempotency-Key", key)
        .json(&{
            let mut body = chat_body("answer once");
            body["stream"] = serde_json::json!(false);
            body
        })
        .send()
        .await?;
    assert_eq!(
        join.status(),
        reqwest::StatusCode::OK,
        "a retry on a finished task is an answer, not a failure"
    );
    // Header before body (`json()` takes the response), and the body then pairs this
    // response to the first one by its `id` — the aggregate face carries the task identity
    // there, not in a header.
    assert_eq!(
        supplier_of(&join),
        None,
        "a join of a terminal task has no lease to name — the hub does not report a \
         supplier it is no longer connected to, and the id the caller pinned on the join \
         does not become one"
    );
    let join_body = assembled_of(join).await?;
    assert_eq!(
        join_body["id"], first_body["id"],
        "the join answers the very task the first request ran, rather than minting a \
         second one: {join_body} / {first_body}"
    );
    assert_eq!(
        join_body["choices"][0]["message"]["content"].as_str(),
        Some("one answer, one execution"),
        "and its body is the recorded answer, re-run by nobody: {join_body}"
    );

    // The independent evidence for that omission, off the header entirely: the task's own
    // status view, which is where `join_existing_job` reads the lease from.
    let view = client
        .get(format!(
            "http://{}/v1/jobs/{job}",
            hub.addr,
            job = job.as_ref()
        ))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        view["state"].as_str(),
        Some("Finished"),
        "the task is terminal: {view}"
    );
    assert!(
        view["lease"].is_null(),
        "and it has no lease left, which is exactly why the join above names no \
         supplier: {view}"
    );
    Ok(())
}

/// An id the hub has never issued is not an error, and cannot name itself back.
///
/// Three shapes of "the caller is wrong about the id", all answered by the only supplier
/// on the hub and reported as that supplier:
///
/// * a minted-shaped UUID no connection ever held — the actor's pin relaxation
///   (`actor.rs:2328`: a hint that does not name a `Live` worker is no pin at all);
/// * a value that is not an id shape at all — the face wraps without validating
///   (`consumer.rs`'s `supplier_hint` uses `WorkerId::new_unchecked`, deliberately,
///   because only the router, which holds the live registry, can judge an id, and its
///   judgement is "no pin", never a `4xx`);
/// * a header present but blank — trimmed to `None`, i.e. byte-identical to sending no
///   header, which is the rule the feature's contract is built on.
///
/// In every case the echoed id is the *leased* one, never the requested one.
#[tokio::test]
async fn a_pin_the_hub_has_never_issued_is_answered_by_the_supplier_it_can() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_unknown_accepted", test_config()).await?;
    let (mut worker, supplier) = dial_supplier(hub.addr).await?;
    let client = reqwest::Client::new();

    let pins = [
        ("a minted-shaped id no supplier ever had", NEVER_ISSUED),
        (
            "a value that is not an id shape at all",
            "alpha main:colon-not-an-id",
        ),
        ("a header that is present but blank", "   "),
    ];
    for (why, pin) in pins {
        let response = client
            .post(format!("http://{}/v1/chat/completions", hub.addr))
            .bearer_auth(API_KEY)
            .header("x-supplier-id", pin)
            .json(&chat_body(why))
            .send()
            .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "{why} is a hint the hub may ignore, not a request it may refuse"
        );
        assert_eq!(
            supplier_of(&response),
            Some(supplier.clone()),
            "{why}: the header reports the supplier that was connected, never the id the \
             caller invented"
        );
        let job = job_of(&response);
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned { job: leased, .. } = assigned.clone() else {
            return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
        };
        assert_eq!(
            leased.as_ref(),
            job,
            "{why}: the task named in the header is the task that was leased"
        );
        assert!(
            serde_json::to_string(&assigned)?.contains(why),
            "{why}: and it carries this request's payload: {assigned:?}"
        );
        answer_claim(&mut worker, &leased, &format!("answered: {why}")).await?;
        let body = streamed_body_of(response).await?;
        assert_eq!(
            body.matches(&format!("answered: {why}")).count(),
            1,
            "{why}: the request was answered normally: {body}"
        );
    }
    Ok(())
}

/// Two suppliers, one pin: the pin decides which socket does the work.
///
/// Both requests name `B`, which is also the supplier that answers them, while `A` —
/// identically warm, identically model-eligible, and first on the socket — is handed
/// nothing. Without the pin the hub dispatches across both suppliers, so `A`'s silence is
/// the half of the proof that cannot be had from the response headers alone: two `200`s
/// naming `B` would also be produced by a hub that leased one task to each and reported
/// `B` twice.
#[tokio::test]
async fn pinning_every_request_to_one_supplier_excludes_the_other() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_excludes_peer", test_config()).await?;
    let (mut other, other_id) = dial_supplier(hub.addr).await?;
    let (mut pinned, pinned_id) = dial_supplier(hub.addr).await?;
    assert_ne!(
        other_id, pinned_id,
        "the two sockets are two suppliers with two minted ids, so the exclusion below is \
         about a leg and not about one connection dialed twice: {other_id} / {pinned_id}"
    );

    let client = reqwest::Client::new();
    let first = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &pinned_id)
        .json(&chat_body("the first pinned prompt"))
        .send()
        .await?;
    let second = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &pinned_id)
        .json(&chat_body("the second pinned prompt"))
        .send()
        .await?;
    for (prompt, response) in [("first", &first), ("second", &second)] {
        assert_eq!(
            supplier_of(response),
            Some(pinned_id.clone()),
            "the {prompt} request is served by the supplier it pinned"
        );
    }
    let jobs = std::collections::BTreeSet::from([job_of(&first), job_of(&second)]);
    assert_eq!(jobs.len(), 2, "two prompts are two tasks: {jobs:?}");

    // The excluded supplier: warm, claiming, and given the full window to say otherwise.
    let stole = drain_assigned(&mut other, Duration::from_millis(1_200)).await?;
    assert!(
        stole.is_empty(),
        "the supplier the caller did not name was handed {stole:?}: a pin is a \
         reservation for that supplier, not a tiebreaker the router may ignore"
    );

    // And the included one holds both leases, each carrying its own request's payload —
    // which is what ties *these* responses to *that* socket rather than to a hub that
    // reported one id for two different legs.
    let mut leased: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let assigned = next_frame_of(&mut pinned, "assigned").await?;
        let HubFrame::Assigned { job, .. } = assigned.clone() else {
            return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
        };
        let raw = serde_json::to_string(&assigned)?;
        let prompt = ["the first pinned prompt", "the second pinned prompt"]
            .into_iter()
            .find(|prompt| raw.contains(prompt))
            .unwrap_or_default()
            .to_owned();
        assert!(
            !prompt.is_empty(),
            "the claim carries one of the two pinned payloads: {assigned:?}"
        );
        leased.insert(job.as_ref().to_owned(), prompt);
    }
    assert_eq!(
        leased,
        std::collections::BTreeMap::from([
            (job_of(&first), "the first pinned prompt".to_owned()),
            (job_of(&second), "the second pinned prompt".to_owned()),
        ]),
        "the two tasks on the pinned supplier's socket are exactly the two the caller \
         pinned there, each carrying the payload of the request whose header named it — \
         the pin selected the leg, and the header reported the selection"
    );

    for (job, prompt) in leased {
        answer_claim(
            &mut pinned,
            &JobId::new_unchecked(&job),
            &prompt.replace(" prompt", " answer"),
        )
        .await?;
    }
    for (prompt, response) in [
        ("the first pinned answer", first),
        ("the second pinned answer", second),
    ] {
        let body = streamed_body_of(response).await?;
        assert_eq!(
            body.matches(prompt).count(),
            1,
            "each caller got its own answer, in the order its task was leased: {body}"
        );
    }
    Ok(())
}

/// The WebSocket face pins on the frame plane and reports on the same plane.
///
/// There is no header on `/v1/consumer`: the pin is `submit.supplier` and the answer is
/// `accepted.supplier`, both typed `Option<WorkerId>` (`protocol.rs:616-642`) — which is
/// also why an id this hub has never issued still deserializes and still cannot make the
/// submit a failure. The verdict is the only frame that carries the field, so the
/// terminal attach of the finished task is the natural negative case: the same task,
/// joined after it stopped running, with no supplier named.
#[tokio::test]
async fn a_pinned_consumer_socket_names_the_supplier_on_its_accepted_frame() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_ws_accepted", test_config()).await?;
    let (mut worker, supplier) = dial_supplier(hub.addr).await?;
    let mut consumer = dial_consumer(hub.addr).await?;

    send_consumer(
        &mut consumer,
        &serde_json::json!({
            "type": "submit",
            "kind": WS_KIND,
            "payload": {"prompt": "pin me over the socket"},
            "detach": false,
            "supplier": &supplier,
        }),
    )
    .await?;
    let job = match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job,
            kind,
            pool,
            detached,
            supplier: named,
        } => {
            assert_eq!(kind, WS_KIND, "the verdict echoes the kind it accepted");
            assert_eq!(pool.as_ref(), POOL);
            assert!(!detached, "this caller stays for its stream");
            assert_eq!(
                named.as_ref().map(|id| id.as_ref()),
                Some(supplier.as_str()),
                "the socket's pin was honoured, and the verdict names the supplier it \
                 connected this task to"
            );
            job
        }
        other => {
            return Err(anyhow::anyhow!(
                "a submit is answered with `accepted`, got {other:?}"
            ))
        }
    };

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased, job,
        "the task the verdict names is the task the pinned supplier was handed"
    );
    assert!(
        serde_json::to_string(&assigned)?.contains("pin me over the socket"),
        "and it carries this submit's payload: {assigned:?}"
    );

    // The answer, on both sockets, in the vocabulary each one speaks.
    let usage = serde_json::json!({"completion_tokens": 1});
    worker
        .send(Message::Text(
            encode_line(
                &WorkerFrame::Frame {
                    job: job.clone(),
                    seq: 1,
                    kind: FrameKind::Delta,
                    content_type: None,
                    text: "pinned over the socket".to_owned(),
                    artifact: None,
                },
                &WireCaps::default(),
            )?
            .into(),
        ))
        .await?;
    worker
        .send(Message::Text(
            encode_line(
                &WorkerFrame::Done {
                    job: job.clone(),
                    seq: 2,
                    finish: FinishKind::Stop,
                    usage: Some(usage.clone()),
                    error: None,
                },
                &WireCaps::default(),
            )?
            .into(),
        ))
        .await?;
    expect_consumer_meta(&mut consumer, &job).await?;
    // Re-arm the supplier for the submit at the end of this test: claim authority is
    // consumed per dispatch (the courtesy `run_whole_task` documents), and a supplier that
    // never re-claims would leave that last submit sitting in the queue with no verdict to
    // assert on.
    let caps = WireCaps::default();
    worker
        .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
        .await?;
    worker
        .send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
        ))
        .await?;
    for want in [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "pinned over the socket".to_owned(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ] {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // The join of that now-finished task, on the same socket: a verdict, but no supplier.
    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "attach", "job": job.to_string(), "from_seq": 0}),
    )
    .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: WS_KIND.to_string(),
            pool: nexus_core::ids::PoolId::new_unchecked(POOL),
            detached: false,
            // The task is Finished, its lease is gone, and `attach` cannot put a supplier
            // back on a task that stopped running. Absent is the honest answer, and the
            // client's own contract is to read the id from the task's status view instead
            // (`protocol.rs:675-687`).
            supplier: None,
        },
    )
    .await?;

    // The replay that join is owed: the same four frames the first stream carried. Read
    // to the end rather than left in the socket's queue, because the next submit's verdict
    // is only the *next frame* once these have been consumed — and reading them is the
    // proof that this socket is still a healthy subscriber, so the absent `supplier` above
    // is a fact about the finished task and not a hub that stopped serving the join.
    expect_consumer_meta(&mut consumer, &job).await?;
    for want in [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "pinned over the socket".to_owned(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            // Byte-for-byte the `usage` the worker sent in its `Done` above: the replay is
            // the same answer, so the frame the hub owes this join is identical.
            usage: Some(serde_json::json!({"completion_tokens": 1})),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ] {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // And a pin this hub has never issued: still accepted, still routed, and named by the
    // supplier that actually took the task — the frame plane relaxes an unknown id exactly
    // as the HTTP header does, because both feed the same `supplier_hint`.
    send_consumer(
        &mut consumer,
        &serde_json::json!({
            "type": "submit",
            "kind": WS_KIND,
            "payload": {"prompt": "pin me to a supplier that never existed"},
            "detach": false,
            "supplier": NEVER_ISSUED,
        }),
    )
    .await?;
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted {
            supplier: named, ..
        } => assert_eq!(
            named.as_ref().map(|id| id.as_ref()),
            Some(supplier.as_str()),
            "an id the hub has never issued is a hint it may drop, not a submit it may \
             refuse — and the verdict names the supplier it really connected"
        ),
        other => {
            return Err(anyhow::anyhow!(
                "a submit is answered with `accepted`, got {other:?}"
            ))
        }
    }
    Ok(())
}

/// The head waits for the lease, and then names it.
///
/// This is `consumer.rs`'s `supplier_before_body` observed from outside, and the only place
/// its **timing** is observable at all: the acceptance suite drives the actor with injected
/// time and never sees a response head, and every other test here has a supplier that claims
/// immediately, where the wait is a no-op.
///
/// THE STATE THAT PRODUCES IT. The supplier is warm with four free slots and no claim
/// authority left: its single `Claim{jobs:1}` was spent on the first task. It is the claim,
/// not the slot count, that gates a dispatch — `observe_documents_...` owns the
/// `outstanding_claims` row that shows the two numbers disagreeing, and
/// `dial_ready_but_idle_worker` documents the warm-but-claimless state as one where the
/// record "reaches `Queued`". A second request is therefore admitted into the queue, and the
/// verdict its face receives is `Effect::Accepted{worker: None}` (`actor.rs:1303`): a verdict
/// with no worker, hence with no supplier id to carry.
///
/// WHY HOLDING IS LEGAL. NEX-3.3 promises no *body bytes* before the actor says the request is
/// serviceable, and none are sent: the head is not the body, the task was admitted, and the
/// hold is bounded by the actor's own `claim_deadline_ms` for the kind (600 s at these
/// settings — a strictly earlier moment than any the surface would answer at anyway, since a
/// task that is never leased sits until that deadline lapses). No new timer, no new knob.
///
/// WHAT IS ASSERTED, AND IN WHICH ORDER. First that the hub holds the second task **queued**
/// and holds no lease for it — waited for on `/v1/observe` rather than slept through, because
/// the missing lease is the fact the missing header would have to contradict. Then that the
/// request's head is still shut at that moment. Then that the head opens *with the id* the
/// moment the supplier re-claims and takes the task. The middle assertion is what makes the
/// other two mean anything: an implementation that flushed the head early and named nobody
/// would satisfy "header present, id correct" on the last one alone.
#[tokio::test]
async fn a_queued_completion_holds_its_head_until_the_lease_it_will_name() -> anyhow::Result<()> {
    let hub = spawn_hub("pin_head_hold", test_config()).await?;
    let mut worker = dial_worker(hub.addr, false).await?;
    let supplier = match next_frame_of(&mut worker, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id.as_ref().to_owned(),
        other => {
            return Err(anyhow::anyhow!(
                "the supplier is owed a `welcome` naming its minted id, got {other:?}"
            ))
        }
    };
    let caps = WireCaps::default();
    worker
        .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
        .await?;
    // One claim slot, so the first task exhausts the supplier's claim authority and the next
    // request queues behind it rather than being leased.
    worker
        .send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(1) }, &caps)?.into(),
        ))
        .await?;

    let client = reqwest::Client::new();
    let busy = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("the task holding the claim"))
        .send()
        .await?;
    assert_eq!(
        busy.status(),
        reqwest::StatusCode::OK,
        "a leased task answers its head immediately"
    );
    let busy_job = job_of(&busy);
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        busy_job,
        "the head that went out at once belongs to the task that was leased"
    );

    // The second request, while the claim is still spent.
    let held = spawn_completion(
        &client,
        hub.addr,
        "the task that waits for a claim",
        None,
        None,
        true,
    );

    // Wait for the admission to be a visible fact, and for the state that leaves the hub with
    // nothing to name: the task queued, and no lease row for it.
    let url = format!("http://{}/v1/observe", hub.addr);
    let doc = observe_until(
        &client,
        &url,
        "a second task queued behind the spent claim",
        |doc| {
            doc["jobs"].as_array().is_some_and(|rows| {
                rows.len() == 2 && rows.iter().any(|row| row["phase"] == "queued")
            })
        },
    )
    .await?;
    let phases: std::collections::BTreeSet<&str> = doc["jobs"]
        .as_array()
        .expect("jobs array")
        .iter()
        .filter_map(|row| row["phase"].as_str())
        .collect();
    assert_eq!(
        phases,
        std::collections::BTreeSet::from(["assigned", "queued"]),
        "one task is leased and the other is waiting for a claim — which is exactly the \
         state a head has no supplier id to carry: {doc}"
    );
    let leases = doc["leases"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        leases.len(),
        1,
        "and the hub holds one lease, for the task that got the claim: {doc}"
    );
    assert_eq!(
        leases[0]["job"].as_str(),
        Some(busy_job.as_str()),
        "which is the first task's, not the queued one's: {doc}"
    );
    assert!(
        !held.is_finished(),
        "the queued request's head is still shut while it has no lease: an implementation \
         that flushed here would have had no supplier to name, and would pass every assertion \
         below with no header at all"
    );

    // The supplier re-claims on its way out of the first task (what a real supplier does
    // between jobs, and what `answer_claim` does for the suite): the queued task is leased,
    // and that one event both writes the relay's supplier cell (`Effect::SendWorker` →
    // `Assigned{worker}`, mirrored in `state.rs`) and releases the head.
    answer_claim(&mut worker, &leased, "the answer that frees the claim").await?;
    let response = held
        .await
        .map_err(|err| anyhow::anyhow!("the held request task panicked: {err}"))??;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "the wait ends in a normal stream, not in a refusal"
    );
    assert_eq!(
        supplier_of(&response),
        Some(supplier.clone()),
        "and it carries the id of the supplier that eventually leased the task: the wait \
         bought the header rather than merely delaying the head"
    );
    let job = job_of(&response);
    assert_ne!(
        job, busy_job,
        "the held request is its own task, and the head it waited for is its own: {job} / \
         {busy_job}"
    );
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned.clone() else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        job,
        "the task that had been queued is the one now leased to that supplier — the header \
         names a lease, and this is it"
    );
    answer_claim(&mut worker, &leased, "the answer the queued request got").await?;

    let held_body = streamed_body_of(response).await?;
    assert_eq!(
        held_body
            .matches("the answer the queued request got")
            .count(),
        1,
        "a head that waited for its supplier still carries the whole answer, once: {held_body}"
    );
    let busy_body = streamed_body_of(busy).await?;
    assert_eq!(
        busy_body.matches("the answer that frees the claim").count(),
        1,
        "and the task that was already streaming is untouched by the one that queued behind \
         it: {busy_body}"
    );
    Ok(())
}

/// A hang-up inside the header hold cancels the queued task instead of letting it age out.
///
/// WHY THIS HANG-UP IS A DIFFERENT ANIMAL FROM THE OTHERS IN THIS FILE.
/// `an_idle_sse_hangup_cancels_the_worker_without_a_failed_write` and
/// `a_hangup_on_a_silent_running_task_releases_its_watcher` both tear down a stream whose head
/// has already gone out. In that position the hub has a pump watching the body channel, and the
/// pump's first poll notices the dropped receiver and posts the `Detach` the streaming contract
/// requires (NEX-6.1). During the head hold (`consumer.rs`'s `supplier_before_body`) there is no
/// pump to notice: the task is queued, so the face has written nothing, and the body — with its
/// channel, its `ReceiverStream`, and the watcher that would have seen the hang-up — is not
/// created until the hold returns. Instrumenting the pump proved the shape: on a mid-hold
/// hang-up the pump starts **already closed**, `tx.closed()` resolves on its first poll, and the
/// detach it posts is a whole claim window late — the window during which the hub still believes
/// the abandoned task is its own business, and a pinned supplier still refuses every other
/// consumer's claim for it (`supplier_pin_allows`, `actor.rs`), turning a consumer's exit into a
/// starvation nobody can cancel.
///
/// WHAT MAKES "RELEASED NOW" OBSERVABLE RATHER THAN MERELY FASTER. The abandoned task's **exit
/// reason**, read from `/metrics`: `nexus_jobs_terminal_total` is labelled with the exact
/// `TerminalReason` the actor applied when it terminated the task (`actor.rs`, the
/// `incr_label(JOBS_TERMINAL, "reason", applied.as_str())` in the terminal helper). A hub that
/// notices the hang-up runs the consumer-gone rule — cancel the queued task and delete its row
/// (NEX-6.3) — and the counter reads `reason="cancelled"`. A hub that does not notice lets the
/// task sit until the kind's `claim_deadline_ms` lapses, and the counter reads
/// `reason="queue_timeout"` (`actor.rs`'s `QueueTimeout` arm). Those two labels are the whole
/// difference between "the consumer's exit ended the task" and "the timer ended it", and each is
/// asserted by exact value. The window is then shortened by **config**, not by a new knob:
/// `claim_deadline_ms` is the same field the hold itself reads as its bound
/// (`consumer.rs`'s `claim_window_ms`), so the wrong implementation reaches its own verdict
/// inside the test's budget instead of ten minutes from now.
///
/// THE WAIT IS A SETTLE, NOT A SLEEP. `metric_lines_until` waits for `cancelled` to appear; a
/// hub that never posts the detach never produces that series and fails there, with its last
/// reading in the message. Everything asserted after the settle is an exact number, including
/// `queue_timeout == 0`, which is the assertion that would fail on the late exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hangup_inside_the_head_hold_cancels_the_queued_task_it_never_leased(
) -> anyhow::Result<()> {
    let mut cfg = test_config();
    // Both bounds at once, deliberately: this is how long a healthy wait may last, and how long
    // an unnoticed hang-up would keep its task.
    cfg.kinds
        .get_mut("llm.chat")
        .expect("the sample topology registers llm.chat")
        .claim_deadline_ms = Some(5_000);
    cfg.validate().expect("test config must validate");
    let hub = spawn_hub("pin_hold_hangup", cfg).await?;
    let mut worker = dial_worker(hub.addr, false).await?;
    let supplier = match next_frame_of(&mut worker, "welcome").await? {
        HubFrame::Welcome { worker_id, .. } => worker_id.as_ref().to_owned(),
        other => {
            return Err(anyhow::anyhow!(
                "the supplier is owed a `welcome` naming its minted id, got {other:?}"
            ))
        }
    };
    let caps = WireCaps::default();
    worker
        .send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
        .await?;
    // One claim slot, so the first task exhausts the supplier's claim authority and the next one
    // queues behind it — the state in which the face holds its head (`Accepted{worker: None}`).
    worker
        .send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(1) }, &caps)?.into(),
        ))
        .await?;

    let client = reqwest::Client::new();
    let busy = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("the task holding the claim"))
        .send()
        .await?;
    assert_eq!(
        busy.status(),
        reqwest::StatusCode::OK,
        "a leased task answers its head immediately"
    );
    let busy_job = job_of(&busy);
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        busy_job.as_str(),
        "the head that went out at once belongs to the task that was leased"
    );

    // The second request, pinned to that same busy supplier, while its claim is still spent.
    let held = spawn_completion(
        &client,
        hub.addr,
        "the task whose consumer leaves",
        Some(&supplier),
        None,
        true,
    );
    let url = format!("http://{}/v1/observe", hub.addr);
    let doc = observe_until(
        &client,
        &url,
        "a second task queued behind the spent claim",
        |doc| {
            doc["jobs"].as_array().is_some_and(|rows| {
                rows.len() == 2 && rows.iter().any(|row| row["phase"] == "queued")
            })
        },
    )
    .await?;
    let held_job = doc["jobs"]
        .as_array()
        .expect("jobs array")
        .iter()
        .find(|row| row["phase"] == "queued")
        .and_then(|row| row["id"].as_str())
        .expect("the queued row carries its id")
        .to_owned();
    assert!(
        !held.is_finished(),
        "the queued request's head is still shut — which is the point: a face that flushed the \
         head here would leave the hang-up below happening outside the hold, where the pump \
         already handles it: {doc}"
    );

    // The consumer hangs up mid-hold: dropping the request task drops the connection, and hyper
    // drops the handler future in the same moment — the only signal the hold can be given.
    held.abort();

    // The exit reason is the test. `cancelled` exists only if the hub turned that dropped future
    // into a `Detach`; `queue_timeout` would be the late exit at the window's end.
    let lines = metric_lines_until(
        &client,
        hub.addr,
        "nexus_jobs_terminal_total",
        "the abandoned task to be cancelled",
        |lines| terminal_count(lines, "cancelled") == 1,
    )
    .await?;
    assert_eq!(
        terminal_count(&lines, "cancelled"),
        1,
        "the consumer's exit ended the task it had abandoned: {lines:?}"
    );
    assert_eq!(
        terminal_count(&lines, "queue_timeout"),
        0,
        "and nothing about it waited for the claim window: a hub that never noticed the hang-up \
         would end this task as a queue timeout instead — which is also the window during which \
         its supplier pin keeps excluding every other supplier: {lines:?}"
    );

    // The row goes with it (NEX-6.3 deletes rather than retains a task nobody is watching), and
    // the supplier that was the only one the pin allowed is handed nothing stale when its claim
    // finally frees.
    observe_until(
        &client,
        &url,
        "the abandoned task's row to be gone from the hub",
        |doc| {
            !doc["jobs"]
                .as_array()
                .is_some_and(|rows| rows.iter().any(|row| row["id"] == held_job))
        },
    )
    .await?;
    answer_claim(&mut worker, &leased, "the answer that frees the claim").await?;
    let stole = drain_assigned(&mut worker, Duration::from_millis(1_500)).await?;
    assert!(
        stole.is_empty(),
        "the freed supplier was handed a task whose consumer had already left: {stole:?}"
    );

    // The task that was never the subject of the hang-up is untouched by it.
    let busy_body = streamed_body_of(busy).await?;
    assert_eq!(
        busy_body.matches("the answer that frees the claim").count(),
        1,
        "the other stream still carries its whole answer, once: {busy_body}"
    );
    Ok(())
}

/// One labelled counter out of a [`metric_lines`] reading, summed across pools.
///
/// `0` for a series that was never written: the hub only creates a
/// `nexus_jobs_terminal_total{pool="…",reason="…"}` line when that exit actually happens
/// (`state.rs` relabels the actor's per-pool counters), so "never written" is the observable
/// form of "that exit did not occur" and an assertion of `0` means something.
fn terminal_count(lines: &[String], reason: &str) -> u64 {
    let needle = format!("reason=\"{reason}\"");
    lines
        .iter()
        .filter(|line| line.contains(&needle))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// The generic job route names its supplier on the `202`, and a pin there is a reservation an
/// idle peer cannot take anyway.
///
/// WHY THIS FACE NEEDS ITS OWN TEST THOUGH THE COMPLETION ROUTES ALREADY NAME THE SUPPLIER.
/// It is a different answer built a different way: `create_job` writes a `202` and hangs up
/// without ever holding a body, so the id it names comes straight off the submit turn's
/// `Gate::Accepted{worker}` — no relay watch, no header hold (`consumer.rs`, the
/// `set_supplier(&mut response, worker.as_ref())` after the verdict). Nothing on this surface
/// waits, so nothing here can excuse a missing id the way a held head can.
///
/// WHERE THE EXCLUSION IS ACTUALLY PROVEN. Not in the first leg: with the pinned supplier able
/// to serve, a hub that ignored the pin entirely might still happen to pick it. The decisive
/// arm is the second leg, where the pinned supplier is parked with `free_slots: 0` — still a
/// `Live` connection, so the pin binds (`live_pin` reads the link, not the engine or the slots)
/// — while the *peer* sits idle with four free slots and claim authority in hand. Without the
/// pin that peer takes the task within milliseconds, because that is what an idle claimant is
/// for. With it, the `202` says out loud that the task is queued and nameless, the peer is
/// handed nothing, and the task moves only when the supplier the consumer named can take it. The
/// completion routes cannot show that combination at all: their header hold hides the routing
/// decision behind the wait.
///
/// WHY THAT LEG SETTLES BEFORE IT POSTS. The park travels on the supplier's socket and the POST
/// on another, so they would otherwise race: the pin left three parked claim slots on that
/// supplier, and a parked row is only deferred while the actor sees zero slots. An observe read
/// goes through the same mailbox as the park, so the `observe_until` below is the actor's own
/// confirmation that it has the zero-slot datum — the leg then races nothing, and the
/// header-absence assertion keeps its meaning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_job_post_names_the_supplier_it_leased_and_holds_the_task_from_the_peer(
) -> anyhow::Result<()> {
    let hub = spawn_hub("pin_jobs_face", test_config()).await?;
    let (mut pinned, pinned_id) = dial_supplier(hub.addr).await?;
    let (mut peer, _peer_id) = dial_supplier(hub.addr).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/observe", hub.addr);

    // ---- leg 1: the supplier that is handed the task is the one the `202` names ----
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &pinned_id)
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "pinned to one supplier"},
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let named = supplier_of(&response);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "the detached job route answers `202`"
    );
    let job = body["job_id"]
        .as_str()
        .expect("the accepted body names the task")
        .to_owned();
    assert_eq!(
        named.as_deref(),
        Some(pinned_id.as_str()),
        "the head names the supplier this request was connected to — the id is read from the \
         same lease `GET /v1/jobs/<id>` reports, not echoed from the header the client sent"
    );
    let assigned = next_frame_of(&mut pinned, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        job.as_str(),
        "the header and the lease are the same fact: this is the task that supplier was handed"
    );

    // ---- leg 2: the pinned supplier cannot serve, and the idle peer still gets nothing ----
    //
    // Park the pinned supplier without disconnecting it. `Live` is what the pin's binding rule
    // reads, so the reservation stays in force for the whole leg; zero slots is what keeps it
    // from taking the task itself.
    let caps = WireCaps::default();
    let parked = WorkerFrame::Ready {
        free_slots: 0,
        free_kv_tokens: Some(524_288),
        engine_ready: true,
        telemetry: None,
    };
    pinned
        .send(Message::Text(encode_line(&parked, &caps)?.into()))
        .await?;

    // The park has to be **inside the actor** before the POST is allowed to arrive, and
    // `/v1/observe` is the surface that reports exactly that: an observe read is itself an
    // `ActorMsg::Observe` in the pool's own mailbox (`state.rs:1076-1098`, answered at
    // `actor.rs:994-1002`), so a document showing `free_slots == 0` for this worker can only
    // have been produced by a turn after the one that wrote the datum (`actor.rs:2009`).
    //
    // WHY THE WAIT IS LOAD-BEARING AND NOT COSMETIC. The park and the POST ride two
    // different sockets, so left alone they race. `dial_supplier` leaves **three** parked
    // claim slots on this supplier (`dial_worker(_, true)` sends `Claim{jobs: Some(4)}`, and
    // leg 1 spent one), and `fulfil_claims` defers a parked row only while
    // `!w.dispatchable()` (`actor.rs:2170`) — which reads `available_slots()`, clamped
    // against that same `free_slots` (`model.rs:209-215`). If the `Submit` is processed
    // first, the supplier still reports four slots, the pinned row is served inside the
    // submit turn, and `x-supplier-id` legitimately names it, so the assertion below fails on
    // a hub that did nothing wrong. Measured before this wait: 3 failures in 16 full-target
    // runs and 0/8 in isolation. No verdict changes — the taker is the pinned supplier either
    // way — so the race is what gets removed here, not the arm it exposes.
    observe_until(
        &client,
        &url,
        "the pinned supplier's zero-slot heartbeat to land in the actor",
        |doc| {
            doc["workers"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|row| row["id"] == pinned_id && row["free_slots"] == 0)
            })
        },
    )
    .await?;
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .header("x-supplier-id", &pinned_id)
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "waiting for a supplier that is busy"},
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let named = supplier_of(&response);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "and again `202`");
    let queued = body["job_id"]
        .as_str()
        .expect("the accepted body names the task")
        .to_owned();
    assert_eq!(
        named, None,
        "a queued task has no supplier to name, and the hub does not invent one from the \
         header the caller sent: an id in that header would read as a lease that never happened"
    );

    // The state the header's absence is a statement about: queued, and no lease row for it —
    // while the first task's lease is still held, which is what stops this from being a hub
    // with no capacity at all.
    let doc = observe_until(
        &client,
        &url,
        "the pinned task to sit queued with no lease of its own",
        |doc| {
            doc["jobs"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|row| row["id"] == queued && row["phase"] == "queued")
            }) && doc["leases"]
                .as_array()
                .is_some_and(|rows| rows.iter().all(|row| row["job"] != queued) && !rows.is_empty())
        },
    )
    .await?;
    // The peer is idle, claiming, and capable of the model. It is not handed the task.
    let took = drain_assigned(&mut peer, Duration::from_millis(1_500)).await?;
    assert!(
        took.is_empty(),
        "the idle peer took a task the pin reserved for somebody else: {took:?} / {doc}"
    );

    // The reservation is not a dead end: the moment the named supplier can serve, the task it
    // was waiting for comes to it — which is also the proof that the peer's silence above was
    // the pin and not a task nobody could run.
    let free = WorkerFrame::Ready {
        free_slots: 4,
        free_kv_tokens: Some(524_288),
        engine_ready: true,
        telemetry: None,
    };
    pinned
        .send(Message::Text(encode_line(&free, &caps)?.into()))
        .await?;
    pinned
        .send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(1) }, &caps)?.into(),
        ))
        .await?;
    let assigned = next_frame_of(&mut pinned, "assigned").await?;
    let HubFrame::Assigned { job: leased, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an `assigned`, got {assigned:?}"));
    };
    assert_eq!(
        leased.as_ref(),
        queued.as_str(),
        "the supplier the consumer pinned is the one the wait ends with, and it is the same id \
         the first leg's header carried"
    );
    Ok(())
}
