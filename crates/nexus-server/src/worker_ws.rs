//! The worker-facing WebSocket endpoint (task §4): one persistent outbound
//! connection per worker process, JSON-lines frames, multiplexed per `job_id`.
//!
//! Runtime boundary: this is the **native** half, and the only half that touches
//! a socket. The actor in `nexus-core` is IO-free, so this module is what reads a
//! frame, hands it to the actor, and writes the returned frames back.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::auth::bearer;
use nexus_core::protocol::decode_worker_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::WorkerFrame;
use nexus_core::ActorMsg;
use nexus_core::BrokerConfig;
use nexus_core::HubFrame;
use tokio::sync::mpsc;

use crate::state;
use crate::state::Hub;
use crate::state::WorkerConn;
use crate::state::WorkerWrite;

/// Outbound queue depth per worker. Bounded deliberately: a worker that cannot
/// keep up is a worker whose engine is already behind, and buffering without
/// bound would just move the RSS problem.
const OUTBOUND_QUEUE: usize = 256;

/// How long a socket may take to send its `Hello`. Unauthenticated sockets are a
/// cheap DoS, so this is short.
const HANDSHAKE_TIMEOUT_SECS: u64 = 20;

/// Floor on the hub's liveness probe period. Below this a hub would ping more
/// often than it can afford to poll 500 sockets.
const MIN_PING_MS: u64 = 250;

/// Ceiling on the probe period: no supplier is ever left unprobed for longer than
/// this, whatever its pool's heartbeat says.
const MAX_PING_MS: u64 = 5_000;

/// The hub's own liveness probe for a supplier socket: `(probe period, the stretch
/// of total silence that means the path is gone)`.
///
/// Both windows derive from the pool's `heartbeat_ms`, because that is the cadence
/// the hub itself announced in `Welcome` and the only send obligation a supplier
/// has: a supplier that is up and reachable sends *something* at least that often
/// — its `Ready`, or, long before that, the `Pong` every WebSocket library sends on
/// the hub's behalf without the runtime being involved at all. So a probe period of
/// a quarter of the heartbeat, and a verdict only after a full heartbeat plus one
/// probe period of hearing **nothing at all** (not one `Ready`, not one `Pong`, with
/// at least four unanswered probes in the window), makes the verdict about the path
/// rather than about how busy the engine is. A supplier that never pongs at all is
/// still judged by its own heartbeat cadence, which is the only thing it promised.
fn keepalive_windows(
    cfg: &BrokerConfig,
    handles: &[Arc<state::PoolHandle>],
) -> (Duration, Duration) {
    // One socket can serve several pools; the most talkative expectation wins, so
    // the probe is as tight as the strictest pool the supplier joined.
    let heartbeat = handles
        .iter()
        .map(|handle| cfg.heartbeat_ms_for(&handle.id))
        .min()
        .unwrap_or(cfg.limits.heartbeat_ms)
        .max(1);
    let probe_ms = (heartbeat / 4).clamp(MIN_PING_MS, MAX_PING_MS);
    let silence_ms = probe_ms
        .saturating_mul(3)
        .max(heartbeat.saturating_add(probe_ms));
    (
        Duration::from_millis(probe_ms),
        Duration::from_millis(silence_ms),
    )
}

/// Upgrade handler: the pool token is checked **before** the upgrade, so an
/// unauthenticated host never becomes a connection the hub has to track.
pub async fn upgrade(
    State(hub): State<Arc<Hub>>,
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
    uri: Uri,
) -> Response {
    let token = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
    // A query-string token is accepted for notebook clients that cannot set a
    // header; it is never logged, because it is a secret.
    .or_else(|| {
        uri.query().unwrap_or_default().split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == "token" && !value.is_empty()).then_some(value)
        })
    });

    match hub.pool_tokens.verify(token) {
        Ok(label) => ws.on_upgrade(move |socket| session(socket, hub, label)),
        Err(err) => {
            tracing::warn!(%err, "worker upgrade refused");
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                format!("{err}\n"),
            )
                .into_response()
        }
    }
}

/// One worker session, for the whole lifetime of its socket.
async fn session(socket: axum::extract::ws::WebSocket, hub: Arc<Hub>, token_label: String) {
    let (mut sink, mut stream) = socket.split();

    // 1. Handshake: the first frame must be a valid `Hello`.
    let first = match tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_TIMEOUT_SECS),
        stream.next(),
    )
    .await
    {
        Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
        Ok(Some(Ok(Message::Binary(bytes)))) => String::from_utf8_lossy(&bytes).into_owned(),
        Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => return,
        Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => {
            tracing::debug!(%token_label, "control frame before hello");
            return;
        }
        Err(_) => {
            tracing::warn!(%token_label, "worker handshake timed out");
            return;
        }
    };

    let caps = hub.cfg.limits.worker_caps();
    let frame = match decode_worker_frame(first.trim(), &caps) {
        Ok(frame) => frame,
        Err(err) => {
            let reject = HubFrame::Reject {
                code: err.code().to_string(),
                message: err.to_string(),
            };
            if let Ok(text) = encode_line(&reject, &hub.cfg.limits.hub_caps()) {
                let _ = sink.send(Message::Text(text.into())).await;
            }
            tracing::warn!(%err, %token_label, "worker sent an unusable hello");
            return;
        }
    };
    let WorkerFrame::Hello { pools, .. } = &frame else {
        tracing::warn!(%token_label, "first worker frame was not a hello");
        return;
    };
    // Identity is minted by the hub, never read from the `Hello`. A connection is
    // the unit: every socket gets a fresh UUIDv4, so two suppliers can never fight
    // over one id and a dropped connection's id is never reused. The supplier sees
    // the value in `Welcome` and reports it for observability only.
    let worker = hub.ids.worker_id();
    let handles: Vec<Arc<state::PoolHandle>> = pools
        .iter()
        .filter_map(|name| nexus_core::PoolId::parse(name).ok())
        .filter_map(|pid| hub.pool(&pid).map(Arc::clone))
        .collect();
    if handles.is_empty() {
        // The actor would reject this too; refusing here avoids registering a
        // connection that can never do anything.
        tracing::warn!(%worker, %token_label, "worker asked for pools this hub does not serve");
        return;
    }

    // 2. Outbound pump. Everything the actor wants to say goes through this
    //    single task, so frame order on the socket equals effect order.
    let conn = hub.ids.conn_id();
    let (out_tx, mut out_rx) = mpsc::channel::<WorkerWrite>(OUTBOUND_QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(item) = out_rx.recv().await {
            let message = match item {
                WorkerWrite::Frame(line) => Message::Text(line.into()),
                WorkerWrite::Pong(payload) => Message::Pong(payload.into()),
                WorkerWrite::Ping => Message::Ping(Vec::<u8>::new().into()),
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    for handle in &handles {
        // One entry per connection, under an id only this socket has ever held.
        // There is nothing to evict: a second connection is a second worker, not a
        // takeover of the first.
        handle.workers.lock().await.insert(
            worker.clone(),
            WorkerConn {
                conn,
                tx: out_tx.clone(),
            },
        );
        let _ = state::send(
            handle.as_ref(),
            ActorMsg::Hello {
                worker: worker.clone(),
                conn,
                hello: nexus_core::HelloInput::from_frame(&frame).unwrap_or_default(),
            },
        )
        .await;
    }
    tracing::info!(%worker, %token_label, conn, pools = handles.len(), "worker joined");

    // 3. Read loop, plus the hub's own keepalive. Each inbound frame is tagged with
    //    this connection's generation, so a frame that races a close can never
    //    mutate state the actor has already retired.
    let (probe_period, silence_after) = keepalive_windows(&hub.cfg, &handles);
    let mut probe = tokio::time::interval(probe_period);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_inbound = tokio::time::Instant::now();
    'read: loop {
        let next = tokio::select! {
            biased;
            message = stream.next() => message,
            // The probe. A quarter of the heartbeat the hub announced in `Welcome`,
            // so a supplier that is merely busy has several chances to be heard
            // before anything is concluded from the silence.
            _ = probe.tick() => {
                let silent = last_inbound.elapsed();
                if silent >= silence_after {
                    // Total silence — no `Ready`, no data frame, and no `Pong`, which
                    // every live WebSocket peer sends on its runtime's behalf without
                    // any application involvement at all. That is the hub's only
                    // evidence that the path is gone, and until now nothing acted on
                    // it: a supplier behind a proxy or a NAT that vanished without a
                    // close left this socket parked in `stream.next()` forever, so the
                    // actor kept a live-looking `WorkerRecord` and `/v1/models` went on
                    // advertising a model nobody could serve.
                    tracing::warn!(%worker, conn, silence_ms = silent.as_millis() as u64, probe_ms = probe_period.as_millis() as u64,
                        "supplier went silent past the keepalive window; closing the socket");
                    let bye = HubFrame::Bye {
                        reason: ByeReason::HeartbeatTimeout,
                    };
                    if let Ok(line) = encode_line(&bye, &hub.cfg.limits.hub_caps()) {
                        let _ = out_tx.send(WorkerWrite::Frame(line)).await;
                    }
                    break 'read;
                }
                let _ = out_tx.send(WorkerWrite::Ping).await;
                continue;
            }
        };
        // Any inbound frame at all — including a control frame the runtime never
        // sees — is liveness. Only the *engine* being quiet is a missed heartbeat,
        // and that verdict stays with the actor (§5.1), which pauses dispatch rather
        // than dropping the link.
        last_inbound = tokio::time::Instant::now();
        let text = match next {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                let _ = out_tx.send(WorkerWrite::Pong(payload.to_vec())).await;
                continue;
            }
            Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
        };
        // A message may hold several lines; the codec enforces the caps either way.
        let lines: Vec<String> = text
            .split('\n')
            .map(str::to_string)
            .filter(|l| !l.trim().is_empty())
            .collect();
        for line in lines {
            // A frame this hub cannot decode means the peer and the hub are out
            // of step (or someone is fuzzing the socket). Dropping the rest of
            // the message and continuing would let a client that never sent a
            // valid frame hold the connection open forever, so the documented
            // behaviour is to close: `break 'read`, not `break` out of the line
            // loop.
            let Ok(frame) = decode_worker_frame(line.trim(), &hub.cfg.limits.worker_caps()) else {
                tracing::warn!(%worker, "undecodable worker frame; closing the socket");
                break 'read;
            };
            for handle in &handles {
                if state::send(
                    handle.as_ref(),
                    ActorMsg::WorkerFrame {
                        worker: worker.clone(),
                        conn,
                        frame: frame.clone(),
                    },
                )
                .await
                .is_err()
                {
                    tracing::error!(%worker, "pool actor mailbox closed");
                    break 'read;
                }
            }
        }
    }

    // 4. Departure. The id is unique to this connection, so this is a plain
    //    removal; the guard stays as a cheap check against a raced teardown.
    for handle in &handles {
        let mut workers = handle.workers.lock().await;
        if workers
            .get(&worker)
            .is_some_and(|existing| existing.conn == conn)
        {
            workers.remove(&worker);
            drop(workers);
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::WorkerGone {
                    worker: worker.clone(),
                    conn,
                },
            )
            .await;
        } else {
            tracing::debug!(%worker, "stale socket closing; leaving the live binding alone");
        }
    }
    drop(out_tx);
    let _ = writer.await;
    tracing::info!(%worker, conn, "worker left");
}

#[cfg(test)]
mod tests {
    //! The keepalive's two windows, pinned without a socket. Everything else about the
    //! probe is timing, and timing is the one thing the end-to-end suite cannot pin
    //! cheaply: it can only show that a verdict eventually arrives, not that the window
    //! is the one the supplier was promised.

    use super::*;
    use nexus_core::Limits;
    use nexus_core::PoolConfig;
    use nexus_core::PoolId;
    use std::collections::HashMap;

    fn cfg(heartbeat_ms: u64) -> BrokerConfig {
        BrokerConfig {
            limits: Limits {
                heartbeat_ms,
                ..Limits::default()
            },
            ..BrokerConfig::default()
        }
    }

    fn handle(pool: &str) -> Arc<state::PoolHandle> {
        let (tx, _rx) = mpsc::channel::<ActorMsg>(4);
        Arc::new(state::PoolHandle {
            id: PoolId::new_unchecked(pool.to_string()),
            tx,
            workers: tokio::sync::Mutex::new(HashMap::new()),
            relays: tokio::sync::Mutex::new(HashMap::new()),
            gauges: tokio::sync::Mutex::new(state::PoolGauges::default()),
            // The two relay gauges are written by the dispatcher's tick, which this rig never
            // runs; they start at zero exactly as the production handle's do.
            relay_rows: std::sync::atomic::AtomicU64::new(0),
            relay_subscribers: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The `Ready` cadence the hub announces in `Welcome` is the only send obligation a
    /// supplier has, so it is the ruler both windows are measured against: a quarter of
    /// it for the probe, a whole one plus a probe for the verdict.
    #[test]
    fn both_windows_track_the_heartbeat_the_hub_announced() {
        let (probe, silence) = keepalive_windows(&cfg(20_000), &[]);
        assert_eq!(probe, Duration::from_millis(5_000));
        assert_eq!(silence, Duration::from_millis(25_000));
    }

    /// The two floors the design rests on, at every sane heartbeat. A verdict needs at
    /// least four unanswered probes, and it never arrives before the supplier has sat
    /// out a full heartbeat of its own cadence. The second rule is the load-bearing one:
    /// a supplier that answers pings is measured by the pings, but a supplier that never
    /// pongs is measured only by what it actually promised to send.
    #[test]
    fn silence_is_never_concluded_before_the_supplier_has_had_its_cadence() {
        for heartbeat_ms in [1_000_u64, 2_000, 5_000, 20_000, 60_000, 600_000] {
            let (probe, silence) = keepalive_windows(&cfg(heartbeat_ms), &[]);
            let (probe_ms, silence_ms) = (probe.as_millis() as u64, silence.as_millis() as u64);
            assert!(
                silence_ms >= probe_ms * 4,
                "heartbeat {heartbeat_ms} ms: a {silence:?} verdict on only {} probes",
                silence_ms / probe_ms
            );
            assert!(
                silence_ms > heartbeat_ms,
                "heartbeat {heartbeat_ms} ms: verdict {silence:?} lands before one heartbeat is missed"
            );
        }
    }

    /// Both ends of the band. A hub with 500 sockets cannot poll more often than the
    /// floor; a pool that settled for a lazy heartbeat still gets probed often enough
    /// that a dead path is noticed in seconds rather than minutes.
    #[test]
    fn the_probe_period_stays_inside_its_band() {
        let (probe, silence) = keepalive_windows(&cfg(1), &[]);
        assert_eq!(probe, Duration::from_millis(MIN_PING_MS));
        assert_eq!(silence, Duration::from_millis(750));

        let (probe, silence) = keepalive_windows(&cfg(600_000), &[]);
        assert_eq!(probe, Duration::from_millis(MAX_PING_MS));
        assert_eq!(
            silence,
            Duration::from_millis(605_000),
            "a lazy pool's verdict stretches with its own heartbeat, it never shrinks"
        );
    }

    /// One socket can join several pools, so the strictest expectation wins: joining a
    /// chatty pool is not a way to buy the quiet pool's slower verdict.
    #[test]
    fn the_tightest_pool_a_socket_joined_sets_the_probe() {
        let mut cfg = cfg(20_000);
        cfg.pools.insert(
            "keep.slow".to_string(),
            PoolConfig {
                heartbeat_ms: Some(40_000),
                ..PoolConfig::default()
            },
        );
        cfg.pools.insert(
            "keep.fast".to_string(),
            PoolConfig {
                heartbeat_ms: Some(4_000),
                ..PoolConfig::default()
            },
        );

        let handles = [handle("keep.slow"), handle("keep.fast")];
        let (probe, silence) = keepalive_windows(&cfg, &handles);
        assert_eq!(
            probe,
            Duration::from_millis(1_000),
            "the 4 s pool sets the cadence"
        );
        assert_eq!(silence, Duration::from_millis(5_000), "and the verdict");

        // Alone, the slow pool is probed on its own window (40 s / 4, capped at the band's
        // ceiling) — the fast pool it did not join does not tighten it.
        let (probe, _) = keepalive_windows(&cfg, &[handle("keep.slow")]);
        assert_eq!(probe, Duration::from_millis(MAX_PING_MS));
    }

    /// A socket that joined no pool at all (the handshake admits pools the config
    /// doesn't know about) still gets a probe: the global heartbeat is the fallback.
    #[test]
    fn a_socket_on_an_unconfigured_pool_is_still_probed() {
        let (probe, silence) = keepalive_windows(&cfg(20_000), &[handle("keep.unknown")]);
        assert_eq!(probe, Duration::from_millis(5_000));
        assert_eq!(silence, Duration::from_millis(25_000));
    }
}
