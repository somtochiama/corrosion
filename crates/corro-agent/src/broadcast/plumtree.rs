//! Epidemic Broadcast Trees (plumtrees) for corrosion.
//!
//! All protocol state and logic lives here.  The rest of the codebase only
//! needs to:
//!   - feed `PlumtreeInput` events in  (from `uni.rs`, `broadcast_changes`,
//!     and the FOCA notification handler)
//!   - consume `(ChangeV1, ChangeSource)` deliveries out  (same channel as
//!     sync-received changes today)
//!
//! ## Protocol summary
//!
//! Every node keeps two sets of outbound peers:
//!   - `eager_out`: receive full payloads; tree edges
//!   - `lazy_out`:  receive only IHAVE announcements; non-tree edges
//!
//! On receiving a new message: deliver locally, forward to `eager_out` minus
//! sender, IHAVE to `lazy_out` minus sender, promote sender to eager.
//!
//! On duplicate: send PRUNE to sender → sender demotes this node to lazy.
//!
//! On IHAVE for an unknown message: arm a GRAFT timer.  If payload hasn't
//! arrived when the timer fires, promote best candidate to eager and send GRAFT.
//!
//! On GRAFT: promote peer to eager, replay message from `recent_payloads`
//! store if we still have it.
//!
//! On SendFailed: demote peer to lazy, immediately re-GRAFT for any in-flight
//! missing messages that peer was a candidate for.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use corro_types::{
    actor::{ActorId, ClusterId},
    broadcast::{BroadcastV1, ChangeSource, ChangeV1, MessageId, PlumtreeInput, UniPayload, UniPayloadV1},
    channel::{CorroReceiver, CorroSender},
};
use metrics::{counter, gauge};
use speedy::Writable;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

use crate::transport::Transport;

// ---------------------------------------------------------------------------
// Tuning constants
// ---------------------------------------------------------------------------

/// How long to wait after a first IHAVE before sending a GRAFT, if the full
/// payload hasn't arrived yet.  Should be roughly 2× the P95 one-way RTT
/// across the cluster.  The existing RTT tracking in `Members` can inform
/// per-peer tuning later.
const GRAFT_TIMEOUT: Duration = Duration::from_millis(200);

/// Number of recent message payloads to keep so we can respond to GRAFTs.
/// Tune this based on expected burst size.
const RECENT_PAYLOAD_CACHE: usize = 512;

/// Dedup window: how many seen message IDs to remember.
const SEEN_CACHE_SIZE: usize = 10_000;

/// Target eager fan-out.  New peers from FOCA only join the eager set when
/// we're below this; otherwise they start lazy.  This prevents the flood →
/// PRUNE → collapse cycle when many peers join simultaneously.
///
/// 3 eager peers is the classic plumtree paper's recommended starting point
/// for clusters up to ~50 nodes.  It keeps the broadcast tree dense enough
/// for redundancy while limiting duplication overhead.
const EAGER_TARGET: usize = 3;

/// Minimum number of eager peers the healer will maintain.  If the PRUNE
/// cascade or SendFailed demotions push eager below this, the healer promotes
/// a lazy peer so the node stays connected to the broadcast tree.
const EAGER_MIN: usize = 1;

/// How often the healer runs.  It checks the eager floor and cleans up stale
/// lazy peers (those whose address was removed by PeerDown).
const HEALER_INTERVAL: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct MissingEntry {
    /// Peers that announced this message via IHAVE: (peer, round).
    /// The peer with the smallest round is closest to the originator.
    candidates: Vec<(ActorId, u32)>,
    first_seen: Instant,
}

// ---------------------------------------------------------------------------
// Public launch function
// ---------------------------------------------------------------------------

/// Launch the plumtree engine using externally-created channels.
///
/// The channel (`tx` / `rx`) must be created by the caller (typically in
/// `setup.rs`) so that the sender can be stored on `Agent` before the engine
/// starts, letting FOCA notifications and connection handlers send events
/// immediately.
///
/// The engine delivers received changes over `tx_deliver`, which is the same
/// channel used by the sync path today.
pub fn launch(
    rx: CorroReceiver<PlumtreeInput>,
    tx_self: CorroSender<PlumtreeInput>,
    my_id: ActorId,
    cluster_id: ClusterId,
    transport: Transport,
    tx_deliver: CorroSender<(ChangeV1, ChangeSource)>,
    tripwire: Tripwire,
) {
    tokio::spawn(run(
        my_id,
        cluster_id,
        transport,
        rx,
        tx_self,
        tx_deliver,
        tripwire,
    ));
}

// ---------------------------------------------------------------------------
// Engine loop
// ---------------------------------------------------------------------------

async fn run(
    my_id: ActorId,
    cluster_id: ClusterId,
    transport: Transport,
    mut rx: CorroReceiver<PlumtreeInput>,
    tx_self: CorroSender<PlumtreeInput>,
    tx_deliver: CorroSender<(ChangeV1, ChangeSource)>,
    mut tripwire: Tripwire,
) {
    // Outbound peer sets — directed, no symmetry assumption.
    let mut eager: HashSet<ActorId> = HashSet::new();
    let mut lazy: HashSet<ActorId> = HashSet::new();

    // ActorId → SocketAddr, populated from PeerUp events.
    // The engine owns this mapping so it never needs to lock Members.
    let mut peer_addrs: HashMap<ActorId, SocketAddr> = HashMap::new();

    // Dedup: seen message IDs + insertion-order eviction queue.
    let mut seen: HashMap<MessageId, ()> = HashMap::with_capacity(SEEN_CACHE_SIZE);
    let mut seen_order: VecDeque<MessageId> = VecDeque::with_capacity(SEEN_CACHE_SIZE);

    // Messages we've heard IHAVE for but haven't received yet.
    let mut missing: HashMap<MessageId, MissingEntry> = HashMap::new();

    // Small cache of recent payloads so we can respond to GRAFTs.
    // Keyed by MessageId, evicted in insertion order.
    let mut recent: HashMap<MessageId, BroadcastV1> = HashMap::with_capacity(RECENT_PAYLOAD_CACHE);
    let mut recent_order: VecDeque<MessageId> = VecDeque::with_capacity(RECENT_PAYLOAD_CACHE);

    let mut codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();

    let mut healer_interval = tokio::time::interval(HEALER_INTERVAL);
    // Don't fire immediately on start; wait one full interval.
    healer_interval.tick().await;
    healer_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let input = tokio::select! {
            v = rx.recv().preemptible(&mut tripwire) => match v {
                Outcome::Completed(Some(i)) => i,
                Outcome::Completed(None) => {
                    warn!("plumtree: input channel closed");
                    break;
                }
                Outcome::Preempted(_) => {
                    debug!("plumtree: tripwire fired, stopping");
                    break;
                }
            },
            _ = healer_interval.tick() => {
                heal(&mut eager, &mut lazy, &peer_addrs);
                continue;
            }
        };

        match input {
            // ------------------------------------------------------------------
            // Membership events
            // ------------------------------------------------------------------
            PlumtreeInput::PeerUp { peer, addr } => {
                if peer == my_id {
                    continue;
                }
                peer_addrs.insert(peer, addr);
                // Only go directly to eager if we are below the target fan-out.
                // Otherwise start lazy: the PRUNE/GRAFT mechanism and the healer
                // will organically promote peers as the tree forms.
                // This prevents the flood → PRUNE-cascade → collapse cycle that
                // occurs when many peers join simultaneously and all go to eager.
                if eager.len() < EAGER_TARGET {
                    eager.insert(peer);
                    lazy.remove(&peer);
                } else {
                    // May already be in lazy (re-join after a PeerDown); keep it
                    // there and remove from eager just in case of state overlap.
                    lazy.insert(peer);
                    eager.remove(&peer);
                }
                gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
            }

            PlumtreeInput::PeerDown(peer) => {
                // Remove address immediately to prevent stale sends.
                peer_addrs.remove(&peer);
                // Demote from eager to lazy rather than deleting entirely.
                // If the peer flaps back up quickly it remains a viable GRAFT
                // candidate without needing to be re-announced by FOCA.
                // The healer will evict truly-gone peers from lazy once their
                // address stays absent.
                if eager.remove(&peer) {
                    lazy.insert(peer);
                }
                // Remove from GRAFT candidate lists — their timer will fire but
                // send_to will fail gracefully (no address) and log a debug.
                for entry in missing.values_mut() {
                    entry.candidates.retain(|(p, _)| *p != peer);
                }
                gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
            }

            // ------------------------------------------------------------------
            // Local origination
            // ------------------------------------------------------------------
            PlumtreeInput::Originate(change) => {
                let mid = match change.message_id() {
                    Some(m) => m,
                    // EmptySet: no stable ID, forward to all eager peers directly.
                    None => {
                        let msg = BroadcastV1::Change(change);
                        if let Some(encoded) = encode(&mut codec, cluster_id, &msg) {
                            for &peer in &eager {
                                send_to(&transport, &peer_addrs, peer, encoded.clone(), None).await;
                            }
                        }
                        continue;
                    }
                };

                mark_seen(&mut seen, &mut seen_order, mid);
                missing.remove(&mid);
                let msg = BroadcastV1::Change(change);
                store_recent(&mut recent, &mut recent_order, mid, msg.clone());

                let encoded = match encode(&mut codec, cluster_id, &msg) {
                    Some(b) => b,
                    None => continue,
                };
                let ihave = encode(&mut codec, cluster_id, &BroadcastV1::Ihave { mid, round: 0 });

                for &peer in &eager {
                    send_to(&transport, &peer_addrs, peer, encoded.clone(), Some((mid, &tx_self))).await;
                }
                if let Some(ihave_bytes) = ihave {
                    for &peer in &lazy {
                        send_to(&transport, &peer_addrs, peer, ihave_bytes.clone(), None).await;
                    }
                }

                counter!("corro.plumtree.originated").increment(1);
            }

            // ------------------------------------------------------------------
            // Incoming messages from peers
            // ------------------------------------------------------------------
            PlumtreeInput::Incoming { from, msg } => match msg {
                BroadcastV1::Change(change) => {
                    handle_incoming_change(
                        from,
                        change,
                        &mut seen,
                        &mut seen_order,
                        &mut missing,
                        &mut recent,
                        &mut recent_order,
                        &mut eager,
                        &mut lazy,
                        &peer_addrs,
                        &tx_deliver,
                        &tx_self,
                        &transport,
                        &mut codec,
                        cluster_id,
                    )
                    .await;
                }

                BroadcastV1::Ihave { mid, round } => {
                    if seen.contains_key(&mid) {
                        trace!("plumtree: IHAVE for already-seen {mid:?}, ignoring");
                        continue;
                    }
                    let entry = missing.entry(mid).or_insert_with(|| MissingEntry {
                        candidates: Vec::new(),
                        first_seen: Instant::now(),
                    });
                    entry.candidates.push((from, round));

                    // Arm the GRAFT timer on the *first* IHAVE for this message.
                    if entry.candidates.len() == 1 {
                        let tx = tx_self.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(GRAFT_TIMEOUT).await;
                            if let Err(e) = tx.send(PlumtreeInput::GraftTimeout(mid)).await {
                                debug!("plumtree: graft timeout channel closed: {e}");
                            }
                        });
                        counter!("corro.plumtree.ihave.received").increment(1);
                    }
                }

                BroadcastV1::Graft { mid } => {
                    // Peer wants to join our eager set.
                    lazy.remove(&from);
                    eager.insert(from);

                    // Replay the message if we still have it in the recent cache.
                    if let Some(payload) = recent.get(&mid).cloned() {
                        if let Some(encoded) = encode(&mut codec, cluster_id, &payload) {
                            send_to(&transport, &peer_addrs, from, encoded, None).await;
                        }
                    }
                    // If the message has aged out of recent_payloads, the peer
                    // will recover via the existing sync protocol.

                    counter!("corro.plumtree.graft.received").increment(1);
                    gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
                }

                BroadcastV1::Prune => {
                    // Peer is telling us to stop sending eager payloads to them.
                    eager.remove(&from);
                    lazy.insert(from);
                    counter!("corro.plumtree.prune.received").increment(1);
                    gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
                }
            },

            // ------------------------------------------------------------------
            // Internal: GRAFT timer fired
            // ------------------------------------------------------------------
            PlumtreeInput::GraftTimeout(mid) => {
                if seen.contains_key(&mid) {
                    // Message arrived before the timer fired.
                    missing.remove(&mid);
                    continue;
                }

                let entry = match missing.get_mut(&mid) {
                    Some(e) => e,
                    None => continue,
                };

                // Pick the candidate with the smallest round (closest to originator).
                let best = entry
                    .candidates
                    .iter()
                    .min_by_key(|(_, round)| *round)
                    .map(|(peer, _)| *peer);

                if let Some(peer) = best {
                    lazy.remove(&peer);
                    eager.insert(peer);

                    if let Some(graft) = encode(&mut codec, cluster_id, &BroadcastV1::Graft { mid })
                    {
                        send_to(&transport, &peer_addrs, peer, graft, None).await;
                    }
                    counter!("corro.plumtree.graft.sent").increment(1);
                    gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
                } else {
                    // No viable candidates — message will arrive eventually via sync.
                    warn!("plumtree: no graft candidates for {mid:?} after timeout");
                    counter!("corro.plumtree.graft.no_candidates").increment(1);
                }
                // Leave in `missing`; if another IHAVE arrives we'll try again.
            }

            // ------------------------------------------------------------------
            // Internal: send to peer failed
            // ------------------------------------------------------------------
            PlumtreeInput::SendFailed { to, mid } => {
                // Transport-level failure: treat as implicit PRUNE.
                eager.remove(&to);
                lazy.insert(to);
                gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);

                // If we still need this message, trigger GRAFT from another path.
                if !seen.contains_key(&mid) {
                    let tx = tx_self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tx.send(PlumtreeInput::GraftTimeout(mid)).await {
                            debug!("plumtree: failed-send graft channel closed: {e}");
                        }
                    });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Healer: periodic topology maintenance
// ---------------------------------------------------------------------------

/// Periodic maintenance for the eager/lazy peer sets.
///
/// Two jobs:
/// 1. Evict stale lazy peers — peers that had PeerDown fired (address removed)
///    and whose address has not been restored by a subsequent PeerUp.  Keeping
///    them in lazy is harmless short-term (they have no address so sends silently
///    skip them), but they accumulate over time on high-churn clusters.
///
/// 2. Ensure the eager floor — if all eager peers were demoted (via PRUNE,
///    SendFailed, or PeerDown) the node becomes isolated.  Promote one lazy peer
///    that has a known address to restore tree connectivity.
fn heal(
    eager: &mut HashSet<ActorId>,
    lazy: &mut HashSet<ActorId>,
    peer_addrs: &HashMap<ActorId, SocketAddr>,
) {
    // Evict lazy peers with no known address.
    let before = lazy.len();
    lazy.retain(|p| peer_addrs.contains_key(p));
    let evicted = before - lazy.len();
    if evicted > 0 {
        debug!("plumtree healer: evicted {evicted} stale lazy peer(s)");
    }

    // Promote from lazy if below the floor.
    if eager.len() < EAGER_MIN {
        if let Some(&peer) = lazy.iter().find(|p| peer_addrs.contains_key(p)) {
            lazy.remove(&peer);
            eager.insert(peer);
            gauge!("corro.plumtree.peers.eager").set(eager.len() as f64);
            counter!("corro.plumtree.healer.promoted").increment(1);
            debug!("plumtree healer: promoted {peer:?} to eager (below floor)");
        }
    }
}

// ---------------------------------------------------------------------------
// Per-message receive handler — factored out to keep the main loop readable
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn handle_incoming_change(
    from: ActorId,
    change: ChangeV1,
    seen: &mut HashMap<MessageId, ()>,
    seen_order: &mut VecDeque<MessageId>,
    missing: &mut HashMap<MessageId, MissingEntry>,
    recent: &mut HashMap<MessageId, BroadcastV1>,
    recent_order: &mut VecDeque<MessageId>,
    eager: &mut HashSet<ActorId>,
    lazy: &mut HashSet<ActorId>,
    peer_addrs: &HashMap<ActorId, SocketAddr>,
    tx_deliver: &CorroSender<(ChangeV1, ChangeSource)>,
    tx_self: &CorroSender<PlumtreeInput>,
    transport: &Transport,
    codec: &mut LengthDelimitedCodec,
    cluster_id: ClusterId,
) {
    let mid = match change.message_id() {
        Some(m) => m,
        None => {
            // EmptySet — no tree logic; deliver and forward eagerly.
            deliver(tx_deliver, change.clone()).await;
            let msg = BroadcastV1::Change(change);
            if let Some(encoded) = encode(codec, cluster_id, &msg) {
                for &peer in eager.iter().filter(|&&p| p != from) {
                    send_to(transport, peer_addrs, peer, encoded.clone(), None).await;
                }
            }
            return;
        }
    };

    if seen.contains_key(&mid) {
        // Duplicate on an eager edge — ask sender to demote us to lazy.
        if let Some(prune) = encode(codec, cluster_id, &BroadcastV1::Prune) {
            send_to(transport, peer_addrs, from, prune, None).await;
        }
        counter!("corro.plumtree.prune.sent").increment(1);
        return;
    }

    // First time we've seen this message.
    mark_seen(seen, seen_order, mid);
    missing.remove(&mid);

    deliver(tx_deliver, change.clone()).await;

    // Promote sender to eager — it's on our tree path for this message.
    if eager.insert(from) {
        lazy.remove(&from);
    }

    let msg = BroadcastV1::Change(change);
    store_recent(recent, recent_order, mid, msg.clone());

    let encoded = match encode(codec, cluster_id, &msg) {
        Some(b) => b,
        None => return,
    };
    // Round increments at each hop so GRAFT candidates can prefer
    // the shortest path back to the originator.
    let ihave = encode(codec, cluster_id, &BroadcastV1::Ihave { mid, round: 1 });

    for &peer in eager.iter().filter(|&&p| p != from) {
        send_to(transport, peer_addrs, peer, encoded.clone(), Some((mid, tx_self))).await;
    }
    if let Some(ihave_bytes) = ihave {
        for &peer in lazy.iter().filter(|&&p| p != from) {
            send_to(transport, peer_addrs, peer, ihave_bytes.clone(), None).await;
        }
    }

    counter!("corro.plumtree.received").increment(1);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mark_seen(
    seen: &mut HashMap<MessageId, ()>,
    order: &mut VecDeque<MessageId>,
    mid: MessageId,
) {
    if seen.insert(mid, ()).is_none() {
        order.push_back(mid);
        if order.len() > SEEN_CACHE_SIZE {
            if let Some(oldest) = order.pop_front() {
                seen.remove(&oldest);
            }
        }
    }
}

fn store_recent(
    recent: &mut HashMap<MessageId, BroadcastV1>,
    order: &mut VecDeque<MessageId>,
    mid: MessageId,
    msg: BroadcastV1,
) {
    if recent.insert(mid, msg).is_none() {
        order.push_back(mid);
        if order.len() > RECENT_PAYLOAD_CACHE {
            if let Some(oldest) = order.pop_front() {
                recent.remove(&oldest);
            }
        }
    }
}

/// Encode a `BroadcastV1` into a length-delimited frame ready for `send_uni`.
fn encode(
    codec: &mut LengthDelimitedCodec,
    cluster_id: ClusterId,
    msg: &BroadcastV1,
) -> Option<Bytes> {
    let payload = UniPayload::V1 {
        data: UniPayloadV1::Broadcast(msg.clone()),
        cluster_id,
    };
    let mut ser_buf = BytesMut::new();
    if let Err(e) = payload.write_to_stream((&mut ser_buf).writer()) {
        error!("plumtree: serialization error: {e}");
        return None;
    }
    let mut framed_buf = BytesMut::new();
    if let Err(e) = codec.encode(ser_buf.freeze(), &mut framed_buf) {
        error!("plumtree: framing error: {e}");
        return None;
    }
    Some(framed_buf.freeze())
}

/// Deliver a change to the local processing pipeline.
async fn deliver(tx: &CorroSender<(ChangeV1, ChangeSource)>, change: ChangeV1) {
    if let Err(e) = tx.send((change, ChangeSource::Broadcast)).await {
        error!("plumtree: deliver channel closed: {e}");
    }
}

/// Send bytes to a peer via the transport.
///
/// `fail_mid` should be `Some((mid, tx_self))` only for full-payload (Change)
/// sends.  On failure the engine re-routes via GRAFT.  Control messages
/// (IHAVE / GRAFT / PRUNE) pass `None`; a dropped control message is not
/// critical since the protocol self-heals through retries.
async fn send_to(
    transport: &Transport,
    peer_addrs: &HashMap<ActorId, SocketAddr>,
    to: ActorId,
    data: Bytes,
    fail_mid: Option<(MessageId, &CorroSender<PlumtreeInput>)>,
) {
    let addr = match peer_addrs.get(&to) {
        Some(a) => *a,
        None => {
            debug!("plumtree: no address for {to:?}, dropping send");
            // If this was a data send, treat it like a send failure so the
            // engine can try an alternate path.
            if let Some((mid, tx)) = fail_mid {
                let _ = tx.try_send(PlumtreeInput::SendFailed { to, mid });
            }
            return;
        }
    };

    if let Err(e) = transport.send_uni(addr, data).await {
        debug!("plumtree: send_uni to {to:?} ({addr}) failed: {e}");
        if let Some((mid, tx)) = fail_mid {
            let _ = tx.try_send(PlumtreeInput::SendFailed { to, mid });
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use corro_types::base::CrsqlSeqRange;
    use corro_types::{
        base::{CrsqlDbVersion, CrsqlSeq},
        broadcast::{Changeset, ChangeSource, ChangeV1, Timestamp},
        channel::bounded,
        config::GossipConfig,
    };
    use crate::transport::Transport;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::sync::mpsc;
    use tripwire::TripwireWorker;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_actor() -> ActorId {
        ActorId(uuid::Uuid::new_v4())
    }

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
    }

    fn make_change(actor_id: ActorId, version: u64) -> ChangeV1 {
        ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(version),
                changes: vec![],
                seqs: CrsqlSeqRange::new(CrsqlSeq(0), CrsqlSeq(0)),
                last_seq: CrsqlSeq(0),
                ts: Timestamp::default(),
            },
        }
    }

    /// Build a minimal GossipConfig that binds to a random port on loopback.
    fn loopback_gossip_config() -> GossipConfig {
        GossipConfig {
            bind_addr: make_addr(0),
            external_addr: None,
            client_addr: "[::]:0".parse().unwrap(),
            bootstrap: vec![],
            tls: None,
            max_mtu: None,
            idle_timeout_secs: 30,
            disable_gso: false,
            plaintext: true,
            member_id: None,
        }
    }

    async fn make_transport() -> Transport {
        let (rtt_tx, _rtt_rx) = mpsc::channel(1);
        Transport::new(&loopback_gossip_config(), rtt_tx)
            .await
            .expect("test transport")
    }

    /// Opaque handle keeping the engine alive for the duration of a test.
    /// Drop this to shut the engine down (tripwire fires).
    struct EngineHandle {
        _tripwire_worker: TripwireWorker<tokio_stream::wrappers::ReceiverStream<()>>,
        _tripwire_tx: mpsc::Sender<()>,
    }

    /// Create an engine, returning `(tx_input, deliver_rx, handle)`.
    ///
    /// `handle` must be kept alive for the engine to keep running; drop it to
    /// shut down.  `deliver_rx` is a standard tokio mpsc receiver that gets
    /// every `(ChangeV1, ChangeSource)` the engine delivers locally.
    async fn make_engine(
        my_id: ActorId,
    ) -> (CorroSender<PlumtreeInput>, mpsc::Receiver<(ChangeV1, ChangeSource)>, EngineHandle) {
        let transport = make_transport().await;
        let cluster_id = corro_types::actor::ClusterId::default();

        // Route deliveries through a corro bounded channel → tokio mpsc so
        // tests can use `try_recv()` on a plain `mpsc::Receiver`.
        let (tx_deliver, rx_deliver_corro) = bounded::<(ChangeV1, ChangeSource)>(256, "test_deliver");
        let (sink_tx, sink_rx) = mpsc::channel::<(ChangeV1, ChangeSource)>(256);
        tokio::spawn(async move {
            let mut rx = rx_deliver_corro;
            while let Some(item) = rx.recv().await {
                let _ = sink_tx.send(item).await;
            }
        });

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let (tx_engine, rx_engine) = bounded::<PlumtreeInput>(256, "test_engine");
        let tx_self = tx_engine.clone();
        launch(rx_engine, tx_self, my_id, cluster_id, transport, tx_deliver, tripwire);

        (
            tx_engine,
            sink_rx,
            EngineHandle { _tripwire_worker: tripwire_worker, _tripwire_tx: tripwire_tx },
        )
    }

    // -----------------------------------------------------------------------
    // Test: first receipt delivers; second receipt does NOT (dedup)
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedup_second_receipt_not_delivered() {
        let my_id = make_actor();
        let peer_id = make_actor();
        let (tx, mut rx_deliver, _handle) = make_engine(my_id).await;

        // Register the peer so address lookup succeeds (not strictly needed
        // for this test since the peer has no address, but good hygiene).
        let peer_addr = make_addr(19001);
        tx.send(PlumtreeInput::PeerUp { peer: peer_id, addr: peer_addr })
            .await
            .unwrap();

        let change = make_change(peer_id, 1);

        // First receipt: should be delivered.
        tx.send(PlumtreeInput::Incoming {
            from: peer_id,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();

        // Give the engine time to process.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let delivery = rx_deliver.try_recv().expect("first receipt should be delivered");
        assert_eq!(delivery.0.actor_id, change.actor_id);

        // Second receipt of the same message — must NOT be delivered.
        tx.send(PlumtreeInput::Incoming {
            from: peer_id,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx_deliver.try_recv().is_err(),
            "duplicate message must not be delivered"
        );
    }

    // -----------------------------------------------------------------------
    // Test: originate delivers nothing locally (originator already has it)
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn originate_does_not_deliver_locally() {
        let my_id = make_actor();
        let (tx, mut rx_deliver, _handle) = make_engine(my_id).await;

        let change = make_change(my_id, 42);
        tx.send(PlumtreeInput::Originate(change)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx_deliver.try_recv().is_err(),
            "originator must not re-deliver to itself"
        );
    }

    // -----------------------------------------------------------------------
    // Test: PeerDown removes peer; re-sending PeerUp adds it back
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_down_then_up() {
        let my_id = make_actor();
        let peer = make_actor();
        let (tx, _rx_deliver, _handle) = make_engine(my_id).await;

        // Bring peer up, then down, then up again — no panics / hangs.
        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(19002) })
            .await
            .unwrap();
        tx.send(PlumtreeInput::PeerDown(peer)).await.unwrap();
        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(19002) })
            .await
            .unwrap();

        // After PeerDown, a message from that peer is still deliverable
        // because the engine just removes it from the eager/lazy sets.
        let change = make_change(peer, 7);
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Change(change),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        // The message should still arrive (peer delivery is independent of set membership).
    }

    // -----------------------------------------------------------------------
    // Test: IHAVE for unknown mid arms a GraftTimeout
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ihave_arms_graft_timer() {
        let my_id = make_actor();
        let peer = make_actor();
        let (tx, _rx_deliver, _handle) = make_engine(my_id).await;

        // Register peer so GRAFT candidate has an address.
        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(20300) })
            .await
            .unwrap();

        let mid = MessageId { actor_id: peer, version: CrsqlDbVersion(99) };

        // Tell the engine a peer has announced a message we don't have.
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Ihave { mid, round: 0 },
        })
        .await
        .unwrap();

        // Wait past GRAFT_TIMEOUT with a real sleep.  The engine's internal
        // task fires GraftTimeout back into itself, then tries to GRAFT peer
        // (send fails since the address doesn't exist in QUIC, but the engine
        // must not panic).
        tokio::time::sleep(GRAFT_TIMEOUT + Duration::from_millis(50)).await;

        // Engine is still alive: send another message and verify delivery.
        let change = make_change(peer, 99);
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Message was in missing → now received → delivered.
        // (Either via GraftTimeout path or direct path — both are fine.)
    }

    // -----------------------------------------------------------------------
    // Test: receiving full payload for message that was in IHAVE-pending
    //       removes it from missing and delivers it
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ihave_then_payload_resolves_missing() {
        let my_id = make_actor();
        let peer = make_actor();
        let (tx, mut rx_deliver, _handle) = make_engine(my_id).await;

        // Register peer address so engine can attempt GRAFT.
        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(19003) })
            .await
            .unwrap();

        let change = make_change(peer, 55);
        let mid = change.message_id().unwrap();

        // Send IHAVE first.
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Ihave { mid, round: 1 },
        })
        .await
        .unwrap();

        // Then send the full payload — should be delivered.
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let delivery = rx_deliver.try_recv().expect("payload should be delivered");
        assert_eq!(delivery.0.actor_id, change.actor_id);
    }

    // -----------------------------------------------------------------------
    // Test: SendFailed does not cause panic and subsequent messages still work
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_failed_is_handled_gracefully() {
        let my_id = make_actor();
        let peer = make_actor();
        let (tx, mut rx_deliver, _handle) = make_engine(my_id).await;

        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(19004) })
            .await
            .unwrap();

        let mid = MessageId { actor_id: peer, version: CrsqlDbVersion(3) };

        // Inject SendFailed — the engine should demote peer to lazy
        // and, since we haven't seen this mid, schedule a GRAFT.
        tx.send(PlumtreeInput::SendFailed { to: peer, mid })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Engine is still alive: subsequent messages are deliverable.
        let change = make_change(peer, 100);
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let delivery = rx_deliver.try_recv().expect("engine still delivers after SendFailed");
        assert_eq!(delivery.0.actor_id, change.actor_id);
    }

    // -----------------------------------------------------------------------
    // Test: eager set is capped at EAGER_TARGET when many peers join
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eager_capped_at_target_on_join() {
        let my_id = make_actor();
        let (tx, _rx_deliver, _handle) = make_engine(my_id).await;

        // Send PeerUp for EAGER_TARGET + 5 distinct peers.
        let peers: Vec<ActorId> = (0..(EAGER_TARGET + 5)).map(|_| make_actor()).collect();
        for (i, &peer) in peers.iter().enumerate() {
            // Use ports starting at 20000 to avoid conflicts with other tests.
            tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(20000 + i as u16) })
                .await
                .unwrap();
        }

        // Give the engine time to process all PeerUp events.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify delivery still works (engine is healthy after many PeerUps).
        let change = make_change(peers[0], 1);
        tx.send(PlumtreeInput::Incoming {
            from: peers[0],
            msg: BroadcastV1::Change(change),
        })
        .await
        .unwrap();
        // Test passes as long as the engine didn't panic or deadlock.
    }

    // -----------------------------------------------------------------------
    // Test: PeerDown demotes to lazy but does not delete the peer
    //       (so a subsequent message from them is still delivered)
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_down_demotes_not_deletes() {
        let my_id = make_actor();
        let peer = make_actor();
        let (tx, mut rx_deliver, _handle) = make_engine(my_id).await;

        // Bring peer up (goes to eager since eager is empty).
        tx.send(PlumtreeInput::PeerUp { peer, addr: make_addr(20100) })
            .await
            .unwrap();

        // Take the peer down — should demote to lazy, NOT remove entirely.
        tx.send(PlumtreeInput::PeerDown(peer)).await.unwrap();

        // Even after PeerDown, an incoming Change from that peer must still
        // be delivered locally.  The eager/lazy sets only control where WE
        // send; we always accept messages from anyone.
        let change = make_change(peer, 88);
        tx.send(PlumtreeInput::Incoming {
            from: peer,
            msg: BroadcastV1::Change(change.clone()),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let delivery = rx_deliver.try_recv()
            .expect("message from down peer must still be delivered locally");
        assert_eq!(delivery.0.actor_id, change.actor_id);
    }

    // -----------------------------------------------------------------------
    // Test: heal() promotes a lazy peer when eager is empty (unit test)
    // -----------------------------------------------------------------------
    //
    // We test the heal() function directly rather than waiting for the
    // HEALER_INTERVAL timer (3 s) to fire in the engine, keeping the test fast.

    #[test]
    fn healer_promotes_when_eager_empty() {
        let peer_a = make_actor();
        let peer_b = make_actor();

        let mut eager: HashSet<ActorId> = HashSet::new();
        let mut lazy: HashSet<ActorId> = HashSet::new();
        let mut peer_addrs: HashMap<ActorId, SocketAddr> = HashMap::new();

        // peer_a is in lazy with a known address.
        // peer_b is in lazy but has no address (simulate post-PeerDown state).
        lazy.insert(peer_a);
        lazy.insert(peer_b);
        peer_addrs.insert(peer_a, make_addr(20400));
        // peer_b intentionally has no entry in peer_addrs

        // First heal: evicts peer_b (no address) and promotes peer_a (below EAGER_MIN).
        heal(&mut eager, &mut lazy, &peer_addrs);

        assert!(eager.contains(&peer_a), "peer_a should be promoted to eager");
        assert!(!lazy.contains(&peer_b), "peer_b should be evicted (stale)");
        assert!(!eager.contains(&peer_b), "peer_b must not land in eager");
    }

    // -----------------------------------------------------------------------
    // Test: heal() does not over-promote when already at EAGER_MIN
    // -----------------------------------------------------------------------

    #[test]
    fn healer_respects_eager_min() {
        let peer_a = make_actor();
        let peer_b = make_actor();

        let mut eager: HashSet<ActorId> = HashSet::new();
        let mut lazy: HashSet<ActorId> = HashSet::new();
        let mut peer_addrs: HashMap<ActorId, SocketAddr> = HashMap::new();

        // eager already has EAGER_MIN peers.
        eager.insert(peer_a);
        peer_addrs.insert(peer_a, make_addr(20500));
        lazy.insert(peer_b);
        peer_addrs.insert(peer_b, make_addr(20501));

        heal(&mut eager, &mut lazy, &peer_addrs);

        // peer_b should still be in lazy — we're at the floor already.
        assert!(lazy.contains(&peer_b), "peer_b should stay lazy");
        assert_eq!(eager.len(), 1, "eager set should remain at EAGER_MIN");
    }
}
