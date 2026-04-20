use corro_types::{
    actor::{ActorId, ClusterId},
    broadcast::{BroadcastV1, ChangeSource, ChangeV1, PlumtreeInput, UniPayload, UniPayloadV1},
    channel::CorroSender,
};
use metrics::counter;
use speedy::Readable;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace};
use tripwire::Tripwire;

/// Spawn a task that accepts unidirectional broadcast streams, then
/// spawns another task for each incoming stream to handle.
///
/// When `tx_plumtree` is provided, all `BroadcastV1` messages (Change,
/// IHAVE, GRAFT, PRUNE) are routed through the plumtree engine which
/// handles dedup, PRUNE/GRAFT, forwarding, and local delivery.
///
/// When `tx_plumtree` is `None` (e.g., in tests), Change payloads fall
/// back to direct delivery via `tx_changes`.
///
/// `from` is the ActorId of the remote peer, needed by the engine so it
/// can issue PRUNE/GRAFT decisions per-sender.  Resolve it from the
/// connection's remote address via `agent.members().read().by_addr`.
pub fn spawn_unipayload_handler(
    tripwire: &Tripwire,
    conn: &quinn::Connection,
    cluster_id: ClusterId,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    plumtree: Option<(ActorId, CorroSender<PlumtreeInput>)>,
) {
    tokio::spawn({
        let conn = conn.clone();
        let mut tripwire = tripwire.clone();
        async move {
            loop {
                let rx = tokio::select! {
                    rx_res = conn.accept_uni() => match rx_res {
                        Ok(rx) => rx,
                        Err(e) => {
                            debug!("could not accept unidirectional stream from connection: {e}");
                            return;
                        }
                    },
                    _ = &mut tripwire => {
                        debug!("connection cancelled");
                        return;
                    }
                };

                counter!("corro.peer.stream.accept.total", "type" => "uni").increment(1);

                trace!(
                    "accepted a unidirectional stream from {}",
                    conn.remote_address()
                );

                tokio::spawn({
                    let tx_changes = tx_changes.clone();
                    let plumtree = plumtree.clone();
                    async move {
                        let mut framed = FramedRead::new(
                            rx,
                            LengthDelimitedCodec::builder()
                                .max_frame_length(100 * 1_024 * 1_024)
                                .new_codec(),
                        );

                        let mut fallback_changes = vec![];

                        loop {
                            match StreamExt::next(&mut framed).await {
                                Some(Ok(b)) => {
                                    counter!("corro.peer.stream.bytes.recv.total", "type" => "uni")
                                        .increment(b.len() as u64);
                                    match UniPayload::read_from_buffer(&b) {
                                        Ok(payload) => {
                                            trace!("parsed a payload: {payload:?}");

                                            let (msg, payload_cluster_id) = match payload {
                                                UniPayload::V1 {
                                                    data: UniPayloadV1::Broadcast(msg),
                                                    cluster_id: cid,
                                                } => (msg, cid),
                                            };

                                            if cluster_id != payload_cluster_id {
                                                continue;
                                            }

                                            match (&msg, &plumtree) {
                                                // All variants go through the plumtree engine
                                                // when it is configured.
                                                (_, Some((from, tx_plumtree))) => {
                                                    let from = *from;
                                                    if let Err(e) = tx_plumtree
                                                        .send(PlumtreeInput::Incoming { from, msg })
                                                        .await
                                                    {
                                                        error!("plumtree input channel closed: {e}");
                                                        return;
                                                    }
                                                }
                                                // Fallback (no plumtree engine): deliver Changes
                                                // directly, drop control messages.
                                                (BroadcastV1::Change(change), None) => {
                                                    fallback_changes.push((
                                                        change.clone(),
                                                        ChangeSource::Broadcast,
                                                    ));
                                                }
                                                (BroadcastV1::Ihave { .. }
                                                | BroadcastV1::Graft { .. }
                                                | BroadcastV1::Prune, None) => {
                                                    // No engine to handle these; ignore.
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("could not decode UniPayload: {e}");
                                            continue;
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("decode error: {e}");
                                }
                                None => break,
                            }
                        }

                        for change in fallback_changes.into_iter().rev() {
                            if let Err(e) = tx_changes.send(change).await {
                                error!("could not send change for processing: {e}");
                                return;
                            }
                        }
                    }
                });
            }
        }
    });
}
