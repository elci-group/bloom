use crate::SharedState;
use crate::error::ApiError;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;
use bloom_core::NodeId;
use futures::{StreamExt, stream};
use std::path::Path;
use std::time::Duration;

pub fn build_peer_client(ca_pem: Option<&Path>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(60))
        .timeout(Duration::from_secs(300))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());
    if let Some(path) = ca_pem {
        let pem = std::fs::read(bloom_core::config::expand_home(path))
            .map_err(|e| format!("cannot read peer CA bundle {}: {e}", path.display()))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|e| format!("invalid peer CA bundle {}: {e}", path.display()))?;
        if certs.is_empty() {
            return Err(format!(
                "peer CA bundle {} contains no certificates",
                path.display()
            ));
        }
        builder = builder.tls_certs_only(certs);
    }
    builder
        .build()
        .map_err(|e| format!("cannot build peer client: {e}"))
}

fn endpoint_is_safe(url: &reqwest::Url, has_token: bool) -> bool {
    url.scheme() == "https" && has_token
}

/// Validate peer settings before the daemon starts accepting routed requests.
pub fn validate_peers(
    peers: &[bloom_core::config::PeerConfig],
    local_node: &NodeId,
) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for peer in peers {
        if peer.node_id.as_str().trim().is_empty() {
            return Err("peer node identity must not be empty".into());
        }
        if peer.node_id == *local_node {
            return Err(format!(
                "peer {} is configured as the local node",
                peer.node_id
            ));
        }
        if !seen.insert(peer.node_id.as_str()) {
            return Err(format!(
                "peer {} is configured more than once",
                peer.node_id
            ));
        }
        let url = reqwest::Url::parse(&peer.url)
            .map_err(|error| format!("invalid URL for peer {}: {error}", peer.node_id))?;
        if url.scheme() != "https"
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(format!(
                "peer {} must use an HTTPS base URL without credentials, query, or fragment",
                peer.node_id
            ));
        }
        if peer.token_env.trim().is_empty() {
            return Err(format!(
                "peer {} must name a bearer token environment variable",
                peer.node_id
            ));
        }
        match std::env::var(&peer.token_env) {
            Ok(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_graphic()) => {
            }
            _ => {
                return Err(format!(
                    "peer {} token variable {} is unset or empty",
                    peer.node_id, peer.token_env
                ));
            }
        }
    }
    Ok(())
}

/// Send an explicitly swarm-routed request to trusted peers in configured order.
/// The caller decides whether a failed/absent result may fall back to local inference.
pub async fn forward_json(
    state: &SharedState,
    preferred_node: Option<&NodeId>,
    allow_failover: bool,
    path: &str,
    body: &mut serde_json::Value,
) -> Result<Option<Response>, ApiError> {
    if preferred_node.is_some_and(|node| node.as_str() == state.node.id.as_str()) {
        return Ok(None);
    }
    let mut candidates = state.peers.iter().collect::<Vec<_>>();
    if let Some(preferred) = preferred_node {
        let configured = candidates
            .iter()
            .any(|peer| peer.node_id.as_str() == preferred.as_str());
        if !configured && !allow_failover {
            candidates.clear();
        }
    }
    candidates.sort_by_key(|peer| {
        !preferred_node.is_some_and(|node| peer.node_id.as_str() == node.as_str())
    });
    if !allow_failover {
        candidates.truncate(1);
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    let candidates = candidates.into_iter().cloned().collect::<Vec<_>>();

    if let Some(bloom) = body
        .get_mut("bloom")
        .and_then(serde_json::Value::as_object_mut)
    {
        bloom.insert("routing".into(), serde_json::Value::String("direct".into()));
        bloom.remove("preferred_node");
        bloom.insert("allow_failover".into(), serde_json::Value::Bool(false));
    }

    let client = state
        .peer_client
        .as_ref()
        .ok_or_else(|| ApiError::internal("peer routing client has not been initialized"))?;
    let model = body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let probes = stream::iter(candidates.into_iter().enumerate().map(|(index, peer)| {
        let client = client.clone();
        let model = model.clone();
        async move {
            let result = probe_peer(&client, peer, model.as_deref()).await;
            (index, result)
        }
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await;
    let mut last_error = None;
    // Every failure so far was a peer declining for lack of capacity (as
    // opposed to a transport, identity or upstream fault).
    let mut only_capacity_refusals = true;
    let mut reachable = Vec::new();
    for (index, probe) in probes {
        match probe {
            Ok(target) => reachable.push((index, target)),
            Err((peer_id, error)) => {
                last_error = Some(format!("peer {peer_id}: {error}"));
                only_capacity_refusals = false;
            }
        }
    }
    let keys = rank_keys(
        &reachable
            .iter()
            .map(|(_, target)| target.hint)
            .collect::<Vec<_>>(),
    );
    let mut ranked = reachable
        .into_iter()
        .zip(keys)
        .map(|((index, target), key)| {
            let preferred =
                preferred_node.is_some_and(|node| target.peer.node_id.as_str() == node.as_str());
            (preferred, key, index, target)
        })
        .collect::<Vec<_>>();
    // Requested node first, then the best hint, then configured order.
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });

    for (_, _, _, target) in ranked {
        let peer = target.peer;
        let token = target.token;
        let base = target.base;

        // Claim an execution slot before the prompt leaves this node, so a
        // competing request cannot take it between ranking and admission.
        let admission = match request_admission(client, &base, &token, &peer.node_id).await {
            Ok(admission) => admission,
            Err(refusal) => {
                only_capacity_refusals &= refusal.capacity;
                last_error = Some(format!("peer {}: {}", peer.node_id, refusal.detail));
                continue;
            }
        };
        if let Some(bloom) = body
            .get_mut("bloom")
            .and_then(serde_json::Value::as_object_mut)
        {
            match &admission {
                Some(id) => bloom.insert("admission".into(), serde_json::Value::String(id.clone())),
                None => bloom.remove("admission"),
            };
        }

        let request = client
            .post(endpoint(&base, path))
            .bearer_auth(token)
            .json(body);
        match request.send().await {
            Ok(response) if retryable_status(response.status()) && allow_failover => {
                only_capacity_refusals &= capacity_status(response.status());
                last_error = Some(format!(
                    "peer {} returned {}",
                    peer.node_id,
                    response.status()
                ));
            }
            Ok(response) => return Ok(Some(proxy_response(response))),
            Err(error) => {
                only_capacity_refusals = false;
                last_error = Some(format!("peer {} request failed: {error}", peer.node_id))
            }
        }
    }
    match last_error {
        Some(error) if only_capacity_refusals => Err(ApiError::unavailable(error)),
        Some(error) => Err(ApiError::upstream(error)),
        None => Ok(None),
    }
}

fn endpoint(base: &reqwest::Url, path: &str) -> reqwest::Url {
    let mut url = base.clone();
    let base_path = base.path().trim_end_matches('/').to_string();
    url.set_path(&format!("{base_path}{path}"));
    url.set_query(None);
    url.set_fragment(None);
    url
}

struct Refusal {
    /// The peer is healthy but has no free slot (or is draining).
    capacity: bool,
    detail: String,
}

/// Ask a verified peer for an execution-slot hold. `Ok(None)` means the peer
/// predates admissions; the request is then forwarded without a hold and the
/// peer applies its ordinary admission check.
async fn request_admission(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: &str,
    node_id: &NodeId,
) -> Result<Option<String>, Refusal> {
    #[derive(serde::Deserialize)]
    struct Granted {
        admission_id: String,
        node_id: NodeId,
    }
    let fault = |detail: String| Refusal {
        capacity: false,
        detail,
    };
    let response = client
        .post(endpoint(base, "/bloom/v1/admissions"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(3))
        .json(&serde_json::json!({ "ttl_ms": ADMISSION_TTL_MS }))
        .send()
        .await
        .map_err(|error| fault(format!("admission request failed: {error}")))?;
    let status = response.status();
    if matches!(
        status,
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::METHOD_NOT_ALLOWED
    ) {
        return Ok(None);
    }
    if capacity_status(status) {
        return Err(Refusal {
            capacity: true,
            detail: format!("declined admission ({status})"),
        });
    }
    if status != reqwest::StatusCode::CREATED {
        return Err(fault(format!("admission request returned {status}")));
    }
    let granted: Granted = response
        .json()
        .await
        .map_err(|error| fault(format!("invalid admission response: {error}")))?;
    if granted.node_id != *node_id {
        return Err(fault(
            "admission granted by an unexpected node identity".into(),
        ));
    }
    Ok(Some(granted.admission_id))
}

/// Long enough to cover the forward that immediately follows the grant.
const ADMISSION_TTL_MS: u64 = 5_000;

fn capacity_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::TOO_MANY_REQUESTS | reqwest::StatusCode::SERVICE_UNAVAILABLE
    )
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(default)]
struct SchedulingHint {
    available_slots: u32,
    available_memory_bytes: u64,
    model_resident: Option<bool>,
    throughput_tps: Option<f64>,
}

/// (has a free slot, model resident, throughput bucket, free slots, free MiB)
type RankKey = (bool, bool, i32, u32, u64);

impl SchedulingHint {
    /// Decode rate on a log2 scale, so only roughly 2x differences reorder
    /// peers and measurement noise does not.
    fn throughput_bucket(self) -> Option<i32> {
        self.throughput_tps
            .filter(|tps| tps.is_finite() && *tps >= 1.0)
            .map(|tps| tps.log2().floor() as i32)
    }
}

/// Rank keys for a set of candidates, compared highest-first. A peer with no
/// throughput measurement gets the median bucket of the measured candidates:
/// treating it as slow would starve it of the work needed to measure it.
fn rank_keys(hints: &[SchedulingHint]) -> Vec<RankKey> {
    let buckets = hints
        .iter()
        .map(|hint| hint.throughput_bucket())
        .collect::<Vec<_>>();
    let mut measured = buckets.iter().flatten().copied().collect::<Vec<_>>();
    measured.sort_unstable();
    let neutral = measured
        .get(measured.len().saturating_sub(1) / 2)
        .copied()
        .unwrap_or(0);
    hints
        .iter()
        .zip(buckets)
        .map(|(hint, bucket)| {
            (
                hint.available_slots > 0,
                hint.model_resident.unwrap_or(false),
                bucket.unwrap_or(neutral),
                hint.available_slots,
                hint.available_memory_bytes / (1024 * 1024),
            )
        })
        .collect()
}

struct PeerTarget {
    peer: bloom_core::config::PeerConfig,
    token: String,
    base: reqwest::Url,
    hint: SchedulingHint,
}

async fn probe_peer(
    client: &reqwest::Client,
    peer: bloom_core::config::PeerConfig,
    model: Option<&str>,
) -> Result<PeerTarget, (NodeId, String)> {
    let fail = |error: String| (peer.node_id.clone(), error);
    let token = std::env::var(&peer.token_env)
        .ok()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_graphic()))
        .ok_or_else(|| {
            fail(format!(
                "credential environment variable {} is unset or invalid",
                peer.token_env
            ))
        })?;
    let base =
        reqwest::Url::parse(&peer.url).map_err(|error| fail(format!("invalid URL: {error}")))?;
    if !endpoint_is_safe(&base, true) {
        return Err(fail(
            "peer URL must use HTTPS with a bearer credential".into(),
        ));
    }
    let base_path = base.path().trim_end_matches('/').to_string();
    let mut status_url = base.clone();
    status_url.set_path(&format!("{base_path}/bloom/v1/nodes"));
    status_url.set_query(None);
    if let Some(model) = model {
        status_url.query_pairs_mut().append_pair("model", model);
    } else {
        status_url.set_query(None);
    }
    status_url.set_fragment(None);
    let hint = verify_peer(client, status_url, &token, &peer.node_id)
        .await
        .map_err(fail)?;
    Ok(PeerTarget {
        peer,
        token,
        base,
        hint,
    })
}

async fn verify_peer(
    client: &reqwest::Client,
    url: reqwest::Url,
    token: &str,
    node_id: &NodeId,
) -> Result<SchedulingHint, String> {
    #[derive(serde::Deserialize)]
    struct Nodes {
        nodes: Vec<Node>,
    }

    #[derive(serde::Deserialize)]
    struct Node {
        descriptor: bloom_core::node::NodeDescriptor,
        status: bloom_core::node::NodeStatus,
        #[serde(default)]
        scheduling: Option<SchedulingHint>,
    }

    let mut response = client
        .get(url)
        .bearer_auth(token)
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| {
            tracing::debug!(%node_id, ?error, "peer status transport failed");
            format!("status request failed: {error}")
        })?
        .error_for_status()
        .map_err(|e| format!("status request failed: {e}"))?;

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err("node status response exceeded 64 KiB".into());
        }
        body.extend_from_slice(&chunk);
    }
    let nodes: Nodes =
        serde_json::from_slice(&body).map_err(|e| format!("invalid node status response: {e}"))?;
    let node = nodes
        .nodes
        .into_iter()
        .find(|node| node.descriptor.id == *node_id)
        .ok_or_else(|| "identity verification failed".to_string())?;
    if !node.status.healthy || !node.status.harness_healthy || node.status.draining {
        return Err("node is unavailable for inference".into());
    }
    Ok(node.scheduling.unwrap_or_default())
}

fn retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::CONFLICT
            | reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

fn proxy_response(upstream: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let stream = upstream.bytes_stream();
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    if let Some(value) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{SchedulingHint, rank_keys};

    const GIB: u64 = 1024 * 1024 * 1024;

    fn hint(slots: u32, memory: u64, resident: bool, tps: Option<f64>) -> SchedulingHint {
        SchedulingHint {
            available_slots: slots,
            available_memory_bytes: memory,
            model_resident: Some(resident),
            throughput_tps: tps,
        }
    }

    #[test]
    fn warm_peer_with_room_ranks_above_cold_or_busy_peer() {
        let keys = rank_keys(&[
            hint(2, 8 * GIB, true, None),
            hint(4, 16 * GIB, false, None),
            hint(0, GIB, true, None),
        ]);
        let (warm, cold, busy) = (keys[0], keys[1], keys[2]);
        assert!(warm > cold);
        assert!(warm > busy);
        assert!(cold > busy);
    }

    #[test]
    fn faster_peer_wins_only_on_a_real_difference() {
        let keys = rank_keys(&[
            hint(1, GIB, true, Some(40.0)),
            hint(4, GIB, true, Some(90.0)),
            hint(4, GIB, true, Some(45.0)),
        ]);
        // 90 tok/s is a full log2 bucket above 40, so it outranks the others.
        assert!(keys[1] > keys[0] && keys[1] > keys[2]);
        // 45 vs 40 is noise; free slots decide instead.
        assert!(keys[2] > keys[0]);
        // Residency still dominates throughput: a cold fast peer must load.
        let cold_fast = rank_keys(&[
            hint(4, GIB, false, Some(500.0)),
            hint(1, GIB, true, Some(10.0)),
        ]);
        assert!(cold_fast[1] > cold_fast[0]);
    }

    #[test]
    fn unmeasured_peer_is_ranked_as_typical_not_slow() {
        let keys = rank_keys(&[
            hint(2, GIB, true, Some(20.0)),
            hint(2, GIB, true, Some(80.0)),
            hint(2, GIB, true, Some(40.0)),
            hint(3, GIB, true, None),
        ]);
        // The median measured bucket is 40 tok/s (bucket 5); the new peer
        // takes that bucket and wins it on free slots.
        assert_eq!(keys[3].2, keys[2].2);
        assert!(keys[3] > keys[2]);
        assert!(keys[1] > keys[3]);
    }

    #[test]
    fn old_peer_without_hints_gets_safe_zero_rank() {
        let old: SchedulingHint = serde_json::from_str("{}").unwrap();
        assert_eq!(rank_keys(&[old]), vec![(false, false, 0, 0, 0)]);
    }
}
