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
    let mut targets = stream::iter(candidates.into_iter().enumerate().map(|(index, peer)| {
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
    targets.sort_by(|(left_index, left), (right_index, right)| {
        let left_target = left.as_ref().ok();
        let right_target = right.as_ref().ok();
        let left_preferred = left_target.is_some_and(|target| {
            preferred_node.is_some_and(|node| target.peer.node_id.as_str() == node.as_str())
        });
        let right_preferred = right_target.is_some_and(|target| {
            preferred_node.is_some_and(|node| target.peer.node_id.as_str() == node.as_str())
        });
        right_preferred
            .cmp(&left_preferred)
            .then_with(|| {
                let left_rank =
                    left_target.map_or((false, false, 0, 0), |target| target.hint.rank());
                let right_rank =
                    right_target.map_or((false, false, 0, 0), |target| target.hint.rank());
                right_rank.cmp(&left_rank)
            })
            .then_with(|| left_index.cmp(right_index))
    });

    let mut last_error = None;
    for (_, target) in targets {
        let target = match target {
            Ok(target) => target,
            Err((peer_id, error)) => {
                last_error = Some(format!("peer {peer_id}: {error}"));
                continue;
            }
        };
        let peer = target.peer;
        let token = target.token;
        let mut base = target.base;
        let base_path = base.path().trim_end_matches('/').to_string();
        base.set_path(&format!("{base_path}{path}"));
        base.set_query(None);
        base.set_fragment(None);

        let mut request = client.post(base).json(body);
        request = request.bearer_auth(token);
        match request.send().await {
            Ok(response) if retryable_status(response.status()) && allow_failover => {
                last_error = Some(format!(
                    "peer {} returned {}",
                    peer.node_id,
                    response.status()
                ));
            }
            Ok(response) => return Ok(Some(proxy_response(response))),
            Err(error) => {
                last_error = Some(format!("peer {} request failed: {error}", peer.node_id))
            }
        }
    }
    match last_error {
        Some(error) => Err(ApiError::upstream(error)),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(default)]
struct SchedulingHint {
    available_slots: u32,
    available_memory_bytes: u64,
    model_resident: Option<bool>,
}

impl SchedulingHint {
    fn rank(self) -> (bool, bool, u32, u64) {
        (
            self.available_slots > 0,
            self.model_resident.unwrap_or(false),
            self.available_slots,
            self.available_memory_bytes / (1024 * 1024),
        )
    }
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
    use super::SchedulingHint;

    #[test]
    fn warm_peer_with_room_ranks_above_cold_or_busy_peer() {
        let warm = SchedulingHint {
            available_slots: 2,
            available_memory_bytes: 8 * 1024 * 1024 * 1024,
            model_resident: Some(true),
        };
        let cold = SchedulingHint {
            available_slots: 4,
            available_memory_bytes: 16 * 1024 * 1024 * 1024,
            model_resident: Some(false),
        };
        let busy = SchedulingHint {
            available_slots: 0,
            available_memory_bytes: 1024 * 1024 * 1024,
            model_resident: Some(true),
        };
        assert!(warm.rank() > cold.rank());
        assert!(warm.rank() > busy.rank());
        assert!(cold.rank() > busy.rank());
    }

    #[test]
    fn old_peer_without_hints_gets_safe_zero_rank() {
        let old: SchedulingHint = serde_json::from_str("{}").unwrap();
        assert_eq!(old.rank(), (false, false, 0, 0));
    }
}
