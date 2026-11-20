use crate::{ApiError, SharedState};
use axum::extract::{Request, State};
use axum::http::{Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

#[derive(Clone, Copy)]
pub(crate) struct PeerRequest;

pub async fn require_token(
    State(state): State<SharedState>,
    mut request: Request,
    next: Next,
) -> Response {
    if state.api_token.is_none() && state.peer_token.is_none() {
        return next.run(request).await;
    }
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    if credential_matches(provided, state.api_token.as_deref()) {
        return next.run(request).await;
    }
    if credential_matches(provided, state.peer_token.as_deref()) {
        if !matches!(
            (request.method(), request.uri().path()),
            (&Method::GET, "/bloom/v1/nodes")
                | (
                    &Method::POST,
                    "/v1/chat/completions" | "/bloom/v1/inference"
                )
        ) {
            return ApiError::forbidden("peer credential cannot access this endpoint")
                .into_response();
        }
        request.extensions_mut().insert(PeerRequest);
        return next.run(request).await;
    }
    ApiError::unauthorized("missing or invalid bearer token").into_response()
}

fn credential_matches(provided: Option<&str>, expected: Option<&str>) -> bool {
    match (provided, expected) {
        (Some(provided), Some(expected)) if !expected.is_empty() => {
            bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
        }
        _ => false,
    }
}
