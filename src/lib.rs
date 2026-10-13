pub mod model;
pub mod store;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::Query,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use model::Page;
use serde_json::json;
use std::sync::Arc;
use store::{Error, Store};

fn error(error: Error) -> Response {
    (
        StatusCode::from_u16(error.0).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({"schema":model::SCHEMA,"error":error.1})),
    )
        .into_response()
}
pub fn router(store: Arc<Store>) -> Router {
    Router::new().fallback(move |request: Request<Body>| {
        let store = store.clone();
        async move {
            let token = request
                .headers()
                .get("authorization")
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.strip_prefix("Bearer "))
                .unwrap_or_default();
            let actor = match store.authenticate(token) {
                Ok(a) => a,
                Err(e) => return error(e),
            };
            let method = request.method().as_str().to_owned();
            let path = request.uri().path().to_owned();
            let page = match Query::<Page>::try_from_uri(request.uri()) {
                Ok(Query(p)) => p,
                Err(_) => return error(Error(400, "invalid_request")),
            };
            if (method == "PUT" || method == "POST")
                && request
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .is_none_or(|v| v.split(';').next() != Some("application/json"))
            {
                return error(Error(415, "unsupported_media_type"));
            }
            let body = match to_bytes(request.into_body(), 16384).await {
                Ok(b) => b,
                Err(_) => return error(Error(413, "payload_too_large")),
            };
            match tokio::task::spawn_blocking(move || {
                store.handle(&actor, &method, &path, page, &body)
            })
            .await
            {
                Ok(Ok((status, value))) => (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
                    Json(value),
                )
                    .into_response(),
                Ok(Err(e)) => error(e),
                Err(_) => error(Error(500, "internal_error")),
            }
        }
    })
}
