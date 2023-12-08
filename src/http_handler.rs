use std::sync::{Arc, RwLock};

use axum::{
    extract::State,
    http::{header, HeaderMap, Method, StatusCode},
    response::{Html, IntoResponse},
    routing::{any, get, post},
    Json, Router,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;

use crate::model::{self, Priority};

async fn root() -> Html<&'static str> {
    Html("<h1>Http Request Duplicator</h1>")
}

#[derive(Deserialize, Debug)]
#[serde(transparent)]
struct DuplicateTargets {
    data: Vec<String>,
}

#[derive(Serialize, Debug)]
enum DuplicateResponse {
    Ok,
    Error,
}

async fn duplicate(
    state: Arc<model::AppState>,
    method: Method,
    mut headers: HeaderMap,
    body: Bytes,
    priority: Priority,
) -> (StatusCode, Json<DuplicateResponse>) {
    let Some(Ok(Ok(targets))) = headers
        .get("x-duplicate-targets")
        .map(|v| v.to_str().map(serde_json::from_str::<DuplicateTargets>))
    else {
        return (StatusCode::BAD_REQUEST, Json(DuplicateResponse::Error));
    };

    headers.remove("x-duplicate-targets");
    headers.remove("host");

    let delayed_clone_objects =
        Arc::new(RwLock::new(model::ReadonlySharedObjectsBetweenContexts {
            headers,
            method,
        }));

    let count = targets.data.len();
    let high = state.counters.get(Priority::High).read_queue_count();
    let low = state.counters.get(Priority::Low).read_queue_count();

    for target in targets.data.into_iter() {
        let context = model::RequestContext {
            target,
            readonly_objects: Arc::clone(&delayed_clone_objects),
            body: body.clone(),
            ttl: 3,
        };

        state.counters.get(priority).enqueue();
        state.channels.get_queue(priority).send(context).unwrap();
    }

    tracing::info!("Enqueue {count}@{priority:?} [H:{high}, L:{low}]");
    (StatusCode::ACCEPTED, Json(DuplicateResponse::Ok))
}

async fn duplicate_low_priority(
    State(state): State<Arc<model::AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<DuplicateResponse>) {
    duplicate(state, method, headers, body, Priority::Low).await
}

async fn duplicate_high_priority(
    State(state): State<Arc<model::AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<DuplicateResponse>) {
    duplicate(state, method, headers, body, Priority::High).await
}

async fn flush_low_priority(State(state): State<Arc<model::AppState>>) -> StatusCode {
    let _ = state.channels.flush_low_priority_queue.send(()).await;
    StatusCode::ACCEPTED
}

async fn metrics(State(state): State<Arc<model::AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json!(state.counters).to_string(),
    )
}

async fn target_metrics(State(state): State<Arc<model::AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json!(state.log.read().await).to_string(),
    )
}

pub async fn run(s: TcpListener, state: Arc<model::AppState>) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/", get(root))
        .route("/metrics", get(metrics))
        .route("/target_metrics", get(target_metrics))
        .route("/flush/low_priority", post(flush_low_priority))
        .route("/duplicate/high_priority", any(duplicate_high_priority))
        .route("/duplicate/low_priority", any(duplicate_low_priority))
        .with_state(state);

    axum::serve(s, app.into_make_service()).await
}
