use std::sync::{Arc, RwLock};
use std::convert::Infallible;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use hyper::{body, Body, Request, Response};
use serde::Deserialize;

use crate::channel;
use crate::counter::COUNTERS;
use crate::model::Priority;

#[derive(Deserialize, Debug)]
#[serde(transparent)]
struct Targets {
    data: Vec<String>,
}

pub async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (mut parts, body) = req.into_parts();

    let channels = channel::CHANNELS.get().unwrap();

    if let Some(Ok("true")) = parts
        .headers
        .get("x-clear-low-priority-queue")
        .map(|v| v.to_str())
    {
        channels.drop_low_priority_requests.send(()).await.unwrap();
    }

    let high_priority = matches!(
        parts.headers.get("x-high-priority").map(|v| v.to_str()),
        Some(Ok("true"))
    );

    let Some(Ok(s)) = parts.headers.get("x-duplicate-targets").map(|v| v.to_str()) else {
        return Ok(Response::new(Body::from("Do nothing")));
    };

    let Ok(targets) = serde_json::from_str::<Targets>(s) else {
        return Ok(Response::new(Body::from(
            "Failed to parse x-duplicate-targets",
        )));
    };

    let Ok(body_bytes) = body::to_bytes(body).await.map(|v| v.to_vec()) else {
        return Ok(Response::new(Body::from("Failed to read body")));
    };

    parts.headers.remove("x-duplicate-targets");
    parts.headers.remove("x-clear-low-priority-queue");
    parts.headers.remove("x-high-priority");
    parts.headers.remove("host");

    let body_bytes = Bytes::from(body_bytes);

    let delayed_clone_objects = Arc::new(RwLock::new(channel::ReadonlySharedObjectsBetweenContexts {
        headers: parts.headers,
        method: parts.method,
    }));

    for target in targets.data.into_iter() {
        let context = channel::RequestContext {
            target,
            readonly_objects: Arc::clone(&delayed_clone_objects),
            body: body_bytes.clone(),
            ttl: 3,
        };

        if high_priority {
            COUNTERS.get(Priority::High)
                .queued
                .fetch_add(1, Ordering::Relaxed);

            channels.high_priority_sock.send(context).unwrap();
        } else {
            COUNTERS.get(Priority::Low)
                .queued
                .fetch_add(1, Ordering::Relaxed);

            channels.low_priority_sock.send(context).unwrap();
        }
    }

    Ok(Response::new(Body::from("OK")))
}

