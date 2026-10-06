// Copyright 2026-present Orbit Contributors.
// Licensed under the Apache License, Version 2.0.

use std::convert::Infallible;

use futures_util::stream;
use http_body_util::BodyExt;
use orbit_realtime_rust::{Event, MAX_EVENT_BYTES, encode_comment, response};

#[tokio::test]
async fn encodes_events_and_sets_safe_headers() {
    let event = Event::new("first\r\nsecond")
        .expect("data")
        .with_event("change")
        .expect("event")
        .with_id("42")
        .expect("id")
        .with_retry(2500)
        .expect("retry");
    assert_eq!(
        event.encode().expect("encoded"),
        "event: change\nid: 42\nretry: 2500\ndata: first\ndata: second\n\n"
    );
    let body = response(stream::iter([Ok::<_, Infallible>(event)])).expect("response");
    assert_eq!(
        body.headers()["content-type"],
        "text/event-stream; charset=utf-8"
    );
    assert_eq!(body.headers()["cache-control"], "no-cache");
    let bytes = body.into_body().collect().await.expect("body").to_bytes();
    assert!(
        std::str::from_utf8(&bytes)
            .expect("utf8")
            .contains("data: second")
    );
}

#[test]
fn bounds_fields_and_hides_sensitive_debug_content() {
    assert!(
        Event::new("x".repeat(MAX_EVENT_BYTES))
            .expect("bounded input")
            .encode()
            .is_err()
    );
    assert!(
        Event::new("x")
            .expect("event")
            .with_event("bad\nevent")
            .is_err()
    );
    assert!(Event::new("x").expect("event").with_id("bad\0id").is_err());
    let event = Event::new("private-payload")
        .expect("event")
        .with_id("secret-id")
        .expect("id");
    let debug = format!("{event:?}");
    assert!(!debug.contains("private-payload"));
    assert!(!debug.contains("secret-id"));
    assert_eq!(
        encode_comment("keep\nalive").expect("comment"),
        ": keep\n: alive\n\n"
    );
}
