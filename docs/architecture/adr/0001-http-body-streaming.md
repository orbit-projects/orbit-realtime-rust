# ADR 0001: Implement SSE as a pull-based http-body

- Status: accepted
- Date: 2026-10-06

## Context

Orbit Realtime has a Python implementation integrated with Orbit Core's ASGI response. The Rust option should stream SSE without requiring an additional web framework or owning an HTTP server. Rust applications use multiple HTTP runtimes, with http-body serving as the common streaming body contract.

## Decision

Expose bounded SSE framing and an http-body::Body over a caller-owned pull-based stream. Polling yields at most one encoded event and creates no intermediate queue. The host HTTP runtime owns socket writes, disconnects, and cancellation. The package supplies content type, cache control, and nosniff response headers.

## Consequences

The body composes with compatible Rust HTTP runtimes while leaving framework and server choices to applications. Applications must clean up source streams when the body is dropped and provide authorization, heartbeats, replay, and fanout as needed. Runtime and proxy disconnect behavior requires deployment-specific validation.
