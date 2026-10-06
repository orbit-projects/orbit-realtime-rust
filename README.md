# Orbit Realtime Rust

`orbit-realtime-rust` provides bounded Server-Sent Events framing and an HTTP body implementation for Rust services. It exposes the standard `http-body` contract so applications can use their chosen compatible HTTP server without adding a framework here.

```toml
[dependencies]
orbit-realtime-rust = "0.1.0-alpha.1"
```

```rust
use futures_util::stream;
use orbit_realtime_rust::{Event, response};
use std::convert::Infallible;

let event = Event::new("updated")?.with_event("record.changed")?;
let response = response(stream::iter([Ok::<_, Infallible>(event)]))?;
```

The body pulls one event per poll, preserving downstream backpressure without an internal queue. It sets `text/event-stream`, `no-cache`, and `nosniff` headers. Encoded events are bounded to 64 KiB. Event payloads and IDs are omitted from debug output.

The package does not provide authentication, authorization, event retention, replay, shared fanout, heartbeat scheduling, or reconnect policy. Secure the route in the application and bound upstream subscriptions.

See [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md), [security](docs/security/overview.md), and [development](docs/development/README.md).

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --release --locked
```

Pre-alpha. Hosted CI, proxy disconnect/flush interoperability, multi-instance load and soak tests, and release provenance remain open release gates. Licensed under Apache-2.0.
