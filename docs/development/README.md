# Development

Requires Rust 1.85 or later. Run `cargo fmt --check`, `cargo test --locked --all-targets`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, and `cargo build --release --locked` before proposing changes.

Tests should remain deterministic and avoid live infrastructure. Update the README and changelog when a public API, security property, lifecycle rule, or operational assumption changes. Hosted CI, dependency review, artifact integrity, provenance, proxy interoperability, and representative load evidence are release gates; local green checks do not establish production readiness.
