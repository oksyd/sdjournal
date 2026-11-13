set shell := ["bash", "-euo", "pipefail", "-c"]

patch:
    cargo release patch --no-publish --execute

publish:
    cargo publish

ci:
    cargo fmt --all --check
    cargo fmt --manifest-path fuzz/Cargo.toml --all --check
    cargo clippy --locked --all-targets -- -D warnings
    cargo clippy --locked --all-targets --all-features -- -D warnings
    cargo clippy --locked --all-targets --no-default-features -- -D warnings
    cargo nextest run --locked
    cargo nextest run --locked --all-features
    cargo nextest run --locked --no-default-features --lib --test offline_journal --test journal_snapshot --test parser_regressions --test live_shared_engine
    cargo test --locked --bench follow_perf -- --test
    cargo test --locked --doc
    cargo test --locked --doc --all-features
    cargo test --locked --doc --no-default-features
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-default-features --no-deps

audit:
    cargo audit --deny warnings
    cargo audit --file fuzz/Cargo.lock --deny warnings

bench:
    cargo bench --bench follow_perf
