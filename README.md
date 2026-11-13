# sdjournal

[![crates.io](https://img.shields.io/crates/v/sdjournal.svg)](https://crates.io/crates/sdjournal)
[![docs.rs](https://img.shields.io/docsrs/sdjournal)](https://docs.rs/sdjournal)

`sdjournal` is a **pure Rust** systemd journal reader and query engine. It reads `*.journal` files directly and does **not** depend on `libsystemd` or invoke `journalctl` (tests may use `journalctl` for golden comparisons).

## Overview

- Pure Rust, with no `libsystemd` dependency
- Bounded, corruption-resistant parsing for production use
- Stable merge ordering, cursor checkpoints, and bounded shared live subscriptions
- Low-memory mode with bounded open files and optional mmap disablement
- Optional mmap, compression backends, tracing, Tokio integration, and FSS verification

## Compatibility

- Minimum supported Rust version (MSRV): **1.100.0**, using Rust 2024
- Target OS: **Linux**
- Other Unix hosts and Windows can parse user-supplied `*.journal` files; `Journal::open_default()` is Linux-only
- Targets outside Unix/Windows are rejected at compile time: native positional file reads are required for concurrent queries
- CI coverage includes Ubuntu 22.04 (systemd 249.x), Ubuntu 24.04 (systemd 255.x), and macOS offline checks

## Install

```bash
cargo add sdjournal
```

## Features

- Default: `mmap`, `lz4`, `zstd`
- Optional:
  - `xz`: enable XZ decompression
  - `tracing`: emit diagnostics via `tracing` (caller installs a subscriber)
  - `tokio`: provides an async live-subscription adapter
  - `verify-seal`: verify Forward Secure Sealing (TAG/FSS) with a systemd verification key

For constrained processes, set `JournalConfig::max_open_files` to a small value and
`JournalConfig::mmap_policy` to `MmapPolicy::Never`.

## Quickstart

```rust
use sdjournal::Journal;

let journal = Journal::open_default()?;
let mut q = journal.query();
q.match_exact("_SYSTEMD_UNIT", b"sshd.service");
q.since_realtime(0);

for item in q.iter()? {
    let entry = item?;
    if let Some(msg) = entry.get("MESSAGE") {
        println!("{}", String::from_utf8_lossy(msg));
    }
}
# Ok::<(), sdjournal::SdJournalError>(())
```

## Resume From Cursor

An end-to-end example that persists the last cursor and resumes via `after_cursor` is in:
- `examples/checkpoint_follow.rs`

Run it on Linux:

```bash
cargo run --example checkpoint_follow -- sshd.service /var/tmp/sdjournal.cursor
```

## Breaking API Changes

Cursors now use native systemd text (`s=...;i=...;b=...;m=...;t=...;x=...`). The retired
`SJ1:` format is rejected. Replace old checkpoints with a new native cursor or choose an explicit
replay start before resuming. Cursor IDs accept 32 hex digits, canonical UUIDs, or UUID URNs;
arbitrarily placed hyphens are rejected.
Partial cursors now require both `s` and `i`, or a realtime `t` field. Boot-relative `b`/`m`
cursors without either are rejected instead of producing incorrect results across boots.
Keep the full cursor returned by an entry; `b`/`m`/`t` partial cursors remain supported.

- `EntryRef::cursor()` and `EntryOwned::cursor()` return `Cursor` directly. The same applies to
  `LiveEntry::cursor()` through dereferencing. Remove `?`, `expect`, or `unwrap` from these calls.
- `Journal::seek_cursor()` returns `JournalQuery` directly. Cursor resolution and read errors are
  still reported by query execution through `iter()` or `collect_owned()`.
- Both historical and live OR-group closures use the public `sdjournal::OrGroupBuilder`.
  The old `LiveOrGroupBuilder` name has been removed.
- `LiveFilter` has been replaced by the shared `Filter`. Create filters with `Journal::filter()`
  or `LiveJournal::filter()`, and reuse them with `Journal::query_with_filter()` or subscriptions.
  Destination limits are checked without rebuilding the filter; invalid subscription filters and
  unresolved starting cursors now fail during registration, before polling begins.
- `LiveSubscription::into_tokio()` returns `Result<tokio::sync::mpsc::Receiver<_>>`. Use
  `subscription.into_tokio()?` to handle forwarding-thread creation failures, then `.recv().await`
  instead of `.next().await`. The receiver uses the subscription's configured channel capacity;
  `TokioSubscription` and `.into_receiver()` have been removed.
- `Journal::live()` retains the journal's committed snapshot boundary in both file-access modes.
  Entries committed after the snapshot was opened are delivered as live entries, including those
  committed before the live engine was created.
- Multi-file queries merge entry metadata before filtering so filters cannot change cursor order
  and cause resume gaps. This scans more metadata than prefiltering each file through its indexes;
  single-file queries retain indexed filtering. The bounded streaming merge coalesces identical
  entries at file heads, but does not promise global deduplication of arbitrarily overlapping files.
- Directory discovery skips missing roots and entries removed during scanning, but propagates other
  traversal errors even when another root contains readable journals.
- `Journal::verify_seal()` requires every opened file to be sealed through its last committed
  object. Unsealed files and unsealed tails now return an error; active journals may need a later
  TAG before verification can succeed. This authenticates FSS-protected object contents, not
  mutable indexes, the complete header, or the presence of every file and newer entry.

## Start Here

To understand the crate quickly, read or run these examples in order:
- `tour`: guided walkthrough of `Journal`, `JournalQuery`, `EntryRef`/`EntryOwned`, `Cursor`, and `LiveJournal`
- `tail`: the smallest “open default journal and read entries” path
- `match_unit`: the most common production filter shape
- `checkpoint_follow`: resume-safe live subscription for long-running consumers

## Examples

Open / discovery:
- `tour`: guided API overview and mental model
- `open_dir`: open one journal directory and print entries
- `open_dirs`: merge multiple journal roots
- `open_with_config`: customize `JournalConfig`

Query builder patterns:
- `tail`: print the newest entries from the default journal
- `filter_exact`: query by an exact field/value pair
- `match_present`: query by field presence
- `match_unit`: query one systemd unit
- `or_units`: OR across two units with `or_group`
- `recent_unit`: query a unit within a recent time window
- `print_fields`: dump all fields from the newest matching entry
- `collect_owned`: detach results into `EntryOwned`

Cursor / resume:
- `cursor_roundtrip`: print a cursor and resume from it with `seek_cursor`
- `after_cursor`: resume strictly after a saved cursor string
- `checkpoint_follow`: persist cursors while following

Streaming / integration:
- `follow_unit`: block and print a few newly appended entries from a live subscription
- `live_multi_subscriptions`: share one `LiveJournal` across multiple unit subscriptions
- `follow_tokio`: bridge a live subscription into Tokio (`--features tokio`)
- `verify_seal`: verify FSS tags (`--features verify-seal`)

## Development

### Known decoder limitation

The `zstd` feature requires ruzstd 0.9.1, which fixes the unbounded sequence expansion described in
[RUSTSEC-2026-0344](https://rustsec.org/advisories/RUSTSEC-2026-0344.html).
That release still accepts some compressed blocks whose literals exceed the declared frame-window
limit. The crate's decoded-output limit remains enforced, but this is not complete Zstd format
validation. The upstream fix is [PR #128](https://github.com/KillingSpark/zstd-rs/pull/128), pending a
published release. A specifically ignored regression tracks that remaining case; no private fork
or partial replacement decoder is bundled.

### Checks

The development toolchain follows `stable`; CI explicitly selects Rust 1.100.0 to verify the MSRV.
Keep both the root and fuzz `Cargo.lock` files in version control for reproducible checks. Library
consumers still resolve dependencies using the version requirements in `Cargo.toml`.
Both manifests deny Cargo's `unused_dependencies` lint for regular and build dependencies;
the feature matrix also checks optional dependencies in isolation.

```bash
just --justfile .justfile ci
just --justfile .justfile audit
```

These recipes require `just`, `cargo-nextest`, and `cargo-audit`. The CI recipe also smoke-tests the
benchmarks with assertions on live deliveries; it does not measure performance. CI tests each
optional feature with default features disabled, builds the fuzz harnesses with all compression backends enabled,
and runs strict `journalctl` comparisons on supported Ubuntu versions. Golden tests need a working
Linux journal and permission to write and read entries. They are explicitly ignored in the regular
suite; run them in a suitable environment with:

```bash
cargo test --locked --all-features --test golden_journalctl -- --ignored
```

Set `SDJOURNAL_TEST_USE_SUDO=1` if journal access requires sudo. The optional format-fixture test is
also ignored by default; provide `tests/fixtures/<case>/journal/` and a sibling `expected.json`, then run
`cargo test --locked --all-features --test format_fixtures -- --ignored`.
Explicitly requested integration tests fail if their dependencies or data are unavailable.

When intentionally updating dependencies, run `cargo update` for both manifests and repeat the
checks before committing the resulting lockfiles:

```bash
cargo update
cargo update --manifest-path fuzz/Cargo.toml
```
