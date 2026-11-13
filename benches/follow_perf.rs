// Reuse the integration fixture so benchmarks exercise the same supported journal layout.
#[allow(dead_code)]
#[path = "../tests/support/synthetic_journal.rs"]
mod synthetic_journal;

use synthetic_journal::{synthetic_realtime_usec, write_synthetic_journal_file};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sdjournal::{Journal, JournalConfig, JournalQuery, LiveJournal, LiveSubscription, MmapPolicy};
use std::fs::{OpenOptions, create_dir_all};
use std::hint::black_box;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::TryRecvError;
use std::time::Duration;
use tempfile::TempDir;

const MACHINE_ID_DIR: &str = "0123456789abcdef0123456789abcdef";
const FILES_PER_DIR: usize = 2;
const PRUNING_OLD_FILES: usize = 24;
const UNIT_FIELDS: [&str; 3] = ["_SYSTEMD_UNIT", "UNIT", "OBJECT_SYSTEMD_UNIT"];
const PRUNING_OLD_UNIT: &str = "bench-old.service";
const PRUNING_RECENT_UNIT: &str = "bench-recent.service";
const BENCH_UNITS: [&str; 8] = [
    "bench-a.service",
    "bench-b.service",
    "bench-c.service",
    "bench-d.service",
    "bench-e.service",
    "bench-f.service",
    "bench-g.service",
    "bench-h.service",
];

struct SyntheticJournalLayout {
    root: TempDir,
    files_per_dir: usize,
}

struct PruningQueryLayout {
    root: TempDir,
    recent_seed: u8,
}

impl SyntheticJournalLayout {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create synthetic journal root");
        let machine_dir = root.path().join(MACHINE_ID_DIR);
        create_dir_all(&machine_dir).expect("create machine-id dir");

        let mut seed = 1u8;
        for idx in 0..FILES_PER_DIR {
            write_synthetic_journal_file(
                &root.path().join(format!("root-{idx}.journal")),
                &BENCH_UNITS,
                seed,
            );
            seed = seed.wrapping_add(1);
        }
        for idx in 0..FILES_PER_DIR {
            write_synthetic_journal_file(
                &machine_dir.join(format!("nested-{idx}.journal")),
                &BENCH_UNITS,
                seed,
            );
            seed = seed.wrapping_add(1);
        }

        Self {
            root,
            files_per_dir: FILES_PER_DIR,
        }
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    fn total_files(&self) -> usize {
        FILES_PER_DIR * 2
    }

    fn latest_file_path(&self) -> PathBuf {
        self.root
            .path()
            .join(MACHINE_ID_DIR)
            .join(format!("nested-{}.journal", self.files_per_dir - 1))
    }

    fn latest_file_seed(&self) -> u8 {
        u8::try_from(self.files_per_dir * 2).expect("latest file seed fits in u8")
    }
}

impl PruningQueryLayout {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create pruning benchmark root");
        for idx in 0..PRUNING_OLD_FILES {
            let seed = u8::try_from(idx + 1).expect("old file seed fits in u8");
            let path = root.path().join(format!("old-{idx}.journal"));
            write_synthetic_journal_file(&path, &[PRUNING_OLD_UNIT], seed);
            archive_journal_file(&path);
        }

        let recent_seed = 200;
        let path = root.path().join("recent.journal");
        write_synthetic_journal_file(&path, &[PRUNING_RECENT_UNIT], recent_seed);
        archive_journal_file(&path);

        Self { root, recent_seed }
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    fn recent_since_realtime(&self) -> u64 {
        synthetic_realtime_usec(self.recent_seed, 0)
    }
}

fn archive_journal_file(path: &Path) {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open query fixture for archiving");
    file.seek(SeekFrom::Start(16)).expect("seek header state");
    file.write_all(&[2]).expect("mark query fixture archived");
}

fn benchmark_config() -> JournalConfig {
    JournalConfig {
        poll_interval: Duration::ZERO,
        ..JournalConfig::default()
    }
}

fn low_memory_benchmark_config() -> JournalConfig {
    JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..benchmark_config()
    }
}

fn open_layout(layout: &SyntheticJournalLayout) -> Journal {
    Journal::open_dir_with_config(layout.root(), benchmark_config())
        .expect("open synthetic journal layout")
}

fn build_match_unit_query(journal: &Journal, unit: &str) -> JournalQuery {
    let mut q = journal.query();
    q.match_unit(unit);
    q
}

fn build_match_exact_query(journal: &Journal, unit: &str) -> JournalQuery {
    let mut q = journal.query();
    q.match_exact("_SYSTEMD_UNIT", unit.as_bytes());
    q
}

fn build_multi_unit_exact_query(journal: &Journal, units: &[&str]) -> JournalQuery {
    let mut q = journal.query();
    for unit in units {
        for field in UNIT_FIELDS {
            q.or_group(|g| {
                g.match_exact(field, unit.as_bytes());
            });
        }
    }
    q
}

fn sanity_check(layout: &SyntheticJournalLayout) {
    let journal = open_layout(layout);
    let expected_per_unit = layout.total_files();
    let unit = BENCH_UNITS[0];

    let exact_count = build_match_exact_query(&journal, unit)
        .collect_owned()
        .expect("exact query should succeed")
        .len();
    assert_eq!(exact_count, expected_per_unit);

    let unit_count = build_match_unit_query(&journal, unit)
        .collect_owned()
        .expect("match_unit query should succeed")
        .len();
    assert_eq!(unit_count, expected_per_unit);

    let combined_count = build_multi_unit_exact_query(&journal, &BENCH_UNITS[..4])
        .collect_owned()
        .expect("combined query should succeed")
        .len();
    assert_eq!(combined_count, expected_per_unit * 4);
}

fn benchmark_query_perf(c: &mut Criterion) {
    let layout = SyntheticJournalLayout::new();
    for idx in 0..FILES_PER_DIR {
        archive_journal_file(&layout.root().join(format!("root-{idx}.journal")));
        archive_journal_file(
            &layout
                .root()
                .join(MACHINE_ID_DIR)
                .join(format!("nested-{idx}.journal")),
        );
    }
    sanity_check(&layout);
    let journal = open_layout(&layout);

    // Only a single archived file can use indexed prefiltering without changing
    // systemd's conditional cross-file merge order.
    let indexed_root = tempfile::tempdir().expect("create indexed query root");
    let indexed_path = indexed_root.path().join("indexed.journal");
    write_synthetic_journal_file(&indexed_path, &BENCH_UNITS, 1);
    archive_journal_file(&indexed_path);
    let indexed_journal = Journal::open_dir_with_config(indexed_root.path(), benchmark_config())
        .expect("open single-file indexed journal");

    let mut group = c.benchmark_group("query_perf");
    group.sample_size(30);

    group.bench_function("open_dir_4_files_8_units", |b| {
        b.iter(|| black_box(open_layout(&layout)));
    });

    group.bench_function("single_unit_match_exact", |b| {
        b.iter(|| {
            let count = build_match_exact_query(&journal, BENCH_UNITS[0])
                .collect_owned()
                .expect("exact query should succeed")
                .len();
            black_box(count)
        });
    });

    group.bench_function("single_unit_match_unit", |b| {
        b.iter(|| {
            let count = build_match_unit_query(&journal, BENCH_UNITS[0])
                .collect_owned()
                .expect("match_unit query should succeed")
                .len();
            black_box(count)
        });
    });

    group.bench_function("single_file_indexed_match_exact", |b| {
        b.iter(|| {
            let count = build_match_exact_query(&indexed_journal, BENCH_UNITS[0])
                .collect_owned()
                .expect("indexed exact query should succeed")
                .len();
            assert_eq!(count, 1);
            black_box(count)
        });
    });

    group.bench_function("single_file_indexed_match_unit", |b| {
        b.iter(|| {
            let count = build_match_unit_query(&indexed_journal, BENCH_UNITS[0])
                .collect_owned()
                .expect("indexed unit query should succeed")
                .len();
            assert_eq!(count, 1);
            black_box(count)
        });
    });

    group.bench_function("multi_unit_independent_match_unit_queries_x4", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for unit in &BENCH_UNITS[..4] {
                count = count.saturating_add(
                    build_match_unit_query(&journal, unit)
                        .collect_owned()
                        .expect("independent match_unit query should succeed")
                        .len(),
                );
            }
            black_box(count)
        });
    });

    group.bench_function("multi_unit_combined_exact_query_x1", |b| {
        b.iter(|| {
            let count = build_multi_unit_exact_query(&journal, &BENCH_UNITS[..4])
                .collect_owned()
                .expect("combined unit query should succeed")
                .len();
            black_box(count)
        });
    });

    let pruning_layout = PruningQueryLayout::new();
    let pruning_journal =
        Journal::open_dir_with_config(pruning_layout.root(), low_memory_benchmark_config())
            .expect("open pruning benchmark journal");
    group.bench_function("many_old_files_since_realtime", |b| {
        b.iter(|| {
            let mut query = pruning_journal.query();
            query
                .since_realtime(pruning_layout.recent_since_realtime())
                .match_unit(PRUNING_RECENT_UNIT);
            let count = query
                .collect_owned()
                .expect("recent query should succeed")
                .len();
            assert_eq!(count, 1);
            black_box(count)
        });
    });

    group.finish();
}

struct IndependentLiveSetup {
    engines: Vec<LiveJournal>,
    subscriptions: Vec<LiveSubscription>,
}

struct SharedLiveSetup {
    engine: LiveJournal,
    subscriptions: Vec<LiveSubscription>,
}

fn build_independent_live_setup(
    layout: &SyntheticJournalLayout,
    units: &[&str],
) -> IndependentLiveSetup {
    let mut engines = Vec::with_capacity(units.len());
    let mut subscriptions = Vec::with_capacity(units.len());

    for unit in units {
        let mut live = LiveJournal::open_dir_with_config(layout.root(), benchmark_config())
            .expect("independent live engine should succeed");
        let mut filter = live.filter();
        filter.match_unit(unit);
        let subscription = live
            .subscribe(filter)
            .expect("independent live subscription should succeed");
        engines.push(live);
        subscriptions.push(subscription);
    }

    IndependentLiveSetup {
        engines,
        subscriptions,
    }
}

fn build_shared_live_setup(layout: &SyntheticJournalLayout, units: &[&str]) -> SharedLiveSetup {
    let mut engine = LiveJournal::open_dir_with_config(layout.root(), benchmark_config())
        .expect("shared live engine should succeed");
    let mut subscriptions = Vec::with_capacity(units.len());

    for unit in units {
        let mut filter = engine.filter();
        filter.match_unit(unit);
        let subscription = engine
            .subscribe(filter)
            .expect("shared live subscription should succeed");
        subscriptions.push(subscription);
    }

    SharedLiveSetup {
        engine,
        subscriptions,
    }
}

fn drain_ready(subscription: &LiveSubscription, expected_unit: &str) -> usize {
    let mut ready = 0usize;
    loop {
        match subscription.try_recv() {
            Ok(item) => {
                let entry = item.expect("live delivery should succeed");
                assert_eq!(entry.get("_SYSTEMD_UNIT"), Some(expected_unit.as_bytes()));
                assert!(
                    entry.seqnum() > BENCH_UNITS.len() as u64,
                    "live delivery must contain a newly appended entry"
                );
                ready += 1;
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => panic!("live benchmark subscription disconnected"),
        }
    }
    ready
}

fn benchmark_follow_perf(c: &mut Criterion) {
    let layout = SyntheticJournalLayout::new();
    sanity_check(&layout);
    let appended_units = appended_units_for_delivery();

    let mut group = c.benchmark_group("follow_perf");
    group.sample_size(20);

    // Live setups retain file descriptors and inotify watches. Keep one setup alive
    // at a time, and exclude its construction and destruction from poll timings.

    group.bench_function("direct_multi_unit_independent_live_engines_x4_setup", |b| {
        b.iter(|| black_box(build_independent_live_setup(&layout, &BENCH_UNITS[..4])));
    });

    group.bench_function("direct_shared_engine_multi_subscriptions_x4_setup", |b| {
        b.iter(|| black_box(build_shared_live_setup(&layout, &BENCH_UNITS[..4])));
    });

    group.bench_function(
        "direct_multi_unit_independent_live_engines_x4_idle_poll",
        |b| {
            b.iter_batched_ref(
                || build_independent_live_setup(&layout, &BENCH_UNITS[..4]),
                |setup| {
                    let mut ready = 0usize;
                    for engine in &mut setup.engines {
                        engine.poll_once().expect("idle live poll should succeed");
                    }
                    for (subscription, unit) in setup.subscriptions.iter().zip(&BENCH_UNITS[..4]) {
                        ready += drain_ready(subscription, unit);
                    }
                    assert_eq!(ready, 0);
                    black_box(ready)
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(
        "direct_shared_engine_multi_subscriptions_x4_idle_poll",
        |b| {
            b.iter_batched_ref(
                || build_shared_live_setup(&layout, &BENCH_UNITS[..4]),
                |setup| {
                    setup
                        .engine
                        .poll_once()
                        .expect("shared idle live poll should succeed");
                    let mut ready = 0usize;
                    for (subscription, unit) in setup.subscriptions.iter().zip(&BENCH_UNITS[..4]) {
                        ready += drain_ready(subscription, unit);
                    }
                    assert_eq!(ready, 0);
                    black_box(ready)
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function("direct_single_unit_live_engine_deliver_appended", |b| {
        b.iter_batched_ref(
            || {
                let layout = SyntheticJournalLayout::new();
                let mut live = LiveJournal::open_dir_with_config(layout.root(), benchmark_config())
                    .expect("single-unit live engine should succeed");
                let mut filter = live.filter();
                filter.match_unit(BENCH_UNITS[0]);
                let subscription = live
                    .subscribe(filter)
                    .expect("single-unit live subscription should succeed");
                rewrite_latest_file(&layout, &appended_units);
                (layout, live, subscription)
            },
            |(_layout, live, subscription)| {
                let delivered = poll_until_ready(live, subscription, BENCH_UNITS[0], 3);
                assert_eq!(delivered, 1, "the appended entry must be delivered");
                black_box(delivered)
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(
        "direct_multi_unit_independent_live_engines_x4_deliver_appended",
        |b| {
            b.iter_batched_ref(
                || {
                    let layout = SyntheticJournalLayout::new();
                    let setup = build_independent_live_setup(&layout, &BENCH_UNITS[..4]);
                    rewrite_latest_file(&layout, &appended_units);
                    (layout, setup)
                },
                |(_layout, setup)| {
                    let mut delivered = 0usize;
                    for ((engine, subscription), unit) in setup
                        .engines
                        .iter_mut()
                        .zip(&setup.subscriptions)
                        .zip(&BENCH_UNITS[..4])
                    {
                        delivered += poll_until_ready(engine, subscription, unit, 3);
                    }
                    assert_eq!(
                        delivered, 4,
                        "each subscription must receive its appended entry"
                    );
                    black_box(delivered)
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(
        "direct_shared_engine_multi_subscriptions_x4_deliver_appended",
        |b| {
            b.iter_batched_ref(
                || {
                    let layout = SyntheticJournalLayout::new();
                    let setup = build_shared_live_setup(&layout, &BENCH_UNITS[..4]);
                    rewrite_latest_file(&layout, &appended_units);
                    (layout, setup)
                },
                |(_layout, setup)| {
                    let delivered = poll_until_all_ready(
                        &mut setup.engine,
                        &setup.subscriptions,
                        &BENCH_UNITS[..4],
                        3,
                    );
                    assert_eq!(
                        delivered, 4,
                        "each subscription must receive its appended entry"
                    );
                    black_box(delivered)
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

fn appended_units_for_delivery() -> Vec<String> {
    let mut units = BENCH_UNITS
        .iter()
        .map(|unit| (*unit).to_string())
        .collect::<Vec<_>>();
    units.extend(
        BENCH_UNITS[..4]
            .iter()
            .map(|unit| (*unit).to_string())
            .collect::<Vec<_>>(),
    );
    units
}

fn rewrite_latest_file(layout: &SyntheticJournalLayout, units: &[String]) {
    write_synthetic_journal_file(&layout.latest_file_path(), units, layout.latest_file_seed());
}

fn poll_until_ready(
    engine: &mut LiveJournal,
    subscription: &LiveSubscription,
    expected_unit: &str,
    max_attempts: usize,
) -> usize {
    for _ in 0..max_attempts {
        engine
            .poll_once()
            .expect("live poll for appended entry should succeed");
        let ready = drain_ready(subscription, expected_unit);
        if ready != 0 {
            assert_eq!(ready, 1, "each subscription must receive exactly one entry");
            return ready;
        }
    }
    panic!("live benchmark did not deliver the appended entry after {max_attempts} polls");
}

fn poll_until_all_ready(
    engine: &mut LiveJournal,
    subscriptions: &[LiveSubscription],
    expected_units: &[&str],
    max_attempts: usize,
) -> usize {
    assert_eq!(subscriptions.len(), expected_units.len());
    let mut delivered = vec![0usize; subscriptions.len()];
    for _ in 0..max_attempts {
        engine
            .poll_once()
            .expect("shared live poll for appended entries should succeed");
        for ((subscription, unit), count) in
            subscriptions.iter().zip(expected_units).zip(&mut delivered)
        {
            *count += drain_ready(subscription, unit);
            assert!(
                *count <= 1,
                "a subscription received duplicate appended entries"
            );
        }
        if delivered.iter().all(|&count| count == 1) {
            return subscriptions.len();
        }
    }
    panic!("live benchmark deliveries per subscription after {max_attempts} polls: {delivered:?}");
}

criterion_group!(benches, benchmark_query_perf, benchmark_follow_perf);
criterion_main!(benches);
