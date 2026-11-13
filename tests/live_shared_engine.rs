mod support;

use sdjournal::{
    Journal, JournalConfig, LiveJournal, LiveQueueFullPolicy, LiveSubscription, MmapPolicy,
    SubscriptionOptions,
};
use std::fs;
use std::path::Path;
#[cfg(feature = "tokio")]
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::time::{Duration, Instant};
use support::synthetic_journal::{
    SyntheticJournalFile, synthetic_message, write_synthetic_journal_file,
};

#[test]
fn compound_filters_agree_between_historical_replay_and_live_delivery() {
    for offline in [false, true] {
        let layout = SyntheticJournalFile::new(&[
            "alpha.service",
            "beta.service",
            "alpha.service",
            "beta.service",
            "alpha.service",
        ]);
        if offline {
            let path = layout.root().join("synthetic.journal");
            let mut bytes = fs::read(&path).unwrap();
            bytes[16] = 0;
            fs::write(path, bytes).unwrap();
        }
        let config = JournalConfig {
            mmap_policy: MmapPolicy::Never,
            ..live_test_config()
        };
        let journal = Journal::open_dir_with_config(layout.root(), config.clone()).unwrap();
        let mut filter: sdjournal::Filter = journal.filter();
        filter
            .match_present("MESSAGE")
            .or_group(|group: &mut sdjournal::OrGroupBuilder| {
                group.match_exact("PRIORITY", b"1");
            })
            .or_group(|group| {
                group.match_exact("PRIORITY", b"3");
            })
            .match_unit("alpha.service")
            .match_unit_bytes(b"alpha.service")
            .or_group(|_| {});
        let expected = [
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(7, 2, "alpha.service"),
        ];
        let query = journal.query_with_filter(filter.clone());
        let historical: Vec<_> = query
            .collect_owned()
            .unwrap()
            .iter()
            .map(|entry| String::from_utf8_lossy(entry.get("MESSAGE").unwrap()).into_owned())
            .collect();
        assert_eq!(historical, expected);

        let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
        let future = live.subscribe(filter.clone()).unwrap();
        let mut options = SubscriptionOptions::new(filter);
        options.since_realtime(0);
        let replay = live.subscribe_with_options(options).unwrap();
        let mut replayed = Vec::new();
        for _ in 0..8 {
            live.poll_once().unwrap();
            drain_ready_messages(&replay, &mut replayed);
        }
        assert_eq!(replayed, expected);
        assert_subscription_empty(&future);

        layout.rewrite(&[
            "alpha.service",
            "beta.service",
            "alpha.service",
            "beta.service",
            "alpha.service",
            "beta.service",
            "alpha.service",
            "alpha.service",
        ]);
        assert_eq!(poll_until_delivered(&mut live, 2), 2);
        let appended = synthetic_message(7, 7, "alpha.service");
        assert_eq!(field(&recv_ready(&future), "MESSAGE"), appended);
        assert_eq!(field(&recv_ready(&replay), "MESSAGE"), appended);
        assert_subscription_empty(&future);
        assert_subscription_empty(&replay);
    }
}

#[test]
fn subscriptions_revalidate_filters_created_with_a_larger_term_budget() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let source = LiveJournal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let mut filter = source.filter();
    filter.match_unit("alpha.service");

    let config = JournalConfig {
        max_query_terms: 2,
        ..live_test_config()
    };
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
    assert!(matches!(
        live.subscribe(filter.clone()),
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::QueryTerms,
            limit: 2,
        })
    ));
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    assert!(matches!(
        live.subscribe_with_options(options),
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::QueryTerms,
            limit: 2,
        })
    ));
}

#[test]
fn reusable_filters_enforce_destination_field_name_limits_for_every_read_mode() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let source = Journal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let mut filter = source.filter();
    filter.or_group(|group| {
        group.match_present("SYSLOG_IDENTIFIER");
    });
    let config = JournalConfig {
        max_field_name_len: 16,
        ..live_test_config()
    };
    let destination = Journal::open_dir_with_config(layout.root(), config.clone()).unwrap();
    assert!(matches!(
        destination.query_with_filter(filter.clone()).iter(),
        Err(sdjournal::SdJournalError::InvalidQuery { .. })
    ));
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
    assert!(matches!(
        live.subscribe(filter.clone()),
        Err(sdjournal::SdJournalError::InvalidQuery { .. })
    ));
    let mut options = SubscriptionOptions::new(filter.clone());
    options.since_realtime(0);
    assert!(matches!(
        live.subscribe_with_options(options),
        Err(sdjournal::SdJournalError::InvalidQuery { .. })
    ));
    assert_eq!(
        source
            .query_with_filter(filter)
            .collect_owned()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn unit_helpers_enforce_field_name_limits_in_queries_and_subscriptions() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let config = JournalConfig {
        max_field_name_len: 13,
        ..live_test_config()
    };
    let journal = Journal::open_dir_with_config(layout.root(), config.clone()).unwrap();
    assert!(matches!(
        journal.query().match_unit("alpha.service").iter(),
        Err(sdjournal::SdJournalError::InvalidQuery { .. })
    ));
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    assert!(matches!(
        live.subscribe(filter),
        Err(sdjournal::SdJournalError::InvalidQuery { .. })
    ));
}

#[test]
fn starting_cursor_errors_are_reported_before_registering_a_replay() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let cursor = sdjournal::Cursor::parse("s=ffffffffffffffffffffffffffffffff;i=1").unwrap();
    let mut options = SubscriptionOptions::new(live.filter());
    options.after_cursor(cursor);
    assert!(matches!(
        live.subscribe_with_options(options),
        Err(sdjournal::SdJournalError::NotFound)
    ));
    let subscription = subscribe_unit(&mut live, "alpha.service");
    layout.rewrite(&["alpha.service", "alpha.service"]);
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&subscription), "MESSAGE"),
        synthetic_message(7, 1, "alpha.service")
    );
}

#[test]
fn resume_rejects_a_missing_checkpoint_when_all_files_have_been_removed() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let journal = Journal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let cursor = journal
        .query()
        .iter()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .cursor();
    let mut live = journal.live().unwrap();
    fs::remove_file(layout.root().join("synthetic.journal")).unwrap();

    let mut options = SubscriptionOptions::new(live.filter());
    options.after_cursor(cursor);
    assert!(matches!(
        live.subscribe_with_options(options),
        Err(sdjournal::SdJournalError::NotFound)
    ));

    let subscription = subscribe_unit(&mut live, "alpha.service");
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["alpha.service"], 9);
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&subscription), "MESSAGE"),
        synthetic_message(9, 0, "alpha.service")
    );
}

#[test]
fn resume_rejects_a_checkpoint_absent_from_an_empty_journal_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let journal = Journal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let cursor = journal
        .query()
        .iter()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .cursor();
    layout.rewrite::<&str>(&[]);
    let path = layout.root().join("synthetic.journal");
    let mut empty = fs::read(&path).unwrap();
    empty[176..184].fill(0);
    fs::write(path, empty).unwrap();

    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let mut options = SubscriptionOptions::new(live.filter());
    options.after_cursor(cursor);
    let error = live
        .subscribe_with_options(options)
        .err()
        .expect("an empty journal cannot contain the checkpoint");
    assert!(
        matches!(error, sdjournal::SdJournalError::NotFound),
        "{error:?}"
    );

    let mut options = SubscriptionOptions::new(live.filter());
    options.since_realtime(0);
    let subscription = live.subscribe_with_options(options).unwrap();
    assert_eq!(live.poll_once().unwrap(), 0);
    layout.rewrite(&["alpha.service"]);
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&subscription), "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
}

#[test]
fn journal_live_keeps_the_same_committed_boundary_in_both_file_access_modes() {
    for max_open_files in [1, 64] {
        let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        write_synthetic_journal_file(&layout.root().join("other.journal"), &["beta.service"], 9);
        let committed = fs::read(&path).unwrap();
        let array = u64::from_le_bytes(committed[176..184].try_into().unwrap()) as usize;
        let first = u64::from_le_bytes(committed[array + 24..array + 32].try_into().unwrap());
        let mut pending = committed.clone();
        pending[136..144].copy_from_slice(&first.to_le_bytes());
        pending[152..160].copy_from_slice(&1u64.to_le_bytes());
        fs::write(&path, pending).unwrap();
        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..live_test_config()
            },
        )
        .unwrap();
        // Only the header changes: the first ENTRY remains at the original committed offset.
        fs::write(&path, committed).unwrap();
        let mut live = journal.live().unwrap();
        let subscription = subscribe_unit(&mut live, "alpha.service");
        assert_eq!(poll_until_delivered(&mut live, 1), 1);
        assert_eq!(
            field(&recv_ready(&subscription), "MESSAGE"),
            synthetic_message(7, 1, "alpha.service")
        );
        assert_subscription_empty(&subscription);
    }
}

#[test]
fn journal_live_discovers_files_created_before_the_watcher_started() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let journal = Journal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["beta.service"], 9);

    let mut live = journal.live().unwrap();
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&beta), "MESSAGE"),
        synthetic_message(9, 0, "beta.service")
    );
}

#[test]
fn replay_registered_during_empty_topology_receives_later_live_entries() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    fs::remove_file(&path).unwrap();
    let mut options = SubscriptionOptions::new(live.filter());
    options.since_realtime(0);
    let subscription = live.subscribe_with_options(options).unwrap();
    write_synthetic_journal_file(&path, &["alpha.service"], 9);
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&subscription), "MESSAGE"),
        synthetic_message(9, 0, "alpha.service")
    );
}

#[test]
fn shared_live_engine_dispatches_appended_entries_to_matching_subscriptions() {
    let initial_units = ["alpha.service", "beta.service"];
    let layout = SyntheticJournalFile::new(&initial_units);
    let journal = Journal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open synthetic journal");
    let mut live = journal.live().expect("create live engine");

    let alpha = subscribe_unit(&mut live, "alpha.service");
    let beta = subscribe_unit(&mut live, "beta.service");
    let alpha_by_message = subscribe_message(&mut live, &synthetic_message(7, 2, "alpha.service"));
    let absent = subscribe_unit(&mut live, "absent.service");

    assert_subscription_empty(&alpha);
    assert_subscription_empty(&beta);
    assert_subscription_empty(&alpha_by_message);
    assert_subscription_empty(&absent);

    let rewritten_units = [
        "alpha.service",
        "beta.service",
        "alpha.service",
        "beta.service",
        "gamma.service",
    ];
    layout.rewrite(&rewritten_units);

    let deliveries = poll_until_delivered(&mut live, 3);
    assert_eq!(deliveries, 3);

    let alpha_entry = recv_ready(&alpha);
    let beta_entry = recv_ready(&beta);
    let alpha_message_entry = recv_ready(&alpha_by_message);

    assert_entry(
        &alpha_entry,
        "alpha.service",
        &synthetic_message(7, 2, "alpha.service"),
    );
    assert_entry(
        &beta_entry,
        "beta.service",
        &synthetic_message(7, 3, "beta.service"),
    );
    assert_eq!(
        alpha_entry.cursor(),
        alpha_message_entry.cursor(),
        "the same appended entry should fan out to both matching subscriptions"
    );

    assert_subscription_empty(&alpha);
    assert_subscription_empty(&beta);
    assert_subscription_empty(&alpha_by_message);
    assert_subscription_empty(&absent);
}

#[test]
fn direct_live_engine_open_dir_dispatches_appended_entries() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine without historical journal");

    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_subscription_empty(&alpha);

    layout.rewrite(&["alpha.service", "alpha.service"]);

    let deliveries = poll_until_delivered(&mut live, 1);
    assert_eq!(deliveries, 1);

    let alpha_entry = recv_ready(&alpha);
    assert_entry(
        &alpha_entry,
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
}

#[test]
fn live_engine_initial_recheck_does_not_replay_existing_entries() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");

    let alpha = subscribe_unit(&mut live, "alpha.service");

    assert_eq!(live.poll_once().expect("initial live recheck"), 0);
    assert_subscription_empty(&alpha);
}

#[test]
fn live_only_tail_checkpoints_keep_older_appends_from_a_different_stream() {
    for from_journal in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let older = root.path().join("older.journal");
        let newer = root.path().join("newer.journal");
        write_synthetic_journal_file(&older, &["alpha.service"], 1);
        write_synthetic_journal_file(&newer, &["alpha.service"], 200);
        let mut live = if from_journal {
            Journal::open_dir_with_config(root.path(), live_test_config())
                .unwrap()
                .live()
                .unwrap()
        } else {
            LiveJournal::open_dir_with_config(root.path(), live_test_config()).unwrap()
        };
        let alpha = subscribe_unit(&mut live, "alpha.service");
        assert_eq!(live.poll_once().unwrap(), 0);
        assert_subscription_empty(&alpha);

        write_synthetic_journal_file(&older, &["alpha.service"; 2], 1);
        assert_eq!(poll_until_delivered(&mut live, 1), 1);
        let entry = recv_ready(&alpha);
        assert_entry(
            &entry,
            "alpha.service",
            &synthetic_message(1, 1, "alpha.service"),
        );
        assert!(
            entry.realtime_usec() < support::synthetic_journal::synthetic_realtime_usec(200, 0)
        );
        assert_subscription_empty(&alpha);
    }
}

#[test]
fn live_open_still_rejects_an_invalid_tail_entry_header() {
    for corruption in ["wrong-type", "missing-items"] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        let tail = u64::from_le_bytes(bytes[136..144].try_into().unwrap()) as usize;
        match corruption {
            "wrong-type" => bytes[tail] = 0,
            "missing-items" => bytes[tail + 8..tail + 16].copy_from_slice(&64u64.to_le_bytes()),
            _ => unreachable!(),
        }
        fs::write(&path, bytes).unwrap();
        for from_journal in [false, true] {
            let result = if from_journal {
                Journal::open_dir_with_config(layout.root(), live_test_config())
                    .unwrap()
                    .live()
            } else {
                LiveJournal::open_dir_with_config(layout.root(), live_test_config())
            };
            assert!(
                matches!(result, Err(sdjournal::SdJournalError::Corrupt { .. })),
                "live open accepted {corruption} (from_journal={from_journal})"
            );
        }
    }
}

#[test]
fn live_engine_drains_large_appends_across_bounded_batches() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine with tiny batches");

    let alpha = subscribe_unit(&mut live, "alpha.service");
    layout.rewrite(&["alpha.service", "alpha.service", "alpha.service"]);

    assert_eq!(live.poll_once().expect("first bounded poll"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );

    assert_eq!(live.poll_once().expect("second bounded poll"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 2, "alpha.service"),
    );
}

#[test]
fn live_engine_disconnects_slow_subscription_when_queue_is_full() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(1, 2, LiveQueueFullPolicy::Disconnect),
    )
    .expect("open live engine with bounded disconnect policy");

    let alpha = subscribe_unit(&mut live, "alpha.service");
    layout.rewrite(&["alpha.service", "alpha.service", "alpha.service"]);

    assert_eq!(live.poll_once().expect("bounded poll"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
    match alpha.try_recv() {
        Err(TryRecvError::Disconnected) => {}
        other => panic!("expected disconnected slow subscription, got {other:?}"),
    }
}

#[test]
fn live_engine_applies_one_batch_budget_across_modified_files() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let second = layout.root().join("second.journal");
    write_synthetic_journal_file(&second, &["alpha.service"], 9);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 3, LiveQueueFullPolicy::Disconnect),
    )
    .expect("open multi-file live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");

    layout.rewrite(&["alpha.service", "alpha.service", "alpha.service"]);
    write_synthetic_journal_file(
        &second,
        &["alpha.service", "alpha.service", "alpha.service"],
        9,
    );

    let first = live.poll_once().expect("first global-budget poll");
    assert_eq!(first, 3, "one cycle must honor the global batch budget");
    let mut messages = Vec::new();
    for _ in 0..first {
        messages.push(field(&recv_ready(&alpha), "MESSAGE"));
    }
    let second_batch = live.poll_once().expect("remaining global-budget poll");
    assert_eq!(second_batch, 1);
    messages.push(field(&recv_ready(&alpha), "MESSAGE"));
    messages.sort();
    assert_eq!(
        messages,
        [
            synthetic_message(7, 1, "alpha.service"),
            synthetic_message(7, 2, "alpha.service"),
            synthetic_message(9, 1, "alpha.service"),
            synthetic_message(9, 2, "alpha.service"),
        ],
        "a newly appended entry must not be rejected by another file's later global watermark"
    );
}

#[test]
fn bounded_batches_do_not_starve_later_files_behind_a_continuously_appending_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let later = layout.root().join("z-later.journal");
    write_synthetic_journal_file(&later, &["beta.service"], 9);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 1, LiveQueueFullPolicy::Block),
    )
    .unwrap();
    let alpha = subscribe_unit(&mut live, "alpha.service");
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(live.poll_once().unwrap(), 0);

    layout.rewrite(&["alpha.service"; 3]);
    write_synthetic_journal_file(&later, &["beta.service"; 2], 9);
    assert_eq!(live.poll_once().unwrap(), 1);
    recv_ready(&alpha);

    layout.rewrite(&["alpha.service"; 4]);
    assert_eq!(live.poll_once().unwrap(), 1);
    assert_eq!(
        field(&recv_ready(&beta), "MESSAGE"),
        synthetic_message(9, 1, "beta.service")
    );
    assert_subscription_empty(&alpha);
}

#[test]
fn topology_refresh_preserves_pending_file_fairness() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let later = layout.root().join("z-later.journal");
    write_synthetic_journal_file(&later, &["beta.service"], 9);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 1, LiveQueueFullPolicy::Block),
    )
    .unwrap();
    let alpha = subscribe_unit(&mut live, "alpha.service");
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(live.poll_once().unwrap(), 0);

    layout.rewrite(&["alpha.service"; 3]);
    write_synthetic_journal_file(&later, &["beta.service"; 2], 9);
    assert_eq!(live.poll_once().unwrap(), 1);
    recv_ready(&alpha);

    // A directory event must not reset the queue to lexical file order, giving the busy
    // first file another turn before the already-pending later file can be read.
    layout.rewrite(&["alpha.service"; 4]);
    fs::create_dir(layout.root().join("topology-change")).unwrap();
    assert_eq!(live.poll_once().unwrap(), 1);
    assert_eq!(
        field(&recv_ready(&beta), "MESSAGE"),
        synthetic_message(9, 1, "beta.service")
    );
    assert_subscription_empty(&alpha);
}

#[test]
fn pending_batches_still_observe_new_journal_files() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 1, LiveQueueFullPolicy::Block),
    )
    .unwrap();
    let alpha = subscribe_unit(&mut live, "alpha.service");
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(live.poll_once().unwrap(), 0);
    layout.rewrite(&["alpha.service"; 3]);
    assert_eq!(live.poll_once().unwrap(), 1);
    recv_ready(&alpha);

    write_synthetic_journal_file(&layout.root().join("z-later.journal"), &["beta.service"], 9);
    for count in [4, 5] {
        layout.rewrite(&vec!["alpha.service"; count]);
        assert_eq!(live.poll_once().unwrap(), 1);
        while let Ok(item) = alpha.try_recv() {
            item.unwrap();
        }
    }
    assert_eq!(
        field(&recv_ready(&beta), "MESSAGE"),
        synthetic_message(9, 0, "beta.service")
    );
}

#[test]
fn live_engine_deduplicates_logical_copies_across_bounded_batches() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let copy = layout.root().join("copy.journal");
    write_synthetic_journal_file(&copy, &["alpha.service"], 7);
    rewrite_file_id(&copy, 0xa7);

    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine with duplicated logical streams");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    layout.rewrite(&["alpha.service", "alpha.service"]);
    write_synthetic_journal_file(&copy, &["alpha.service", "alpha.service"], 7);
    rewrite_file_id(&copy, 0xa7);

    let mut messages = Vec::new();
    for _ in 0..6 {
        live.poll_once().expect("drain bounded logical copies");
        drain_ready_messages(&alpha, &mut messages);
    }

    assert_eq!(
        messages,
        [synthetic_message(7, 1, "alpha.service")],
        "the same logical entry copied into another journal file must be delivered only once"
    );
}

#[test]
fn topology_refresh_keeps_older_append_from_another_stream() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let later_stream = layout.root().join("later-stream.journal");
    write_synthetic_journal_file(&later_stream, &["alpha.service"], 9);

    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open multi-stream live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    // This appended entry has an earlier realtime timestamp than the existing seed-9 stream.
    // Creating another file in the same change window forces the topology-refresh path.
    layout.rewrite(&["alpha.service", "alpha.service"]);
    write_synthetic_journal_file(
        &layout.root().join("topology-trigger.journal"),
        &["beta.service"],
        11,
    );

    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
    assert_subscription_empty(&alpha);
}

#[test]
fn topology_refresh_retries_corrupt_payload_without_blocking_healthy_files() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let healthy = layout.root().join("healthy.journal");
    write_synthetic_journal_file(&healthy, &["beta.service"], 9);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open two healthy journals");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    layout.rewrite(&["alpha.service", "alpha.service"]);
    let damaged = layout.root().join("synthetic.journal");
    let valid = fs::read(&damaged).expect("read valid append");
    let mut bytes = valid.clone();
    let array = u64::from_le_bytes(bytes[176..184].try_into().expect("array offset")) as usize;
    let entry = u64::from_le_bytes(
        bytes[array + 32..array + 40]
            .try_into()
            .expect("second entry"),
    ) as usize;
    let data = u64::from_le_bytes(
        bytes[entry + 64..entry + 72]
            .try_into()
            .expect("DATA offset"),
    ) as usize;
    bytes[data] = 0;
    fs::write(&damaged, bytes).expect("invalidate appended DATA object only");
    write_synthetic_journal_file(&healthy, &["beta.service", "beta.service"], 9);
    fs::write(layout.root().join("topology-trigger"), []).expect("force topology refresh");

    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&beta), "MESSAGE"),
        synthetic_message(9, 1, "beta.service")
    );
    assert_subscription_empty(&alpha);

    fs::write(&damaged, valid).expect("repair appended DATA object");
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&alpha), "MESSAGE"),
        synthetic_message(7, 1, "alpha.service")
    );
    assert_subscription_empty(&beta);
}

#[test]
fn live_engine_survives_temporarily_empty_topology() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    fs::remove_file(layout.root().join("synthetic.journal")).expect("remove the last journal file");
    assert_eq!(
        live.poll_once()
            .expect("an empty topology must not stop the live engine"),
        0
    );
    assert_subscription_empty(&alpha);

    write_synthetic_journal_file(
        &layout.root().join("replacement.journal"),
        &["alpha.service"],
        9,
    );
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(9, 0, "alpha.service"),
    );
}

#[test]
fn live_engine_resumes_after_its_root_disappears_without_replaying_old_entries() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config()).unwrap();
    let subscription = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().unwrap(), 0);

    let parked = tempfile::tempdir().unwrap();
    let parked_root = parked.path().join("journal-root");
    fs::rename(layout.root(), &parked_root).unwrap();
    for _ in 0..2 {
        assert_eq!(live.poll_once().expect("missing roots are temporary"), 0);
        assert_subscription_empty(&subscription);
    }

    let mut resume = SubscriptionOptions::new(live.filter());
    resume
        .after_cursor(sdjournal::Cursor::parse("s=ffffffffffffffffffffffffffffffff;i=1").unwrap());
    assert!(matches!(
        live.subscribe_with_options(resume),
        Err(sdjournal::SdJournalError::NotFound)
    ));

    let mut replay = SubscriptionOptions::new(live.filter());
    replay.since_realtime(0);
    let late_subscription = live.subscribe_with_options(replay).unwrap();
    fs::rename(parked_root, layout.root()).unwrap();
    layout.rewrite(&["alpha.service", "alpha.service"]);
    assert_eq!(poll_until_delivered(&mut live, 2), 2);
    for receiver in [&subscription, &late_subscription] {
        assert_eq!(
            field(&recv_ready(receiver), "MESSAGE"),
            synthetic_message(7, 1, "alpha.service")
        );
        assert_subscription_empty(receiver);
    }
}

#[test]
fn one_missing_root_keeps_its_checkpoint_while_another_root_remains_healthy() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let other = tempfile::tempdir().unwrap();
    write_synthetic_journal_file(&other.path().join("healthy.journal"), &["beta.service"], 9);
    let mut live = LiveJournal::open_dirs_with_config(
        &[layout.root().to_path_buf(), other.path().to_path_buf()],
        live_test_config(),
    )
    .unwrap();
    let subscription = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().unwrap(), 0);

    let parked = tempfile::tempdir().unwrap();
    let parked_root = parked.path().join("journal-root");
    fs::rename(layout.root(), &parked_root).unwrap();
    for _ in 0..2 {
        assert_eq!(live.poll_once().unwrap(), 0);
    }
    fs::rename(parked_root, layout.root()).unwrap();
    layout.rewrite(&["alpha.service", "alpha.service"]);

    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_eq!(
        field(&recv_ready(&subscription), "MESSAGE"),
        synthetic_message(7, 1, "alpha.service"),
    );
    assert_subscription_empty(&subscription);
}

#[test]
fn live_rejects_a_channel_capacity_that_cannot_be_represented_in_memory() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let item_size = std::mem::size_of::<sdjournal::Result<sdjournal::LiveEntry>>();
    for capacity in [isize::MAX as usize / item_size + 1, usize::MAX] {
        let result = LiveJournal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                live_channel_capacity: capacity,
                ..live_test_config()
            },
        );
        assert!(matches!(
            result,
            Err(sdjournal::SdJournalError::InvalidQuery { .. })
        ));
    }
}

#[test]
fn live_engine_waits_for_header_commit_before_consuming_entry_array_slot() {
    const HEADER_N_ENTRIES_OFFSET: usize = 152;

    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let staged_path = layout.root().join("staged.bin");
    write_synthetic_journal_file(&staged_path, &["alpha.service", "alpha.service"], 7);
    let mut staged = fs::read(&staged_path).expect("read staged append");
    fs::remove_file(&staged_path).expect("remove staged append");

    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    // Model the journal writer's publication order: the new ENTRY_ARRAY slot and ENTRY object
    // become visible first, while header.n_entries still advertises only the committed prefix.
    staged[HEADER_N_ENTRIES_OFFSET..HEADER_N_ENTRIES_OFFSET + 8]
        .copy_from_slice(&1u64.to_le_bytes());
    fs::write(layout.root().join("synthetic.journal"), &staged)
        .expect("publish uncommitted entry-array slot");
    assert_eq!(
        live.poll_once()
            .expect("ignore entry-array slot before header commit"),
        0
    );
    assert_subscription_empty(&alpha);

    staged[HEADER_N_ENTRIES_OFFSET..HEADER_N_ENTRIES_OFFSET + 8]
        .copy_from_slice(&2u64.to_le_bytes());
    fs::write(layout.root().join("synthetic.journal"), &staged)
        .expect("commit staged entry in header");
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
    assert_subscription_empty(&alpha);
}

#[test]
fn live_engine_does_not_skip_a_zero_committed_slot_to_consume_a_future_entry() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    layout.rewrite(&["alpha.service", "alpha.service", "alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let valid = fs::read(&path).expect("read complete append");
    let mut incomplete = valid.clone();
    let array = u64::from_le_bytes(incomplete[176..184].try_into().expect("array offset")) as usize;
    incomplete[152..160].copy_from_slice(&2u64.to_le_bytes());
    incomplete[array + 32..array + 40].fill(0);
    fs::write(&path, incomplete).expect("leave a missing committed slot before a future slot");

    assert_eq!(live.poll_once().expect("retry incomplete ENTRY_ARRAY"), 0);
    assert_subscription_empty(&alpha);

    fs::write(&path, valid).expect("finish both appended entries");
    assert_eq!(poll_until_delivered(&mut live, 2), 2);
    assert_eq!(
        field(&recv_ready(&alpha), "MESSAGE"),
        synthetic_message(7, 1, "alpha.service")
    );
    assert_eq!(
        field(&recv_ready(&alpha), "MESSAGE"),
        synthetic_message(7, 2, "alpha.service")
    );
}

#[test]
fn unmatched_pending_batches_do_not_wait_for_another_change() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut cfg = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    cfg.poll_interval = Duration::from_millis(500);
    let mut live =
        LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open bounded live engine");
    let absent = subscribe_unit(&mut live, "absent.service");

    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);
    layout.rewrite(&["alpha.service", "alpha.service", "alpha.service"]);

    let started = Instant::now();
    assert_eq!(live.poll_once().expect("drain unmatched pending batch"), 0);
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "ready work should not wait for a second filesystem change"
    );
    assert_subscription_empty(&absent);
}

#[test]
fn live_replay_is_delivered_in_bounded_batches() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service", "alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine with bounded replay");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe with replay");

    assert_eq!(live.poll_once().expect("first replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );

    assert_eq!(live.poll_once().expect("second replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );

    assert_eq!(live.poll_once().expect("third replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 2, "alpha.service"),
    );
}

#[test]
fn live_replay_has_no_default_total_entry_cap() {
    const ENTRIES: usize = 4097;

    let units = vec!["alpha.service"; ENTRIES];
    let layout = SyntheticJournalFile::new(&units);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(1024, 1024, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine with unbounded replay");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe with replay");

    let mut received = 0usize;
    while received < ENTRIES {
        let delivered = live.poll_once().expect("replay batch");
        assert_ne!(delivered, 0, "replay should continue until all entries");

        for _ in 0..delivered {
            let entry = recv_ready(&alpha);
            if received == 0 || received == ENTRIES - 1 {
                assert_entry(
                    &entry,
                    "alpha.service",
                    &synthetic_message(7, received, "alpha.service"),
                );
            }
            received = received.saturating_add(1);
        }
    }

    assert_eq!(received, ENTRIES);
}

#[test]
fn replay_subscription_does_not_advance_existing_live_tail() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");
    let alpha = subscribe_unit(&mut live, "alpha.service");

    layout.rewrite(&["alpha.service", "alpha.service"]);

    let mut replay_filter = live.filter();
    replay_filter.match_unit("absent.service");
    let mut options = SubscriptionOptions::new(replay_filter);
    options.since_realtime(0);
    let absent_replay = live
        .subscribe_with_options(options)
        .expect("subscribe replaying absent unit");

    let deliveries = poll_until_delivered(&mut live, 1);
    assert_eq!(deliveries, 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
    assert_subscription_empty(&absent_replay);
}

#[test]
fn replay_snapshot_append_is_not_repeated_by_the_older_live_tail() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine");
    let keeper = subscribe_unit(&mut live, "absent.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    // The engine still points at the one-entry live tail, while the isolated replay snapshot
    // opened below already contains the append.
    layout.rewrite(&["alpha.service", "alpha.service"]);
    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe with append in replay snapshot");

    let mut messages = Vec::new();
    for _ in 0..8 {
        live.poll_once()
            .expect("drain replay and the older live-tail refresh");
        drain_ready_messages(&alpha, &mut messages);
    }

    assert_eq!(
        messages,
        [
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(7, 1, "alpha.service"),
        ],
        "the append covered by replay must not be delivered again by live tailing"
    );
    assert_subscription_empty(&keeper);
}

#[test]
fn replay_deduplicates_rotated_logical_copies_with_new_file_ids() {
    for (historical_count, copied_count) in [(2, 2), (6, 1)] {
        let layout = SyntheticJournalFile::new(&vec!["alpha.service"; historical_count]);
        let mut config = live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block);
        // The replay identity window is three entries. The second case checks that an early
        // deferred copy is removed even when historical replay outlives that window.
        config.max_journal_files = 2;
        let mut live =
            LiveJournal::open_dir_with_config(layout.root(), config).expect("open replay engine");
        let keeper = subscribe_unit(&mut live, "absent.service");
        assert_eq!(live.poll_once().expect("clear initial recheck"), 0);
        let mut options = SubscriptionOptions::new(live.filter());
        options.since_realtime(0);
        let replay = live
            .subscribe_with_options(options)
            .expect("subscribe replay");

        let copy = layout.root().join("rotated.journal");
        write_synthetic_journal_file(&copy, &vec!["alpha.service"; copied_count], 7);
        rewrite_file_id(&copy, 0xa7);

        let mut messages = Vec::new();
        for _ in 0..20 {
            live.poll_once()
                .expect("drain replay and rotated logical copies");
            drain_ready_messages(&replay, &mut messages);
        }
        let expected: Vec<_> = (0..historical_count)
            .map(|index| synthetic_message(7, index, "alpha.service"))
            .collect();
        assert_eq!(
            messages, expected,
            "a snapshot entry copied into a new file during replay must not be delivered twice"
        );
        assert_subscription_empty(&keeper);
    }
}

#[test]
fn active_replay_checks_late_copies_after_recent_identity_eviction() {
    let layout = SyntheticJournalFile::new(&["alpha.service"; 6]);
    let mut config = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    config.max_journal_files = 2;
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).expect("open replay");
    let mut options = SubscriptionOptions::new(live.filter());
    options.since_realtime(0);
    let replay = live
        .subscribe_with_options(options)
        .expect("subscribe replay");
    let mut messages = Vec::new();
    for _ in 0..5 {
        assert_eq!(
            live.poll_once().expect("replay through identity eviction"),
            1
        );
        drain_ready_messages(&replay, &mut messages);
    }

    let copy = layout.root().join("late-copy.journal");
    write_synthetic_journal_file(&copy, &["alpha.service"], 7);
    rewrite_file_id(&copy, 0xa7);
    for _ in 0..10 {
        live.poll_once()
            .expect("finish replay with late historical copy");
        drain_ready_messages(&replay, &mut messages);
    }
    let expected: Vec<_> = (0..6)
        .map(|index| synthetic_message(7, index, "alpha.service"))
        .collect();
    assert_eq!(messages, expected);
}

#[test]
fn replay_snapshot_switches_to_live_entries_created_after_subscription() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
    let mut cfg = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    cfg.max_live_replay_entries = Some(2);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open live engine");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replay");

    write_synthetic_journal_file(&layout.root().join("later.journal"), &["alpha.service"], 9);

    assert_eq!(live.poll_once().expect("first replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );

    assert_eq!(live.poll_once().expect("second replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );

    let deliveries = poll_until_delivered(&mut live, 1);
    assert_eq!(deliveries, 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(9, 0, "alpha.service"),
    );
}

#[test]
fn replay_buffers_post_subscription_entries_across_file_removal() {
    let layout = SyntheticJournalFile::new(&[
        "alpha.service",
        "alpha.service",
        "alpha.service",
        "alpha.service",
    ]);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(8, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open replay buffering engine");

    let keeper = subscribe_unit(&mut live, "absent.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replay");
    let observer = subscribe_unit(&mut live, "alpha.service");

    let later = layout.root().join("later.journal");
    write_synthetic_journal_file(&later, &["alpha.service"], 9);
    let mut replay_messages = Vec::new();
    let mut observed_later_file = false;
    for _ in 0..8 {
        live.poll_once().expect("observe post-subscription file");
        drain_ready_messages(&alpha, &mut replay_messages);
        if let Ok(item) = observer.try_recv() {
            let entry = item.expect("observer live entry");
            assert_eq!(
                field(&entry, "MESSAGE"),
                synthetic_message(9, 0, "alpha.service")
            );
            observed_later_file = true;
            break;
        }
    }
    assert!(observed_later_file, "later file was not observed");
    assert!(
        replay_messages.len() < 4,
        "the later file must be observed while historical replay is still active"
    );
    fs::remove_file(&later).expect("remove post-subscription journal before replay finishes");

    for _ in 0..16 {
        if replay_messages.len() == 5 {
            break;
        }
        live.poll_once().expect("drain replay or deferred live");
        drain_ready_messages(&alpha, &mut replay_messages);
    }
    assert_eq!(
        replay_messages,
        [
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(7, 1, "alpha.service"),
            synthetic_message(7, 2, "alpha.service"),
            synthetic_message(7, 3, "alpha.service"),
            synthetic_message(9, 0, "alpha.service"),
        ]
    );
    assert_subscription_empty(&keeper);
}

#[test]
fn replay_continues_when_the_previous_batch_cursor_file_is_removed() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["alpha.service", "alpha.service"],
        9,
    );
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open replay engine");
    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replay");

    assert_eq!(live.poll_once().expect("first replay batch"), 1);
    assert_eq!(
        field(&recv_ready(&alpha), "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
    fs::remove_file(layout.root().join("synthetic.journal"))
        .expect("vacuum the already replayed file");

    let mut messages = Vec::new();
    for _ in 0..8 {
        live.poll_once()
            .expect("continue replay from healthy files");
        drain_ready_messages(&alpha, &mut messages);
        if messages.len() == 2 {
            break;
        }
    }
    assert_eq!(
        messages,
        [
            synthetic_message(9, 0, "alpha.service"),
            synthetic_message(9, 1, "alpha.service"),
        ]
    );
}

#[test]
fn replay_retains_collected_entries_when_a_later_entry_returns_a_hard_error() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let middle = layout.root().join("middle.journal");
    let later = layout.root().join("later.journal");
    write_synthetic_journal_file(&middle, &["alpha.service"], 9);
    write_synthetic_journal_file(&later, &["alpha.service"], 11);
    // Only the middle file exceeds the configured field count. The other files remain valid
    // after omitting their optional final SYSLOG_IDENTIFIER field from the ENTRY item list.
    for path in [layout.root().join("synthetic.journal"), later] {
        let mut bytes = fs::read(&path).expect("read synthetic journal");
        let entry = u64::from_le_bytes(bytes[136..144].try_into().expect("tail offset")) as usize;
        bytes[entry + 8..entry + 16].copy_from_slice(&144u64.to_le_bytes());
        fs::write(path, bytes).expect("omit final ENTRY field");
    }
    let mut cfg = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    cfg.max_fields_per_entry = 5;
    let mut live = LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open live engine");
    let mut options = SubscriptionOptions::new(live.filter());
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replay");

    assert!(matches!(
        live.poll_once(),
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::FieldsPerEntry,
            limit: 5,
        })
    ));
    assert_subscription_empty(&alpha);
    fs::remove_file(layout.root().join("synthetic.journal"))
        .expect("remove the source of the entry collected before the error");

    let mut messages = Vec::new();
    for _ in 0..8 {
        live.poll_once()
            .expect("resume after the reported replay error");
        drain_ready_messages(&alpha, &mut messages);
        if messages.len() == 2 {
            break;
        }
    }
    assert_eq!(
        messages,
        [
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(11, 0, "alpha.service"),
        ]
    );
}

#[test]
fn historical_replay_cap_does_not_count_post_subscription_live_entries() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut cfg = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    cfg.max_live_replay_entries = Some(1);
    let mut live =
        LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open replay engine");
    let keeper = subscribe_unit(&mut live, "absent.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe capped replay");

    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["alpha.service", "alpha.service"],
        9,
    );
    let mut messages = Vec::new();
    for _ in 0..16 {
        live.poll_once()
            .expect("historical cap must not count deferred live entries");
        drain_ready_messages(&alpha, &mut messages);
        if messages.len() == 3 {
            break;
        }
    }
    assert_eq!(
        messages,
        [
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(9, 0, "alpha.service"),
            synthetic_message(9, 1, "alpha.service"),
        ]
    );
    assert_subscription_empty(&keeper);
}

#[test]
fn replay_buffer_overflow_reports_without_blocking_block_policy() {
    let layout = SyntheticJournalFile::new(&[
        "alpha.service",
        "alpha.service",
        "alpha.service",
        "alpha.service",
    ]);
    let mut cfg = live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block);
    cfg.poll_interval = Duration::from_millis(500);
    let mut live =
        LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open replay overflow engine");
    let keeper = subscribe_unit(&mut live, "absent.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    let mut replay_filter = live.filter();
    replay_filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(replay_filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replay");
    let observer = subscribe_unit(&mut live, "alpha.service");

    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["alpha.service", "alpha.service"],
        9,
    );

    let mut observed_later_file = false;
    for _ in 0..8 {
        live.poll_once().expect("observe first deferred entry");
        if let Ok(item) = alpha.try_recv() {
            item.expect("early historical replay entry");
        }
        if let Ok(item) = observer.try_recv() {
            let entry = item.expect("observer live entry");
            assert_eq!(
                field(&entry, "MESSAGE"),
                synthetic_message(9, 0, "alpha.service")
            );
            observed_later_file = true;
            break;
        }
    }
    assert!(observed_later_file, "first deferred entry was not observed");

    assert_eq!(
        live.poll_once().expect("fill replay subscription channel"),
        1
    );
    let started = Instant::now();
    assert_eq!(
        live.poll_once()
            .expect("overflow must become a deferred terminal error"),
        1
    );
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "a full channel must not make replay-buffer overflow self-block"
    );
    assert_eq!(
        field(&recv_ready(&observer), "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );

    let started = Instant::now();
    assert_eq!(
        live.poll_once().expect("defer error behind full channel"),
        0
    );
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "a terminal overflow error must not block behind a full channel"
    );

    recv_ready(&alpha);
    let mut overflow_error = None;
    for _ in 0..8 {
        live.poll_once().expect("deliver deferred terminal error");
        match alpha.try_recv() {
            Ok(Err(err)) => {
                overflow_error = Some(err);
                break;
            }
            Ok(Ok(entry)) => panic!(
                "unexpected replay entry after overflow: {}",
                field(&entry, "MESSAGE")
            ),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                panic!("subscription disconnected before reporting overflow")
            }
        }
    }
    let error = overflow_error.expect("replay buffer overflow error");
    assert!(
        error
            .to_string()
            .contains("live replay catch-up buffer exceeded 1 entries")
    );
    assert_subscription_empty(&keeper);
}

#[test]
fn replay_preserves_global_and_or_group_term_accounting() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let mut cfg = live_test_config();
    cfg.max_query_terms = 3;
    let mut live = LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open live engine");
    let mut filter = live.filter();
    filter
        .match_present("MESSAGE")
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        })
        .or_group(|group| {
            group.match_exact("PRIORITY", b"missing");
        });
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("accepted live filter must also be accepted by replay query");

    assert_eq!(live.poll_once().expect("replay matching entry"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );
}

#[test]
fn live_engine_watches_empty_machine_id_directories() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let machine_dir = layout.root().join("0123456789abcdef0123456789abcdef");
    fs::create_dir(&machine_dir).expect("create empty machine-id directory");
    let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
        .expect("open live engine");
    let beta = subscribe_unit(&mut live, "beta.service");
    assert_eq!(live.poll_once().expect("clear initial recheck"), 0);

    write_synthetic_journal_file(&machine_dir.join("nested.journal"), &["beta.service"], 9);
    assert_eq!(poll_until_delivered(&mut live, 1), 1);
    assert_entry(
        &recv_ready(&beta),
        "beta.service",
        &synthetic_message(9, 0, "beta.service"),
    );
}

#[cfg(feature = "tokio")]
#[test]
fn dropping_tokio_receiver_releases_underlying_subscription() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let config = live_test_config();
    let capacity = config.live_channel_capacity;
    let mut live =
        LiveJournal::open_dir_with_config(layout.root(), config).expect("open live engine");
    let receiver = subscribe_unit(&mut live, "alpha.service")
        .into_tokio()
        .expect("start Tokio forwarding thread");
    assert_eq!(receiver.max_capacity(), capacity);
    let (done_tx, done_rx) = mpsc::channel();
    let engine = thread::spawn(move || {
        let result = live.run();
        let _ = done_tx.send(result);
    });

    drop(receiver);
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("engine should observe Tokio receiver cancellation")
        .expect("live engine should exit cleanly");
    engine.join().expect("live engine thread should join");
}

#[test]
fn live_only_subscription_is_not_blocked_by_another_subscription_replay() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
    let later = layout.root().join("later.journal");
    write_synthetic_journal_file(&later, &["gamma.service"], 9);
    let mut live = LiveJournal::open_dir_with_config(
        layout.root(),
        live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block),
    )
    .expect("open live engine");

    let mut replay_filter = live.filter();
    replay_filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(replay_filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe replaying alpha");
    let beta = subscribe_unit(&mut live, "beta.service");

    write_synthetic_journal_file(&later, &["gamma.service", "beta.service"], 9);

    assert_eq!(live.poll_once().expect("dispatch ready live entry"), 1);
    assert_entry(
        &recv_ready(&beta),
        "beta.service",
        &synthetic_message(9, 1, "beta.service"),
    );
    assert_subscription_empty(&alpha);

    assert_eq!(live.poll_once().expect("dispatch alpha replay"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );
}

#[test]
fn replay_limit_counts_matching_entries_not_unrelated_entries() {
    let layout = SyntheticJournalFile::new(&["beta.service", "alpha.service"]);
    let mut cfg = live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block);
    cfg.max_live_replay_entries = Some(1);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open live engine");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe alpha replay");

    assert_eq!(live.poll_once().expect("single matching replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
}

#[test]
fn live_replay_configured_total_entry_cap_returns_limit_error() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
    let mut cfg = live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block);
    cfg.max_live_replay_entries = Some(1);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open live engine");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe capped replay");

    assert_eq!(live.poll_once().expect("first capped replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );

    match live.poll_once() {
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::LiveReplayEntries,
            limit,
        }) => assert_eq!(limit, 1),
        Ok(value) => panic!("expected replay limit error, got Ok({value})"),
        Err(err) => panic!("expected replay limit error, got {err}"),
    }
}

#[test]
fn replay_limit_errors_do_not_starve_healthy_live_subscriptions() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
    let mut config = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    config.max_live_replay_entries = Some(1);
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let replay = live.subscribe_with_options(options).unwrap();
    let healthy = subscribe_unit(&mut live, "beta.service");

    assert_eq!(live.poll_once().unwrap(), 1);
    recv_ready(&replay);
    layout.rewrite(&["alpha.service", "alpha.service", "beta.service"]);
    assert_eq!(live.poll_once().unwrap(), 1);
    recv_ready(&healthy);
    assert!(matches!(
        live.poll_once(),
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::LiveReplayEntries,
            ..
        })
    ));

    layout.rewrite(&[
        "alpha.service",
        "alpha.service",
        "beta.service",
        "beta.service",
    ]);
    for _ in 0..4 {
        match live.poll_once() {
            Ok(_) => {}
            Err(sdjournal::SdJournalError::LimitExceeded {
                kind: sdjournal::LimitKind::LiveReplayEntries,
                ..
            }) => {}
            Err(error) => panic!("unexpected live error: {error}"),
        }
        if let Ok(entry) = healthy.try_recv() {
            assert_entry(
                &entry.unwrap(),
                "beta.service",
                &synthetic_message(7, 3, "beta.service"),
            );
            return;
        }
    }
    panic!("a failed replay must not keep priority over ready live entries");
}

#[test]
fn live_file_errors_do_not_starve_healthy_replay_subscriptions() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let original = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&original).unwrap();
    let entry = u64::from_le_bytes(bytes[136..144].try_into().unwrap()) as usize;
    // The replay entry has five fields; the subsequently created live file exceeds the limit.
    bytes[entry + 8..entry + 16].copy_from_slice(&144u64.to_le_bytes());
    fs::write(original, bytes).unwrap();
    let mut config = live_test_config_with_live_limits(4, 1, LiveQueueFullPolicy::Block);
    config.max_fields_per_entry = 5;
    let mut live = LiveJournal::open_dir_with_config(layout.root(), config).unwrap();
    let mut options = SubscriptionOptions::new(live.filter());
    options.since_realtime(0);
    let replay = live.subscribe_with_options(options).unwrap();

    write_synthetic_journal_file(&layout.root().join("later.journal"), &["alpha.service"], 9);
    assert!(matches!(
        live.poll_once(),
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::FieldsPerEntry,
            limit: 5,
        })
    ));
    assert_eq!(
        live.poll_once().expect("yield failed live turn to replay"),
        1
    );
    assert_entry(
        &recv_ready(&replay),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );
}

#[test]
fn replay_snapshot_uses_the_same_healthy_copy_as_its_tail_checkpoint() {
    for corruption in ["entry-array", "tail-entry"] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let healthy = layout.root().join("synthetic.journal");
        let bad_copy = layout.root().join("aaa-invalid-copy.journal");
        let mut bytes = fs::read(&healthy).expect("read healthy journal");
        let header_offset = match corruption {
            "entry-array" => 176,
            "tail-entry" => 136,
            _ => unreachable!(),
        };
        let offset = u64::from_le_bytes(bytes[header_offset..header_offset + 8].try_into().unwrap())
            as usize;
        bytes[offset] = 0;
        // Both physical files have the same file ID. The earlier path opens successfully,
        // but only the healthy copy can provide a validated live-tail checkpoint.
        fs::write(&bad_copy, bytes).expect("write untrackable copy");
        let mut live = LiveJournal::open_dir_with_config(layout.root(), live_test_config())
            .expect("track the healthy copy");
        let mut options = SubscriptionOptions::new(live.filter());
        options.since_realtime(0);
        let replay = live
            .subscribe_with_options(options)
            .expect("snapshot the healthy copy");

        assert_eq!(live.poll_once().expect("replay healthy snapshot"), 1);
        assert_entry(
            &recv_ready(&replay),
            "alpha.service",
            &synthetic_message(7, 0, "alpha.service"),
        );
        assert_subscription_empty(&replay);
    }
}

#[test]
fn live_replay_snapshot_obeys_a_single_open_file_budget() {
    let root = tempfile::tempdir().unwrap();
    for seed in 0..96u8 {
        write_synthetic_journal_file(
            &root.path().join(format!("{seed:03}.journal")),
            &["alpha.service"],
            seed,
        );
    }
    let mut config = live_test_config_with_live_limits(128, 128, LiveQueueFullPolicy::Block);
    config.max_open_files = 1;
    config.mmap_policy = MmapPolicy::Never;
    for from_snapshot in [false, true] {
        let mut live = if from_snapshot {
            Journal::open_dir_with_config(root.path(), config.clone())
                .unwrap()
                .live()
                .unwrap()
        } else {
            LiveJournal::open_dir_with_config(root.path(), config.clone()).unwrap()
        };
        let mut options = SubscriptionOptions::new(live.filter());
        options.since_realtime(0);
        let replay = live.subscribe_with_options(options).unwrap();

        // Neither journal-to-live conversion nor replay snapshot construction may retain all
        // 96 handles, including under a smaller process-wide descriptor limit.
        assert_eq!(live.poll_once().unwrap(), 96);
        let mut messages = Vec::new();
        drain_ready_messages(&replay, &mut messages);
        assert_eq!(
            messages,
            (0..96u8)
                .map(|seed| synthetic_message(seed, 0, "alpha.service"))
                .collect::<Vec<_>>()
        );
        assert_subscription_empty(&replay);
    }
}

#[test]
fn live_replay_works_with_low_memory_journal_snapshot() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["beta.service", "alpha.service"],
        9,
    );
    let mut cfg = live_test_config_with_live_limits(1, 1, LiveQueueFullPolicy::Block);
    cfg.max_open_files = 1;
    cfg.mmap_policy = MmapPolicy::Never;
    let mut live =
        LiveJournal::open_dir_with_config(layout.root(), cfg).expect("open low-memory live engine");

    let mut filter = live.filter();
    filter.match_unit("alpha.service");
    let mut options = SubscriptionOptions::new(filter);
    options.since_realtime(0);
    let alpha = live
        .subscribe_with_options(options)
        .expect("subscribe with low-memory replay");

    assert_eq!(live.poll_once().expect("first low-memory replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(7, 0, "alpha.service"),
    );

    assert_eq!(live.poll_once().expect("second low-memory replay batch"), 1);
    assert_entry(
        &recv_ready(&alpha),
        "alpha.service",
        &synthetic_message(9, 1, "alpha.service"),
    );
}

#[test]
fn live_engine_skips_corrupt_tilde_journal_and_tracks_healthy_files() {
    let initial_units = ["alpha.service"];
    let layout = SyntheticJournalFile::new(&initial_units);
    write_corrupt_tilde_journal(layout.root());

    let journal = Journal::open_dir_with_config(layout.root(), live_test_config_with_tilde())
        .expect("open journal set containing a corrupt tilde file");
    let mut live = journal
        .live()
        .expect("live engine should skip the corrupt tilde file");

    let alpha = subscribe_unit(&mut live, "alpha.service");
    assert_subscription_empty(&alpha);

    let rewritten_units = ["alpha.service", "alpha.service"];
    layout.rewrite(&rewritten_units);

    let deliveries = poll_until_delivered(&mut live, 1);
    assert_eq!(deliveries, 1);

    let alpha_entry = recv_ready(&alpha);
    assert_entry(
        &alpha_entry,
        "alpha.service",
        &synthetic_message(7, 1, "alpha.service"),
    );
}

#[test]
fn live_engine_fails_when_all_journal_files_are_untrackable() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_corrupt_tilde_journal(layout.root());
    fs::remove_file(layout.root().join("synthetic.journal")).expect("remove healthy journal");

    let journal = Journal::open_dir_with_config(layout.root(), live_test_config_with_tilde())
        .expect("open corrupt journal header");

    match journal.live() {
        Err(sdjournal::SdJournalError::Corrupt { .. }) => {}
        Err(sdjournal::SdJournalError::Transient { .. }) => {}
        Err(err) => panic!("unexpected live error: {err}"),
        Ok(_) => panic!("expected live engine to fail without any trackable journal files"),
    }
}

fn live_test_config() -> JournalConfig {
    JournalConfig {
        poll_interval: Duration::from_millis(10),
        ..JournalConfig::default()
    }
}

fn live_test_config_with_tilde() -> JournalConfig {
    JournalConfig {
        include_journal_tilde: true,
        ..live_test_config()
    }
}

fn live_test_config_with_live_limits(
    live_channel_capacity: usize,
    max_live_batch_entries: usize,
    live_queue_full_policy: LiveQueueFullPolicy,
) -> JournalConfig {
    JournalConfig {
        live_channel_capacity,
        max_live_batch_entries,
        live_queue_full_policy,
        ..live_test_config()
    }
}

fn write_corrupt_tilde_journal(root: &Path) {
    let mut bytes = fs::read(root.join("synthetic.journal")).expect("read synthetic journal");

    bytes[24] = bytes[24].wrapping_add(101);
    let array = u64::from_le_bytes(bytes[176..184].try_into().expect("entry array offset"));
    bytes[usize::try_from(array).expect("entry array offset fits")] = 0;

    fs::write(root.join("corrupt.journal~"), bytes).expect("write corrupt tilde journal");
}

fn rewrite_file_id(path: &Path, first_byte: u8) {
    let mut bytes = fs::read(path).expect("read journal before changing file id");
    bytes[24] = first_byte;
    fs::write(path, bytes).expect("write journal with distinct file id");
}

fn subscribe_unit(live: &mut LiveJournal, unit: &str) -> LiveSubscription {
    let mut filter = live.filter();
    filter.match_unit(unit);
    live.subscribe(filter).expect("subscribe unit")
}

fn subscribe_message(live: &mut LiveJournal, message: &str) -> LiveSubscription {
    let mut filter = live.filter();
    filter.match_exact("MESSAGE", message.as_bytes());
    live.subscribe(filter).expect("subscribe message")
}

fn poll_until_delivered(live: &mut LiveJournal, expected: usize) -> usize {
    let mut delivered = 0usize;
    for _ in 0..10 {
        delivered = delivered.saturating_add(live.poll_once().expect("poll live engine"));
        if delivered >= expected {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    delivered
}

fn recv_ready(subscription: &LiveSubscription) -> sdjournal::LiveEntry {
    subscription
        .try_recv()
        .expect("subscription should have one ready item")
        .expect("live entry should decode")
}

fn drain_ready_messages(subscription: &LiveSubscription, out: &mut Vec<String>) {
    loop {
        match subscription.try_recv() {
            Ok(Ok(entry)) => out.push(field(&entry, "MESSAGE")),
            Ok(Err(err)) => panic!("subscription unexpectedly received error: {err}"),
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Disconnected) => panic!("subscription disconnected"),
        }
    }
}

fn assert_entry(entry: &sdjournal::LiveEntry, unit: &str, message: &str) {
    assert_eq!(field(entry, "_SYSTEMD_UNIT"), unit);
    assert_eq!(field(entry, "MESSAGE"), message);
}

fn field(entry: &sdjournal::LiveEntry, name: &str) -> String {
    String::from_utf8_lossy(entry.get(name).expect("entry field")).into_owned()
}

fn assert_subscription_empty(subscription: &LiveSubscription) {
    match subscription.try_recv() {
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => panic!("subscription disconnected"),
        Ok(Ok(entry)) => panic!(
            "subscription unexpectedly received MESSAGE={}",
            String::from_utf8_lossy(entry.get("MESSAGE").unwrap_or(b"<missing>"))
        ),
        Ok(Err(err)) => panic!("subscription unexpectedly received error: {err}"),
    }
}
