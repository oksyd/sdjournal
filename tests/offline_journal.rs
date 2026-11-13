mod support;

use sdjournal::SdJournalError;
use sdjournal::{Cursor, Journal, MmapPolicy};
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use support::synthetic_journal::{
    SyntheticJournalFile, synthetic_message, synthetic_realtime_usec, write_synthetic_journal_file,
};

#[test]
fn user_supplied_journal_dir_supports_query_and_cursor_resume() {
    let units = ["alpha.service", "beta.service", "gamma.service"];
    let layout = SyntheticJournalFile::new(&units);
    layout.rewrite(&units);
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let all = journal
        .query()
        .collect_owned()
        .expect("collect all offline entries");
    assert_eq!(all.len(), 3);

    let mut alpha_query = journal.query();
    alpha_query.match_unit("alpha.service");
    let alpha_entries = alpha_query
        .collect_owned()
        .expect("query alpha entries from offline journal");
    assert_eq!(alpha_entries.len(), 1);
    assert_eq!(
        field(&alpha_entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );

    let cursor_text = alpha_entries[0].cursor().to_string();
    assert!(cursor_text.starts_with("s="));
    let cursor = Cursor::parse(&cursor_text).expect("parse cursor");

    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    let resumed_entries = resumed.collect_owned().expect("resume after stored cursor");

    assert_eq!(resumed_entries.len(), 2);
    assert_eq!(
        field(&resumed_entries[0], "MESSAGE"),
        synthetic_message(7, 1, "beta.service")
    );
    assert_eq!(
        field(&resumed_entries[1], "MESSAGE"),
        synthetic_message(7, 2, "gamma.service")
    );
}

#[test]
fn partial_native_cursors_resume_in_both_file_access_modes() {
    let layout = SyntheticJournalFile::new(&["first.service", "second.service", "third.service"]);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["later.service"], 9);
    for max_open_files in [1, 64] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            sdjournal::JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let entries = journal.query().collect_owned().unwrap();
        let full = entries[1].cursor().to_string();
        let sequence = full.split(';').take(2).collect::<Vec<_>>().join(";");
        let realtime = format!("t={:x}", entries[1].realtime_usec());
        for text in [sequence, realtime] {
            let cursor = Cursor::parse(&text).unwrap();
            assert_eq!(cursor.to_string(), text);
            for reverse in [false, true] {
                let mut inclusive = journal.seek_cursor(&cursor);
                inclusive.reverse(reverse);
                let mut expected = messages(&entries[1..]);
                if reverse {
                    expected.reverse();
                }
                assert_eq!(messages(&inclusive.collect_owned().unwrap()), expected);

                let mut exclusive = journal.query();
                exclusive.after_cursor(cursor.clone()).reverse(reverse);
                let mut expected = messages(&entries[2..]);
                if reverse {
                    expected.reverse();
                }
                assert_eq!(messages(&exclusive.collect_owned().unwrap()), expected);
            }
        }
    }
}

#[test]
fn monotonic_native_cursors_require_realtime_to_resume_across_boots() {
    let layout = SyntheticJournalFile::new(&["first.service", "anchor.service", "last.service"]);
    write_synthetic_journal_file(&layout.root().join("older.journal"), &["older.service"], 6);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["later.service"], 9);
    for max_open_files in [1, 64] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            sdjournal::JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let entries = journal.query().collect_owned().unwrap();
        let cursor_text = entries[2]
            .cursor()
            .to_string()
            .split(';')
            .filter(|part| part.starts_with("b=") || part.starts_with("m="))
            .collect::<Vec<_>>()
            .join(";");
        assert!(matches!(
            Cursor::parse(&cursor_text),
            Err(SdJournalError::InvalidQuery { .. })
        ));
        let cursor =
            Cursor::parse(&format!("{cursor_text};t={:x}", entries[2].realtime_usec())).unwrap();

        for reverse in [false, true] {
            let mut inclusive = journal.seek_cursor(&cursor);
            inclusive.reverse(reverse);
            let mut expected_inclusive = messages(&entries[2..]);
            if reverse {
                expected_inclusive.reverse();
            }

            let mut exclusive = journal.query();
            exclusive.after_cursor(cursor.clone()).reverse(reverse);
            let mut expected_exclusive = messages(&entries[3..]);
            if reverse {
                expected_exclusive.reverse();
            }
            assert_eq!(
                (
                    messages(&inclusive.collect_owned().unwrap()),
                    messages(&exclusive.collect_owned().unwrap()),
                ),
                (expected_inclusive, expected_exclusive),
            );
        }
    }
}

#[test]
fn open_dirs_deduplicates_user_supplied_roots() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let paths: Vec<PathBuf> = vec![layout.root().to_path_buf(), layout.root().to_path_buf()];

    let journal = Journal::open_dirs(&paths).expect("open duplicated roots");
    let entries = journal
        .query()
        .collect_owned()
        .expect("collect entries from deduplicated roots");

    assert_eq!(entries.len(), 2);
}

#[test]
fn open_dirs_rejects_non_directory_roots_even_with_healthy_journals() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let file_root = layout.root().join("not-a-directory");
    fs::write(&file_root, b"not a directory").unwrap();

    for paths in [
        vec![layout.root().to_path_buf(), file_root.clone()],
        vec![file_root.clone(), layout.root().to_path_buf()],
    ] {
        assert!(matches!(
            Journal::open_dirs(&paths),
            Err(SdJournalError::Io {
                op: "read_dir",
                path: Some(path),
                source,
            }) if path == file_root && source.kind() == std::io::ErrorKind::NotADirectory
        ));
    }
}

#[test]
fn open_dirs_keeps_reading_existing_roots_when_another_root_is_missing() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let journal = Journal::open_dirs(&[
        layout.root().join("missing-root"),
        layout.root().to_path_buf(),
    ])
    .unwrap();
    assert_eq!(journal.query().collect_owned().unwrap().len(), 1);
}

#[test]
fn exact_query_returns_all_entries_with_duplicate_data_payloads() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service", "alpha.service"]);
    mark_journal_offline(&layout.root().join("synthetic.journal"));
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let mut query = journal.query();
    query.match_unit("alpha.service");
    let entries = query
        .collect_owned()
        .expect("collect repeated alpha entries");

    assert_eq!(entries.len(), 3);
    for (idx, entry) in entries.iter().enumerate() {
        assert_eq!(
            field(entry, "MESSAGE"),
            synthetic_message(7, idx, "alpha.service")
        );
    }
}

#[test]
fn unit_filter_is_anded_with_or_groups_in_either_builder_order() {
    for offline in [false, true] {
        let units = ["alpha.service", "beta.service", "alpha.service"];
        let layout = SyntheticJournalFile::new(&units);
        write_synthetic_journal_file(&layout.root().join("later.journal"), &units, 9);
        if offline {
            mark_journal_offline(&layout.root().join("synthetic.journal"));
            mark_journal_offline(&layout.root().join("later.journal"));
        }
        for max_open_files in [1, 64] {
            let journal = Journal::open_dir_with_config(
                layout.root(),
                sdjournal::JournalConfig {
                    max_open_files,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            for reverse in [false, true] {
                for unit_first in [false, true] {
                    let mut query = journal.query();
                    if unit_first {
                        query.match_unit("alpha.service");
                    }
                    query
                        .or_group(|group| {
                            group.match_exact("PRIORITY", b"1");
                        })
                        .or_group(|group| {
                            group.match_exact("PRIORITY", b"2");
                        });
                    if !unit_first {
                        query.match_unit("alpha.service");
                    }
                    query.reverse(reverse);
                    let mut expected = vec![
                        synthetic_message(7, 0, "alpha.service"),
                        synthetic_message(9, 0, "alpha.service"),
                    ];
                    if reverse {
                        expected.reverse();
                    }
                    assert_eq!(
                        messages(&query.collect_owned().unwrap()),
                        expected,
                        "offline={offline}, max_open_files={max_open_files}, reverse={reverse}, unit_first={unit_first}"
                    );
                }
            }
        }
    }
}

#[test]
fn indexed_and_queries_advance_sparse_postings_in_both_directions() {
    let units: Vec<_> = (0..18)
        .map(|index| {
            if index % 2 == 0 {
                "alpha.service"
            } else {
                "beta.service"
            }
        })
        .collect();
    for multiple_files in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first.journal");
        write_synthetic_journal_file(&first, &units, 7);
        mark_journal_offline(&first);
        if multiple_files {
            let second = root.path().join("second.journal");
            write_synthetic_journal_file(&second, &units, 8);
            mark_journal_offline(&second);
        }

        for max_open_files in [1, 8] {
            let journal = Journal::open_dir_with_config(
                root.path(),
                sdjournal::JournalConfig {
                    max_open_files,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            for reverse in [false, true] {
                let mut query = journal.query();
                query
                    .match_exact("_SYSTEMD_UNIT", b"alpha.service")
                    .match_exact("PRIORITY", b"3")
                    .match_exact("SYSLOG_IDENTIFIER", b"synthetic-7")
                    .reverse(reverse);
                let mut expected = vec![
                    synthetic_message(7, 2, "alpha.service"),
                    synthetic_message(7, 16, "alpha.service"),
                ];
                if reverse {
                    expected.reverse();
                }
                assert_eq!(messages(&query.collect_owned().unwrap()), expected);
                assert_eq!(
                    messages(&query.collect_owned_limited(1).unwrap()),
                    expected[..1]
                );
                assert_eq!(messages(&query.collect_owned().unwrap()), expected);
            }
        }
    }
}

#[test]
fn indexed_sparse_boolean_queries_continue_after_midstream_posting_errors() {
    let units: Vec<_> = (0..18)
        .map(|index| {
            if index % 2 == 0 {
                "alpha.service"
            } else {
                "beta.service"
            }
        })
        .collect();
    for reverse in [false, true] {
        for error_during_seek in [false, true] {
            let layout = SyntheticJournalFile::new(&units);
            let path = layout.root().join("synthetic.journal");
            let mut bytes = fs::read(&path).unwrap();
            let array = read_u64(&bytes, 176) as usize;
            let entries: Vec<_> = (0..units.len())
                .map(|index| read_u64(&bytes, array + 24 + index * 8))
                .collect();
            let first_data = read_u64(&bytes, entries[0] as usize + 64) as usize;
            // Other DATA objects retain healthy postings for the same values. This one
            // supplies duplicate matches, then fails either while advancing after the first
            // intersection or while seeking across a gap to the next intersection.
            let postings = match (reverse, error_during_seek) {
                (false, false) => vec![entries[2], entries[4] + 8, entries[16]],
                (false, true) => vec![entries[2], entries[4], entries[6] + 8, entries[16]],
                (true, false) => vec![entries[2], entries[14] + 8, entries[16]],
                (true, true) => vec![
                    entries[2],
                    entries[10] + 8,
                    entries[12],
                    entries[14],
                    entries[16],
                ],
            };
            append_data_posting_array(&mut bytes, first_data, &postings);
            bytes[16] = 0;
            fs::write(&path, bytes).unwrap();

            let journal = Journal::open_dir_with_config(
                layout.root(),
                sdjournal::JournalConfig {
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            for or_groups in [false, true] {
                let mut query = journal.query();
                query
                    .match_exact("SYSLOG_IDENTIFIER", b"synthetic-7")
                    .reverse(reverse);
                if or_groups {
                    query
                        .or_group(|group| {
                            group
                                .match_exact("_SYSTEMD_UNIT", b"alpha.service")
                                .match_exact("PRIORITY", b"3");
                        })
                        .or_group(|group| {
                            group
                                .match_exact("_SYSTEMD_UNIT", b"beta.service")
                                .match_exact("PRIORITY", b"7");
                        });
                } else {
                    query
                        .match_exact("_SYSTEMD_UNIT", b"alpha.service")
                        .match_exact("PRIORITY", b"3");
                }
                let mut expected = if or_groups {
                    vec![3, 14, 17]
                } else {
                    vec![3, 17]
                };
                if reverse {
                    expected.reverse();
                }
                let limited = query.collect_owned_limited(1).unwrap();
                assert_eq!(limited[0].seqnum(), expected[0]);

                let mut iter = query.iter().unwrap();
                assert_eq!(iter.next().unwrap().unwrap().seqnum(), expected[0]);
                assert!(matches!(
                    iter.next(),
                    Some(Err(SdJournalError::Corrupt { .. }))
                ));
                let rest: Vec<_> = iter.by_ref().map(|item| item.unwrap().seqnum()).collect();
                assert_eq!(rest, expected[1..]);
                assert!(iter.next().is_none());
            }
        }
    }
}

#[test]
fn indexed_queries_leave_successor_postings_unread_until_the_next_request() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let array = read_u64(&bytes, 176) as usize;
    let first = read_u64(&bytes, array + 24) as usize;
    let second = read_u64(&bytes, array + 32);
    let data = read_u64(&bytes, first + 64) as usize;
    let postings_array = append_data_posting_array(&mut bytes, data, &[second + 8]);
    bytes[16] = 0;
    fs::write(&path, bytes).unwrap();

    let journal = Journal::open_dir_with_config(
        layout.root(),
        sdjournal::JournalConfig {
            mmap_policy: MmapPolicy::Never,
            ..Default::default()
        },
    )
    .unwrap();
    let mut query = journal.query();
    query
        .match_exact("_SYSTEMD_UNIT", b"alpha.service")
        .match_exact("SYSLOG_IDENTIFIER", b"synthetic-7");
    let mut iter = query.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap().seqnum(), 1);

    // Repair the next posting after delivering the current match. Eager advancement would
    // already have read the bad member and cached an error, even if a limit could hide it.
    let mut writer = fs::OpenOptions::new().write(true).open(&path).unwrap();
    write_u64_at(&mut writer, postings_array as u64 + 24, second);
    assert_eq!(iter.next().unwrap().unwrap().seqnum(), 2);
    assert!(iter.next().is_none());
}

#[test]
fn collect_owned_limited_applies_a_hard_result_cap() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service", "gamma.service"]);
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let entries = journal
        .query()
        .collect_owned_limited(2)
        .expect("collect at most two entries");
    assert_eq!(entries.len(), 2);

    let mut already_limited = journal.query();
    already_limited.limit(1);
    let entries = already_limited
        .collect_owned_limited(2)
        .expect("keep the lower existing query limit");
    assert_eq!(entries.len(), 1);
}

#[test]
fn query_limit_does_not_surface_errors_after_the_requested_result_count() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let tail = u64::from_le_bytes(bytes[264..272].try_into().unwrap()) as usize;
    bytes[tail] = 0; // The second ENTRY is corrupt; the requested first ENTRY is intact.
    fs::write(&path, bytes).unwrap();
    let journal = Journal::open_dir(layout.root()).unwrap();

    let entries = journal.query().collect_owned_limited(1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(field(&entries[0], "_SYSTEMD_UNIT"), "alpha.service");
    assert!(journal.query().collect_owned().is_err());
}

#[test]
fn low_memory_query_merges_files_and_resumes_cursor() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["beta.service", "alpha.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut alpha = journal.query();
    alpha.match_unit("alpha.service");
    let entries = alpha
        .collect_owned()
        .expect("collect low-memory merged entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(
        field(&entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
    assert_eq!(
        field(&entries[1], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );

    let cursor = Cursor::parse(&entries[0].cursor().to_string()).expect("parse cursor");
    let mut resumed = journal.query();
    resumed.match_unit("alpha.service").after_cursor(cursor);
    let resumed_entries = resumed
        .collect_owned()
        .expect("resume low-memory query after cursor");
    assert_eq!(resumed_entries.len(), 1);
    assert_eq!(
        field(&resumed_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );

    let mut newest = journal.query();
    newest.match_unit("alpha.service").reverse(true);
    let newest_entries = newest.collect_owned().expect("reverse low-memory query");
    assert_eq!(newest_entries.len(), 2);
    assert_eq!(
        field(&newest_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );
}

#[test]
fn low_memory_query_honors_or_groups_limit_and_reverse_order() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["gamma.service", "alpha.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut forward = journal.query();
    forward
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        })
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"gamma.service");
        })
        .limit(2);
    let forward_entries = forward
        .collect_owned()
        .expect("collect limited low-memory OR query");
    assert_eq!(forward_entries.len(), 2);
    assert_eq!(
        field(&forward_entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
    assert_eq!(
        field(&forward_entries[1], "MESSAGE"),
        synthetic_message(9, 0, "gamma.service")
    );

    let mut reverse = journal.query();
    reverse
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        })
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"gamma.service");
        })
        .reverse(true)
        .limit(2);
    let reverse_entries = reverse
        .collect_owned()
        .expect("collect reverse low-memory OR query");
    assert_eq!(reverse_entries.len(), 2);
    assert_eq!(
        field(&reverse_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );
    assert_eq!(
        field(&reverse_entries[1], "MESSAGE"),
        synthetic_message(9, 0, "gamma.service")
    );
}

#[test]
fn low_memory_query_reports_a_missing_file_instead_of_using_unsafe_time_pruning() {
    let layout = SyntheticJournalFile::new(&["old.service"]);
    let old_path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(
        &layout.root().join("recent.journal"),
        &["recent.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    fs::remove_file(&old_path).expect("remove old file after discovery");

    let mut query = journal.query();
    query.since_realtime(synthetic_realtime_usec(9, 0));
    assert!(matches!(
        query.collect_owned(),
        Err(SdJournalError::Io { op: "open", .. })
    ));
}

#[test]
fn low_memory_query_reports_a_missing_file_after_cursor() {
    let layout = SyntheticJournalFile::new(&["old.service"]);
    let old_path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(
        &layout.root().join("recent.journal"),
        &["recent.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut old_query = journal.query();
    old_query.match_unit("old.service");
    let old_entries = old_query.collect_owned().expect("read old cursor");
    let cursor = Cursor::parse(&old_entries[0].cursor().to_string()).expect("parse old cursor");

    fs::remove_file(&old_path).expect("remove old file after cursor capture");

    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    assert!(matches!(
        resumed.collect_owned(),
        Err(SdJournalError::Io { op: "open", .. })
    ));
}

#[test]
fn query_order_time_bounds_and_cursor_survive_realtime_rollback() {
    let units = ["first.service", "second.service", "third.service"];
    let layout = SyntheticJournalFile::new(&units);
    let path = layout.root().join("synthetic.journal");
    let base = synthetic_realtime_usec(7, 0);
    patch_entry_timestamps(&path, &[base + 300, base + 100, base + 200]);

    let journal = Journal::open_dir(layout.root()).expect("open journal with clock rollback");
    let all = journal
        .query()
        .collect_owned()
        .expect("collect rollback journal");
    assert_eq!(
        messages(&all),
        vec![
            synthetic_message(7, 0, units[0]),
            synthetic_message(7, 1, units[1]),
            synthetic_message(7, 2, units[2]),
        ]
    );

    let mut bounded = journal.query();
    bounded.since_realtime(base + 150);
    let bounded = bounded.collect_owned().expect("apply realtime lower bound");
    assert_eq!(
        messages(&bounded),
        vec![
            synthetic_message(7, 0, units[0]),
            synthetic_message(7, 2, units[2]),
        ]
    );

    let cursor = all[0].cursor();
    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    let resumed = resumed
        .collect_owned()
        .expect("resume across clock rollback");
    assert_eq!(
        messages(&resumed),
        vec![
            synthetic_message(7, 1, units[1]),
            synthetic_message(7, 2, units[2]),
        ]
    );
}

#[test]
fn indexed_candidates_are_verified_against_entry_payloads() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service", "alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    redirect_data_index_entry(&path, b"_SYSTEMD_UNIT=alpha.service", 1);
    mark_journal_offline(&path);

    let journal = Journal::open_dir(layout.root()).expect("open journal with damaged index");
    for reverse in [false, true] {
        let mut query = journal.query();
        query
            .match_exact("_SYSTEMD_UNIT", b"alpha.service")
            .reverse(reverse);
        let results: Vec<_> = query.iter().unwrap().collect();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results
                .iter()
                .filter(|item| matches!(item,
                    Err(SdJournalError::Corrupt { reason, .. })
                        if reason.contains("does not contain the indexed field")
                ))
                .count(),
            1
        );
        let seqnums: Vec<_> = results
            .into_iter()
            .filter_map(Result::ok)
            .map(|entry| entry.seqnum())
            .collect();
        assert_eq!(seqnums, [3]);
    }
}

#[test]
fn lazy_reopen_rejects_a_replaced_journal_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["beta.service"], 9);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        ..Default::default()
    };
    let journal = Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy journal");

    write_synthetic_journal_file(
        &layout.root().join("synthetic.journal"),
        &["replacement.service"],
        11,
    );

    assert!(matches!(
        journal.query().collect_owned(),
        Err(SdJournalError::Transient { reason, .. })
            if reason == "journal file was replaced after discovery"
    ));
}

#[test]
fn lazy_index_continuation_does_not_rescan_or_skip_prior_offsets() {
    let units = vec!["alpha.service"; 40];
    let layout = SyntheticJournalFile::new(&units);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &units, 9);
    mark_journal_offline(&layout.root().join("synthetic.journal"));
    mark_journal_offline(&layout.root().join("later.journal"));
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal = Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy journal");

    let mut query = journal.query();
    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
    let entries = query.collect_owned().expect("collect indexed lazy query");
    assert_eq!(entries.len(), 80);
    assert_eq!(
        field(&entries[39], "MESSAGE"),
        synthetic_message(7, 39, "alpha.service")
    );
    assert_eq!(
        field(&entries[40], "MESSAGE"),
        synthetic_message(9, 0, "alpha.service")
    );

    let mut resumed = journal.query();
    resumed
        .match_exact("_SYSTEMD_UNIT", b"alpha.service")
        .after_cursor(entries[29].cursor());
    let resumed = resumed.collect_owned().expect("resume indexed lazy query");
    assert_eq!(resumed.len(), 50);
    assert_eq!(
        field(&resumed[0], "MESSAGE"),
        synthetic_message(7, 30, "alpha.service")
    );
}

#[test]
fn low_memory_iterator_is_a_stable_online_snapshot_across_an_in_place_append() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["later.service"], 9);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let old_journal =
        Journal::open_dir_with_config(layout.root(), cfg.clone()).expect("open old lazy snapshot");
    let mut old_iter = old_journal
        .query()
        .iter()
        .expect("create old lazy snapshot iterator");

    #[cfg(unix)]
    let inode_before = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&path)
            .expect("stat journal before append")
            .ino()
    };

    append_duplicate_entry_in_place(&path);

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            fs::metadata(&path)
                .expect("stat journal after append")
                .ino(),
            inode_before,
            "the fixture must append to the same inode"
        );
    }

    let old_entries = old_iter
        .by_ref()
        .map(|item| item.map(|entry| entry.to_owned()))
        .collect::<Result<Vec<_>, _>>()
        .expect("old snapshot remains readable after append");
    assert_eq!(
        messages(&old_entries),
        vec![
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(9, 0, "later.service"),
        ],
        "an iterator created before the append must not grow its snapshot"
    );

    let new_journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open new lazy snapshot");
    let new_entries = new_journal
        .query()
        .collect_owned()
        .expect("collect the snapshot opened after append");
    assert_eq!(
        messages(&new_entries),
        vec![
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(9, 0, "later.service"),
        ],
        "a newly opened snapshot must include the committed append"
    );
}

#[test]
fn entry_array_rejects_an_entry_inside_the_arena_but_past_the_committed_tail() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    point_entry_array_past_committed_tail(&path);

    let journal = Journal::open_dir(layout.root()).expect("open damaged journal");
    assert!(matches!(
        journal.query().collect_owned(),
        Err(SdJournalError::Corrupt { reason, .. })
            if reason.contains("tail_object_offset")
                || reason.contains("outside committed journal data")
    ));
}

#[test]
fn data_hash_chain_self_loop_falls_back_without_hanging_or_losing_matches() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let path = layout.root().join("synthetic.journal");
    make_first_data_hash_link_self_referential(&path);
    mark_journal_offline(&path);

    let journal =
        Journal::open_dir(layout.root()).expect("open journal with cyclic DATA hash link");
    let mut query = journal.query();
    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
    let entries = query
        .collect_owned()
        .expect("cyclic hash index should safely fall back to scanning");
    assert_eq!(
        messages(&entries),
        vec![synthetic_message(7, 0, "alpha.service")]
    );
}

#[test]
fn online_exact_queries_include_committed_entries_before_data_postings_are_updated() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    // systemd commits the file-level entry count before linking the ENTRY into each DATA index.
    // This append reuses the first ENTRY's DATA objects and leaves their old postings intact.
    append_duplicate_entry_in_place(&path);

    let journal =
        Journal::open_dir(layout.root()).expect("open ONLINE journal during DATA linking");
    for reverse in [false, true] {
        let mut query = journal.query();
        query
            .match_exact("_SYSTEMD_UNIT", b"alpha.service")
            .reverse(reverse);
        let entries = query
            .collect_owned()
            .expect("query committed ONLINE entries");
        let seqnums: Vec<_> = entries.iter().map(sdjournal::EntryOwned::seqnum).collect();
        assert_eq!(seqnums, if reverse { vec![2, 1] } else { vec![1, 2] });
    }
}

#[test]
fn exact_cursor_excludes_cross_file_copy_of_the_same_logical_entry() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let original = layout.root().join("synthetic.journal");
    let original_journal = Journal::open_dir(layout.root()).expect("open original journal");
    let cursor = original_journal
        .query()
        .collect_owned()
        .expect("read original entry")[0]
        .cursor();

    let duplicate = layout.root().join("duplicate.journal");
    fs::copy(&original, &duplicate).expect("copy logical entry to a rotated file");
    patch_file_id(&duplicate, [0xa5; 16]);

    let journal = Journal::open_dir(layout.root()).expect("open journal and rotated copy");
    for reverse in [false, true] {
        let mut resumed = journal.query();
        resumed.after_cursor(cursor.clone()).reverse(reverse);
        assert!(
            resumed
                .collect_owned()
                .expect("resume across a copied logical entry")
                .is_empty(),
            "exclusive cursor leaked a cross-file logical duplicate (reverse={reverse})"
        );
    }
}

#[test]
fn in_memory_cursor_resumes_from_a_copy_after_the_original_file_is_removed() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let original = layout.root().join("synthetic.journal");
    let original_journal = Journal::open_dir(layout.root()).expect("open original journal");
    let cursor = original_journal
        .query()
        .collect_owned()
        .expect("read original entries")[0]
        .cursor();
    drop(original_journal);

    let duplicate = layout.root().join("duplicate.journal");
    fs::copy(&original, &duplicate).expect("copy entries to another journal");
    patch_file_id(&duplicate, [0xa5; 16]);
    fs::remove_file(&original).expect("remove original journal");

    let journal = Journal::open_dir(layout.root()).expect("open remaining copy");
    let persisted = Cursor::parse(&cursor.to_string()).expect("round-trip cursor");
    for cursor in [persisted, cursor] {
        for reverse in [false, true] {
            let mut query = journal.query();
            query.after_cursor(cursor.clone()).reverse(reverse);
            let entries = query.collect_owned().expect("resume from remaining copy");
            assert_eq!(
                messages(&entries),
                vec![synthetic_message(7, 1, "beta.service")]
            );
        }
    }
}

#[test]
fn cursor_seek_cannot_inject_an_entry_beyond_the_snapshot_entry_count() {
    let layout = SyntheticJournalFile::new(&["committed.service", "uncommitted.service"]);
    let path = layout.root().join("synthetic.journal");
    let full_journal = Journal::open_dir(layout.root()).expect("open complete fixture");
    let cursor = full_journal
        .query()
        .collect_owned()
        .expect("read full fixture")[1]
        .cursor();
    drop(full_journal);

    let mut bytes = fs::read(&path).expect("read fixture before withholding the commit");
    let array = usize::try_from(read_u64(&bytes, 176)).expect("array offset fits");
    let first = read_u64(&bytes, array + 24);
    write_u64(&mut bytes, 152, 1);
    write_u64(&mut bytes, 160, 1);
    write_u64(&mut bytes, 192, synthetic_realtime_usec(7, 0));
    write_u64(&mut bytes, 264, first);
    fs::write(&path, bytes).expect("leave second ENTRY outside the committed entry count");

    let journal = Journal::open_dir(layout.root()).expect("open one-entry snapshot");
    let persisted = Cursor::parse(&cursor.to_string()).expect("round-trip full cursor");
    for cursor in [persisted, cursor] {
        assert!(matches!(
            journal.seek_cursor(&cursor).collect_owned(),
            Err(SdJournalError::NotFound)
        ));
    }
}

#[test]
fn a_filtered_out_entry_does_not_hide_a_prefetched_read_error() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "broken.service", "beta.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).expect("read synthetic journal");
    let array = usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits");
    let entry = usize::try_from(read_u64(&bytes, array + 32)).expect("second ENTRY offset fits");
    bytes[entry] = 1; // DATA, not ENTRY; the endpoints remain valid for discovery.
    bytes[16] = 0; // Exercise individual OR branch iterators as well as the global merger.
    fs::write(&path, bytes).expect("damage middle ENTRY type");

    let journal = Journal::open_dir(layout.root()).expect("open journal with damaged middle ENTRY");
    for reverse in [false, true] {
        for or_groups in [false, true] {
            let mut query = journal.query();
            query.match_present("ABSENT_FIELD").reverse(reverse);
            if or_groups {
                query
                    .or_group(|group| {
                        group.match_present("MESSAGE");
                    })
                    .or_group(|group| {
                        group.match_present("PRIORITY");
                    });
            }
            assert!(
                matches!(
                    query.collect_owned(),
                    Err(SdJournalError::Corrupt { .. } | SdJournalError::Transient { .. })
                ),
                "a query must surface the read error even with no matches (reverse={reverse}, or_groups={or_groups})"
            );
        }
    }
}

#[test]
fn conditional_merge_cycle_has_stable_pairwise_order_and_exact_cursor_resume() {
    let layout = SyntheticJournalFile::new(&["a.service"]);
    let a_path = layout.root().join("a.journal");
    fs::rename(layout.root().join("synthetic.journal"), &a_path).expect("rename A journal");
    let b_path = layout.root().join("b.journal");
    let c_path = layout.root().join("c.journal");
    write_synthetic_journal_file(&b_path, &["b.service"], 8);
    write_synthetic_journal_file(&c_path, &["c.service"], 9);

    let stream_one = [0x11; 16];
    let stream_two = [0x22; 16];
    let boot_one = [0x33; 16];
    let boot_two = [0x44; 16];
    patch_single_entry_key(&a_path, stream_one, 1, boot_one, 1, 300);
    patch_single_entry_key(&b_path, stream_one, 2, boot_two, 1, 100);
    patch_single_entry_key(&c_path, stream_two, 1, boot_two, 2, 200);

    // A < B by sequence, B < C by monotonic time, but C < A by realtime.
    let journal = Journal::open_dir(layout.root()).expect("open cyclic-order fixture");
    let forward = journal
        .query()
        .collect_owned()
        .expect("pairwise forward merge");
    assert_eq!(
        messages(&forward),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(7, 0, "a.service"),
            synthetic_message(8, 0, "b.service"),
        ]
    );

    let mut reverse_query = journal.query();
    reverse_query.reverse(true);
    let reverse = reverse_query
        .collect_owned()
        .expect("pairwise reverse merge");
    assert_eq!(
        messages(&reverse),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(8, 0, "b.service"),
            synthetic_message(7, 0, "a.service"),
        ]
    );

    let a_cursor = forward[1].cursor();
    let mut after_a = journal.query();
    after_a.after_cursor(a_cursor.clone());
    assert_eq!(
        messages(
            &after_a
                .collect_owned()
                .expect("forward resume within comparison cycle")
        ),
        vec![synthetic_message(8, 0, "b.service")]
    );

    let mut reverse_after_a = journal.query();
    reverse_after_a.after_cursor(a_cursor).reverse(true);
    assert_eq!(
        messages(
            &reverse_after_a
                .collect_owned()
                .expect("reverse resume within comparison cycle")
        ),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(8, 0, "b.service"),
        ]
    );
}

#[test]
fn filters_preserve_conditional_merge_order_and_cursor_position() {
    for offline in [false, true] {
        let layout = SyntheticJournalFile::new(&["a.service"]);
        let a_path = layout.root().join("a.journal");
        fs::rename(layout.root().join("synthetic.journal"), &a_path).unwrap();
        let b_path = layout.root().join("b.journal");
        let c_path = layout.root().join("c.journal");
        write_synthetic_journal_file(&b_path, &["b.service"], 8);
        write_synthetic_journal_file(&c_path, &["c.service"], 9);
        patch_single_entry_key(&a_path, [0x11; 16], 1, [0x33; 16], 1, 300);
        patch_single_entry_key(&b_path, [0x11; 16], 2, [0x44; 16], 1, 100);
        patch_single_entry_key(&c_path, [0x22; 16], 1, [0x44; 16], 2, 200);
        if offline {
            for path in [&a_path, &b_path, &c_path] {
                mark_journal_offline(path);
            }
        }

        for max_open_files in [1, 64] {
            let journal = Journal::open_dir_with_config(
                layout.root(),
                sdjournal::JournalConfig {
                    max_open_files,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            // The complete forward order is C,A,B; reverse is C,B,A.
            let entries = journal.query().collect_owned().unwrap();
            for reverse in [false, true] {
                for time_bound in [false, true] {
                    let mut filtered = journal.query();
                    filtered.reverse(reverse);
                    if time_bound {
                        filtered.until_realtime(200);
                    } else {
                        filtered
                            .or_group(|group| {
                                group.match_exact(
                                    "MESSAGE",
                                    synthetic_message(8, 0, "b.service").as_bytes(),
                                );
                            })
                            .or_group(|group| {
                                group.match_exact(
                                    "MESSAGE",
                                    synthetic_message(9, 0, "c.service").as_bytes(),
                                );
                            });
                    }
                    let matches = filtered.collect_owned().unwrap();
                    assert_eq!(
                        messages(&matches),
                        vec![
                            synthetic_message(9, 0, "c.service"),
                            synthetic_message(8, 0, "b.service")
                        ],
                        "offline={offline}, max_open_files={max_open_files}, reverse={reverse}, time_bound={time_bound}"
                    );
                    for (index, entry) in matches.iter().enumerate() {
                        let mut resumed = filtered.clone();
                        resumed.after_cursor(entry.cursor());
                        let expected = if reverse {
                            &matches[..index]
                        } else {
                            &matches[index + 1..]
                        };
                        assert_eq!(
                            messages(&resumed.collect_owned().unwrap()),
                            messages(expected)
                        );
                    }
                }
            }
            for (reverse, anchor, expected, realtime) in [
                (
                    false,
                    &entries[0],
                    synthetic_message(8, 0, "b.service"),
                    100,
                ),
                (true, &entries[1], synthetic_message(9, 0, "c.service"), 200),
            ] {
                for time_bound in [false, true] {
                    let mut query = journal.query();
                    query.after_cursor(anchor.cursor()).reverse(reverse);
                    if time_bound {
                        query.since_realtime(realtime).until_realtime(realtime);
                    } else {
                        query.match_exact("MESSAGE", expected.as_bytes());
                    }
                    assert_eq!(
                        messages(&query.collect_owned().unwrap()),
                        vec![expected.clone()],
                        "offline={offline}, max_open_files={max_open_files}, reverse={reverse}, time_bound={time_bound}"
                    );
                }
            }
        }
    }
}

#[test]
fn cursor_and_time_bounds_do_not_decode_excluded_field_payloads() {
    let layout = SyntheticJournalFile::new(&["before.service", "anchor.service", "after.service"]);
    let path = layout.root().join("synthetic.journal");
    let original = Journal::open_dir(layout.root()).unwrap();
    let entries = original.query().collect_owned().unwrap();
    let anchor = entries[1].cursor();
    let since = entries[1].realtime_usec();
    drop(original);

    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let first_entry = read_u64(&bytes, entry_array + 24) as usize;
    let first_data = read_u64(&bytes, first_entry + 64) as usize;
    bytes[first_data] = 0; // ENTRY metadata is valid, but decoding its DATA must fail.
    fs::write(&path, bytes).unwrap();
    mark_journal_offline(&path);

    for multiple_files in [false, true] {
        if multiple_files {
            write_synthetic_journal_file(
                &layout.root().join("later.journal"),
                &["later.service"],
                9,
            );
        }
        for max_open_files in [1, 64] {
            let journal = Journal::open_dir_with_config(
                layout.root(),
                sdjournal::JournalConfig {
                    max_open_files,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(journal.query().collect_owned().is_err());
            for reverse in [false, true] {
                let mut resumed = journal.query();
                resumed.after_cursor(anchor.clone()).reverse(reverse);
                let mut expected = vec![synthetic_message(7, 2, "after.service")];
                if multiple_files {
                    expected.push(synthetic_message(9, 0, "later.service"));
                }
                if reverse {
                    expected.reverse();
                }
                assert_eq!(messages(&resumed.collect_owned().unwrap()), expected);

                let mut bounded = journal.query();
                bounded.since_realtime(since).reverse(reverse);
                let mut expected = messages(&entries[1..]);
                if multiple_files {
                    expected.push(synthetic_message(9, 0, "later.service"));
                }
                if reverse {
                    expected.reverse();
                }
                assert_eq!(messages(&bounded.collect_owned().unwrap()), expected);
            }
        }
    }
}

#[test]
fn reverse_inclusive_cursor_stops_before_older_metadata_corruption() {
    let layout = SyntheticJournalFile::new(&["before.service", "anchor.service", "after.service"]);
    let original = Journal::open_dir(layout.root()).unwrap();
    let entries = original.query().collect_owned().unwrap();
    let anchor = entries[1].cursor();
    drop(original);

    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let first_entry = read_u64(&bytes, entry_array + 24) as usize;
    bytes[first_entry] = 0;
    fs::write(path, bytes).unwrap();

    let journal = Journal::open_dir(layout.root()).unwrap();
    let mut query = journal.seek_cursor(&anchor);
    query.reverse(true);
    assert_eq!(
        messages(&query.collect_owned().unwrap()),
        vec![
            synthetic_message(7, 2, "after.service"),
            synthetic_message(7, 1, "anchor.service")
        ]
    );
}

#[test]
fn a_corrupt_copy_does_not_discard_a_readable_copy_at_the_same_merge_position() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let original = layout.root().join("synthetic.journal");
    let healthy = layout.root().join("z-copy.journal");
    fs::copy(&original, &healthy).unwrap();
    patch_file_id(&healthy, [0xff; 16]);
    let snapshot = Journal::open_dir(layout.root()).unwrap();
    let cursor = snapshot.query().collect_owned().unwrap()[0].cursor();
    drop(snapshot);

    let mut bytes = fs::read(&original).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let entry = read_u64(&bytes, entry_array + 24) as usize;
    let data = read_u64(&bytes, entry + 64) as usize;
    bytes[data] = 0;
    fs::write(original, bytes).unwrap();

    for max_open_files in [1, 64] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            sdjournal::JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        for reverse in [false, true] {
            for exact_cursor in [false, true] {
                let mut query = if exact_cursor {
                    journal.seek_cursor(&cursor)
                } else {
                    journal.query()
                };
                query.reverse(reverse);
                let mut iter = query.iter().unwrap();
                assert!(matches!(
                    iter.next(),
                    Some(Err(SdJournalError::Corrupt { .. }))
                ));
                let entry = iter
                    .next()
                    .expect("readable copy must remain after a corrupt copy")
                    .unwrap();
                assert_eq!(
                    entry.get("MESSAGE"),
                    Some(synthetic_message(7, 0, "alpha.service").as_bytes())
                );
                assert!(iter.next().is_none());
            }
        }
    }
}

#[test]
fn cross_file_copies_are_deduplicated_inside_a_conditional_merge_cycle() {
    let layout = SyntheticJournalFile::new(&["a.service"]);
    let a_path = layout.root().join("a.journal");
    fs::rename(layout.root().join("synthetic.journal"), &a_path).unwrap();
    let c_path = layout.root().join("b.journal");
    let b_path = layout.root().join("c.journal");
    write_synthetic_journal_file(&b_path, &["b.service"], 8);
    write_synthetic_journal_file(&c_path, &["c.service"], 9);
    patch_single_entry_key(&a_path, [0x11; 16], 1, [0x33; 16], 1, 300);
    patch_single_entry_key(&b_path, [0x11; 16], 2, [0x44; 16], 1, 100);
    patch_single_entry_key(&c_path, [0x22; 16], 1, [0x44; 16], 2, 200);
    let copy = layout.root().join("d.journal");
    fs::copy(&a_path, &copy).unwrap();
    patch_file_id(&copy, [0xff; 16]);

    for max_open_files in [1, 64] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            sdjournal::JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        for reverse in [false, true] {
            let entries = journal.query().reverse(reverse).collect_owned().unwrap();
            assert_eq!(
                entries.len(),
                3,
                "duplicate copies: {:?}",
                messages(&entries)
            );
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry.cursor())
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                3
            );

            for (index, entry) in entries.iter().enumerate() {
                let mut resumed = journal.query();
                resumed.after_cursor(entry.cursor()).reverse(reverse);
                let expected = if reverse {
                    &entries[..index]
                } else {
                    &entries[index + 1..]
                };
                assert_eq!(
                    messages(&resumed.collect_owned().unwrap()),
                    messages(expected)
                );
            }
            for time_bound in [false, true] {
                let mut filtered = journal.query();
                filtered.reverse(reverse);
                if time_bound {
                    filtered.until_realtime(200);
                } else {
                    filtered
                        .or_group(|group| {
                            group.match_exact(
                                "MESSAGE",
                                synthetic_message(8, 0, "b.service").as_bytes(),
                            );
                        })
                        .or_group(|group| {
                            group.match_exact(
                                "MESSAGE",
                                synthetic_message(9, 0, "c.service").as_bytes(),
                            );
                        });
                }
                let matches = filtered.collect_owned().unwrap();
                let expected: Vec<_> = entries
                    .iter()
                    .filter(|entry| entry.realtime_usec() <= 200)
                    .map(|entry| field(entry, "MESSAGE"))
                    .collect();
                assert_eq!(messages(&matches), expected);
                for (index, entry) in matches.iter().enumerate() {
                    let mut resumed = filtered.clone();
                    resumed.after_cursor(entry.cursor());
                    let expected = if reverse {
                        &matches[..index]
                    } else {
                        &matches[index + 1..]
                    };
                    assert_eq!(
                        messages(&resumed.collect_owned().unwrap()),
                        messages(expected)
                    );
                }
            }
        }
    }
}

#[test]
fn streaming_merge_keeps_distinct_entries_without_global_overlap_deduplication() {
    let layout = SyntheticJournalFile::new(&["p.service", "q.service"]);
    let a_path = layout.root().join("a.journal");
    fs::rename(layout.root().join("synthetic.journal"), &a_path).unwrap();
    let c_path = layout.root().join("b.journal");
    let b_path = layout.root().join("c.journal");

    let mut bytes = fs::read(&a_path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let p = read_u64(&bytes, entry_array + 24) as usize;
    let q = read_u64(&bytes, entry_array + 32) as usize;
    bytes[72..88].copy_from_slice(&[0x11; 16]);
    bytes[56..72].copy_from_slice(&[0x44; 16]);
    write_u64(&mut bytes, 184, 300);
    write_u64(&mut bytes, 192, 100);
    for (entry, realtime, monotonic, boot) in [(p, 300, 10, 0x33), (q, 100, 1, 0x44)] {
        write_u64(&mut bytes, entry + 24, realtime);
        write_u64(&mut bytes, entry + 32, monotonic);
        bytes[entry + 40..entry + 56].fill(boot);
    }
    fs::write(&a_path, &bytes).unwrap();

    // The last source contains only Q's logical copy; its original precedes no other
    // candidate until source A has advanced past P.
    bytes[24..40].fill(0xff);
    write_u64(&mut bytes, 152, 1);
    write_u64(&mut bytes, 168, 2);
    write_u64(&mut bytes, 184, 100);
    write_u64(&mut bytes, entry_array + 24, q as u64);
    write_u64(&mut bytes, entry_array + 32, 0);
    fs::write(&b_path, bytes).unwrap();
    write_synthetic_journal_file(&c_path, &["r.service"], 8);
    patch_single_entry_key(&c_path, [0x22; 16], 1, [0x33; 16], 5, 200);

    // P < Q by sequence, Q < R by realtime, and R < P by monotonic time. Q's
    // candidates are never at their file heads together, so bounded streaming does
    // not promise global deduplication. Crucially, no distinct entry may be dropped.
    for max_open_files in [1, 64] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            sdjournal::JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let entries = journal.query().collect_owned().unwrap();
        assert_eq!(
            messages(&entries),
            vec![
                synthetic_message(7, 1, "q.service"),
                synthetic_message(8, 0, "r.service"),
                synthetic_message(7, 0, "p.service"),
                synthetic_message(7, 1, "q.service"),
            ]
        );
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.cursor())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }
}

#[test]
fn query_iteration_continues_after_a_corrupt_entry_metadata_record() {
    let layout = SyntheticJournalFile::new(&["first.service", "broken.service", "last.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let broken = read_u64(&bytes, entry_array + 32) as usize;
    bytes[broken] = 0;
    fs::write(path, bytes).unwrap();

    let journal = Journal::open_dir(layout.root()).unwrap();
    for reverse in [false, true] {
        let mut query = journal.query();
        query.reverse(reverse);
        let mut iter = query.iter().unwrap();
        assert_eq!(
            iter.next().unwrap().unwrap().seqnum(),
            if reverse { 3 } else { 1 }
        );
        assert!(matches!(
            iter.next(),
            Some(Err(SdJournalError::Corrupt { .. }))
        ));
        assert_eq!(
            iter.next()
                .expect("later valid metadata must remain iterable")
                .unwrap()
                .seqnum(),
            if reverse { 1 } else { 3 }
        );
        assert!(iter.next().is_none());
    }
}

#[test]
fn indexed_queries_keep_healthy_matches_after_an_entry_decode_error() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service", "alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let broken = read_u64(&bytes, entry_array + 32) as usize;
    let size = read_u64(&bytes, broken + 8) as usize;
    write_u64(&mut bytes, broken + size - 16, 0);
    fs::write(&path, bytes).unwrap();
    mark_journal_offline(&path);

    let journal = Journal::open_dir(layout.root()).unwrap();
    for reverse in [false, true] {
        for or_groups in [false, true] {
            let mut query = journal.query();
            query.reverse(reverse);
            if or_groups {
                query
                    .or_group(|group| {
                        group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
                    })
                    .or_group(|group| {
                        group.match_exact("_SYSTEMD_UNIT", b"beta.service");
                    });
            } else {
                query.match_exact("SYSLOG_IDENTIFIER", b"synthetic-7");
            }
            let results: Vec<_> = query.iter().unwrap().collect();
            assert_eq!(results.iter().filter(|item| item.is_err()).count(), 1);
            let seqnums: Vec<_> = results
                .into_iter()
                .filter_map(Result::ok)
                .map(|entry| entry.seqnum())
                .collect();
            assert_eq!(
                seqnums,
                if reverse { vec![3, 1] } else { vec![1, 3] },
                "or_groups={or_groups}, reverse={reverse}"
            );
        }
    }
}

#[test]
fn indexed_queries_preserve_healthy_postings_after_membership_errors() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service", "alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    for index in [0, 1] {
        let entry = read_u64(&bytes, entry_array + 24 + index * 8) as usize;
        let data = read_u64(&bytes, entry + 64) as usize;
        // The ENTRY and its fields remain readable; only this DATA posting is invalid.
        write_u64(&mut bytes, data + 40, data as u64);
    }
    bytes[16] = 0;
    fs::write(&path, bytes).unwrap();

    let journal = Journal::open_dir(layout.root()).unwrap();
    assert_eq!(journal.query().collect_owned().unwrap().len(), 3);
    for reverse in [false, true] {
        for unit in [false, true] {
            for intersection in [false, true] {
                let mut query = journal.query();
                query.reverse(reverse);
                if unit {
                    query.match_unit("alpha.service");
                } else {
                    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
                }
                if intersection {
                    query.match_exact("SYSLOG_IDENTIFIER", b"synthetic-7");
                }
                let results: Vec<_> = query.iter().unwrap().collect();
                assert_eq!(
                    results
                        .iter()
                        .filter(|item| matches!(item, Err(SdJournalError::Corrupt { .. })))
                        .count(),
                    2,
                    "unit={unit}, intersection={intersection}, reverse={reverse}"
                );
                let seqnums: Vec<_> = results
                    .into_iter()
                    .filter_map(Result::ok)
                    .map(|entry| entry.seqnum())
                    .collect();
                let mut expected = if unit { vec![1, 2, 3] } else { vec![3] };
                if reverse {
                    expected.reverse();
                }
                assert_eq!(seqnums, expected);
            }
        }
    }
}

#[test]
fn query_reports_each_source_initialization_error_and_keeps_healthy_sources() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let paths = [
        layout.root().join("synthetic.journal"),
        layout.root().join("second.journal"),
    ];
    write_synthetic_journal_file(&paths[1], &["alpha.service"], 8);
    for path in &paths {
        let mut bytes = fs::read(path).unwrap();
        let entry_array = read_u64(&bytes, 176) as usize;
        bytes[entry_array] = 0;
        bytes[16] = 0;
        fs::write(path, bytes).unwrap();
    }
    for healthy_source in [false, true] {
        if healthy_source {
            write_synthetic_journal_file(
                &layout.root().join("healthy.journal"),
                &["healthy.service"],
                9,
            );
        }
        for max_open_files in [1, 8] {
            let journal = Journal::open_dir_with_config(
                layout.root(),
                sdjournal::JournalConfig {
                    max_open_files,
                    ..Default::default()
                },
            )
            .unwrap();
            let results: Vec<_> = journal.query().iter().unwrap().collect();
            assert_eq!(results.len(), 2 + usize::from(healthy_source));
            assert_eq!(
                results.iter().filter(|item| item.is_ok()).count(),
                usize::from(healthy_source)
            );
            for path in &paths {
                assert_eq!(results.iter().filter(|item| matches!(item,
                    Err(SdJournalError::Corrupt { path: Some(error_path), .. }) if error_path == path
                )).count(), 1);
            }
        }
    }
}

#[test]
fn exact_cursor_physical_hint_errors_do_not_block_a_healthy_logical_copy() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let original = layout.root().join("synthetic.journal");
    let snapshot = Journal::open_dir(layout.root()).unwrap();
    let cursor = snapshot.query().collect_owned().unwrap()[0].cursor();
    let persisted = Cursor::parse(&cursor.to_string()).unwrap();
    drop(snapshot);
    let copy = layout.root().join("z-copy.journal");
    fs::copy(&original, &copy).unwrap();
    patch_file_id(&copy, [0xff; 16]);
    let mut bytes = fs::read(&original).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let entry = read_u64(&bytes, entry_array + 24) as usize;
    bytes[entry] = 0;
    fs::write(original, bytes).unwrap();

    let journal = Journal::open_dir(layout.root()).unwrap();
    for cursor in [&persisted, &cursor] {
        let results: Vec<_> = journal
            .seek_cursor(cursor)
            .iter()
            .expect("a stale location hint must allow logical cursor recovery")
            .collect();
        assert_eq!(results.iter().filter(|item| item.is_err()).count(), 1);
        let entries: Vec<_> = results.into_iter().filter_map(Result::ok).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(&entries[0].cursor(), cursor);
    }
    drop(journal);
    fs::remove_file(copy).unwrap();
    let journal = Journal::open_dir(layout.root()).unwrap();
    assert!(matches!(
        journal.seek_cursor(&cursor).iter(),
        Err(SdJournalError::Corrupt { .. })
    ));
}

#[test]
fn persisted_cursor_resolution_continues_past_unrelated_corrupt_metadata() {
    let layout = SyntheticJournalFile::new(&["broken.service", "anchor.service"]);
    let snapshot = Journal::open_dir(layout.root()).unwrap();
    let cursor = snapshot.query().collect_owned().unwrap()[1].cursor();
    let persisted = Cursor::parse(&cursor.to_string()).unwrap();
    drop(snapshot);

    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let entry_array = read_u64(&bytes, 176) as usize;
    let first = read_u64(&bytes, entry_array + 24) as usize;
    bytes[first] = 0;
    fs::write(path, bytes).unwrap();

    let journal = Journal::open_dir(layout.root()).unwrap();
    for cursor in [&cursor, &persisted] {
        let mut iter = journal
            .seek_cursor(cursor)
            .iter()
            .expect("cursor resolution must reach the healthy later ENTRY");
        assert!(matches!(
            iter.next(),
            Some(Err(SdJournalError::Corrupt { .. }))
        ));
        assert_eq!(&iter.next().unwrap().unwrap().cursor(), cursor);
        assert!(iter.next().is_none());
    }
}

#[test]
fn indexed_and_queries_continue_healthy_posting_alternatives_after_errors() {
    for broken_fields in [1, 2] {
        let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        let array = read_u64(&bytes, 176) as usize;
        let first_entry = read_u64(&bytes, array + 24) as usize;
        for item in 0..broken_fields {
            let data = read_u64(&bytes, first_entry + 64 + item * 16) as usize;
            // The DATA itself is valid and still part of the hash chain, but its
            // posting points outside the committed file-level ENTRY array.
            write_u64(&mut bytes, data + 40, data as u64);
        }
        fs::write(&path, bytes).unwrap();
        mark_journal_offline(&path);

        let journal = Journal::open_dir(layout.root()).unwrap();
        assert_eq!(journal.query().collect_owned().unwrap().len(), 2);
        for reverse in [false, true] {
            for unit in [false, true] {
                for priority in [None, Some(b"1".as_slice()), Some(b"2".as_slice())] {
                    let mut query = journal.query();
                    query.reverse(reverse);
                    if unit {
                        query.match_unit("alpha.service");
                    } else {
                        query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
                    }
                    if let Some(priority) = priority {
                        query.match_exact("PRIORITY", priority);
                    }
                    let results: Vec<_> = query.iter().unwrap().collect();
                    assert_eq!(
                        results.iter().filter(|item| item.is_err()).count(),
                        if unit { broken_fields } else { 1 }
                    );
                    assert!(
                        results
                            .iter()
                            .filter_map(|item| item.as_ref().err())
                            .all(|error| matches!(error, SdJournalError::Corrupt { .. }))
                    );
                    let actual: Vec<_> = results
                        .into_iter()
                        .filter_map(Result::ok)
                        .map(|entry| entry.seqnum())
                        .collect();
                    let mut expected: Vec<_> = (1..=2)
                        .filter(|seqnum| unit || *seqnum == 2)
                        .filter(|seqnum| {
                            priority.is_none_or(|priority| {
                                priority == if *seqnum == 1 { b"1" } else { b"2" }
                            })
                        })
                        .collect();
                    if reverse {
                        expected.reverse();
                    }
                    assert_eq!(
                        actual, expected,
                        "broken_fields={broken_fields}, unit={unit}, priority={priority:?}, reverse={reverse}"
                    );
                }
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "mmap"))]
#[test]
fn lazy_merge_heap_does_not_retain_one_mmap_per_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    for seed in 8..16 {
        write_synthetic_journal_file(
            &layout.root().join(format!("{seed}.journal")),
            &["alpha.service"],
            seed,
        );
    }
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Auto,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy mmap journal");
    let query = journal.query();
    let mut iter = query.iter().expect("build lazy iterator");

    assert!(
        mapped_files_under(layout.root()) <= 1,
        "lazy heap retained journal mappings before yielding an entry"
    );
    let entry = iter.next().expect("one item").expect("read one entry");
    assert!(
        mapped_files_under(layout.root()) <= 1,
        "lazy heap retained more mappings than the current entry"
    );
    drop(entry);
}

#[test]
fn open_rejects_zero_max_open_files() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 0,
        ..Default::default()
    };

    match Journal::open_dir_with_config(layout.root(), cfg) {
        Err(sdjournal::SdJournalError::InvalidQuery { reason }) => {
            assert_eq!(reason, "max_open_files must be greater than zero");
        }
        Ok(_) => panic!("expected InvalidQuery"),
        Err(err) => panic!("unexpected error: {err}"),
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn open_default_is_explicitly_linux_only() {
    match Journal::open_default() {
        Err(SdJournalError::Unsupported { reason }) => {
            assert!(reason.contains("only supported on Linux"));
        }
        Ok(_) => panic!("expected Unsupported on non-Linux, got Ok"),
        Err(err) => panic!("expected Unsupported on non-Linux, got {err}"),
    }
}

fn field(entry: &sdjournal::EntryOwned, name: &str) -> String {
    String::from_utf8_lossy(entry.get(name).expect("entry field")).into_owned()
}

fn messages(entries: &[sdjournal::EntryOwned]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| field(entry, "MESSAGE"))
        .collect()
}

fn mark_journal_offline(path: &Path) {
    let mut bytes = fs::read(path).expect("read synthetic journal state");
    bytes[16] = 0;
    fs::write(path, bytes).expect("mark synthetic journal OFFLINE");
}

fn patch_entry_timestamps(path: &std::path::Path, timestamps: &[u64]) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits usize");
    for (index, realtime_usec) in timestamps.iter().copied().enumerate() {
        let item_offset = entry_array_offset + 24 + (index * 8);
        let entry_offset =
            usize::try_from(read_u64(&bytes, item_offset)).expect("entry offset fits usize");
        write_u64(&mut bytes, entry_offset + 24, realtime_usec);
        write_u64(
            &mut bytes,
            entry_offset + 32,
            realtime_usec.saturating_sub(123),
        );
    }
    fs::write(path, bytes).expect("write patched synthetic journal");
}

fn redirect_data_index_entry(
    path: &std::path::Path,
    target_payload: &[u8],
    replacement_entry_index: usize,
) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits usize");
    let replacement_entry = read_u64(
        &bytes,
        entry_array_offset + 24 + (replacement_entry_index * 8),
    );
    let hash_table_offset =
        usize::try_from(read_u64(&bytes, 104)).expect("hash table offset fits usize");
    let mut data_offset =
        usize::try_from(read_u64(&bytes, hash_table_offset)).expect("data offset fits usize");

    while data_offset != 0 {
        let size = usize::try_from(read_u64(&bytes, data_offset + 8))
            .expect("DATA object size fits usize");
        if bytes
            .get(data_offset + 64..data_offset + size)
            .is_some_and(|payload| payload == target_payload)
        {
            write_u64(&mut bytes, data_offset + 40, replacement_entry);
            fs::write(path, bytes).expect("write damaged synthetic journal index");
            return;
        }
        data_offset =
            usize::try_from(read_u64(&bytes, data_offset + 24)).expect("next DATA offset fits");
    }

    panic!("target DATA payload was not found");
}

fn append_data_posting_array(bytes: &mut Vec<u8>, data: usize, postings: &[u64]) -> usize {
    let offset = bytes.len();
    let size = 24 + postings.len() * 8;
    bytes.resize(offset + size, 0);
    bytes[offset] = 6;
    write_u64(bytes, offset + 8, size as u64);
    for (index, entry_offset) in postings.iter().copied().enumerate() {
        write_u64(bytes, offset + 24 + index * 8, entry_offset);
    }
    write_u64(bytes, data + 48, offset as u64);
    write_u64(bytes, data + 56, postings.len() as u64 + 1);
    let arena_size = bytes.len() as u64 - read_u64(bytes, 88);
    write_u64(bytes, 96, arena_size);
    write_u64(bytes, 136, offset as u64);
    offset
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("synthetic u64 is in bounds"),
    )
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn append_duplicate_entry_in_place(path: &Path) {
    const HEADER_SIZE: u64 = 272;
    const ENTRY_ARRAY_ITEMS_OFFSET: u64 = 24;

    let bytes = fs::read(path).expect("read journal before append");
    assert_eq!(bytes[16], 1, "fixture must be STATE_ONLINE");
    assert_eq!(read_u64(&bytes, 152), 1, "fixture must have one entry");

    let entry_array_offset = read_u64(&bytes, 176);
    let first_entry_offset = read_u64(
        &bytes,
        usize::try_from(entry_array_offset + ENTRY_ARRAY_ITEMS_OFFSET)
            .expect("entry-array item offset fits"),
    );
    let first_entry_offset_usize =
        usize::try_from(first_entry_offset).expect("entry offset fits usize");
    let entry_size =
        usize::try_from(read_u64(&bytes, first_entry_offset_usize + 8)).expect("entry size fits");
    let entry_end = first_entry_offset_usize
        .checked_add(entry_size)
        .expect("entry range fits");
    let entry_bytes = bytes[first_entry_offset_usize..entry_end].to_vec();

    let new_entry_offset = u64::try_from(bytes.len()).expect("journal length fits u64");
    assert!(new_entry_offset.is_multiple_of(8));
    let old_realtime = read_u64(&entry_bytes, 24);
    let old_monotonic = read_u64(&entry_bytes, 32);
    let mut appended_entry = entry_bytes;
    write_u64(&mut appended_entry, 16, 2);
    write_u64(&mut appended_entry, 24, old_realtime + 100);
    write_u64(&mut appended_entry, 32, old_monotonic + 100);

    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open journal for in-place append");
    file.seek(SeekFrom::End(0))
        .expect("seek to append position");
    file.write_all(&appended_entry)
        .expect("append ENTRY object");

    write_u64_at(
        &mut file,
        entry_array_offset + ENTRY_ARRAY_ITEMS_OFFSET + 8,
        new_entry_offset,
    );
    let appended_len = file.metadata().expect("stat appended journal").len();
    write_u64_at(&mut file, 96, appended_len - HEADER_SIZE);
    write_u64_at(&mut file, 136, new_entry_offset);
    write_u64_at(&mut file, 152, 2);
    write_u64_at(&mut file, 160, 2);
    write_u64_at(&mut file, 192, old_realtime + 100);
    write_u64_at(&mut file, 264, new_entry_offset);
    file.flush().expect("flush appended journal");
}

fn point_entry_array_past_committed_tail(path: &Path) {
    const HEADER_SIZE: u64 = 272;

    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits");
    let original_entry_offset =
        usize::try_from(read_u64(&bytes, entry_array_offset + 24)).expect("entry offset fits");
    let entry_size =
        usize::try_from(read_u64(&bytes, original_entry_offset + 8)).expect("entry size fits");
    let fake_offset = u64::try_from(bytes.len()).expect("file length fits");
    assert!(fake_offset.is_multiple_of(8));
    let fake_entry = bytes[original_entry_offset..original_entry_offset + entry_size].to_vec();
    bytes.extend_from_slice(&fake_entry);
    bytes[16] = 0; // STATE_OFFLINE: an uncommitted reference cannot be a concurrent writer.
    let extended_len = u64::try_from(bytes.len()).expect("extended arena fits");
    write_u64(&mut bytes, 96, extended_len - HEADER_SIZE);
    write_u64(&mut bytes, entry_array_offset + 24, fake_offset);
    fs::write(path, bytes).expect("write journal with an uncommitted ENTRY reference");
}

fn make_first_data_hash_link_self_referential(path: &Path) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let hash_table_offset = usize::try_from(read_u64(&bytes, 104)).expect("hash-table offset fits");
    let first_data_offset =
        usize::try_from(read_u64(&bytes, hash_table_offset)).expect("DATA offset fits");
    write_u64(
        &mut bytes,
        first_data_offset + 24,
        u64::try_from(first_data_offset).expect("DATA offset fits u64"),
    );
    fs::write(path, bytes).expect("write cyclic DATA hash link");
}

fn patch_file_id(path: &Path, file_id: [u8; 16]) {
    let mut bytes = fs::read(path).expect("read journal file ID");
    bytes[24..40].copy_from_slice(&file_id);
    fs::write(path, bytes).expect("write journal file ID");
}

fn patch_single_entry_key(
    path: &Path,
    seqnum_id: [u8; 16],
    seqnum: u64,
    boot_id: [u8; 16],
    monotonic_usec: u64,
    realtime_usec: u64,
) {
    let mut bytes = fs::read(path).expect("read single-entry journal");
    assert_eq!(read_u64(&bytes, 152), 1, "fixture must have one entry");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry-array offset fits");
    let entry_offset =
        usize::try_from(read_u64(&bytes, entry_array_offset + 24)).expect("entry offset fits");

    bytes[56..72].copy_from_slice(&boot_id);
    bytes[72..88].copy_from_slice(&seqnum_id);
    write_u64(&mut bytes, 160, seqnum);
    write_u64(&mut bytes, 168, seqnum);
    write_u64(&mut bytes, 184, realtime_usec);
    write_u64(&mut bytes, 192, realtime_usec);

    write_u64(&mut bytes, entry_offset + 16, seqnum);
    write_u64(&mut bytes, entry_offset + 24, realtime_usec);
    write_u64(&mut bytes, entry_offset + 32, monotonic_usec);
    bytes[entry_offset + 40..entry_offset + 56].copy_from_slice(&boot_id);
    fs::write(path, bytes).expect("write patched entry key");
}

fn write_u64_at(file: &mut fs::File, offset: u64, value: u64) {
    file.seek(SeekFrom::Start(offset))
        .expect("seek to journal metadata");
    file.write_all(&value.to_le_bytes())
        .expect("patch journal metadata");
}

#[cfg(all(target_os = "linux", feature = "mmap"))]
fn mapped_files_under(root: &std::path::Path) -> usize {
    let root = root.to_string_lossy();
    fs::read_to_string("/proc/self/maps")
        .expect("read process mappings")
        .lines()
        .filter(|line| line.contains(root.as_ref()) && line.contains(".journal"))
        .count()
}
