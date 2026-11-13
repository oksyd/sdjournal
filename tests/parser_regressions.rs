#[allow(dead_code)]
mod support;

use sdjournal::{Journal, JournalConfig, LimitKind, MmapPolicy, SdJournalError};
use std::fs;
use support::synthetic_journal::{SyntheticJournalFile, write_synthetic_journal_file};

#[test]
fn committed_entries_cannot_have_a_zero_sequence_number() {
    for max_open_files in [1, 64] {
        for online in [false, true] {
            let layout = SyntheticJournalFile::new(&["alpha.service"; 3]);
            write_synthetic_journal_file(
                &layout.root().join("other.journal"),
                &["other.service"],
                9,
            );
            let path = layout.root().join("synthetic.journal");
            let mut bytes = fs::read(&path).unwrap();
            bytes[16] = u8::from(online);
            let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
            let entry =
                u64::from_le_bytes(bytes[array + 32..array + 40].try_into().unwrap()) as usize;
            bytes[entry + 16..entry + 24].fill(0);
            fs::write(&path, bytes).unwrap();
            let journal = Journal::open_dir_with_config(
                layout.root(),
                JournalConfig {
                    max_open_files,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();

            for reverse in [false, true] {
                let mut query = journal.query();
                query.reverse(reverse);
                let mut successes = 0;
                let mut errors = 0;
                for item in query.iter().unwrap() {
                    match item {
                        Ok(entry) => {
                            assert_ne!(
                                entry.seqnum(),
                                0,
                                "returned a zero-sequence entry (max_open_files={max_open_files}, online={online}, reverse={reverse})",
                            );
                            successes += 1;
                        }
                        Err(SdJournalError::Corrupt {
                            path: actual_path,
                            offset: Some(offset),
                            reason,
                        }) => {
                            assert_eq!(actual_path.as_ref(), Some(&path));
                            assert_eq!(offset, (entry + 16) as u64);
                            assert_eq!(reason, "ENTRY sequence number must be non-zero");
                            errors += 1;
                        }
                        Err(error) => panic!("unexpected ENTRY error: {error}"),
                    }
                }
                assert_eq!((successes, errors), (3, 1));
            }
        }
    }
}

#[test]
fn journal_open_rejects_a_tail_object_smaller_than_its_header() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("short-tail.journal");
    let mut bytes = vec![0u8; 224];
    bytes[..8].copy_from_slice(b"LPKSHHRH");
    bytes[16] = 2;
    bytes[88..96].copy_from_slice(&208u64.to_le_bytes());
    bytes[96..104].copy_from_slice(&16u64.to_le_bytes());
    bytes[136..144].copy_from_slice(&208u64.to_le_bytes());
    bytes[208] = 6;
    fs::write(&path, bytes).unwrap();

    assert!(matches!(
        Journal::open_dir_with_config(
            root.path(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        ),
        Err(SdJournalError::Corrupt {
            offset: Some(208),
            ..
        })
    ));
}

#[test]
fn entry_and_entry_array_cannot_claim_compressed_payloads() {
    for target in ["entry", "entry-array"] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        bytes[16] = 0;
        let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
        let entry = u64::from_le_bytes(bytes[array + 24..array + 32].try_into().unwrap()) as usize;
        let offset = if target == "entry" { entry } else { array };
        bytes[offset + 1] = 2; // Compression flags only apply to DATA objects.
        fs::write(&path, bytes).unwrap();

        let result = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .and_then(|journal| journal.query().collect_owned());
        assert!(
            matches!(
                result,
                Err(SdJournalError::Corrupt { offset: Some(actual), .. }) if actual == offset as u64
            ),
            "accepted compression flag on {target}"
        );
    }
}

#[test]
fn entry_field_validation_preserves_limits_and_rejects_invalid_names() {
    for invalid in [b'=', 0, 0xff] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
        let entry = u64::from_le_bytes(bytes[array + 24..array + 32].try_into().unwrap()) as usize;
        let data = u64::from_le_bytes(bytes[entry + 64..entry + 72].try_into().unwrap()) as usize;
        bytes[data + 64] = invalid;
        fs::write(&path, bytes).unwrap();
        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            journal.query().collect_owned(),
            Err(SdJournalError::Corrupt { reason, offset, .. })
                if offset == Some(data as u64)
                    && reason == "DATA payload contains invalid field name"
        ));
    }

    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    for (max_fields, max_name_len, expected_limit) in [
        (6, 19, None),
        (5, 19, Some((LimitKind::FieldsPerEntry, 5))),
        (6, 12, Some((LimitKind::FieldNameLen, 12))),
    ] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                max_fields_per_entry: max_fields,
                max_field_name_len: max_name_len,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        match (journal.query().collect_owned(), expected_limit) {
            (Ok(entries), None) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].iter_fields().count(), 6);
            }
            (Err(SdJournalError::LimitExceeded { kind, limit }), Some(expected)) => {
                assert_eq!((kind, limit), expected);
            }
            (result, expected) => panic!("unexpected field-limit result: {result:?}, {expected:?}"),
        }
    }
}

#[test]
fn object_readers_still_reject_unaligned_offsets() {
    for online in [false, true] {
        for corruption in ["entry-array", "entry", "data"] {
            let layout = SyntheticJournalFile::new(&["alpha.service"; 2]);
            let path = layout.root().join("synthetic.journal");
            let mut bytes = fs::read(&path).unwrap();
            bytes[16] = u8::from(online);
            let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
            let entry =
                u64::from_le_bytes(bytes[array + 24..array + 32].try_into().unwrap()) as usize;
            let data = u64::from_le_bytes(bytes[entry + 64..entry + 72].try_into().unwrap());
            let (slot, offset) = match corruption {
                "entry-array" => (176, array as u64),
                "entry" => (array + 24, entry as u64),
                "data" => (entry + 64, data),
                _ => unreachable!(),
            };
            bytes[slot..slot + 8].copy_from_slice(&(offset + 1).to_le_bytes());
            fs::write(&path, bytes).unwrap();
            let journal = Journal::open_dir_with_config(
                layout.root(),
                JournalConfig {
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            let error = journal.query().collect_owned().unwrap_err();
            let reason = match error {
                SdJournalError::Transient {
                    path: actual,
                    reason,
                } if online => {
                    assert_eq!(actual, Some(path));
                    reason
                }
                SdJournalError::Corrupt {
                    path: actual,
                    reason,
                    ..
                } if !online => {
                    assert_eq!(actual, Some(path));
                    reason
                }
                error => panic!("unexpected {corruption} alignment error: {error}"),
            };
            assert!(
                reason.contains("8-byte aligned") || reason.contains("invalid object offset"),
                "unexpected {corruption} alignment reason: {reason}"
            );
        }
    }
}

#[test]
fn malformed_hash_table_locations_fall_back_to_matching_entries() {
    for corruption in [
        "missing-table",
        "inside-header",
        "wrong-object-type",
        "wrong-object-size",
    ] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        bytes[16] = 0; // Exercise the index optimization for an immutable file.
        let table_offset = u64::from_le_bytes(bytes[104..112].try_into().unwrap()) as usize;
        match corruption {
            "missing-table" => bytes[104..120].fill(0),
            "inside-header" => bytes[104..112].copy_from_slice(&208u64.to_le_bytes()),
            "wrong-object-type" => {
                bytes[table_offset - 16] = 6;
                bytes[table_offset..table_offset + 16].fill(0);
            }
            "wrong-object-size" => {
                bytes[table_offset - 8..table_offset].copy_from_slice(&48u64.to_le_bytes());
                bytes[table_offset..table_offset + 16].fill(0);
            }
            _ => unreachable!(),
        }
        fs::write(&path, bytes).unwrap();
        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let mut query = journal.query();
        query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        let entries = query.collect_owned().unwrap();
        assert_eq!(entries.len(), 1, "{corruption}");
    }
}

#[test]
fn data_postings_cannot_return_entries_outside_the_file_entry_array() {
    for orphan_index in [1, 2] {
        let layout = SyntheticJournalFile::new(&["alpha.service"; 3]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        bytes[16] = 0;
        let array_offset = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
        let third = bytes[array_offset + 40..array_offset + 48].to_vec();
        // Leave one otherwise valid ENTRY in the object arena and DATA postings, but remove
        // it from the committed file-level index. Test both an interior and a tail orphan.
        if orphan_index == 1 {
            bytes[array_offset + 32..array_offset + 40].copy_from_slice(&third);
        }
        bytes[array_offset + 40..array_offset + 48].fill(0);
        bytes[152..160].copy_from_slice(&2u64.to_le_bytes());
        fs::write(&path, bytes).unwrap();

        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(journal.query().collect_owned().unwrap().len(), 2);

        for reverse in [false, true] {
            let mut query = journal.query();
            query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
            query.reverse(reverse);
            assert!(
                matches!(query.collect_owned(), Err(SdJournalError::Corrupt { .. })),
                "indexed query accepted orphan {orphan_index} (reverse={reverse})"
            );
        }
    }
}

#[test]
fn compressed_data_requires_its_file_header_compatibility_flag() {
    for (flag, name) in [(1u8, "XZ"), (2, "LZ4"), (4, "ZSTD")] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
        let entry = u64::from_le_bytes(bytes[array + 24..array + 32].try_into().unwrap()) as usize;
        let data = u64::from_le_bytes(bytes[entry + 64..entry + 72].try_into().unwrap()) as usize;
        bytes[data + 1] = flag;
        fs::write(&path, bytes).unwrap();

        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            journal.query().collect_owned(),
            Err(SdJournalError::Corrupt { reason, offset, .. })
                if offset == Some(data as u64)
                    && reason == format!("DATA has {name} flag but file header missing incompatible flag")
        ));
    }
}

#[test]
fn data_metadata_still_rejects_invalid_sizes_before_reading_payloads() {
    for corruption in ["short-prefix", "too-large", "past-committed-tail"] {
        let layout = SyntheticJournalFile::new(&["alpha.service"]);
        let path = layout.root().join("synthetic.journal");
        let mut bytes = fs::read(&path).unwrap();
        let array = u64::from_le_bytes(bytes[176..184].try_into().unwrap()) as usize;
        let entry = u64::from_le_bytes(bytes[array + 24..array + 32].try_into().unwrap()) as usize;
        let data = u64::from_le_bytes(bytes[entry + 64..entry + 72].try_into().unwrap()) as usize;
        let config = JournalConfig {
            mmap_policy: MmapPolicy::Never,
            ..Default::default()
        };
        let declared_size = match corruption {
            "short-prefix" => 63,
            "too-large" => config.max_object_size_bytes + 1,
            "past-committed-tail" => (bytes.len() - data) as u64 + 8,
            _ => unreachable!(),
        };
        bytes[data + 8..data + 16].copy_from_slice(&declared_size.to_le_bytes());
        fs::write(&path, bytes).unwrap();

        let journal = Journal::open_dir_with_config(layout.root(), config).unwrap();
        let result = journal.query().collect_owned();
        match corruption {
            "too-large" => assert!(matches!(
                result,
                Err(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectSizeBytes,
                    ..
                })
            )),
            "short-prefix" => assert!(matches!(
                result,
                Err(SdJournalError::Corrupt { reason, .. }) if reason.contains("DATA object too small")
            )),
            _ => assert!(matches!(
                result,
                Err(SdJournalError::Transient { reason, .. }) if reason.contains("DATA object extends beyond")
            )),
        }
    }
}

#[test]
fn hash_table_objects_obey_the_configured_object_size_limit() {
    const BUCKETS: usize = 64;
    const TABLE_SIZE: usize = 16 + BUCKETS * 16;
    const OBJECT_LIMIT: u64 = 1024;

    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    bytes[16] = 0;
    let old_table = u64::from_le_bytes(bytes[104..112].try_into().unwrap()) as usize;
    let mut current = u64::from_le_bytes(bytes[old_table..old_table + 8].try_into().unwrap());
    let mut buckets = vec![Vec::new(); BUCKETS];
    while current != 0 {
        let offset = current as usize;
        let hash = u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
        buckets[(hash % BUCKETS as u64) as usize].push(current);
        current = u64::from_le_bytes(bytes[offset + 24..offset + 32].try_into().unwrap());
    }

    // Rehash the original DATA objects into a larger, otherwise valid table.
    let table = bytes.len();
    bytes.resize(table + TABLE_SIZE, 0);
    bytes[table] = 4;
    bytes[table + 8..table + 16].copy_from_slice(&(TABLE_SIZE as u64).to_le_bytes());
    for (bucket, offsets) in buckets.iter().enumerate() {
        let Some((&head, &tail)) = offsets.first().zip(offsets.last()) else {
            continue;
        };
        let slot = table + 16 + bucket * 16;
        bytes[slot..slot + 8].copy_from_slice(&head.to_le_bytes());
        bytes[slot + 8..slot + 16].copy_from_slice(&tail.to_le_bytes());
        for (i, &offset) in offsets.iter().enumerate() {
            let slot = offset as usize + 24;
            bytes[slot..slot + 8]
                .copy_from_slice(&offsets.get(i + 1).copied().unwrap_or(0).to_le_bytes());
        }
    }
    let header_size = u64::from_le_bytes(bytes[88..96].try_into().unwrap());
    let arena_size = bytes.len() as u64 - header_size;
    bytes[96..104].copy_from_slice(&arena_size.to_le_bytes());
    bytes[104..112].copy_from_slice(&(table as u64 + 16).to_le_bytes());
    bytes[112..120].copy_from_slice(&((BUCKETS * 16) as u64).to_le_bytes());
    bytes[136..144].copy_from_slice(&(table as u64).to_le_bytes());
    fs::write(&path, bytes).unwrap();

    for limit in [TABLE_SIZE as u64, OBJECT_LIMIT] {
        let journal = Journal::open_dir_with_config(
            layout.root(),
            JournalConfig {
                max_object_size_bytes: limit,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let mut query = journal.query();
        query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        let result = query.collect_owned();
        if limit == TABLE_SIZE as u64 {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert!(matches!(
                result,
                Err(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectSizeBytes,
                    limit: OBJECT_LIMIT,
                })
            ));
        }
    }
}
