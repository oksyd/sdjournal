use crate::config::JournalConfig;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::file::{JournalFile, LiveFileState};
use crate::journal::{Journal, is_machine_id_dir, journal_from_snapshot_files};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{is_skippable_live_file_error, warn_skipped_live_file};

#[derive(Clone)]
pub(super) struct TrackedFile {
    pub(super) path: PathBuf,
    pub(super) file_id: [u8; 16],
    pub(super) seqnum_id: [u8; 16],
    pub(super) live_state: LiveFileState,
    pub(super) tail: FileTailCursor,
    pub(super) modified: Option<SystemTime>,
}

pub(super) struct LiveSnapshot {
    pub(super) file_tails: HashMap<[u8; 16], u64>,
    pub(super) journal: Journal,
}

pub(super) struct OffsetBatch {
    pub(super) offsets: Vec<u64>,
    pub(super) exhausted: bool,
}

pub(super) struct FallbackDirState {
    pub(super) path: PathBuf,
    pub(super) modified: Option<SystemTime>,
}

pub(super) fn file_metadata_changed(
    known_file_len: u64,
    known_modified: Option<SystemTime>,
    metadata: &std::fs::Metadata,
) -> bool {
    let modified = metadata.modified().ok();
    metadata.len() != known_file_len || modified.is_none() || modified != known_modified
}

#[derive(Clone)]
pub(super) struct FileTailCursor {
    current_array_offset: Option<u64>,
    next_item_idx: usize,
    last_entry_offset: Option<u64>,
    consumed_entries: u64,
}

impl FileTailCursor {
    pub(super) fn at_start() -> Self {
        Self {
            current_array_offset: None,
            next_item_idx: 0,
            last_entry_offset: None,
            consumed_entries: 0,
        }
    }

    pub(super) fn last_entry_offset(&self) -> Option<u64> {
        self.last_entry_offset
    }

    fn at_end(file: &JournalFile) -> Result<Self> {
        let mut next = file.live_state().entry_array_offset;
        let mut current_array_offset = None;
        let mut next_item_idx = 0usize;
        let mut last_entry_offset = None;
        let mut remaining = file.live_state().n_entries;
        let mut steps = 0usize;

        while next != 0 && remaining != 0 {
            current_array_offset = Some(next);
            let items = file.read_entry_array_items(next)?;
            let used = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(items.len());
            next_item_idx = used;
            for &entry_offset in &items[..used] {
                validate_tail_entry_offset(file, entry_offset, last_entry_offset)?;
                last_entry_offset = Some(entry_offset);
            }
            remaining = remaining.saturating_sub(u64::try_from(used).unwrap_or(u64::MAX));
            if remaining == 0 {
                break;
            }
            next = file.read_entry_array_next_offset(next)?;

            steps = steps.saturating_add(1);
            if steps > file.max_object_chain_steps() {
                return Err(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectChainSteps,
                    limit: u64::try_from(file.max_object_chain_steps()).unwrap_or(u64::MAX),
                });
            }
        }

        if remaining != 0 {
            return Err(SdJournalError::Transient {
                path: Some(file.path().to_path_buf()),
                reason: format!(
                    "ENTRY_ARRAY exposes fewer committed entries than header n_entries ({remaining} missing)"
                ),
            });
        }

        Ok(Self {
            current_array_offset,
            next_item_idx,
            last_entry_offset,
            consumed_entries: file.live_state().n_entries,
        })
    }

    pub(super) fn drain_new_offsets(
        &mut self,
        file: &JournalFile,
        limit: usize,
    ) -> Result<OffsetBatch> {
        let mut out = Vec::new();
        let mut steps = 0usize;
        let target_entries = file.live_state().n_entries;
        if self.consumed_entries > target_entries {
            return Err(SdJournalError::Transient {
                path: Some(file.path().to_path_buf()),
                reason: "journal n_entries moved backwards while tailing".to_string(),
            });
        }
        let mut remaining = target_entries.saturating_sub(self.consumed_entries);
        if remaining == 0 {
            return Ok(OffsetBatch {
                offsets: out,
                exhausted: true,
            });
        }

        // Resuming the current array does not consume a chain-growth step. After that,
        // the same loop reads later arrays and resets their item index to zero.
        let mut is_new_array = self.current_array_offset.is_none();
        let (mut current, mut item_idx) = match self.current_array_offset {
            Some(offset) => (offset, self.next_item_idx),
            None => (file.live_state().entry_array_offset, 0),
        };

        while current != 0 && remaining != 0 {
            let items = file.read_entry_array_items(current)?;
            if item_idx != 0 && items.get(item_idx - 1).copied() != self.last_entry_offset {
                return Err(SdJournalError::Corrupt {
                    path: Some(file.path().to_path_buf()),
                    offset: Some(current),
                    reason:
                        "ENTRY_ARRAY no longer contains the live checkpoint at its committed slot"
                            .to_string(),
                });
            }
            while item_idx < items.len() && remaining != 0 {
                let entry_offset = items[item_idx];
                item_idx = item_idx.saturating_add(1);
                validate_tail_entry_offset(file, entry_offset, self.last_entry_offset)?;
                out.push(entry_offset);
                self.last_entry_offset = Some(entry_offset);
                self.consumed_entries = self.consumed_entries.saturating_add(1);
                remaining = remaining.saturating_sub(1);
                if out.len() >= limit {
                    self.current_array_offset = Some(current);
                    self.next_item_idx = item_idx;
                    return Ok(OffsetBatch {
                        offsets: out,
                        exhausted: false,
                    });
                }
            }
            self.current_array_offset = Some(current);
            self.next_item_idx = item_idx;
            current = if remaining == 0 {
                0
            } else {
                file.read_entry_array_next_offset(current)?
            };

            if is_new_array {
                steps = steps.saturating_add(1);
                if steps > file.max_object_chain_steps() {
                    return Err(SdJournalError::Transient {
                        path: Some(file.path().to_path_buf()),
                        reason: "entry array chain refresh exceeded expected growth".to_string(),
                    });
                }
            }
            is_new_array = true;
            item_idx = 0;
        }

        if remaining != 0 {
            return Err(SdJournalError::Transient {
                path: Some(file.path().to_path_buf()),
                reason: format!(
                    "ENTRY_ARRAY exposes fewer committed entries than header n_entries ({remaining} missing)"
                ),
            });
        }

        Ok(OffsetBatch {
            offsets: out,
            exhausted: true,
        })
    }
}

fn validate_tail_entry_offset(
    file: &JournalFile,
    offset: u64,
    previous: Option<u64>,
) -> Result<()> {
    if offset == 0 || previous.is_some_and(|previous| offset <= previous) {
        return Err(SdJournalError::Transient {
            path: Some(file.path().to_path_buf()),
            reason: format!(
                "committed ENTRY_ARRAY offsets must be nonzero and strictly increasing ({previous:?} -> {offset})"
            ),
        });
    }
    Ok(())
}

struct TrackedFileBuilder {
    tracked_files: Vec<TrackedFile>,
    skipped_paths: Vec<PathBuf>,
    first_skipped_error: Option<SdJournalError>,
    seen: HashSet<[u8; 16]>,
}

impl TrackedFileBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            tracked_files: Vec::with_capacity(capacity),
            skipped_paths: Vec::new(),
            first_skipped_error: None,
            seen: HashSet::new(),
        }
    }

    // Report whether this exact physical file was accepted, not merely whether its ID is known.
    fn push(&mut self, file: &JournalFile) -> Result<bool> {
        let file_id = file.file_id();
        if self.seen.contains(&file_id) {
            return Ok(false);
        }

        match track_file(file) {
            Ok(tracked) => {
                self.seen.insert(file_id);
                self.tracked_files.push(tracked);
                Ok(true)
            }
            Err(err) if is_skippable_live_file_error(&err) => {
                self.skip_path(file.path(), err);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn open_path(&mut self, path: &Path, config: &JournalConfig) -> Result<Option<JournalFile>> {
        match JournalFile::open(path.to_path_buf(), config) {
            Ok(file) => Ok(self.push(&file)?.then_some(file)),
            Err(err) if is_skippable_live_file_error(&err) => {
                self.skip_path(path, err);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn skip_path(&mut self, path: &Path, err: SdJournalError) {
        warn_skipped_live_file(path, &err);
        self.skipped_paths.push(path.to_path_buf());
        self.first_skipped_error.get_or_insert(err);
    }

    fn finish(self) -> Result<(Vec<TrackedFile>, Vec<PathBuf>)> {
        if self.tracked_files.is_empty()
            && let Some(err) = self.first_skipped_error
        {
            return Err(err);
        }
        Ok((self.tracked_files, self.skipped_paths))
    }
}

#[cfg(target_os = "linux")]
pub(super) fn collect_watch_paths(
    roots: &[PathBuf],
    tracked_files: &[TrackedFile],
) -> Vec<PathBuf> {
    let mut watch_paths = collect_live_dirs(roots, tracked_files);
    for tracked in tracked_files {
        watch_paths.push(tracked.path.clone());
    }
    watch_paths.sort();
    watch_paths.dedup();
    watch_paths
}

pub(super) fn collect_fallback_dirs(
    roots: &[PathBuf],
    tracked_files: &[TrackedFile],
) -> Vec<FallbackDirState> {
    collect_live_dirs(roots, tracked_files)
        .into_iter()
        .map(|path| FallbackDirState {
            modified: std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok(),
            path,
        })
        .collect()
}

fn collect_live_dirs(roots: &[PathBuf], tracked_files: &[TrackedFile]) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = roots.to_vec();
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir()
                && !file_type.is_symlink()
                && is_machine_id_dir(&entry.file_name())
            {
                dirs.push(entry.path());
            }
        }
    }
    for tracked in tracked_files {
        if let Some(parent) = tracked.path.parent() {
            dirs.push(parent.to_path_buf());
        }
    }
    dirs.sort();
    dirs.dedup();
    dirs
}

pub(super) fn build_tracked_files_from_open_files<'a>(
    files: impl ExactSizeIterator<Item = &'a JournalFile>,
) -> Result<Vec<TrackedFile>> {
    let mut builder = TrackedFileBuilder::new(files.len());

    for file in files {
        builder.push(file)?;
    }

    builder.finish().map(|(tracked, _)| tracked)
}

pub(super) fn build_tracked_files_from_paths(
    paths: &[PathBuf],
    config: &JournalConfig,
) -> Result<(Vec<TrackedFile>, Vec<PathBuf>)> {
    let mut builder = TrackedFileBuilder::new(paths.len());

    for path in paths {
        builder.open_path(path, config)?;
    }

    builder.finish()
}

pub(super) fn build_live_snapshot(
    paths: &[PathBuf],
    config: &JournalConfig,
) -> Result<LiveSnapshot> {
    let mut builder = TrackedFileBuilder::new(paths.len());
    let mut opened = Vec::with_capacity(paths.len());
    for path in paths {
        if let Some(file) = builder.open_path(path, config)? {
            // Keep only the validated copy, and release its handle/mapping before opening
            // the next file. Replay reads retain these committed bounds, not a new header.
            opened.push(file.reopen_snapshot());
        }
    }
    let (tracked_files, _) = builder.finish()?;
    let file_tails = tracked_files
        .into_iter()
        .filter_map(|tracked| {
            tracked
                .tail
                .last_entry_offset()
                .map(|offset| (tracked.file_id, offset))
        })
        .collect();
    let journal = journal_from_snapshot_files(opened, config.clone())?;

    Ok(LiveSnapshot {
        file_tails,
        journal,
    })
}

fn track_file(file: &JournalFile) -> Result<TrackedFile> {
    file.validate_reopen_identity()?;
    let tail = FileTailCursor::at_end(file)?;
    // Validate the tail ENTRY even though live checkpoints only need its physical offset.
    if let Some(offset) = tail.last_entry_offset {
        file.read_entry_meta(offset)?;
    }

    Ok(TrackedFile {
        path: file.path().to_path_buf(),
        file_id: file.file_id(),
        seqnum_id: file.seqnum_id(),
        live_state: file.live_state(),
        tail,
        modified: file.modified_at_open(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{
        HEADER_INCOMPATIBLE_COMPACT, OBJECT_ENTRY, OBJECT_ENTRY_ARRAY, STATE_ONLINE,
    };
    use std::fs;

    fn tail_fixture(
        n_entries: u64,
        compact: bool,
        max_chain_steps: usize,
    ) -> (tempfile::TempDir, JournalFile) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("tail.journal");
        let mut bytes = vec![0u8; 720];
        bytes[..8].copy_from_slice(b"LPKSHHRH");
        bytes[16] = STATE_ONLINE;
        if compact {
            bytes[12..16].copy_from_slice(&HEADER_INCOMPATIBLE_COMPACT.to_le_bytes());
        }
        let put = |bytes: &mut [u8], offset: usize, value: u64| {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        };
        put(&mut bytes, 88, 272);
        put(&mut bytes, 96, 448);
        put(&mut bytes, 136, 640);
        put(&mut bytes, 152, n_entries);
        put(&mut bytes, 176, 272);
        let item_size = if compact { 4 } else { 8 };
        for (array, next, items) in [(272, 312, [400u64, 480]), (312, 0, [560, 640])] {
            bytes[array] = OBJECT_ENTRY_ARRAY;
            put(&mut bytes, array + 8, (24 + 2 * item_size) as u64);
            put(&mut bytes, array + 16, next);
            for (idx, entry_offset) in items.into_iter().enumerate() {
                let start = array + 24 + idx * item_size;
                bytes[start..start + item_size]
                    .copy_from_slice(&entry_offset.to_le_bytes()[..item_size]);
            }
        }
        for (idx, entry) in [400, 480, 560, 640].into_iter().enumerate() {
            bytes[entry] = OBJECT_ENTRY;
            put(&mut bytes, entry + 8, 80);
            put(&mut bytes, entry + 16, idx as u64 + 1);
        }
        fs::write(&path, bytes).unwrap();
        let file = JournalFile::open(
            path,
            &JournalConfig {
                max_object_chain_steps: max_chain_steps,
                ..Default::default()
            },
        )
        .unwrap();
        (root, file)
    }

    #[test]
    fn tail_batches_preserve_checkpoints_across_array_boundaries() {
        for compact in [false, true] {
            let (_root, file) = tail_fixture(4, compact, 8);
            for resume in [false, true] {
                for limit in [1, 2, 3, 8] {
                    let mut tail = if resume {
                        FileTailCursor {
                            current_array_offset: Some(272),
                            next_item_idx: 1,
                            last_entry_offset: Some(400),
                            consumed_entries: 1,
                        }
                    } else {
                        FileTailCursor::at_start()
                    };
                    let expected = if resume {
                        vec![480, 560, 640]
                    } else {
                        vec![400, 480, 560, 640]
                    };
                    let mut seen = Vec::new();
                    for _ in 0..5 {
                        let batch = tail.drain_new_offsets(&file, limit).unwrap();
                        assert!(batch.offsets.len() <= limit);
                        let reached_limit = batch.offsets.len() == limit;
                        seen.extend(batch.offsets);
                        assert_eq!(batch.exhausted, !reached_limit);
                        if batch.exhausted {
                            break;
                        }
                    }
                    assert_eq!(seen, expected);
                    assert_eq!(tail.current_array_offset, Some(312));
                    assert_eq!(tail.next_item_idx, 2);
                    assert_eq!(tail.last_entry_offset, Some(640));
                    assert_eq!(tail.consumed_entries, 4);
                    let empty = tail.drain_new_offsets(&file, limit).unwrap();
                    assert!(empty.offsets.is_empty());
                    assert!(empty.exhausted);
                }
            }
        }
    }

    #[test]
    fn tail_growth_guard_distinguishes_current_and_new_arrays() {
        let (_root, file) = tail_fixture(2, false, 0);
        assert!(matches!(
            FileTailCursor::at_start().drain_new_offsets(&file, 8),
            Err(SdJournalError::Transient { .. })
        ));
        let mut resumed = FileTailCursor {
            current_array_offset: Some(272),
            next_item_idx: 1,
            last_entry_offset: Some(400),
            consumed_entries: 1,
        };
        let batch = resumed.drain_new_offsets(&file, 8).unwrap();
        assert_eq!(batch.offsets, [480]);
        assert!(batch.exhausted);

        for max_steps in [0, 1] {
            let (_root, file) = tail_fixture(4, false, max_steps);
            let mut resumed = FileTailCursor {
                current_array_offset: Some(272),
                next_item_idx: 1,
                last_entry_offset: Some(400),
                consumed_entries: 1,
            };
            let batch = resumed.drain_new_offsets(&file, 8);
            if max_steps == 0 {
                assert!(matches!(batch, Err(SdJournalError::Transient { .. })));
            } else {
                let batch = batch.unwrap();
                assert_eq!(batch.offsets, [480, 560, 640]);
                assert!(batch.exhausted);
            }
        }
    }

    #[test]
    fn tail_rejects_missing_and_non_increasing_committed_offsets() {
        let (_root, file) = tail_fixture(5, false, 8);
        assert!(matches!(
            FileTailCursor::at_start().drain_new_offsets(&file, 8),
            Err(SdJournalError::Transient { .. })
        ));
        let (_root, file) = tail_fixture(4, false, 8);
        let mut bytes = fs::read(file.path()).unwrap();
        // Both arrays are individually sorted, but the second restarts at an earlier ENTRY.
        bytes[336..344].copy_from_slice(&400u64.to_le_bytes());
        fs::write(file.path(), bytes).unwrap();
        assert!(matches!(
            FileTailCursor::at_start().drain_new_offsets(&file, 8),
            Err(SdJournalError::Transient { .. })
        ));
    }

    #[test]
    fn tail_rejects_an_array_that_lost_already_consumed_slots() {
        for compact in [false, true] {
            for corruption in ["shrink", "replace-checkpoint"] {
                let (_root, original) = tail_fixture(2, compact, 8);
                let mut tail = FileTailCursor::at_end(&original).unwrap();
                assert_eq!(tail.next_item_idx, 2);

                let mut bytes = fs::read(original.path()).unwrap();
                let item_size = if compact { 4 } else { 8 };
                match corruption {
                    "shrink" => {
                        let size = 24 + item_size as u64;
                        bytes[280..288].copy_from_slice(&size.to_le_bytes());
                    }
                    "replace-checkpoint" => {
                        let item = 296 + item_size;
                        bytes[item..item + item_size]
                            .copy_from_slice(&448u64.to_le_bytes()[..item_size]);
                    }
                    _ => unreachable!(),
                }
                bytes[152..160].copy_from_slice(&3u64.to_le_bytes());
                fs::write(original.path(), bytes).unwrap();
                let updated =
                    JournalFile::open(original.path().to_path_buf(), &JournalConfig::default())
                        .unwrap();

                // The second array still offers a newer ENTRY. It must not conceal the loss
                // of the checkpoint by silently rewinding or trusting its saved item index.
                assert!(
                    matches!(
                        tail.drain_new_offsets(&updated, 8),
                        Err(SdJournalError::Corrupt { .. })
                    ),
                    "accepted {corruption}"
                );
            }
        }
    }

    #[test]
    fn live_dirs_include_empty_machine_id_directories() {
        let root = tempfile::tempdir().expect("create live root");
        let machine_dir = root.path().join("0123456789abcdef0123456789abcdef");
        fs::create_dir(&machine_dir).expect("create machine-id directory");

        let dirs = collect_live_dirs(&[root.path().to_path_buf()], &[]);
        assert!(dirs.contains(&root.path().to_path_buf()));
        assert!(dirs.contains(&machine_dir));
    }

    #[test]
    fn fallback_metadata_detects_same_length_timestamp_change() {
        let root = tempfile::tempdir().expect("create metadata root");
        let path = root.path().join("active.journal");
        fs::write(&path, [0u8; 16]).expect("write test file");
        let metadata = fs::metadata(&path).expect("read metadata");

        assert!(!file_metadata_changed(
            metadata.len(),
            metadata.modified().ok(),
            &metadata
        ));
        assert!(file_metadata_changed(
            metadata.len(),
            Some(SystemTime::UNIX_EPOCH),
            &metadata
        ));
    }
}
