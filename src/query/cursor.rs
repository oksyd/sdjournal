use super::JournalQuery;
use crate::cursor::{Cursor, SystemdCursor, compare_entry_keys, same_entry};
use crate::error::{Result, SdJournalError};
use crate::file::{EntryMeta, JournalFile};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub(super) struct CursorStart {
    boundary: CursorBoundary,
    inclusive: bool,
}

#[derive(Debug, Clone)]
enum CursorBoundary {
    Exact(EntryMeta),
    Systemd(SystemdCursor),
}

impl CursorStart {
    pub(super) fn exact_meta(&self) -> Option<EntryMeta> {
        match self.boundary {
            CursorBoundary::Exact(meta) => Some(meta),
            CursorBoundary::Systemd(_) => None,
        }
    }

    pub(super) fn matches_exact(&self, meta: &EntryMeta) -> bool {
        match &self.boundary {
            CursorBoundary::Exact(key) => same_entry(meta, key),
            CursorBoundary::Systemd(_) => false,
        }
    }

    pub(super) fn inclusive(&self) -> bool {
        self.inclusive
    }

    pub(super) fn accepts(&self, meta: &EntryMeta) -> bool {
        let ordering = match &self.boundary {
            CursorBoundary::Exact(key) => compare_entry_keys(meta, key),
            CursorBoundary::Systemd(cursor) => compare_systemd(meta, cursor),
        };

        if self.inclusive {
            ordering != Ordering::Less
        } else {
            ordering == Ordering::Greater
        }
    }
}

pub(super) fn build_cursor_start(query: &JournalQuery) -> Result<Option<CursorStart>> {
    let (cursor, inclusive) = match &query.cursor_start {
        Some(value) => value,
        None => return Ok(None),
    };

    let boundary = if cursor.fields.seqnum_id.is_some() && cursor.fields.seqnum.is_some() {
        CursorBoundary::Exact(
            find_exact_systemd_cursor(query, cursor)?.ok_or(SdJournalError::NotFound)?,
        )
    } else {
        CursorBoundary::Systemd(cursor.fields.clone())
    };
    Ok(Some(CursorStart {
        boundary,
        inclusive: *inclusive,
    }))
}

fn committed_meta_at_offset(file: &JournalFile, entry_offset: u64) -> Result<Option<EntryMeta>> {
    // A syntactically valid ENTRY may have been prewritten before n_entries was committed, or
    // may not belong to the file-level ENTRY array at all. Cursors must not inject such objects
    // into a query. Seek in the validated, snapshotted array instead of reading the offset raw.
    let mut entries = file.entry_iter_after_offset(false, entry_offset.saturating_sub(1))?;
    match entries.next().transpose()? {
        Some(meta) if meta.entry_offset == entry_offset => Ok(Some(meta)),
        _ => Ok(None),
    }
}

fn compare_systemd(meta: &EntryMeta, cursor: &SystemdCursor) -> Ordering {
    // Sequence cursors are resolved to exact entry positions before reaching this comparison.
    if let (Some(boot_id), Some(monotonic_usec)) = (cursor.boot_id, cursor.monotonic_usec)
        && meta.boot_id == boot_id
    {
        let ordering = meta.monotonic_usec.cmp(&monotonic_usec);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    if let Some(realtime_usec) = cursor.realtime_usec {
        let ordering = meta.realtime_usec.cmp(&realtime_usec);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    cursor
        .xor_hash
        .map_or(Ordering::Equal, |xor_hash| meta.xor_hash.cmp(&xor_hash))
}

fn find_exact_systemd_cursor(query: &JournalQuery, cursor: &Cursor) -> Result<Option<EntryMeta>> {
    let systemd = &cursor.fields;
    let mut first_error = None;
    if let Some(location) = cursor.location_hint
        && let Some(info) = query
            .journal
            .inner
            .file_infos
            .iter()
            .find(|info| info.file_id == location.file_id)
    {
        match info
            .open()
            .and_then(|file| committed_meta_at_offset(&file, location.entry_offset))
        {
            Ok(Some(meta)) if systemd_cursor_matches(&meta, systemd) => return Ok(Some(meta)),
            Err(error) => {
                first_error = Some(error);
            }
            _ => {}
        }
    }

    // The logical entry can survive rotation in a different physical file. Persisted cursors
    // take this same path because the text contains no location hint.
    for info in &query.journal.inner.file_infos {
        if let Some(seqnum_id) = systemd.seqnum_id
            && info.seqnum_id != seqnum_id
        {
            continue;
        }

        let file = match info.open() {
            Ok(file) => file,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };

        let iter = match file.entry_iter(false) {
            Ok(iter) => iter,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };

        for item in iter {
            let meta = match item {
                Ok(meta) => meta,
                Err(error) => {
                    first_error.get_or_insert(error);
                    continue;
                }
            };
            if systemd_cursor_matches(&meta, systemd) {
                return Ok(Some(meta));
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

fn systemd_cursor_matches(meta: &EntryMeta, cursor: &SystemdCursor) -> bool {
    cursor.seqnum_id.is_none_or(|value| meta.seqnum_id == value)
        && cursor.seqnum.is_none_or(|value| meta.seqnum == value)
        && cursor.boot_id.is_none_or(|value| meta.boot_id == value)
        && cursor
            .monotonic_usec
            .is_none_or(|value| meta.monotonic_usec == value)
        && cursor
            .realtime_usec
            .is_none_or(|value| meta.realtime_usec == value)
        && cursor.xor_hash.is_none_or(|value| meta.xor_hash == value)
}
