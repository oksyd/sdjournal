use crate::config::JournalConfig;
use crate::cursor::{Cursor, SdJournalEntryKey, same_entry};
use crate::entry::EntryRef;
use crate::error::Result;
use crate::filter::Filter;
use crate::journal::Journal;
use crate::query::JournalIter;
use std::collections::VecDeque;

use super::{is_skippable_live_file_error, warn_live_file_error};

pub(super) struct ReplayState {
    journal: Journal,
    pub(super) remaining: Option<usize>,
    // Keep subscription state small while retaining the concrete iterator type.
    iter: Box<JournalIter>,
    buffered: VecDeque<EntryRef>,
    exhausted: bool,
}

pub(super) struct EntryBatch {
    pub(super) entries: Vec<EntryRef>,
    pub(super) exhausted: bool,
}

impl ReplayState {
    pub(super) fn new(
        journal: Journal,
        filter: &Filter,
        after_cursor: Option<Cursor>,
        since_realtime: Option<u64>,
        config: &JournalConfig,
    ) -> Result<Self> {
        let mut query = journal.query_with_filter(filter.clone());
        if let Some(since) = since_realtime {
            query.since_realtime(since);
        }
        if let Some(cursor) = after_cursor {
            query.after_cursor(cursor);
        }
        // Resolve the starting cursor once, before registering the subscription. A previous
        // batch's source may be vacuumed later without invalidating the remaining iterator.
        let iter = Box::new(query.into_iter()?);
        Ok(Self {
            journal,
            remaining: config.max_live_replay_entries,
            iter,
            buffered: VecDeque::new(),
            exhausted: false,
        })
    }

    pub(super) fn collect_batch(&mut self, limit: usize) -> Result<EntryBatch> {
        while !self.exhausted && self.buffered.len() <= limit {
            match self.iter.next() {
                Some(Ok(entry)) => self.buffered.push_back(entry),
                Some(Err(err)) if is_skippable_live_file_error(&err) => {
                    warn_live_file_error("skipping live replay entry", &err);
                }
                // Retain already decoded entries if the caller handles this error and polls
                // again. The buffer is bounded to one batch plus its lookahead entry.
                Some(Err(err)) => return Err(err),
                None => self.exhausted = true,
            }
        }
        let entries = self
            .buffered
            .drain(..limit.min(self.buffered.len()))
            .collect();
        Ok(EntryBatch {
            entries,
            exhausted: self.exhausted && self.buffered.is_empty(),
        })
    }

    pub(super) fn contains_key(&self, key: SdJournalEntryKey) -> Result<bool> {
        for info in &self.journal.inner.file_infos {
            if info.seqnum_id != key.seqnum_id {
                continue;
            }
            let file = info.open()?;
            // New live entries normally follow this snapshot's tail. Check that tail first
            // so duplicate detection does not repeatedly scan the entire replay history.
            for item in file.entry_iter(true)? {
                let meta = item?;
                if meta.seqnum < key.seqnum {
                    break;
                }
                if same_entry(&meta, &key) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
