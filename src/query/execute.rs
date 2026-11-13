use super::JournalQuery;
use super::cursor::{CursorStart, build_cursor_start};
use crate::cursor::{compare_entry_keys, same_entry};
use crate::entry::EntryRef;
use crate::error::{Result, SdJournalError};
use crate::file::{DataEntryOffsetIter, EntryMeta, FileEntryIter, JournalFile};
use crate::filter::MatchTerm;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

enum FileMetaIter {
    Empty,
    Single(FileBranchIter),
    Or(SortedUnion<FileBranchIter, EntryMeta>),
}

impl FileMetaIter {
    fn from_branch_iters(mut iters: Vec<FileBranchIter>, reverse: bool) -> Self {
        iters.retain(|it| !matches!(&it.kind, BranchKind::Empty));
        match iters.len() {
            0 => FileMetaIter::Empty,
            1 => FileMetaIter::Single(iters.remove(0)),
            _ => FileMetaIter::Or(SortedUnion::new(iters, reverse)),
        }
    }
}

impl Iterator for FileMetaIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FileMetaIter::Empty => None,
            FileMetaIter::Single(it) => it.next(),
            FileMetaIter::Or(it) => it.next(),
        }
    }
}

// Both unions merge sorted postings within a single file. Cross-file systemd ordering is
// deliberately handled by JournalIter's pairwise selection, not by this heap.
trait UnionValue: Copy {
    fn compare(&self, other: &Self) -> std::cmp::Ordering;
    fn same_value(&self, other: &Self) -> bool;
}

impl UnionValue for u64 {
    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }

    fn same_value(&self, other: &Self) -> bool {
        self == other
    }
}

impl UnionValue for EntryMeta {
    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.seqnum
            .cmp(&other.seqnum)
            .then_with(|| self.entry_offset.cmp(&other.entry_offset))
    }

    fn same_value(&self, other: &Self) -> bool {
        same_entry(self, other)
    }
}

#[derive(Clone, Copy)]
struct UnionHeapItem<T> {
    value: T,
    iter_idx: usize,
}

impl<T: UnionValue> PartialEq for UnionHeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: UnionValue> Eq for UnionHeapItem<T> {}

impl<T: UnionValue> PartialOrd for UnionHeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: UnionValue> Ord for UnionHeapItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value
            .compare(&other.value)
            .then_with(|| self.iter_idx.cmp(&other.iter_idx))
    }
}

enum UnionHeap<T: UnionValue> {
    Forward(BinaryHeap<Reverse<UnionHeapItem<T>>>),
    Reverse(BinaryHeap<UnionHeapItem<T>>),
}

impl<T: UnionValue> UnionHeap<T> {
    fn new(reverse: bool) -> Self {
        if reverse {
            Self::Reverse(BinaryHeap::new())
        } else {
            Self::Forward(BinaryHeap::new())
        }
    }

    fn pop(&mut self) -> Option<UnionHeapItem<T>> {
        match self {
            Self::Forward(heap) => heap.pop().map(|item| item.0),
            Self::Reverse(heap) => heap.pop(),
        }
    }

    fn push(&mut self, item: UnionHeapItem<T>) {
        match self {
            Self::Forward(heap) => heap.push(Reverse(item)),
            Self::Reverse(heap) => heap.push(item),
        }
    }
}

struct SortedUnion<I, T: UnionValue> {
    heap: UnionHeap<T>,
    iters: Vec<I>,
    initialized: usize,
    pending_iter: Option<usize>,
    last_emitted: Option<T>,
}

impl<I, T> SortedUnion<I, T>
where
    I: Iterator<Item = Result<T>>,
    T: UnionValue,
{
    fn new(iters: Vec<I>, reverse: bool) -> Self {
        Self {
            heap: UnionHeap::new(reverse),
            iters,
            initialized: 0,
            pending_iter: None,
            last_emitted: None,
        }
    }
}

impl<I, T> Iterator for SortedUnion<I, T>
where
    I: Iterator<Item = Result<T>>,
    T: UnionValue,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while self.initialized < self.iters.len() {
                let iter_idx = self.initialized;
                match self.iters[iter_idx].next() {
                    Some(Ok(value)) => self.heap.push(UnionHeapItem { value, iter_idx }),
                    Some(Err(error)) => return Some(Err(error)),
                    None => {}
                }
                self.initialized += 1;
            }
            if let Some(iter_idx) = self.pending_iter {
                match self.iters[iter_idx].next() {
                    Some(Ok(value)) => self.heap.push(UnionHeapItem { value, iter_idx }),
                    Some(Err(error)) => return Some(Err(error)),
                    None => {}
                }
                self.pending_iter = None;
            }
            let item = self.heap.pop()?;
            self.pending_iter = Some(item.iter_idx);
            if self
                .last_emitted
                .is_some_and(|last| last.same_value(&item.value))
            {
                continue;
            }
            self.last_emitted = Some(item.value);
            return Some(Ok(item.value));
        }
    }
}

struct AndOffsetIter {
    reverse: bool,
    iters: Vec<OffsetIter>,
    cursors: Vec<Option<u64>>,
    pending_advance: Option<usize>,
    done: bool,
}

impl AndOffsetIter {
    fn new(iters: Vec<OffsetIter>, reverse: bool) -> Self {
        let cursors = vec![None; iters.len()];
        Self {
            reverse,
            iters,
            cursors,
            pending_advance: Some(0),
            done: false,
        }
    }

    fn target(&self) -> Option<u64> {
        let mut it = self.cursors.iter().copied();
        let mut target = it.next()??;
        for v in it {
            let v = v?;
            target = if self.reverse {
                target.min(v)
            } else {
                target.max(v)
            };
        }
        Some(target)
    }

    fn advance_to(&mut self, idx: usize, target: u64) -> Option<Result<()>> {
        loop {
            let cur = self.cursors.get(idx).copied().flatten()?;

            let needs_advance = if self.reverse {
                cur > target
            } else {
                cur < target
            };
            if !needs_advance {
                return Some(Ok(()));
            }

            match self.iters[idx].next() {
                Some(Ok(v)) => self.cursors[idx] = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.cursors[idx] = None;
                    return None;
                }
            }
        }
    }
}

impl Iterator for AndOffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Initialize all heads, or advance them after the previous intersection. Remember
        // progress across errors so healthy heads are not advanced twice or discarded.
        if let Some(start) = self.pending_advance {
            for i in start..self.iters.len() {
                match self.iters[i].next() {
                    Some(Ok(value)) => self.cursors[i] = Some(value),
                    Some(Err(error)) => return Some(Err(error)),
                    None => {
                        self.done = true;
                        return None;
                    }
                }
                self.pending_advance = Some(i + 1);
            }
            self.pending_advance = None;
        }

        loop {
            let target = match self.target() {
                Some(v) => v,
                None => {
                    self.done = true;
                    return None;
                }
            };

            for i in 0..self.iters.len() {
                match self.advance_to(i, target) {
                    Some(Ok(())) => {}
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            // At least one cursor was already at the selected minimum/maximum and stayed
            // there while the others advanced. Their intersection is therefore this target.
            if !self.cursors.iter().all(|cursor| *cursor == Some(target)) {
                continue;
            }

            // Leave successors unread until the caller asks for another match.
            self.pending_advance = Some(0);
            return Some(Ok(target));
        }
    }
}

enum OffsetIter {
    Single(DataEntryOffsetIter),
    Or(SortedUnion<DataEntryOffsetIter, u64>),
}

impl Iterator for OffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OffsetIter::Single(iter) => iter.next(),
            OffsetIter::Or(iter) => iter.next(),
        }
    }
}

struct FileBranchIter {
    file: JournalFile,
    kind: BranchKind,
    indexed_terms: Vec<MatchTerm>,
}

enum BranchKind {
    Empty,
    Indexed { offset_iter: AndOffsetIter },
    Scan { iter: FileEntryIter },
}

impl FileBranchIter {
    fn new(file: JournalFile, terms: &[&MatchTerm], reverse: bool) -> Result<Self> {
        let indexed_terms: Vec<MatchTerm> = if file.is_online() {
            Vec::new()
        } else {
            terms
                .iter()
                .filter(|term| !term.exact_payloads().is_empty())
                .map(|term| (*term).clone())
                .collect()
        };

        // While ONLINE, systemd commits the file-level ENTRY array/count before updating DATA
        // postings. Even an internally valid index may therefore omit committed matches.
        let kind = if indexed_terms.is_empty() {
            BranchKind::Scan {
                iter: file.entry_iter(reverse)?,
            }
        } else {
            match Self::indexed_branch(&file, &indexed_terms, reverse) {
                Ok(kind) => kind,
                Err(SdJournalError::Corrupt { .. } | SdJournalError::Transient { .. }) => {
                    BranchKind::Scan {
                        iter: file.entry_iter(reverse)?,
                    }
                }
                Err(error) => return Err(error),
            }
        };

        Ok(Self {
            file,
            kind,
            indexed_terms,
        })
    }

    fn indexed_branch(
        file: &JournalFile,
        terms: &[MatchTerm],
        reverse: bool,
    ) -> Result<BranchKind> {
        let mut data_refs = Vec::with_capacity(terms.len());
        for term in terms {
            let mut refs = Vec::new();
            for payload in term.exact_payloads() {
                refs.extend(file.find_data_objects(payload)?);
            }
            if refs.is_empty() {
                return Ok(BranchKind::Empty);
            }
            data_refs.push(refs);
        }
        data_refs.sort_by_key(|refs| {
            refs.iter()
                .fold(0u64, |total, data| total.saturating_add(data.n_entries))
        });

        let mut iters = Vec::with_capacity(data_refs.len());
        for refs in data_refs {
            let mut term_iters = refs
                .into_iter()
                .map(|data_ref| file.data_entry_offsets(data_ref, reverse))
                .collect::<Result<Vec<_>>>()?;
            if term_iters.len() == 1 {
                iters.push(OffsetIter::Single(term_iters.remove(0)));
            } else {
                iters.push(OffsetIter::Or(SortedUnion::new(term_iters, reverse)));
            }
        }
        Ok(BranchKind::Indexed {
            offset_iter: AndOffsetIter::new(iters, reverse),
        })
    }
}

impl Iterator for FileBranchIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            BranchKind::Empty => None,
            BranchKind::Indexed { offset_iter } => {
                let entry_offset = match offset_iter.next()? {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                let entry = match self.file.read_entry_ref(entry_offset) {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error)),
                };
                let meta = entry.entry_key();
                if !self
                    .indexed_terms
                    .iter()
                    .all(|term| term.matches(entry.iter_fields()))
                {
                    return Some(Err(self.file.metadata_error(
                        Some(entry_offset),
                        "DATA index references an ENTRY that does not contain the indexed field",
                    )));
                }

                Some(Ok(meta))
            }
            BranchKind::Scan { iter } => iter.next(),
        }
    }
}

fn time_matches(meta: &EntryMeta, since: Option<u64>, until: Option<u64>) -> bool {
    since.is_none_or(|value| meta.realtime_usec >= value)
        && until.is_none_or(|value| meta.realtime_usec <= value)
}

pub(crate) struct JournalIter {
    query: JournalQuery,
    cursor_start: Option<CursorStart>,
    cursor_reached: bool,
    produced: usize,
    last_selected: Option<EntryMeta>,
    sources: Vec<FileSource>,
    candidates: Vec<Option<EntryMeta>>,
    initialized_sources: usize,
    failed_source: Option<usize>,
    initialization_errors: std::vec::IntoIter<SdJournalError>,
    done: bool,
}

struct FileSource {
    file: JournalFile,
    iter: FileMetaIter,
}

impl JournalIter {
    pub(super) fn new(query: JournalQuery) -> Result<Self> {
        if matches!(query.limit, Some(0)) {
            return Ok(Self {
                query,
                cursor_start: None,
                cursor_reached: true,
                produced: 0,
                last_selected: None,
                sources: Vec::new(),
                candidates: Vec::new(),
                initialized_sources: 0,
                failed_source: None,
                initialization_errors: Vec::new().into_iter(),
                done: true,
            });
        }

        let mut initialization_errors = Vec::new();
        let cursor_start = build_cursor_start(&query)?;
        let cursor_reached = cursor_start
            .as_ref()
            .is_none_or(|cursor| cursor.exact_meta().is_none());
        // Conditional systemd ordering is not transitive across files. Removing a candidate
        // before the merge can change the order of the remaining entries and invalidate a
        // checkpoint. Keep the complete metadata stream for multi-file queries and exact
        // cursor resumes; apply time and field filters only after selecting the next entry.
        let branches = if query.journal.inner.file_infos.len() > 1
            || cursor_start
                .as_ref()
                .is_some_and(|cursor| cursor.exact_meta().is_some())
        {
            vec![Vec::new()]
        } else {
            query.filter.branches()
        };
        let mut sources = Vec::with_capacity(query.journal.inner.file_infos.len());
        for info in &query.journal.inner.file_infos {
            let file = match info.open() {
                Ok(file) => file,
                Err(e) => {
                    initialization_errors.push(e);
                    continue;
                }
            };
            // Every ONLINE branch scans the same ENTRY array. One scan is enough because the
            // complete boolean expression is evaluated after the cross-file merge.
            let branch_count = if file.is_online() { 1 } else { branches.len() };
            let mut branch_iters = Vec::with_capacity(branch_count);
            for terms in branches.iter().take(branch_count) {
                match FileBranchIter::new(file.clone(), terms, query.reverse) {
                    Ok(it) => branch_iters.push(it),
                    Err(e) => {
                        initialization_errors.push(e);
                    }
                }
            }
            sources.push(FileSource {
                iter: FileMetaIter::from_branch_iters(branch_iters, query.reverse),
                file,
            });
        }

        // Keep the frequently scanned candidates separate from the larger iterator state.
        let candidates = vec![None; sources.len()];

        Ok(Self {
            query,
            cursor_start,
            cursor_reached,
            produced: 0,
            last_selected: None,
            sources,
            candidates,
            initialized_sources: 0,
            failed_source: None,
            initialization_errors: initialization_errors.into_iter(),
            done: false,
        })
    }

    fn select_next(&self) -> Option<(usize, EntryMeta)> {
        let mut selected: Option<(usize, EntryMeta)> = None;
        for (idx, candidate) in self.candidates.iter().copied().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            let Some((_, best)) = selected else {
                selected = Some((idx, candidate));
                continue;
            };
            let ordering = compare_entry_keys(&candidate, &best);
            let replaces = if self.query.reverse {
                ordering.is_gt()
            } else {
                ordering.is_lt()
            };
            // Equal entries retain the first source, providing a stable file-index tie break.
            if replaces {
                selected = Some((idx, candidate));
            }
        }
        selected
    }

    fn passes_cursor(&mut self, meta: &EntryMeta) -> bool {
        let Some(cursor_start) = &self.cursor_start else {
            return true;
        };
        if cursor_start.exact_meta().is_none() {
            return cursor_start.accepts(meta);
        }
        if self.query.reverse {
            if cursor_start.matches_exact(meta) {
                self.done = true;
                return cursor_start.inclusive();
            }
            return true;
        }
        if self.cursor_reached {
            !cursor_start.matches_exact(meta)
        } else if cursor_start.matches_exact(meta) {
            self.cursor_reached = true;
            cursor_start.inclusive()
        } else {
            false
        }
    }
}

impl Iterator for JournalIter {
    type Item = Result<EntryRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if let Some(limit) = self.query.limit
            && self.produced >= limit
        {
            self.done = true;
            return None;
        }

        loop {
            if self.done {
                return None;
            }
            if let Some(err) = self.initialization_errors.next() {
                return Some(Err(err));
            }
            // An iterator error is not exhaustion: retain its position and advance again on
            // the next caller request. Each failing read is surfaced once, without retrying
            // indefinitely inside next() or discarding later healthy entries.
            while self.initialized_sources < self.sources.len() {
                let index = self.initialized_sources;
                match self.sources[index].iter.next().transpose() {
                    Ok(meta) => self.candidates[index] = meta,
                    Err(error) => return Some(Err(error)),
                }
                self.initialized_sources += 1;
            }
            if let Some(index) = self.failed_source {
                match self.sources[index].iter.next().transpose() {
                    Ok(meta) => self.candidates[index] = meta,
                    Err(error) => return Some(Err(error)),
                }
                self.failed_source = None;
            }
            // Advance all physical copies together before selecting another entry. Conditional
            // cross-file ordering can otherwise separate identical candidates by unrelated
            // entries. Defer this work until the next request so a satisfied limit never reads
            // a successor. Track selection, not delivery: filters must not change merge order.
            if let Some(last) = self.last_selected {
                for (source, candidate) in self.sources.iter_mut().zip(&mut self.candidates) {
                    while candidate.is_some_and(|meta| same_entry(&last, &meta)) {
                        match source.iter.next().transpose() {
                            Ok(meta) => *candidate = meta,
                            Err(error) => return Some(Err(error)),
                        }
                    }
                }
                self.last_selected = None;
            }
            let (source_idx, meta) = match self.select_next() {
                Some(item) => item,
                None => {
                    self.done = true;
                    return None;
                }
            };

            self.last_selected = Some(meta);

            let cursor_was_reached = self.cursor_reached;
            if !self.passes_cursor(&meta) {
                if self.done {
                    return None;
                }
                continue;
            }
            if !time_matches(&meta, self.query.since_realtime, self.query.until_realtime) {
                continue;
            }

            let entry = match self.sources[source_idx]
                .file
                .read_entry_ref(meta.entry_offset)
            {
                Ok(e) => e,
                Err(e) => {
                    // A corrupt physical copy must not consume another readable copy of the
                    // same logical entry. Only advance the failed source, and let an inclusive
                    // checkpoint be retried when another copy remains at a candidate head.
                    self.last_selected = None;
                    self.candidates[source_idx] = None;
                    self.failed_source = Some(source_idx);
                    if self
                        .candidates
                        .iter()
                        .flatten()
                        .any(|candidate| same_entry(candidate, &meta))
                    {
                        self.cursor_reached = cursor_was_reached;
                        self.done = false;
                    }
                    return Some(Err(e));
                }
            };
            if !self.query.filter.matches(&entry) {
                continue;
            }

            self.produced = self.produced.saturating_add(1);
            return Some(Ok(entry));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_union_deduplicates_and_preserves_errors_in_both_directions() {
        for (reverse, sources, expected) in [
            (
                false,
                vec![
                    vec![Ok(1u64), Ok(3), Err(SdJournalError::NotFound)],
                    vec![Ok(1), Ok(2), Ok(4)],
                ],
                vec![Some(1), Some(2), Some(3), None, Some(4)],
            ),
            (
                true,
                vec![
                    vec![Ok(4u64), Ok(2), Err(SdJournalError::NotFound)],
                    vec![Ok(4), Ok(3), Ok(1)],
                ],
                vec![Some(4), Some(3), Some(2), None, Some(1)],
            ),
        ] {
            let iters = sources.into_iter().map(Vec::into_iter).collect();
            let mut union = SortedUnion::new(iters, reverse);
            for value in expected {
                match value {
                    Some(value) => assert_eq!(union.next().unwrap().unwrap(), value),
                    None => assert!(matches!(union.next(), Some(Err(SdJournalError::NotFound)))),
                }
            }
            assert!(union.next().is_none());
            assert!(union.next().is_none());
        }
    }

    #[test]
    fn sorted_union_reports_initial_errors_without_discarding_healthy_sources() {
        let mut union = SortedUnion::new(
            vec![
                vec![Err(SdJournalError::NotFound)].into_iter(),
                vec![Ok(1u64)].into_iter(),
            ],
            false,
        );
        assert!(matches!(union.next(), Some(Err(SdJournalError::NotFound))));
        assert_eq!(union.next().unwrap().unwrap(), 1);
        assert!(union.next().is_none());
    }

    #[test]
    fn sorted_union_resumes_the_same_source_after_initial_and_midstream_errors() {
        let mut union = SortedUnion::new(
            vec![
                vec![
                    Err(SdJournalError::NotFound),
                    Ok(1u64),
                    Err(SdJournalError::NotFound),
                    Ok(3),
                ]
                .into_iter(),
                vec![Ok(2)].into_iter(),
            ],
            false,
        );
        let actual: Vec<_> = union.by_ref().map(Result::ok).collect();
        assert_eq!(actual, vec![None, Some(1), None, Some(2), Some(3)]);
        assert!(union.next().is_none());
    }

    #[test]
    fn sorted_union_yields_repeated_errors_without_retrying_inside_next() {
        let calls = std::cell::Cell::new(0);
        let errors = std::iter::from_fn(|| {
            calls.set(calls.get() + 1);
            Some(Err::<u64, _>(SdJournalError::NotFound))
        });
        let mut union = SortedUnion::new(vec![errors], false);
        for expected in 1..=3 {
            assert!(matches!(union.next(), Some(Err(SdJournalError::NotFound))));
            assert_eq!(calls.get(), expected);
        }
    }

    fn meta(seqnum: u64, realtime_usec: u64, entry_offset: u64) -> EntryMeta {
        EntryMeta {
            file_id: [0x11; 16],
            entry_offset,
            seqnum_id: [0x22; 16],
            seqnum,
            boot_id: [0x33; 16],
            monotonic_usec: seqnum * 10,
            realtime_usec,
            xor_hash: seqnum,
        }
    }

    #[test]
    fn metadata_union_keeps_file_order_and_deduplicates_logical_entries() {
        let first = meta(1, 100, 8);
        let copy = EntryMeta {
            entry_offset: 16,
            ..first
        };
        let second = meta(2, 50, 24);
        let third = meta(3, 75, 32);
        for reverse in [false, true] {
            let mut sources = [vec![first, third], vec![copy, second]];
            if reverse {
                sources.iter_mut().for_each(|source| source.reverse());
            }
            let iters = sources
                .into_iter()
                .map(|source| source.into_iter().map(Ok))
                .collect();
            let actual: Vec<_> = SortedUnion::new(iters, reverse)
                .map(|item| item.unwrap().seqnum)
                .collect();
            assert_eq!(
                actual,
                if reverse {
                    vec![3, 2, 1]
                } else {
                    vec![1, 2, 3]
                }
            );
        }
    }

    #[test]
    fn heap_item_equality_agrees_with_its_ordering() {
        let left = UnionHeapItem {
            value: meta(1, 100, 8),
            iter_idx: 0,
        };
        let right = UnionHeapItem {
            value: meta(1, 200, 8),
            iter_idx: 0,
        };
        assert_eq!(left.cmp(&right), std::cmp::Ordering::Equal);
        assert!(left == right);
    }
}
