use super::index::DataObjectRef;
use super::{EntryMeta, JournalFile};
use crate::error::{LimitKind, Result, SdJournalError};
use crate::format::{OBJECT_ENTRY_ARRAY, ObjectHeader, STATE_ONLINE};
use crate::util::{read_u32_le, read_u64_le};
use std::collections::VecDeque;
use std::sync::Arc;

const ENTRY_ARRAY_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct EntryArrayDescriptor {
    offset: u64,
    layout: EntryArrayLayout,
    used_items: usize,
    first: u64,
    last: u64,
}

impl EntryArrayDescriptor {
    fn validate_layout(&self, file: &JournalFile) -> Result<()> {
        let current = read_entry_array_layout(file, self.offset)?;
        // Appending may extend an originally final link, but cannot change a committed
        // array's size or redirect an existing link to a different array.
        if current.size != self.layout.size
            || (self.layout.next_entry_array_offset != 0
                && current.next_entry_array_offset != self.layout.next_entry_array_offset)
            || (current.next_entry_array_offset != 0
                && current.next_entry_array_offset <= self.offset)
        {
            return Err(file.metadata_error(
                Some(self.offset),
                "ENTRY_ARRAY layout changed after snapshot validation",
            ));
        }
        Ok(())
    }
}

pub(super) struct EntryIndex {
    arrays: Vec<EntryArrayDescriptor>,
}

impl EntryIndex {
    fn contains(&self, file: &JournalFile, entry_offset: u64) -> Result<bool> {
        let array_index = self
            .arrays
            .partition_point(|array| array.last < entry_offset);
        let Some(array) = self.arrays.get(array_index) else {
            return Ok(false);
        };
        if entry_offset < array.first {
            return Ok(false);
        }
        let item_index = partition_entry_array(file, array, entry_offset, true)?;
        if item_index == array.used_items {
            return Ok(false);
        }
        Ok(read_entry_array_item(file, array.offset, &array.layout, item_index)? == entry_offset)
    }
}

struct EntryArrayCursor {
    file: JournalFile,
    reverse: bool,
    index: Arc<EntryIndex>,
    current_array_idx: usize,
    next_item_idx: usize,
    current_items: VecDeque<u64>,
    previous_offset: Option<u64>,
    first_refill_single: bool,
    exhausted: bool,
}

impl EntryArrayCursor {
    fn new(file: JournalFile, index: Arc<EntryIndex>, reverse: bool) -> Self {
        let arrays = &index.arrays;
        let current_array_idx = if reverse {
            arrays.len().saturating_sub(1)
        } else {
            0
        };
        let next_item_idx = if reverse {
            arrays.last().map_or(0, |array| array.used_items)
        } else {
            0
        };
        let exhausted = arrays.is_empty();
        Self {
            file,
            reverse,
            index,
            current_array_idx,
            next_item_idx,
            current_items: VecDeque::new(),
            previous_offset: None,
            first_refill_single: false,
            exhausted,
        }
    }

    fn new_after_offset(
        file: JournalFile,
        index: Arc<EntryIndex>,
        reverse: bool,
        after_entry_offset: u64,
    ) -> Result<Self> {
        let arrays = &index.arrays;
        let Some(array_idx) = locate_entry_array_for_offset(arrays, reverse, after_entry_offset)
        else {
            let mut cursor = Self::new(file, index, reverse);
            cursor.exhausted = true;
            return Ok(cursor);
        };

        let split = partition_entry_array(&file, &arrays[array_idx], after_entry_offset, reverse)?;
        let mut cursor = Self {
            file,
            reverse,
            index,
            current_array_idx: array_idx,
            next_item_idx: split,
            current_items: VecDeque::new(),
            previous_offset: Some(after_entry_offset),
            // Cursor resolution normally reads one offset and then drops this iterator. Read
            // just that item before switching to full chunks for callers that keep iterating.
            first_refill_single: true,
            exhausted: false,
        };
        cursor.skip_exhausted_array();
        Ok(cursor)
    }

    fn skip_exhausted_array(&mut self) {
        loop {
            let Some(array) = self.index.arrays.get(self.current_array_idx) else {
                self.exhausted = true;
                return;
            };

            let has_items = if self.reverse {
                self.next_item_idx != 0
            } else {
                self.next_item_idx < array.used_items
            };
            if has_items {
                return;
            }

            if self.reverse {
                let Some(previous) = self.current_array_idx.checked_sub(1) else {
                    self.exhausted = true;
                    return;
                };
                self.current_array_idx = previous;
                self.next_item_idx = self.index.arrays[previous].used_items;
            } else {
                self.current_array_idx += 1;
                self.next_item_idx = 0;
            }
        }
    }

    fn refill(&mut self) -> Result<()> {
        if !self.current_items.is_empty() || self.exhausted {
            return Ok(());
        }

        self.skip_exhausted_array();
        if self.exhausted {
            return Ok(());
        }

        let array = self.index.arrays[self.current_array_idx];
        // Reuse the validated snapshot prefix, but do not trust a cached header after the
        // underlying file changes.
        array.validate_layout(&self.file)?;
        let items_per_chunk = if self.first_refill_single {
            1
        } else {
            (ENTRY_ARRAY_CHUNK_BYTES / array.layout.item_size).max(1)
        };
        self.first_refill_single = false;

        let (start, end) = entry_array_chunk_range(
            self.reverse,
            self.next_item_idx,
            array.used_items,
            items_per_chunk,
        );
        let mut items = read_entry_array_item_range(&self.file, &array, start, end)?;
        self.next_item_idx = if self.reverse { start } else { end };
        if self.reverse {
            items.reverse();
        }

        for &offset in &items {
            if let Some(previous) = self.previous_offset {
                let valid = if self.reverse {
                    offset < previous
                } else {
                    offset > previous
                };
                if !valid {
                    return Err(self.file.metadata_error(
                        Some(array.offset),
                        format!(
                            "ENTRY_ARRAY entry offsets are not strictly increasing ({offset} vs {previous})"
                        ),
                    ));
                }
            }
            self.previous_offset = Some(offset);
        }

        self.current_items = items.into();
        Ok(())
    }
}

impl Iterator for EntryArrayCursor {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.refill() {
                self.exhausted = true;
                return Some(Err(error));
            }
            if let Some(offset) = self.current_items.pop_front() {
                return Some(Ok(offset));
            }
            self.skip_exhausted_array();
        }
    }
}

pub(crate) struct DataEntryOffsetIter {
    first_entry_offset: Option<u64>,
    arrays: EntryArrayCursor,
    entry_index: Arc<EntryIndex>,
    exhausted: bool,
}

impl DataEntryOffsetIter {
    pub(crate) fn new(file: JournalFile, data: DataObjectRef, reverse: bool) -> Result<Self> {
        validate_data_entry_metadata(&file, data)?;
        let entry_index = file.committed_entry_index()?;
        let descriptors = collect_entry_array_descriptors(
            &file,
            data.entry_array_offset,
            data.n_entries
                .saturating_sub(u64::from(data.entry_offset != 0)),
            true,
        )?;
        if let Some(first_array) = descriptors.first()
            && first_array.first <= data.entry_offset
        {
            return Err(file.metadata_error(
                Some(first_array.offset),
                format!(
                    "DATA entry offsets are not strictly increasing ({} <= {})",
                    first_array.first, data.entry_offset
                ),
            ));
        }
        Ok(Self {
            first_entry_offset: (data.entry_offset != 0).then_some(data.entry_offset),
            arrays: EntryArrayCursor::new(
                file,
                Arc::new(EntryIndex {
                    arrays: descriptors,
                }),
                reverse,
            ),
            entry_index,
            exhausted: false,
        })
    }

    fn checked_entry_offset(&mut self, offset: u64) -> Result<u64> {
        let result = self.entry_index.contains(&self.arrays.file, offset);
        match result {
            Ok(true) => Ok(offset),
            Ok(false) => {
                self.exhausted = true;
                Err(self.arrays.file.metadata_error(
                    Some(offset),
                    "DATA index references an ENTRY outside the committed file-level ENTRY_ARRAY",
                ))
            }
            Err(error) => {
                self.exhausted = true;
                Err(error)
            }
        }
    }
}

impl Iterator for DataEntryOffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        if !self.arrays.reverse
            && let Some(offset) = self.first_entry_offset.take()
        {
            self.arrays.previous_offset = Some(offset);
            return Some(self.checked_entry_offset(offset));
        }

        if let Some(offset) = self.arrays.next() {
            if offset.is_err() {
                self.exhausted = true;
            }
            return Some(offset.and_then(|offset| self.checked_entry_offset(offset)));
        }

        if self.arrays.reverse
            && let Some(offset) = self.first_entry_offset.take()
        {
            if let Some(previous) = self.arrays.previous_offset
                && offset >= previous
            {
                self.exhausted = true;
                return Some(Err(self.arrays.file.metadata_error(
                    Some(offset),
                    format!(
                        "DATA entry offsets are not strictly increasing ({offset} >= {previous})"
                    ),
                )));
            }
            return Some(self.checked_entry_offset(offset));
        }

        self.exhausted = true;
        None
    }
}

pub(crate) struct FileEntryIter {
    arrays: EntryArrayCursor,
}

impl FileEntryIter {
    pub(crate) fn new(file: JournalFile, reverse: bool) -> Result<Self> {
        let index = file.committed_entry_index()?;
        Ok(Self {
            arrays: EntryArrayCursor::new(file, index, reverse),
        })
    }

    pub(crate) fn new_after_offset(
        file: JournalFile,
        reverse: bool,
        after_entry_offset: u64,
    ) -> Result<Self> {
        let index = file.committed_entry_index()?;
        Ok(Self {
            arrays: EntryArrayCursor::new_after_offset(file, index, reverse, after_entry_offset)?,
        })
    }
}

impl JournalFile {
    fn committed_entry_index(&self) -> Result<Arc<EntryIndex>> {
        if let Some(index) = self.inner.entry_index.get() {
            return Ok(Arc::clone(index));
        }
        self.validate_entry_index_metadata()?;
        // Keep only per-array descriptors, not every entry offset. This validation is shared
        // across DATA postings and queries of the same snapshot; failed reads are not cached.
        let arrays = collect_entry_array_descriptors(
            self,
            self.inner.header.entry_array_offset,
            self.inner.header.n_entries,
            false,
        )?;
        let index = Arc::new(EntryIndex { arrays });
        let _ = self.inner.entry_index.set(Arc::clone(&index));
        Ok(self
            .inner
            .entry_index
            .get()
            .map(Arc::clone)
            .unwrap_or(index))
    }

    pub(crate) fn read_entry_array_items(&self, offset: u64) -> Result<Vec<u64>> {
        let layout = read_entry_array_layout(self, offset)?;
        let obj = self.read_object(offset, layout.size)?;
        let obj = obj.as_slice();

        let items_bytes = obj.get(24..).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 24),
            reason: "missing ENTRY_ARRAY items".to_string(),
        })?;

        let mut items = Vec::with_capacity(layout.item_count);
        for item in items_bytes.chunks_exact(layout.item_size) {
            let offset = if layout.item_size == 4 {
                read_u32_le(item, 0).map(u64::from)
            } else {
                read_u64_le(item, 0)
            }
            .ok_or_else(|| SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: "ENTRY_ARRAY item truncated".to_string(),
            })?;
            items.push(offset);
        }

        while matches!(items.last(), Some(0)) {
            items.pop();
        }
        if items.is_empty() {
            return Err(self.metadata_error(Some(offset), "ENTRY_ARRAY contains no entry offsets"));
        }
        if items.contains(&0) {
            return Err(self.metadata_error(
                Some(offset),
                "ENTRY_ARRAY contains a zero offset before its unused tail",
            ));
        }
        if items.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(self.metadata_error(
                Some(offset),
                "ENTRY_ARRAY entry offsets are not strictly increasing",
            ));
        }

        Ok(items)
    }

    pub(crate) fn read_entry_array_next_offset(&self, offset: u64) -> Result<u64> {
        Ok(read_entry_array_layout(self, offset)?.next_entry_array_offset)
    }
}

impl Iterator for FileEntryIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrays
            .next()
            .map(|offset| offset.and_then(|offset| self.arrays.file.read_entry_meta(offset)))
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryArrayLayout {
    size: u64,
    next_entry_array_offset: u64,
    item_size: usize,
    item_count: usize,
}

fn validate_data_entry_metadata(file: &JournalFile, data: DataObjectRef) -> Result<()> {
    let valid = match data.n_entries {
        0 => data.entry_offset == 0 && data.entry_array_offset == 0,
        1 => data.entry_offset != 0 && data.entry_array_offset == 0,
        _ => data.entry_offset != 0 && data.entry_array_offset != 0,
    };
    if valid {
        return Ok(());
    }

    Err(file.metadata_error(
        Some(data.entry_offset),
        format!(
            "inconsistent DATA entry metadata (n_entries={}, entry_offset={}, entry_array_offset={})",
            data.n_entries, data.entry_offset, data.entry_array_offset
        ),
    ))
}

fn collect_entry_array_descriptors(
    file: &JournalFile,
    start: u64,
    expected_items: u64,
    allow_online_snapshot_truncate: bool,
) -> Result<Vec<EntryArrayDescriptor>> {
    let mut arrays = Vec::new();
    let mut current = start;
    let mut previous_last = None;
    let mut remaining = expected_items;

    while current != 0 {
        if arrays.len() >= file.inner.config.max_object_chain_steps {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectChainSteps,
                limit: u64::try_from(file.inner.config.max_object_chain_steps).unwrap_or(u64::MAX),
            });
        }

        let layout = read_entry_array_layout(file, current)?;
        let item_count = u64::try_from(layout.item_count).unwrap_or(u64::MAX);
        let nominal_used_u64 = remaining.min(item_count);
        let nominal_used =
            usize::try_from(nominal_used_u64).map_err(|_| SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: file.inner.config.max_object_size_bytes,
            })?;
        if nominal_used == 0 {
            if file.inner.header.state == STATE_ONLINE && allow_online_snapshot_truncate {
                break;
            }
            return Err(file.metadata_error(
                Some(current),
                "ENTRY_ARRAY chain contains more objects than entry metadata requires",
            ));
        }
        let (used_items, first, last, reached_snapshot_tail) = validate_entry_array_prefix(
            file,
            current,
            layout,
            nominal_used,
            previous_last,
            allow_online_snapshot_truncate,
        )?;
        if used_items != 0 {
            previous_last = Some(last);
            arrays.push(EntryArrayDescriptor {
                offset: current,
                layout,
                used_items,
                first,
                last,
            });
            remaining = remaining.saturating_sub(u64::try_from(used_items).unwrap_or(u64::MAX));
        }
        if reached_snapshot_tail {
            remaining = 0;
            break;
        }

        let next = layout.next_entry_array_offset;
        // Strictly increasing links also exclude cycles, without a separate visited set.
        if next != 0 && next <= current {
            return Err(file.metadata_error(
                Some(current),
                format!("ENTRY_ARRAY next offset does not move forward ({next} <= {current})"),
            ));
        }
        if file.inner.header.state != STATE_ONLINE && used_items < layout.item_count {
            let first_unused = read_entry_array_item(file, current, &layout, used_items)?;
            if first_unused != 0 {
                return Err(file.metadata_error(
                    Some(current),
                    "ENTRY_ARRAY unused tail starts with a non-zero offset",
                ));
            }
            if next != 0 {
                return Err(file.metadata_error(
                    Some(current),
                    "partially used ENTRY_ARRAY unexpectedly links to another object",
                ));
            }
        }
        if remaining == 0 {
            if file.inner.header.state != STATE_ONLINE && next != 0 {
                return Err(file.metadata_error(
                    Some(current),
                    "ENTRY_ARRAY chain contains more objects than entry metadata requires",
                ));
            }
            break;
        }
        current = next;
    }

    if remaining != 0 {
        return Err(file.metadata_error(
            Some(start),
            format!(
                "ENTRY_ARRAY chain contains fewer offsets than entry metadata requires ({remaining} missing)"
            ),
        ));
    }
    if expected_items == 0 && !arrays.is_empty() {
        return Err(file.metadata_error(
            Some(start),
            "ENTRY_ARRAY chain is present for zero expected entries",
        ));
    }

    Ok(arrays)
}

fn validate_entry_array_prefix(
    file: &JournalFile,
    offset: u64,
    layout: EntryArrayLayout,
    nominal_used: usize,
    previous_last: Option<u64>,
    allow_online_snapshot_truncate: bool,
) -> Result<(usize, u64, u64, bool)> {
    let provisional = EntryArrayDescriptor {
        offset,
        layout,
        used_items: nominal_used,
        first: 0,
        last: 0,
    };
    let items_per_chunk = (ENTRY_ARRAY_CHUNK_BYTES / layout.item_size).max(1);
    let mut validated = 0usize;
    let mut first = None;
    let mut last = previous_last;

    while validated < nominal_used {
        let end = validated.saturating_add(items_per_chunk).min(nominal_used);
        let items = read_entry_array_item_range(file, &provisional, validated, end)?;
        for value in items {
            if value > file.inner.header.tail_object_offset {
                if file.inner.header.state == STATE_ONLINE && allow_online_snapshot_truncate {
                    return Ok((validated, first.unwrap_or(0), last.unwrap_or(0), true));
                }
                return Err(file.metadata_error(
                    Some(offset),
                    format!(
                        "ENTRY_ARRAY references an object beyond tail_object_offset ({value} > {})",
                        file.inner.header.tail_object_offset
                    ),
                ));
            }
            if value < file.inner.header.header_size || !value.is_multiple_of(8) {
                return Err(file.metadata_error(
                    Some(offset),
                    format!("ENTRY_ARRAY contains an invalid object offset: {value}"),
                ));
            }
            if let Some(previous) = last
                && value <= previous
            {
                return Err(file.metadata_error(
                    Some(offset),
                    format!(
                        "ENTRY_ARRAY entry offsets are not strictly increasing ({value} <= {previous})"
                    ),
                ));
            }
            first.get_or_insert(value);
            last = Some(value);
            validated = validated.saturating_add(1);
        }
    }

    Ok((validated, first.unwrap_or(0), last.unwrap_or(0), false))
}

fn locate_entry_array_for_offset(
    arrays: &[EntryArrayDescriptor],
    reverse: bool,
    after: u64,
) -> Option<usize> {
    if arrays.is_empty() {
        return None;
    }

    let mut left = 0usize;
    let mut right = arrays.len();
    while left < right {
        let mid = left + (right - left) / 2;
        let last = arrays[mid].last;
        let before_target = if reverse { last < after } else { last <= after };
        if before_target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if reverse {
        Some(left.min(arrays.len().saturating_sub(1)))
    } else {
        (left < arrays.len()).then_some(left)
    }
}

fn partition_entry_array(
    file: &JournalFile,
    array: &EntryArrayDescriptor,
    after: u64,
    reverse: bool,
) -> Result<usize> {
    array.validate_layout(file)?;
    let split = partition_point_result(array.used_items, |index| {
        let offset = read_entry_array_item(file, array.offset, &array.layout, index)?;
        if offset == 0 {
            return Err(file.metadata_error(
                Some(array.offset),
                "ENTRY_ARRAY contains a zero offset before its unused tail",
            ));
        }
        Ok(if reverse {
            offset < after
        } else {
            offset <= after
        })
    })?;

    let before = if split == 0 {
        None
    } else {
        Some(read_entry_array_item(
            file,
            array.offset,
            &array.layout,
            split - 1,
        )?)
    };
    let after_split = if split == array.used_items {
        None
    } else {
        Some(read_entry_array_item(
            file,
            array.offset,
            &array.layout,
            split,
        )?)
    };
    if before == Some(0) || after_split == Some(0) {
        return Err(file.metadata_error(
            Some(array.offset),
            "ENTRY_ARRAY contains a zero offset before its unused tail",
        ));
    }
    if let (Some(left), Some(right)) = (before, after_split)
        && left >= right
    {
        return Err(file.metadata_error(
            Some(array.offset),
            format!("ENTRY_ARRAY entry offsets are not strictly increasing ({left} >= {right})"),
        ));
    }

    Ok(split)
}

fn partition_point_result<E>(
    len: usize,
    mut predicate: impl FnMut(usize) -> std::result::Result<bool, E>,
) -> std::result::Result<usize, E> {
    let mut left = 0usize;
    let mut right = len;
    while left < right {
        let mid = left + (right - left) / 2;
        if predicate(mid)? {
            left = mid.saturating_add(1);
        } else {
            right = mid;
        }
    }
    Ok(left)
}

fn read_entry_array_layout(file: &JournalFile, offset: u64) -> Result<EntryArrayLayout> {
    file.validate_object_offset(offset)?;

    let buf = file.read_bytes(offset, 24)?;
    let oh = ObjectHeader::parse(buf.as_slice(), file.path(), offset)?;
    if oh.object_type != OBJECT_ENTRY_ARRAY {
        return Err(SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("expected ENTRY_ARRAY object, found type {}", oh.object_type),
        });
    }
    let item_size = if file.inner.header.is_compact() {
        4usize
    } else {
        8usize
    };
    let items_size = oh
        .size
        .checked_sub(24)
        .ok_or_else(|| SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("ENTRY_ARRAY object too small: {}", oh.size),
        })?;
    if items_size == 0 || !items_size.is_multiple_of(item_size as u64) {
        return Err(SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("ENTRY_ARRAY object has invalid item area size: {items_size}"),
        });
    }
    if oh.size > file.inner.config.max_object_size_bytes {
        return Err(SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: file.inner.config.max_object_size_bytes,
        });
    }
    let end = offset
        .checked_add(oh.size)
        .ok_or_else(|| file.metadata_error(Some(offset), "ENTRY_ARRAY object range overflow"))?;
    if end > file.inner.used_size {
        return Err(file.metadata_error(
            Some(offset),
            format!(
                "ENTRY_ARRAY extends beyond used journal data (end={end}, used_size={})",
                file.inner.used_size
            ),
        ));
    }

    let next_entry_array_offset =
        read_u64_le(buf.as_slice(), 16).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset + 16),
            reason: "missing next_entry_array_offset".to_string(),
        })?;

    let item_count = usize::try_from(items_size / item_size as u64).map_err(|_| {
        SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: file.inner.config.max_object_size_bytes,
        }
    })?;

    Ok(EntryArrayLayout {
        size: oh.size,
        next_entry_array_offset,
        item_size,
        item_count,
    })
}

fn read_entry_array_item(
    file: &JournalFile,
    array_offset: u64,
    layout: &EntryArrayLayout,
    index: usize,
) -> Result<u64> {
    if index >= layout.item_count {
        return Err(file.metadata_error(
            Some(array_offset),
            format!(
                "ENTRY_ARRAY item index out of bounds ({index} >= {})",
                layout.item_count
            ),
        ));
    }
    let relative = 24u64
        .checked_add(
            u64::try_from(index)
                .unwrap_or(u64::MAX)
                .saturating_mul(layout.item_size as u64),
        )
        .ok_or_else(|| file.metadata_error(Some(array_offset), "ENTRY_ARRAY item overflow"))?;
    let item_offset = array_offset
        .checked_add(relative)
        .ok_or_else(|| file.metadata_error(Some(array_offset), "ENTRY_ARRAY item overflow"))?;
    let bytes = file.read_bytes(item_offset, layout.item_size)?;
    if layout.item_size == 4 {
        read_u32_le(bytes.as_slice(), 0)
            .map(u64::from)
            .ok_or_else(|| file.metadata_error(Some(item_offset), "ENTRY_ARRAY item truncated"))
    } else {
        read_u64_le(bytes.as_slice(), 0)
            .ok_or_else(|| file.metadata_error(Some(item_offset), "ENTRY_ARRAY item truncated"))
    }
}

fn read_entry_array_item_range(
    file: &JournalFile,
    array: &EntryArrayDescriptor,
    start: usize,
    end: usize,
) -> Result<Vec<u64>> {
    if start > end || end > array.used_items {
        return Err(file.metadata_error(
            Some(array.offset),
            format!(
                "ENTRY_ARRAY item range is out of bounds ({start}..{end}, used={})",
                array.used_items
            ),
        ));
    }
    if start == end {
        return Ok(Vec::new());
    }

    let count = end - start;
    let byte_len = count
        .checked_mul(array.layout.item_size)
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    if byte_len > ENTRY_ARRAY_CHUNK_BYTES {
        return Err(file.metadata_error(
            Some(array.offset),
            format!("ENTRY_ARRAY chunk exceeds {ENTRY_ARRAY_CHUNK_BYTES} bytes"),
        ));
    }
    let relative = 24u64
        .checked_add(
            u64::try_from(start)
                .unwrap_or(u64::MAX)
                .saturating_mul(array.layout.item_size as u64),
        )
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    let chunk_offset = array
        .offset
        .checked_add(relative)
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    let bytes = file.read_bytes(chunk_offset, byte_len)?;

    let mut items = Vec::with_capacity(count);
    for index in 0..count {
        let byte_index = index * array.layout.item_size;
        let value = if array.layout.item_size == 4 {
            read_u32_le(bytes.as_slice(), byte_index).map(u64::from)
        } else {
            read_u64_le(bytes.as_slice(), byte_index)
        }
        .ok_or_else(|| {
            file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                "ENTRY_ARRAY item truncated",
            )
        })?;
        if value == 0 {
            return Err(file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                "ENTRY_ARRAY contains a zero offset before its unused tail",
            ));
        }
        if let Some(previous) = items.last()
            && value <= *previous
        {
            return Err(file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                format!(
                    "ENTRY_ARRAY entry offsets are not strictly increasing ({value} <= {previous})"
                ),
            ));
        }
        items.push(value);
    }

    Ok(items)
}

fn entry_array_chunk_range(
    reverse: bool,
    next_item_idx: usize,
    used_items: usize,
    max_items: usize,
) -> (usize, usize) {
    if reverse {
        let end = next_item_idx.min(used_items);
        (end.saturating_sub(max_items), end)
    } else {
        let start = next_item_idx.min(used_items);
        (start, start.saturating_add(max_items).min(used_items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{JournalConfig, MmapPolicy};
    use std::cell::Cell;

    fn range_fixture(n_entries: u64, tail_hint_seqnum: u64) -> (tempfile::TempDir, JournalFile) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("range.journal");
        let mut bytes = vec![0u8; 472];
        bytes[..8].copy_from_slice(b"LPKSHHRH");
        bytes[16] = STATE_ONLINE;
        let put = |bytes: &mut [u8], offset: usize, value: u64| {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        };
        put(&mut bytes, 88, 272);
        put(&mut bytes, 96, 200);
        put(&mut bytes, 136, 392);
        put(&mut bytes, 152, n_entries);
        put(&mut bytes, 160, tail_hint_seqnum);
        put(&mut bytes, 176, 272);
        put(&mut bytes, 264, 312 + (tail_hint_seqnum - 1) * 80);
        bytes[272] = OBJECT_ENTRY_ARRAY;
        put(&mut bytes, 280, 40);
        put(&mut bytes, 296, 312);
        put(&mut bytes, 304, 392);
        for (offset, seqnum) in [(312, 1), (392, 2)] {
            bytes[offset] = crate::format::OBJECT_ENTRY;
            put(&mut bytes, offset + 8, 80);
            put(&mut bytes, offset + 16, seqnum);
        }
        std::fs::write(&path, bytes).unwrap();
        let file = JournalFile::open(
            path,
            &JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        (root, file)
    }

    #[test]
    fn entry_iterator_ignores_slots_beyond_snapshotted_n_entries() {
        let (_root, file) = range_fixture(1, 2);
        for reverse in [false, true] {
            let entries = file
                .entry_iter(reverse)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].seqnum, 1);
        }
    }

    #[test]
    fn data_postings_emit_inline_entries_once_in_both_directions() {
        let (_root, original) = range_fixture(2, 2);
        let mut bytes = std::fs::read(original.path()).unwrap();
        bytes.resize(504, 0);
        bytes[96..104].copy_from_slice(&232u64.to_le_bytes());
        bytes[136..144].copy_from_slice(&472u64.to_le_bytes());
        bytes[472] = OBJECT_ENTRY_ARRAY;
        bytes[480..488].copy_from_slice(&32u64.to_le_bytes());
        bytes[496..504].copy_from_slice(&392u64.to_le_bytes());
        std::fs::write(original.path(), bytes).unwrap();
        let file =
            JournalFile::open(original.path().to_path_buf(), &original.inner.config).unwrap();

        for reverse in [false, true] {
            for count in 0..=2 {
                let data = DataObjectRef {
                    n_entries: count,
                    entry_offset: if count == 0 { 0 } else { 312 },
                    entry_array_offset: if count == 2 { 472 } else { 0 },
                };
                let mut iter = file.data_entry_offsets(data, reverse).unwrap();
                let mut expected = match count {
                    0 => vec![],
                    1 => vec![312],
                    _ => vec![312, 392],
                };
                if reverse {
                    expected.reverse();
                }
                for offset in expected {
                    assert_eq!(iter.next().unwrap().unwrap(), offset);
                }
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            let mut invalid = file
                .data_entry_offsets(
                    DataObjectRef {
                        n_entries: 1,
                        entry_offset: 320, // Aligned, but not a committed ENTRY offset.
                        entry_array_offset: 0,
                    },
                    reverse,
                )
                .unwrap();
            assert!(matches!(
                invalid.next(),
                Some(Err(SdJournalError::Transient { .. }))
            ));
            assert!(invalid.next().is_none());
            assert!(invalid.next().is_none());
        }
    }

    #[test]
    fn file_entry_iterator_preserves_entry_errors_and_stops_after_array_errors() {
        for damaged_offset in [312, 392] {
            let (_root, file) = range_fixture(2, 2);
            let mut bytes = std::fs::read(file.path()).unwrap();
            bytes[damaged_offset] = 0;
            std::fs::write(file.path(), &bytes).unwrap();
            for reverse in [false, true] {
                let mut iter = file.entry_iter(reverse).unwrap();
                let error_first = reverse == (damaged_offset == 392);
                let healthy_seqnum = if damaged_offset == 312 { 2 } else { 1 };
                if !error_first {
                    assert_eq!(iter.next().unwrap().unwrap().seqnum, healthy_seqnum);
                }
                assert!(matches!(
                    iter.next(),
                    Some(Err(SdJournalError::Corrupt { .. }))
                ));
                if error_first {
                    assert_eq!(iter.next().unwrap().unwrap().seqnum, healthy_seqnum);
                }
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }
        }

        let (_root, file) = range_fixture(2, 2);
        let mut bytes = std::fs::read(file.path()).unwrap();
        let mut iter = file.entry_iter(false).unwrap();
        bytes[304..312].fill(0); // Damage the array after its constructor validated the prefix.
        std::fs::write(file.path(), bytes).unwrap();
        assert!(matches!(
            iter.next(),
            Some(Err(SdJournalError::Transient { .. }))
        ));
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn cached_entry_arrays_still_validate_their_headers_when_read() {
        for corruption in ["type", "size", "backwards-link"] {
            let (_root, file) = range_fixture(2, 2);
            assert_eq!(file.entry_iter(false).unwrap().count(), 2);
            let mut bytes = std::fs::read(file.path()).unwrap();
            match corruption {
                "type" => bytes[272] = crate::format::OBJECT_DATA,
                "size" => bytes[280..288].copy_from_slice(&32u64.to_le_bytes()),
                "backwards-link" => bytes[288..296].copy_from_slice(&272u64.to_le_bytes()),
                _ => unreachable!(),
            }
            std::fs::write(file.path(), bytes).unwrap();

            for reverse in [false, true] {
                let mut iter = file.entry_iter(reverse).unwrap();
                assert!(matches!(iter.next(), Some(Err(_))), "accepted {corruption}");
                assert!(iter.next().is_none());
            }
        }
    }

    #[test]
    fn data_postings_validate_the_cached_committed_array_header() {
        for corruption in ["type", "size", "backwards-link"] {
            let (_root, file) = range_fixture(2, 2);
            let data = DataObjectRef {
                n_entries: 1,
                entry_offset: 312,
                entry_array_offset: 0,
            };
            assert_eq!(
                file.data_entry_offsets(data, false)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap(),
                312
            );
            let mut bytes = std::fs::read(file.path()).unwrap();
            match corruption {
                "type" => bytes[272] = crate::format::OBJECT_DATA,
                "size" => bytes[280..288].copy_from_slice(&32u64.to_le_bytes()),
                "backwards-link" => bytes[288..296].copy_from_slice(&272u64.to_le_bytes()),
                _ => unreachable!(),
            }
            std::fs::write(file.path(), bytes).unwrap();
            for reverse in [false, true] {
                let mut iter = file.data_entry_offsets(data, reverse).unwrap();
                assert!(matches!(iter.next(), Some(Err(_))), "accepted {corruption}");
                assert!(iter.next().is_none());
            }
        }
    }

    #[test]
    fn cached_entry_arrays_keep_the_snapshot_when_a_writer_links_another_array() {
        let (_root, file) = range_fixture(2, 2);
        assert_eq!(file.entry_iter(false).unwrap().count(), 2);
        let mut bytes = std::fs::read(file.path()).unwrap();
        bytes.resize(584, 0);
        for (offset, value) in [
            (96, 312u64), // Expanded arena.
            (136, 504),   // New tail ENTRY.
            (152, 3),     // New committed entry count.
            (288, 472),   // Link the previously final ENTRY_ARRAY to a new array.
            (480, 32),    // New ENTRY_ARRAY size.
            (496, 504),   // New ENTRY_ARRAY's entry offset.
            (512, 80),    // New ENTRY size.
            (520, 3),     // New ENTRY sequence number.
        ] {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        bytes[472] = OBJECT_ENTRY_ARRAY;
        bytes[504] = crate::format::OBJECT_ENTRY;
        std::fs::write(file.path(), bytes).unwrap();

        let newer = JournalFile::open(file.path().to_path_buf(), &file.inner.config).unwrap();
        for reverse in [false, true] {
            let old_entries = file
                .entry_iter(reverse)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let new_entries = newer
                .entry_iter(reverse)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(old_entries.len(), 2);
            assert_eq!(new_entries.len(), 3);
            assert!(old_entries.iter().all(|entry| entry.seqnum <= 2));
        }
    }

    #[test]
    fn entry_array_links_reject_cycles_and_backwards_offsets_without_a_visited_set() {
        for next in [272u64, 264] {
            let (_root, file) = range_fixture(3, 2);
            let mut bytes = std::fs::read(file.path()).unwrap();
            bytes[288..296].copy_from_slice(&next.to_le_bytes());
            std::fs::write(file.path(), bytes).unwrap();
            let error = file
                .entry_iter(false)
                .err()
                .expect("reject invalid array chain");
            assert!(matches!(
                error,
                SdJournalError::Transient { reason, .. }
                    if reason.contains("next offset does not move forward")
            ));
        }
    }

    #[test]
    fn entry_iterator_does_not_trust_tail_hints_from_the_previous_append() {
        let (_root, file) = range_fixture(2, 1);
        for reverse in [false, true] {
            let entries = file
                .entry_iter(reverse)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let seqnums: Vec<_> = entries.iter().map(|entry| entry.seqnum).collect();
            assert_eq!(seqnums, if reverse { vec![2, 1] } else { vec![1, 2] });
        }
    }

    #[test]
    fn entry_iterator_rejects_a_chain_shorter_than_n_entries() {
        let (_root, file) = range_fixture(3, 2);
        for reverse in [false, true] {
            assert!(matches!(
                file.entry_iter(reverse),
                Err(SdJournalError::Transient { .. })
            ));
        }
    }

    #[test]
    fn entry_iterator_is_empty_when_no_entries_are_committed() {
        let (_root, original) = range_fixture(0, 1);
        let mut bytes = std::fs::read(original.path()).unwrap();
        bytes[176..184].fill(0);
        std::fs::write(original.path(), bytes).unwrap();
        let file =
            JournalFile::open(original.path().to_path_buf(), &original.inner.config).unwrap();
        for reverse in [false, true] {
            let mut iter = file.entry_iter(reverse).unwrap();
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn after_offset_partition_is_logarithmic_for_a_large_array() {
        let len = 1usize << 20;
        let boundary = len / 2;
        let calls = Cell::new(0usize);

        let split = partition_point_result(len, |index| {
            calls.set(calls.get().saturating_add(1));
            Ok::<bool, ()>(index < boundary)
        })
        .unwrap();

        assert_eq!(split, boundary);
        assert!(
            calls.get() <= 21,
            "binary positioning read {} items for {len} offsets",
            calls.get()
        );
    }

    #[test]
    fn entry_array_refills_are_bounded_and_after_offset_starts_with_one_item() {
        let used_items = 1usize << 20;
        let regular_chunk_items = ENTRY_ARRAY_CHUNK_BYTES / 8;

        let (start, end) =
            entry_array_chunk_range(false, used_items / 2, used_items, regular_chunk_items);
        assert_eq!(end - start, regular_chunk_items);

        let (start, end) =
            entry_array_chunk_range(true, used_items / 2, used_items, regular_chunk_items);
        assert_eq!(end - start, regular_chunk_items);

        let (start, end) = entry_array_chunk_range(false, used_items / 2, used_items, 1);
        assert_eq!(end - start, 1);

        let (start, end) = entry_array_chunk_range(true, used_items / 2, used_items, 1);
        assert_eq!(end - start, 1);
    }
}
