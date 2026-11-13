use crate::error::{CompressionAlgo, LimitKind, Result, SdJournalError};
use crate::util::{read_id128, read_u8, read_u32_le, read_u64_le};
use std::path::Path;

pub(crate) type FileOffset = u64;

pub(crate) const HEADER_SIGNATURE: &[u8; 8] = b"LPKSHHRH";
pub(crate) const HEADER_SIZE_MIN: u64 = 208;

pub(crate) const HEADER_INCOMPATIBLE_COMPRESSED_XZ: u32 = 1 << 0;
pub(crate) const HEADER_INCOMPATIBLE_COMPRESSED_LZ4: u32 = 1 << 1;
pub(crate) const HEADER_INCOMPATIBLE_KEYED_HASH: u32 = 1 << 2;
pub(crate) const HEADER_INCOMPATIBLE_COMPRESSED_ZSTD: u32 = 1 << 3;
pub(crate) const HEADER_INCOMPATIBLE_COMPACT: u32 = 1 << 4;

#[cfg(any(feature = "tracing", feature = "verify-seal"))]
pub(crate) const HEADER_COMPATIBLE_SEALED: u32 = 1 << 0;
#[cfg(any(feature = "tracing", feature = "verify-seal"))]
pub(crate) const HEADER_COMPATIBLE_SEALED_CONTINUOUS: u32 = 1 << 2;

const KNOWN_INCOMPATIBLE_FLAGS: u32 = HEADER_INCOMPATIBLE_COMPRESSED_XZ
    | HEADER_INCOMPATIBLE_COMPRESSED_LZ4
    | HEADER_INCOMPATIBLE_KEYED_HASH
    | HEADER_INCOMPATIBLE_COMPRESSED_ZSTD
    | HEADER_INCOMPATIBLE_COMPACT;

#[cfg(feature = "mmap")]
pub(crate) const STATE_OFFLINE: u8 = 0;
pub(crate) const STATE_ONLINE: u8 = 1;
pub(crate) const STATE_ARCHIVED: u8 = 2;

pub(crate) const OBJECT_DATA: u8 = 1;
pub(crate) const OBJECT_FIELD: u8 = 2;
pub(crate) const OBJECT_ENTRY: u8 = 3;
pub(crate) const OBJECT_DATA_HASH_TABLE: u8 = 4;
pub(crate) const OBJECT_FIELD_HASH_TABLE: u8 = 5;
pub(crate) const OBJECT_ENTRY_ARRAY: u8 = 6;
pub(crate) const OBJECT_TAG: u8 = 7;

pub(crate) const OBJECT_COMPRESSED_XZ: u8 = 1 << 0;
pub(crate) const OBJECT_COMPRESSED_LZ4: u8 = 1 << 1;
pub(crate) const OBJECT_COMPRESSED_ZSTD: u8 = 1 << 2;

#[derive(Debug, Clone)]
// Store only metadata used by the reader. Mutable tail hints are intentionally ignored;
// committed entry bounds come from n_entries and the file-level ENTRY_ARRAY instead.
pub(crate) struct Header {
    #[cfg(any(feature = "tracing", feature = "verify-seal"))]
    pub(crate) compatible_flags: u32,
    pub(crate) incompatible_flags: u32,
    pub(crate) state: u8,
    pub(crate) file_id: [u8; 16],
    pub(crate) seqnum_id: [u8; 16],
    pub(crate) header_size: u64,
    pub(crate) arena_size: u64,
    pub(crate) data_hash_table_offset: u64,
    pub(crate) data_hash_table_size: u64,
    pub(crate) tail_object_offset: u64,
    pub(crate) n_entries: u64,
    pub(crate) entry_array_offset: u64,
}

impl Header {
    pub(crate) fn parse(buf: &[u8], path: &Path) -> Result<Self> {
        if buf.len() < HEADER_SIZE_MIN as usize {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(0),
                reason: "file too small for journal header".to_string(),
            });
        }

        let sig = buf.get(0..8).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(0),
            reason: "missing signature".to_string(),
        })?;
        if sig != HEADER_SIGNATURE {
            return Err(SdJournalError::Unsupported {
                reason: "not a systemd journal file (bad signature)".to_string(),
            });
        }

        #[cfg(any(feature = "tracing", feature = "verify-seal"))]
        let compatible_flags = read_u32_le(buf, 8).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(8),
            reason: "missing compatible_flags".to_string(),
        })?;
        let incompatible_flags = read_u32_le(buf, 12).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(12),
            reason: "missing incompatible_flags".to_string(),
        })?;
        let state = read_u8(buf, 16).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(16),
            reason: "missing state".to_string(),
        })?;

        if incompatible_flags & !KNOWN_INCOMPATIBLE_FLAGS != 0 {
            return Err(SdJournalError::Unsupported {
                reason: format!("unknown incompatible flags: 0x{incompatible_flags:08x}"),
            });
        }

        let file_id = read_id128(buf, 24).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(24),
            reason: "missing file_id".to_string(),
        })?;
        let seqnum_id = read_id128(buf, 72).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(72),
            reason: "missing seqnum_id".to_string(),
        })?;

        let header_size = read_u64_le(buf, 88).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(88),
            reason: "missing header_size".to_string(),
        })?;
        if header_size < HEADER_SIZE_MIN || !header_size.is_multiple_of(8) {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(88),
                reason: format!("invalid header_size: {header_size}"),
            });
        }
        let arena_size = read_u64_le(buf, 96).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(96),
            reason: "missing arena_size".to_string(),
        })?;
        let data_hash_table_offset =
            read_u64_le(buf, 104).ok_or_else(|| SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(104),
                reason: "missing data_hash_table_offset".to_string(),
            })?;
        let data_hash_table_size =
            read_u64_le(buf, 112).ok_or_else(|| SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(112),
                reason: "missing data_hash_table_size".to_string(),
            })?;
        let tail_object_offset = read_u64_le(buf, 136).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(136),
            reason: "missing tail_object_offset".to_string(),
        })?;
        let n_entries = read_u64_le(buf, 152).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(152),
            reason: "missing n_entries".to_string(),
        })?;
        let entry_array_offset = read_u64_le(buf, 176).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(176),
            reason: "missing entry_array_offset".to_string(),
        })?;
        Ok(Self {
            #[cfg(any(feature = "tracing", feature = "verify-seal"))]
            compatible_flags,
            incompatible_flags,
            state,
            file_id,
            seqnum_id,
            header_size,
            arena_size,
            data_hash_table_offset,
            data_hash_table_size,
            tail_object_offset,
            n_entries,
            entry_array_offset,
        })
    }

    pub(crate) fn is_compact(&self) -> bool {
        self.incompatible_flags & HEADER_INCOMPATIBLE_COMPACT != 0
    }

    pub(crate) fn is_keyed_hash(&self) -> bool {
        self.incompatible_flags & HEADER_INCOMPATIBLE_KEYED_HASH != 0
    }

    #[cfg(any(feature = "tracing", feature = "verify-seal"))]
    pub(crate) fn is_sealed(&self) -> bool {
        self.compatible_flags & HEADER_COMPATIBLE_SEALED != 0
    }

    #[cfg(any(feature = "tracing", feature = "verify-seal"))]
    pub(crate) fn is_sealed_continuous(&self) -> bool {
        self.compatible_flags & HEADER_COMPATIBLE_SEALED_CONTINUOUS != 0
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectHeader {
    pub(crate) object_type: u8,
    pub(crate) flags: u8,
    pub(crate) size: u64,
}

impl ObjectHeader {
    pub(crate) fn parse(buf: &[u8], path: &Path, offset: FileOffset) -> Result<Self> {
        if buf.len() < 16 {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(offset),
                reason: "object header truncated".to_string(),
            });
        }

        let object_type = buf[0];
        let flags = buf[1];
        let size = read_u64_le(buf, 8).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(offset),
            reason: "object size missing".to_string(),
        })?;
        if size < 16 {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(offset),
                reason: format!("object size too small: {size}"),
            });
        }
        let compression_mask =
            OBJECT_COMPRESSED_XZ | OBJECT_COMPRESSED_LZ4 | OBJECT_COMPRESSED_ZSTD;
        if flags & compression_mask != 0
            && matches!(
                object_type,
                OBJECT_FIELD
                    | OBJECT_ENTRY
                    | OBJECT_DATA_HASH_TABLE
                    | OBJECT_FIELD_HASH_TABLE
                    | OBJECT_ENTRY_ARRAY
                    | OBJECT_TAG
            )
        {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(offset),
                reason: "compression flags are only valid on DATA objects".to_string(),
            });
        }

        Ok(Self {
            object_type,
            flags,
            size,
        })
    }
}

pub(crate) fn parse_entry_data_offsets_bytes(
    items_bytes: &[u8],
    compact: bool,
    max_fields_per_entry: usize,
    path: &Path,
    entry_offset: FileOffset,
) -> Result<Vec<FileOffset>> {
    if items_bytes.is_empty() {
        return Err(SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(entry_offset),
            reason: "ENTRY contains no items".to_string(),
        });
    }

    // Compact items store a u32 offset; regular items store a u64 offset plus a u64 hash.
    let item_size = if compact { 4 } else { 16 };
    if !items_bytes.len().is_multiple_of(item_size) {
        return Err(SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(entry_offset),
            reason: if compact {
                "ENTRY compact items not aligned"
            } else {
                "ENTRY regular items not aligned"
            }
            .to_string(),
        });
    }
    let count = items_bytes.len() / item_size;
    crate::util::ensure_limit_usize(LimitKind::FieldsPerEntry, max_fields_per_entry, count)?;

    let mut data_offsets = Vec::with_capacity(count);
    for (index, item) in items_bytes.chunks_exact(item_size).enumerate() {
        let offset = if compact {
            read_u32_le(item, 0).map(u64::from)
        } else {
            read_u64_le(item, 0)
        }
        .ok_or_else(|| SdJournalError::Corrupt {
            path: Some(path.to_path_buf()),
            offset: Some(entry_offset),
            reason: if compact {
                "ENTRY compact item truncated"
            } else {
                "ENTRY item truncated"
            }
            .to_string(),
        })?;
        if offset == 0 {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: Some(entry_offset.saturating_add(64 + (index * item_size) as u64)),
                reason: "ENTRY item has zero object offset".to_string(),
            });
        }
        data_offsets.push(offset);
    }

    Ok(data_offsets)
}

pub(crate) fn compression_from_object_flags(flags: u8) -> Result<Option<CompressionAlgo>> {
    let mask = OBJECT_COMPRESSED_XZ | OBJECT_COMPRESSED_LZ4 | OBJECT_COMPRESSED_ZSTD;
    match flags & mask {
        0 => Ok(None),
        OBJECT_COMPRESSED_XZ => Ok(Some(CompressionAlgo::Xz)),
        OBJECT_COMPRESSED_LZ4 => Ok(Some(CompressionAlgo::Lz4)),
        OBJECT_COMPRESSED_ZSTD => Ok(Some(CompressionAlgo::Zstd)),
        _ => Err(SdJournalError::Corrupt {
            path: None,
            offset: None,
            reason: "multiple compression flags set".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::LimitKind;
    use std::path::Path;

    fn sample_header_bytes(compatible_flags: u32, incompatible_flags: u32) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_SIZE_MIN as usize];
        buf[0..8].copy_from_slice(HEADER_SIGNATURE);
        buf[8..12].copy_from_slice(&compatible_flags.to_le_bytes());
        buf[12..16].copy_from_slice(&incompatible_flags.to_le_bytes());
        buf[16] = STATE_ARCHIVED;
        buf[24..40].copy_from_slice(&[0x11; 16]);
        buf[40..56].copy_from_slice(&[0x22; 16]);
        buf[56..72].copy_from_slice(&[0x33; 16]);
        buf[72..88].copy_from_slice(&[0x44; 16]);
        buf[88..96].copy_from_slice(&HEADER_SIZE_MIN.to_le_bytes());
        buf[96..104].copy_from_slice(&4096u64.to_le_bytes());
        buf[104..112].copy_from_slice(&256u64.to_le_bytes());
        buf[112..120].copy_from_slice(&64u64.to_le_bytes());
        buf[120..128].copy_from_slice(&320u64.to_le_bytes());
        buf[128..136].copy_from_slice(&64u64.to_le_bytes());
        buf[136..144].copy_from_slice(&512u64.to_le_bytes());
        buf[152..160].copy_from_slice(&7u64.to_le_bytes());
        buf[176..184].copy_from_slice(&768u64.to_le_bytes());
        buf
    }

    #[test]
    fn header_parse_accepts_minimal_header_and_reports_flags() {
        #[cfg(any(feature = "tracing", feature = "verify-seal"))]
        let compatible_flags = HEADER_COMPATIBLE_SEALED | HEADER_COMPATIBLE_SEALED_CONTINUOUS;
        #[cfg(not(any(feature = "tracing", feature = "verify-seal")))]
        let compatible_flags = 0u32;

        let header = Header::parse(
            &sample_header_bytes(
                compatible_flags,
                HEADER_INCOMPATIBLE_COMPACT | HEADER_INCOMPATIBLE_KEYED_HASH,
            ),
            Path::new("dummy"),
        )
        .unwrap();

        assert_eq!(header.state, STATE_ARCHIVED);
        assert_eq!(header.file_id, [0x11; 16]);
        assert_eq!(header.seqnum_id, [0x44; 16]);
        assert_eq!(header.header_size, HEADER_SIZE_MIN);
        assert_eq!(header.n_entries, 7);
        assert_eq!(header.entry_array_offset, 768);
        assert!(header.is_compact());
        assert!(header.is_keyed_hash());
        #[cfg(any(feature = "tracing", feature = "verify-seal"))]
        {
            assert!(header.is_sealed());
            assert!(header.is_sealed_continuous());
        }
    }

    #[test]
    fn header_parse_accepts_modern_headers_without_using_mutable_tail_hints() {
        let mut buf = sample_header_bytes(0, 0);
        buf.resize(272, 0);
        buf[88..96].copy_from_slice(&272u64.to_le_bytes());
        buf[160..168].copy_from_slice(&99u64.to_le_bytes());
        buf[168..176].copy_from_slice(&42u64.to_le_bytes());
        buf[184..192].copy_from_slice(&1_700_000_000_000_000u64.to_le_bytes());
        buf[192..200].copy_from_slice(&1_700_000_000_000_500u64.to_le_bytes());
        buf[264..272].copy_from_slice(&4096u64.to_le_bytes());

        let header = Header::parse(&buf, Path::new("dummy")).unwrap();

        assert_eq!(header.header_size, 272);
        assert_eq!(header.n_entries, 7);
        assert_eq!(header.entry_array_offset, 768);
        assert_eq!(header.tail_object_offset, 512);
    }

    #[test]
    fn header_parse_ignores_extensions_past_declared_size() {
        let mut buf = sample_header_bytes(0, 0);
        buf.resize(272, 0xff);

        let header = Header::parse(&buf, Path::new("dummy")).unwrap();

        assert_eq!(header.header_size, HEADER_SIZE_MIN);
        assert_eq!(header.n_entries, 7);
        assert_eq!(header.entry_array_offset, 768);
    }

    #[test]
    fn header_parse_rejects_short_or_unaligned_declared_size() {
        for invalid_size in [200u64, 209] {
            let mut buf = sample_header_bytes(0, 0);
            buf[88..96].copy_from_slice(&invalid_size.to_le_bytes());
            match Header::parse(&buf, Path::new("dummy")) {
                Err(SdJournalError::Corrupt { offset, reason, .. }) => {
                    assert_eq!(offset, Some(88));
                    assert_eq!(reason, format!("invalid header_size: {invalid_size}"));
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
    }

    #[test]
    fn header_parse_rejects_a_physically_truncated_minimum_header() {
        let buf = vec![0u8; HEADER_SIZE_MIN as usize - 1];
        match Header::parse(&buf, Path::new("dummy")) {
            Err(SdJournalError::Corrupt { offset, reason, .. }) => {
                assert_eq!(offset, Some(0));
                assert_eq!(reason, "file too small for journal header");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn header_parse_rejects_unknown_incompatible_flags() {
        match Header::parse(
            &sample_header_bytes(0, HEADER_INCOMPATIBLE_COMPACT | (1 << 31)),
            Path::new("dummy"),
        ) {
            Err(SdJournalError::Unsupported { reason }) => {
                assert_eq!(reason, "unknown incompatible flags: 0x80000010");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn object_header_parse_rejects_truncated_input() {
        match ObjectHeader::parse(&[OBJECT_ENTRY, 0], Path::new("dummy"), 64) {
            Err(SdJournalError::Corrupt {
                path,
                offset,
                reason,
            }) => {
                assert_eq!(path, Some(Path::new("dummy").to_path_buf()));
                assert_eq!(offset, Some(64));
                assert_eq!(reason, "object header truncated");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn object_header_rejects_sizes_smaller_than_its_header() {
        for size in 0..16u64 {
            let mut bytes = [0u8; 16];
            bytes[0] = OBJECT_DATA;
            bytes[8..].copy_from_slice(&size.to_le_bytes());
            assert!(
                matches!(
                    ObjectHeader::parse(&bytes, Path::new("dummy"), 64),
                    Err(SdJournalError::Corrupt {
                        offset: Some(64),
                        ..
                    })
                ),
                "accepted object size {size}"
            );
        }
    }

    #[test]
    fn object_header_rejects_compression_on_non_data_objects() {
        for object_type in 2..=7 {
            for flag in [
                OBJECT_COMPRESSED_XZ,
                OBJECT_COMPRESSED_LZ4,
                OBJECT_COMPRESSED_ZSTD,
            ] {
                let mut bytes = [0u8; 16];
                bytes[0] = object_type;
                bytes[1] = flag;
                bytes[8..].copy_from_slice(&64u64.to_le_bytes());
                assert!(
                    matches!(
                        ObjectHeader::parse(&bytes, Path::new("dummy"), 64),
                        Err(SdJournalError::Corrupt {
                            offset: Some(64),
                            ..
                        })
                    ),
                    "accepted compressed object type {object_type}"
                );
            }
        }
    }

    #[test]
    fn object_header_preserves_unknown_types_flags_and_reserved_bytes() {
        for object_type in [OBJECT_DATA, OBJECT_ENTRY, 8, u8::MAX] {
            let mut bytes = [0xff; 16];
            bytes[0] = object_type;
            bytes[1] = 0x80;
            bytes[8..].copy_from_slice(&64u64.to_le_bytes());
            let header = ObjectHeader::parse(&bytes, Path::new("dummy"), 64).unwrap();
            assert_eq!(header.object_type, object_type);
            assert_eq!(header.flags, 0x80);
            assert_eq!(header.size, 64);
        }
    }

    #[test]
    fn compression_flags_accept_single_algorithm_and_reject_conflicts() {
        for flags in 0..=u8::MAX {
            let bits = [
                (OBJECT_COMPRESSED_XZ, CompressionAlgo::Xz),
                (OBJECT_COMPRESSED_LZ4, CompressionAlgo::Lz4),
                (OBJECT_COMPRESSED_ZSTD, CompressionAlgo::Zstd),
            ];
            let selected: Vec<_> = bits
                .into_iter()
                .filter_map(|(bit, algo)| (flags & bit != 0).then_some(algo))
                .collect();
            let result = compression_from_object_flags(flags);
            match selected.as_slice() {
                [] => assert_eq!(result.unwrap(), None),
                [algo] => assert_eq!(result.unwrap(), Some(*algo)),
                _ => assert!(matches!(result, Err(SdJournalError::Corrupt { .. }))),
            }
        }

        assert_eq!(compression_from_object_flags(0).unwrap(), None);
        assert_eq!(
            compression_from_object_flags(OBJECT_COMPRESSED_LZ4).unwrap(),
            Some(CompressionAlgo::Lz4)
        );
        assert_eq!(
            compression_from_object_flags(OBJECT_COMPRESSED_XZ).unwrap(),
            Some(CompressionAlgo::Xz)
        );
        assert_eq!(
            compression_from_object_flags(OBJECT_COMPRESSED_ZSTD).unwrap(),
            Some(CompressionAlgo::Zstd)
        );

        match compression_from_object_flags(OBJECT_COMPRESSED_LZ4 | OBJECT_COMPRESSED_ZSTD) {
            Err(SdJournalError::Corrupt { reason, .. }) => {
                assert_eq!(reason, "multiple compression flags set");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn compact_entry_items_offsets_only() {
        let mut items = Vec::new();

        items.extend_from_slice(&100u32.to_le_bytes());
        items.extend_from_slice(&200u32.to_le_bytes());

        let got = parse_entry_data_offsets_bytes(&items, true, 1024, Path::new("dummy"), 0)
            .expect("parse compact entry items");
        assert_eq!(got, vec![100, 200]);
    }

    #[test]
    fn regular_entry_items_skip_hashes() {
        let mut items = Vec::new();

        items.extend_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes());
        items.extend_from_slice(&0u64.to_le_bytes());

        items.extend_from_slice(&0x5555_6666_7777_8888u64.to_le_bytes());
        items.extend_from_slice(&0xffff_eeee_dddd_ccccu64.to_le_bytes());

        let got = parse_entry_data_offsets_bytes(&items, false, 1024, Path::new("dummy"), 0)
            .expect("parse regular entry items");
        assert_eq!(got, vec![0x1111_2222_3333_4444, 0x5555_6666_7777_8888]);
    }

    #[test]
    fn entry_items_reject_zero_offsets() {
        let compact = 0u32.to_le_bytes();
        match parse_entry_data_offsets_bytes(&compact, true, 1024, Path::new("dummy"), 9) {
            Err(SdJournalError::Corrupt { reason, offset, .. }) => {
                assert_eq!(offset, Some(73));
                assert_eq!(reason, "ENTRY item has zero object offset");
            }
            other => panic!("unexpected result: {other:?}"),
        }

        let mut regular = Vec::new();
        regular.extend_from_slice(&0u64.to_le_bytes());
        regular.extend_from_slice(&123u64.to_le_bytes());
        match parse_entry_data_offsets_bytes(&regular, false, 1024, Path::new("dummy"), 9) {
            Err(SdJournalError::Corrupt { reason, offset, .. }) => {
                assert_eq!(offset, Some(73));
                assert_eq!(reason, "ENTRY item has zero object offset");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn entry_items_reject_misalignment_and_field_limit() {
        match parse_entry_data_offsets_bytes(&[1, 2, 3], true, 1024, Path::new("dummy"), 9) {
            Err(SdJournalError::Corrupt { reason, offset, .. }) => {
                assert_eq!(offset, Some(9));
                assert_eq!(reason, "ENTRY compact items not aligned");
            }
            other => panic!("unexpected result: {other:?}"),
        }

        let mut regular = Vec::new();
        regular.extend_from_slice(&100u64.to_le_bytes());
        regular.extend_from_slice(&0u64.to_le_bytes());
        regular.extend_from_slice(&200u64.to_le_bytes());
        regular.extend_from_slice(&0u64.to_le_bytes());

        match parse_entry_data_offsets_bytes(&regular, false, 1, Path::new("dummy"), 0) {
            Err(SdJournalError::LimitExceeded { kind, limit }) => {
                assert_eq!(kind, LimitKind::FieldsPerEntry);
                assert_eq!(limit, 1);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
