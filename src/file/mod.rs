mod decompress;
mod entries;
mod index;
mod iter;

use crate::config::JournalConfig;
#[cfg(feature = "mmap")]
use crate::config::MmapPolicy;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::format::{HEADER_SIZE_MIN, Header, ObjectHeader, STATE_ARCHIVED};
#[cfg(feature = "mmap")]
use crate::format::{STATE_OFFLINE, STATE_ONLINE};
use crate::reader::{ByteBuf, FileAccess, open_readonly, read_file_at};
use crate::util::checked_add_u64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

#[cfg(feature = "tracing")]
use tracing::debug;

pub(crate) use self::iter::{DataEntryOffsetIter, FileEntryIter};
pub(crate) use crate::cursor::SdJournalEntryKey as EntryMeta;

#[derive(Clone)]
pub(crate) struct JournalFile {
    inner: Arc<JournalFileInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveFileState {
    pub(crate) used_size: u64,
    pub(crate) file_len: u64,
    pub(crate) n_entries: u64,
    pub(crate) entry_array_offset: u64,
    pub(crate) tail_object_offset: u64,
}

struct JournalFileInner {
    path: PathBuf,
    config: JournalConfig,
    header: Header,
    used_size: u64,
    file_len: u64,
    modified_at_open: Option<SystemTime>,
    entry_index: OnceLock<Arc<self::iter::EntryIndex>>,
    access: FileAccess,
}

impl JournalFile {
    pub(crate) fn open(path: PathBuf, config: &JournalConfig) -> Result<Self> {
        Self::open_with_access(path, config, false)
    }

    pub(crate) fn open_reopen(path: PathBuf, config: &JournalConfig) -> Result<Self> {
        Self::open_with_access(path, config, true)
    }

    pub(crate) fn reopen_snapshot(&self) -> Self {
        if matches!(&self.inner.access, FileAccess::Reopen { .. }) {
            return self.clone();
        }
        Self {
            inner: Arc::new(JournalFileInner {
                path: self.inner.path.clone(),
                config: self.inner.config.clone(),
                header: self.inner.header.clone(),
                used_size: self.inner.used_size,
                file_len: self.inner.file_len,
                modified_at_open: self.inner.modified_at_open,
                entry_index: self.inner.entry_index.clone(),
                access: FileAccess::Reopen {
                    expected_file_id: self.inner.header.file_id,
                    expected_seqnum_id: self.inner.header.seqnum_id,
                },
            }),
        }
    }

    pub(crate) fn validate_reopen_identity(&self) -> Result<()> {
        self.inner.access.validate_identity(self.path())
    }

    fn open_with_access(
        path: PathBuf,
        config: &JournalConfig,
        reopen_each_read: bool,
    ) -> Result<Self> {
        let file = open_readonly(&path)?;

        let opening_metadata = file
            .metadata()
            .map_err(|e| SdJournalError::io("metadata", Some(path.clone()), e))?;
        let file_len = opening_metadata.len();
        let modified_at_open = opening_metadata.modified().ok();

        let header_len = usize::try_from(file_len)
            .unwrap_or(usize::MAX)
            .min(HEADER_SIZE_MIN as usize);
        let header_buf = read_file_at(&file, &path, 0, header_len)?;
        let header = Header::parse(header_buf.as_slice(), &path)?;
        let arena_end = checked_add_u64(
            header.header_size,
            header.arena_size,
            "header_size+arena_size",
        )?;

        #[cfg(feature = "tracing")]
        debug!(
            path = %path.display(),
            state = header.state,
            compact = header.is_compact(),
            keyed_hash = header.is_keyed_hash(),
            sealed = header.is_sealed(),
            sealed_continuous = header.is_sealed_continuous(),
            compressed_xz = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_XZ != 0,
            compressed_lz4 = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_LZ4 != 0,
            compressed_zstd = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_ZSTD != 0,
            file_len,
            arena_end,
            "parsed journal header"
        );
        if arena_end > file_len {
            return Err(SdJournalError::Transient {
                path: Some(path.clone()),
                reason: format!(
                    "file shorter than header reports (file_len={file_len}, arena_end={arena_end})"
                ),
            });
        }
        if header.state > STATE_ARCHIVED {
            return Err(SdJournalError::Corrupt {
                path: Some(path.clone()),
                offset: Some(16),
                reason: format!("unknown header state: {}", header.state),
            });
        }
        let used_size = if header.tail_object_offset == 0 {
            header.header_size
        } else {
            let tail_offset = header.tail_object_offset;
            if tail_offset < header.header_size
                || !tail_offset.is_multiple_of(8)
                || tail_offset >= arena_end
            {
                return Err(SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: "tail_object_offset is outside the committed object arena".to_string(),
                });
            }
            let tail_header = read_file_at(&file, &path, tail_offset, 16)?;
            let tail_object = ObjectHeader::parse(tail_header.as_slice(), &path, tail_offset)?;
            let raw_committed_end =
                checked_add_u64(tail_offset, tail_object.size, "tail object committed range")?;
            let committed_end = raw_committed_end
                .checked_next_multiple_of(8)
                .ok_or_else(|| SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: "tail object aligned range overflows".to_string(),
                })?;
            if committed_end > arena_end {
                return Err(SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: format!(
                        "tail object extends beyond the object arena (end={committed_end}, arena_end={arena_end})"
                    ),
                });
            }
            committed_end
        };

        let access = if reopen_each_read {
            FileAccess::Reopen {
                expected_file_id: header.file_id,
                expected_seqnum_id: header.seqnum_id,
            }
        } else {
            #[cfg(feature = "mmap")]
            {
                let use_mmap = match config.mmap_policy {
                    MmapPolicy::Never => false,
                    MmapPolicy::Auto => match header.state {
                        STATE_ONLINE => config.allow_mmap_online,
                        STATE_OFFLINE | STATE_ARCHIVED => true,
                        _ => false,
                    },
                };

                if use_mmap {
                    let mmap_len =
                        usize::try_from(used_size).map_err(|_| SdJournalError::Unsupported {
                            reason: format!(
                                "journal snapshot is too large to mmap: {used_size} bytes"
                            ),
                        })?;
                    // SAFETY: Mapping a file is inherently `unsafe` because the file may change on disk.
                    // We reduce risk by:
                    // - validating `used_size <= file_len` before mapping;
                    // - mapping only the committed prefix, not preallocated/uncommitted space;
                    // - defaulting to mmap only for STATE_OFFLINE/STATE_ARCHIVED files unless explicitly enabled;
                    // - performing bounds checks on every read.
                    let mmap = unsafe { memmap2::MmapOptions::new().len(mmap_len).map(&file) }
                        .map_err(|e| SdJournalError::io("mmap", Some(path.clone()), e))?;
                    FileAccess::Mmap(Arc::new(mmap))
                } else {
                    FileAccess::File(file)
                }
            }

            #[cfg(not(feature = "mmap"))]
            {
                FileAccess::File(file)
            }
        };

        Ok(Self {
            inner: Arc::new(JournalFileInner {
                path,
                config: config.clone(),
                header,
                used_size,
                file_len,
                modified_at_open,
                entry_index: OnceLock::new(),
                access,
            }),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    pub(crate) fn file_id(&self) -> [u8; 16] {
        self.inner.header.file_id
    }

    pub(crate) fn seqnum_id(&self) -> [u8; 16] {
        self.inner.header.seqnum_id
    }

    pub(crate) fn is_online(&self) -> bool {
        self.inner.header.state == crate::format::STATE_ONLINE
    }

    pub(crate) fn live_state(&self) -> LiveFileState {
        LiveFileState {
            used_size: self.inner.used_size,
            file_len: self.inner.file_len,
            n_entries: self.inner.header.n_entries,
            entry_array_offset: self.inner.header.entry_array_offset,
            tail_object_offset: self.inner.header.tail_object_offset,
        }
    }

    pub(crate) fn modified_at_open(&self) -> Option<SystemTime> {
        self.inner.modified_at_open
    }

    pub(crate) fn max_object_chain_steps(&self) -> usize {
        self.inner.config.max_object_chain_steps
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn header(&self) -> &Header {
        &self.inner.header
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn used_size(&self) -> u64 {
        self.inner.used_size
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn config(&self) -> &JournalConfig {
        &self.inner.config
    }

    pub(crate) fn entry_iter(&self, reverse: bool) -> Result<FileEntryIter> {
        FileEntryIter::new(self.clone(), reverse)
    }

    pub(crate) fn entry_iter_after_offset(
        &self,
        reverse: bool,
        entry_offset: u64,
    ) -> Result<FileEntryIter> {
        FileEntryIter::new_after_offset(self.clone(), reverse, entry_offset)
    }

    pub(crate) fn read_bytes(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let end = checked_add_u64(offset, u64::try_from(len).unwrap_or(u64::MAX), "read range")?;
        if end > self.inner.used_size {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: "read out of bounds".to_string(),
            });
        }

        self.inner.access.read_known_valid(self.path(), offset, len)
    }

    pub(crate) fn read_object(&self, offset: u64, size: u64) -> Result<ByteBuf> {
        self.validate_object_offset(offset)?;
        let max = usize::try_from(self.inner.config.max_object_size_bytes).unwrap_or(usize::MAX);
        let size_usize = usize::try_from(size).map_err(|_| SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: self.inner.config.max_object_size_bytes,
        })?;
        crate::util::ensure_limit_usize(LimitKind::ObjectSizeBytes, max, size_usize)?;
        self.read_bytes(offset, size_usize)
    }

    pub(crate) fn validate_object_offset(&self, offset: u64) -> Result<()> {
        if !offset.is_multiple_of(8) {
            return Err(self.metadata_error(
                Some(offset),
                format!("object offset is not 8-byte aligned: {offset}"),
            ));
        }
        if offset < self.inner.header.header_size
            || self.inner.header.tail_object_offset == 0
            || offset > self.inner.header.tail_object_offset
        {
            return Err(self.metadata_error(
                Some(offset),
                format!(
                    "object offset is outside committed journal data (header_size={}, tail_object_offset={})",
                    self.inner.header.header_size, self.inner.header.tail_object_offset
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn metadata_error(
        &self,
        offset: Option<u64>,
        reason: impl Into<String>,
    ) -> SdJournalError {
        let reason = reason.into();
        if self.inner.header.state == crate::format::STATE_ONLINE {
            SdJournalError::Transient {
                path: Some(self.inner.path.clone()),
                reason,
            }
        } else {
            SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset,
                reason,
            }
        }
    }

    fn validate_entry_index_metadata(&self) -> Result<()> {
        let has_entries = self.inner.header.n_entries != 0;
        let has_entry_array = self.inner.header.entry_array_offset != 0;
        if has_entries != has_entry_array {
            return Err(self.metadata_error(
                Some(if has_entries { 176 } else { 152 }),
                format!(
                    "inconsistent entry metadata (n_entries={}, entry_array_offset={})",
                    self.inner.header.n_entries, self.inner.header.entry_array_offset
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "mmap"))]
mod tests {
    use super::*;

    #[test]
    fn mmap_only_maps_the_committed_snapshot_prefix() {
        const ALLOCATED_SIZE: u64 = 1024 * 1024;
        for state in [STATE_OFFLINE, STATE_ONLINE, STATE_ARCHIVED] {
            for has_objects in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("preallocated.journal");
                let used_size = if has_objects { 240usize } else { 208 };
                let mut bytes = vec![0; used_size];
                bytes[..8].copy_from_slice(crate::format::HEADER_SIGNATURE);
                bytes[16] = state;
                bytes[88..96].copy_from_slice(&208u64.to_le_bytes());
                bytes[96..104].copy_from_slice(&(ALLOCATED_SIZE - 208).to_le_bytes());
                if has_objects {
                    bytes[136..144].copy_from_slice(&208u64.to_le_bytes());
                    bytes[208] = crate::format::OBJECT_DATA_HASH_TABLE;
                    bytes[216..224].copy_from_slice(&32u64.to_le_bytes());
                }
                std::fs::write(&path, bytes).unwrap();
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(ALLOCATED_SIZE)
                    .unwrap();

                let file = JournalFile::open(
                    path,
                    &JournalConfig {
                        mmap_policy: MmapPolicy::Auto,
                        allow_mmap_online: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let FileAccess::Mmap(map) = &file.inner.access else {
                    panic!("expected mmap access");
                };
                assert_eq!(map.len(), used_size);
                assert_eq!(file.inner.file_len, ALLOCATED_SIZE);
                assert_eq!(
                    file.read_bytes(0, used_size).unwrap().as_slice().len(),
                    used_size
                );
                assert!(file.read_bytes(used_size as u64, 1).is_err());
            }
        }
    }
}
