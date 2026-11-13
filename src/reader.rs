use crate::error::{Result, SdJournalError};
use std::fs::File;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "mmap")]
use memmap2::Mmap;

/// Bytes read from a journal file.
///
/// When mmap is enabled and safe to use, this may borrow from an mmap'd region.
/// Otherwise, this owns the bytes read from the underlying file.
#[derive(Debug, Clone)]
pub(crate) enum ByteBuf {
    #[cfg(feature = "mmap")]
    Mmap {
        map: Arc<Mmap>,
        range: std::ops::Range<usize>,
    },
    Owned(Arc<[u8]>),
}

impl ByteBuf {
    #[cfg(feature = "mmap")]
    pub(crate) fn from_mmap(
        map: Arc<Mmap>,
        range: std::ops::Range<usize>,
        path: &Path,
    ) -> Result<Self> {
        if map.get(range.clone()).is_none() {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: None,
                reason: "mmap range out of bounds".to_string(),
            });
        }
        Ok(ByteBuf::Mmap { map, range })
    }

    pub(crate) fn from_vec(v: Vec<u8>) -> Self {
        ByteBuf::Owned(Arc::<[u8]>::from(v))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            ByteBuf::Mmap { map, range } => &map[range.clone()],
            ByteBuf::Owned(b) => b,
        }
    }
}

impl AsRef<[u8]> for ByteBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// A journal snapshot's I/O strategy. The enclosing JournalFile owns its path and bounds.
pub(crate) enum FileAccess {
    File(File),
    Reopen {
        expected_file_id: [u8; 16],
        expected_seqnum_id: [u8; 16],
    },
    #[cfg(feature = "mmap")]
    Mmap(Arc<Mmap>),
}

impl FileAccess {
    pub(crate) fn read_known_valid(&self, path: &Path, offset: u64, len: usize) -> Result<ByteBuf> {
        match self {
            Self::File(file) => read_file_at(file, path, offset, len),
            Self::Reopen {
                expected_file_id,
                expected_seqnum_id,
            } => {
                let file = open_validated(path, expected_file_id, expected_seqnum_id)?;
                read_file_at(&file, path, offset, len)
            }
            #[cfg(feature = "mmap")]
            Self::Mmap(map) => {
                let start = usize::try_from(offset).map_err(|_| SdJournalError::Corrupt {
                    path: Some(path.to_path_buf()),
                    offset: Some(offset),
                    reason: "offset out of range".to_string(),
                })?;
                let end = start
                    .checked_add(len)
                    .ok_or_else(|| SdJournalError::Corrupt {
                        path: Some(path.to_path_buf()),
                        offset: Some(offset),
                        reason: "range overflow".to_string(),
                    })?;
                ByteBuf::from_mmap(map.clone(), start..end, path)
            }
        }
    }

    pub(crate) fn validate_identity(&self, path: &Path) -> Result<()> {
        if let Self::Reopen {
            expected_file_id,
            expected_seqnum_id,
        } = self
        {
            open_validated(path, expected_file_id, expected_seqnum_id)?;
        }
        Ok(())
    }
}

pub(crate) fn open_readonly(path: &Path) -> Result<File> {
    File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            SdJournalError::PermissionDenied {
                path: path.to_path_buf(),
            }
        } else {
            SdJournalError::io("open", Some(path.to_path_buf()), error)
        }
    })
}

fn open_validated(
    path: &Path,
    expected_file_id: &[u8; 16],
    expected_seqnum_id: &[u8; 16],
) -> Result<File> {
    let file = open_readonly(path)?;
    let mut identity = [0u8; 88];
    if let Err(error) = read_exact_at(&file, 0, &mut identity) {
        return Err(read_failure(path, 0, identity.len(), error));
    }
    if &identity[24..40] != expected_file_id || &identity[72..88] != expected_seqnum_id {
        return Err(SdJournalError::Transient {
            path: Some(path.to_path_buf()),
            reason: "journal file was replaced after discovery".to_string(),
        });
    }
    Ok(file)
}

pub(crate) fn read_file_at(file: &File, path: &Path, offset: u64, len: usize) -> Result<ByteBuf> {
    let mut buf = vec![0u8; len];
    if let Err(error) = read_exact_at(file, offset, &mut buf) {
        return Err(read_failure(path, offset, len, error));
    }
    Ok(ByteBuf::from_vec(buf))
}

fn read_failure(path: &Path, offset: u64, len: usize, error: std::io::Error) -> SdJournalError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        SdJournalError::Transient {
            path: Some(path.to_path_buf()),
            reason: format!("file was truncated while reading (offset={offset}, len={len})"),
        }
    } else {
        SdJournalError::io("read_at", Some(path.to_path_buf()), error)
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    // The standard exact-read loop retries Interrupted without losing partial progress.
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    while !buf.is_empty() {
        let n = match file.seek_read(buf, offset) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        offset = offset.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        buf = &mut buf[n..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
compile_error!("sdjournal requires a Unix or Windows target for positional file reads");

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::tempdir;

    fn temp_file(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, File) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reader.bin");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
        }
        let file = File::open(&path).unwrap();
        (dir, path, file)
    }

    #[test]
    fn bytebuf_from_vec_exposes_original_bytes() {
        let buf = ByteBuf::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(buf.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn file_access_reads_ranges_and_reports_eof() {
        let (_dir, path, file) = temp_file(b"abcdef");
        let access = FileAccess::File(file);

        assert_eq!(
            access.read_known_valid(&path, 1, 3).unwrap().as_slice(),
            b"bcd"
        );

        match access.read_known_valid(&path, 4, 3) {
            Err(SdJournalError::Transient {
                path: err_path,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "file was truncated while reading (offset=4, len=3)");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn shared_file_access_reads_concurrent_ranges_independently() {
        let bytes: Vec<_> = (0..4096u32).flat_map(u32::to_le_bytes).collect();
        let (_dir, path, file) = temp_file(&bytes);
        let access = FileAccess::File(file);
        let start = std::sync::Barrier::new(4);

        std::thread::scope(|scope| {
            for worker in 0..4 {
                let (access, path, bytes, start) = (&access, &path, &bytes, &start);
                scope.spawn(move || {
                    start.wait();
                    for iteration in 0..128 {
                        let offset = worker * 1024 + iteration * 4;
                        let actual = access.read_known_valid(path, offset as u64, 64).unwrap();
                        assert_eq!(actual.as_slice(), &bytes[offset..offset + 64]);
                    }
                });
            }
        });
    }

    #[test]
    fn file_access_reports_concurrent_truncation_as_transient() {
        let (_dir, path, file) = temp_file(b"abcdef");
        let access = FileAccess::File(file);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(2)
            .unwrap();

        match access.read_known_valid(&path, 0, 6) {
            Err(SdJournalError::Transient {
                path: err_path,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "file was truncated while reading (offset=0, len=6)");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn reopen_access_validates_both_identifiers_on_every_read() {
        for changed_offset in [24, 72] {
            let mut identity = vec![0u8; 88];
            identity[24..40].fill(0x11);
            identity[72..88].fill(0x22);
            identity.extend_from_slice(b"abcdef");
            let (_dir, path, _file) = temp_file(&identity);
            let access = FileAccess::Reopen {
                expected_file_id: [0x11; 16],
                expected_seqnum_id: [0x22; 16],
            };
            access.validate_identity(&path).unwrap();
            assert_eq!(
                access.read_known_valid(&path, 89, 3).unwrap().as_slice(),
                b"bcd"
            );

            identity[changed_offset] ^= 1;
            std::fs::write(&path, identity).unwrap();
            for result in [
                access.validate_identity(&path),
                access.read_known_valid(&path, 89, 3).map(|_| ()),
            ] {
                assert!(matches!(
                    result,
                    Err(SdJournalError::Transient { path: actual, reason })
                        if actual == Some(path.clone())
                            && reason == "journal file was replaced after discovery"
                ));
            }
        }
    }

    #[test]
    fn reopen_access_reports_a_truncated_identity_without_panicking() {
        let (_dir, path, _file) = temp_file(&[0; 87]);
        let access = FileAccess::Reopen {
            expected_file_id: [0; 16],
            expected_seqnum_id: [0; 16],
        };
        for result in [
            access.validate_identity(&path),
            access.read_known_valid(&path, 0, 0).map(|_| ()),
        ] {
            assert!(matches!(
                result,
                Err(SdJournalError::Transient { path: actual, reason })
                    if actual == Some(path.clone())
                        && reason == "file was truncated while reading (offset=0, len=88)"
            ));
        }
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn mmap_access_reads_ranges_and_rejects_beyond_eof() {
        let (_dir, path, file) = temp_file(b"abcdef");
        // SAFETY: The file is opened read-only for the duration of the test and is not mutated.
        let map = Arc::new(unsafe { memmap2::MmapOptions::new().map(&file).unwrap() });
        let access = FileAccess::Mmap(map);

        let bytes = access.read_known_valid(&path, 2, 2).unwrap();
        assert_eq!(bytes.as_slice(), b"cd");

        match access.read_known_valid(&path, 5, 2) {
            Err(SdJournalError::Corrupt {
                path: err_path,
                offset,
                reason,
            }) => {
                assert_eq!(offset, None);
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "mmap range out of bounds");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        drop(access);
        assert_eq!(bytes.as_slice(), b"cd");
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn bytebuf_from_mmap_rejects_out_of_bounds_range() {
        let (_dir, path, file) = temp_file(b"abcdef");
        // SAFETY: The file is opened read-only for the duration of the test and is not mutated.
        let map = Arc::new(unsafe { memmap2::MmapOptions::new().map(&file).unwrap() });

        match ByteBuf::from_mmap(map, 2..99, &path) {
            Err(SdJournalError::Corrupt {
                path: err_path,
                offset,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(offset, None);
                assert_eq!(reason, "mmap range out of bounds");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
