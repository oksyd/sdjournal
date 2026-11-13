use crate::config::JournalConfig;
use crate::cursor::Cursor;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::file::JournalFile;
use crate::filter::Filter;
use crate::live::LiveJournal;
use crate::query::JournalQuery;
use std::collections::{BTreeSet, HashSet};
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "tracing")]
use tracing::{debug, warn};

/// An opened set of journal files.
///
/// Cloning a `Journal` is cheap; the underlying journal-file set is reference counted.
#[derive(Clone)]
pub struct Journal {
    pub(crate) inner: Arc<JournalInner>,
}

pub(crate) struct JournalInner {
    pub(crate) config: JournalConfig,
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) file_infos: Vec<JournalFileInfo>,
}

pub(crate) struct JournalFileInfo {
    pub(crate) file_id: [u8; 16],
    pub(crate) seqnum_id: [u8; 16],
    pub(crate) file: JournalFile,
}

pub(crate) struct JournalDiscovery {
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) candidates: Vec<PathBuf>,
    pub(crate) missing_roots: Vec<PathBuf>,
}

impl Journal {
    /// Open the default system journal roots on Linux.
    ///
    /// This scans `/run/log/journal` and `/var/log/journal`, opens any discovered journal files,
    /// and deduplicates them by journal file ID.
    ///
    /// On non-Linux targets this returns [`SdJournalError::Unsupported`].
    pub fn open_default() -> Result<Self> {
        Self::open_default_with_config(JournalConfig::default())
    }

    /// Open the default system journal roots with a custom [`JournalConfig`].
    ///
    /// See [`Journal::open_default`] for the scanned locations and platform behavior.
    pub fn open_default_with_config(config: JournalConfig) -> Result<Self> {
        core::cfg_select! {
            target_os = "linux" => {
                let paths = vec![
                    PathBuf::from("/run/log/journal"),
                    PathBuf::from("/var/log/journal"),
                ];
                Self::open_dirs_with_config(&paths, config)
            }
            _ => {
                let _ = config;
                Err(SdJournalError::Unsupported {
                    reason: "Journal::open_default is only supported on Linux".to_string(),
                })
            }
        }
    }

    /// Open journal files discovered under a single root directory.
    ///
    /// Discovery includes `*.journal` files in the directory itself and in immediate machine-ID
    /// subdirectories such as `/var/log/journal/<machine-id>/`.
    pub fn open_dir(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_dir_with_config(path, JournalConfig::default())
    }

    /// Open journal files discovered under a single root directory with a custom configuration.
    pub fn open_dir_with_config(path: impl AsRef<Path>, config: JournalConfig) -> Result<Self> {
        let paths = vec![path.as_ref().to_path_buf()];
        Self::open_dirs_with_config(&paths, config)
    }

    /// Open journal files discovered under multiple root directories.
    ///
    /// Roots are sorted and deduplicated before discovery.
    pub fn open_dirs(paths: &[PathBuf]) -> Result<Self> {
        Self::open_dirs_with_config(paths, JournalConfig::default())
    }

    /// Open journal files discovered under multiple root directories using a custom configuration.
    ///
    /// Discovery skips symlinks, only keeps files with supported journal extensions, and
    /// deduplicates opened files by journal file ID. Missing roots and entries that disappear
    /// during discovery are skipped. Other discovery errors are returned even if another root
    /// contains readable journals.
    ///
    /// When discovery finds more files than [`JournalConfig::max_open_files`], the returned
    /// [`Journal`] stores file paths and opens files on demand while queries perform a streaming
    /// merge. Combine this with [`crate::MmapPolicy::Never`] for strict virtual-memory limits.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`SdJournalError::InvalidQuery`] when `paths` is empty.
    /// - [`SdJournalError::NotFound`] when no usable journal files are found.
    /// - [`SdJournalError::LimitExceeded`] when discovery exceeds configured file limits.
    /// - An underlying directory error when discovery fails for a reason other than a missing path.
    /// - An underlying file or parse error when all candidates fail to open.
    pub fn open_dirs_with_config(paths: &[PathBuf], config: JournalConfig) -> Result<Self> {
        validate_journal_config(&config)?;
        let discovery = discover_journal_candidates(paths, &config)?;
        let file_infos = open_journal_files(discovery.candidates, &config)?;

        #[cfg(feature = "tracing")]
        debug!(n_files = file_infos.len(), "journal opened");

        Ok(Self {
            inner: Arc::new(JournalInner {
                config,
                roots: discovery.roots,
                file_infos,
            }),
        })
    }

    /// Start building a query over the currently opened journal files.
    pub fn query(&self) -> JournalQuery {
        self.query_with_filter(self.filter())
    }

    /// Create a reusable filter builder using this reader's query limits.
    ///
    /// The same [`Filter`] can be used with [`Journal::query_with_filter`] and live subscriptions.
    pub fn filter(&self) -> Filter {
        Filter::new(&self.inner.config)
    }

    /// Create a query with a reusable filter, enforcing this reader's limits during execution.
    pub fn query_with_filter(&self, filter: Filter) -> JournalQuery {
        JournalQuery::new(self.clone(), filter)
    }

    /// Create a shared live engine from this journal set.
    ///
    /// The returned [`LiveJournal`] shares one watcher and one live cursor across all registered
    /// subscriptions.
    pub fn live(&self) -> Result<LiveJournal> {
        LiveJournal::from_journal(self)
    }

    /// Create a query positioned at `cursor`, inclusive.
    ///
    /// This is a convenience wrapper around [`Journal::query`] plus cursor positioning.
    ///
    /// Cursor resolution and read errors are reported when the query is executed.
    pub fn seek_cursor(&self, cursor: &Cursor) -> JournalQuery {
        let mut q = self.query();
        q.set_cursor_start(cursor.clone(), true);
        q
    }

    /// Verify Forward Secure Sealing for all committed objects in every opened journal file.
    ///
    /// Every file must be sealed and end at a valid TAG. Unsealed files and an unsealed tail
    /// after the last TAG return [`SdJournalError::Unsupported`], even when earlier TAGs verify.
    /// Active journals commonly have such a tail until the writer emits its next TAG.
    ///
    /// Authentication covers the immutable object contents specified by systemd's FSS format.
    /// It does not authenticate mutable indexes or all header fields, replace full structural
    /// validation, or prove that files or newer authenticated entries have not been removed.
    ///
    /// This is available when the `verify-seal` feature is enabled.
    #[cfg(feature = "verify-seal")]
    pub fn verify_seal(&self, verification_key: &str) -> Result<()> {
        let key = crate::seal::parse_verification_key(verification_key)?;
        let params = crate::seal::FsprgParams::new(key.seed())?;

        for info in &self.inner.file_infos {
            let f = info.open()?;
            crate::seal::verify_file_seal(&f, &params, &key)?;
        }
        Ok(())
    }
}

impl JournalFileInfo {
    pub(crate) fn open(&self) -> Result<JournalFile> {
        // Validate the current path, but retain the header and committed bounds captured at
        // discovery. A new, partially written tail must not invalidate the existing snapshot.
        self.file.validate_reopen_identity()?;
        Ok(self.file.clone())
    }

    fn from_open_file(file: JournalFile) -> Self {
        Self {
            file_id: file.file_id(),
            seqnum_id: file.seqnum_id(),
            file,
        }
    }
}

// Files have already been validated, deduplicated, and converted to reopen snapshots by the
// live snapshot builder. Take ownership so the selected physical copies cannot be reselected.
pub(crate) fn journal_from_snapshot_files(
    files: Vec<JournalFile>,
    config: JournalConfig,
) -> Result<Journal> {
    validate_journal_config(&config)?;
    let file_infos: Vec<JournalFileInfo> = files
        .into_iter()
        .map(JournalFileInfo::from_open_file)
        .collect();
    if file_infos.is_empty() {
        return Err(SdJournalError::NotFound);
    }
    Ok(Journal {
        inner: Arc::new(JournalInner {
            config,
            roots: Vec::new(),
            file_infos,
        }),
    })
}

fn validate_journal_config(config: &JournalConfig) -> Result<()> {
    if config.max_open_files == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_open_files must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn open_journal_files(paths: Vec<PathBuf>, config: &JournalConfig) -> Result<Vec<JournalFileInfo>> {
    let keep_open = paths.len() <= config.max_open_files;
    let mut file_infos = Vec::new();
    let mut first_error = None;
    let mut seen = HashSet::new();

    for path in paths {
        let opened = if keep_open {
            JournalFile::open(path, config)
        } else {
            JournalFile::open_reopen(path, config)
        };
        match opened {
            Ok(file) => {
                #[cfg(feature = "tracing")]
                debug!(path = %file.path().display(), lazy = !keep_open, "opened journal file");
                if seen.insert(file.file_id()) {
                    file_infos.push(JournalFileInfo::from_open_file(file));
                }
            }
            Err(error) => {
                #[cfg(feature = "tracing")]
                warn!(error = %error, "failed to open journal file");
                first_error.get_or_insert(error);
            }
        }
    }
    if file_infos.is_empty() {
        return Err(first_error.unwrap_or(SdJournalError::NotFound));
    }
    Ok(file_infos)
}

pub(crate) fn discover_journal_candidates(
    paths: &[PathBuf],
    config: &JournalConfig,
) -> Result<JournalDiscovery> {
    if paths.is_empty() {
        return Err(SdJournalError::InvalidQuery {
            reason: "open_dirs requires at least one path".to_string(),
        });
    }

    let mut roots = paths.to_vec();
    roots.sort();
    roots.dedup();

    let mut candidates = BTreeSet::new();
    let mut missing_roots = Vec::new();
    for p in &roots {
        match discover_journal_paths(p, config, &mut candidates) {
            Ok(()) => {}
            Err(error) if matches!(&error, SdJournalError::Io { source, .. } if source.kind() == ErrorKind::NotFound) =>
            {
                // The default runtime and persistent roots need not both exist.
                missing_roots.push(p.clone());
            }
            Err(error) => return Err(error),
        }
    }

    Ok(JournalDiscovery {
        roots,
        candidates: candidates.into_iter().collect(),
        missing_roots,
    })
}

fn discover_journal_paths(
    dir: &Path,
    config: &JournalConfig,
    out: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let mut pending_dirs = vec![(dir.to_path_buf(), true)];
    while let Some((path, is_root)) = pending_dirs.pop() {
        let rd = match std::fs::read_dir(&path) {
            Ok(rd) => rd,
            Err(error) if !is_root && error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(SdJournalError::io("read_dir", Some(path), error)),
        };
        for entry in rd {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(SdJournalError::io(
                        "read_dir_entry",
                        Some(path.clone()),
                        error,
                    ));
                }
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(SdJournalError::io("file_type", Some(entry.path()), error));
                }
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() && is_root && is_machine_id_dir(&entry.file_name()) {
                pending_dirs.push((entry.path(), false));
            } else if file_type.is_file() && is_journal_file(&entry.path(), config) {
                insert_journal_candidate(out, entry.path(), config)?;
            }
        }
    }

    Ok(())
}

fn insert_journal_candidate(
    out: &mut BTreeSet<PathBuf>,
    path: PathBuf,
    config: &JournalConfig,
) -> Result<()> {
    out.insert(path);
    if out.len() > config.max_journal_files {
        return Err(SdJournalError::LimitExceeded {
            kind: LimitKind::JournalFiles,
            limit: u64::try_from(config.max_journal_files).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

pub(crate) fn is_machine_id_dir(name: &OsStr) -> bool {
    let s = match name.to_str() {
        Some(s) => s,
        None => return false,
    };
    if s.len() != 32 {
        return false;
    }
    s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn is_journal_file(path: &Path, config: &JournalConfig) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some("journal") => true,
        Some("journal~") => config.include_journal_tilde,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use tempfile::tempdir;

    #[test]
    fn open_dirs_rejects_empty_path_list() {
        match Journal::open_dirs_with_config(&[], JournalConfig::default()) {
            Err(SdJournalError::InvalidQuery { reason }) => {
                assert_eq!(reason, "open_dirs requires at least one path");
            }
            Ok(_) => panic!("expected InvalidQuery"),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn discovery_distinguishes_missing_roots_from_existing_empty_roots() {
        let empty = tempdir().unwrap();
        let missing = empty.path().join("not-present");
        let roots = [empty.path().to_path_buf(), missing.clone()];
        let discovery = discover_journal_candidates(&roots, &JournalConfig::default()).unwrap();
        assert!(discovery.candidates.is_empty());
        assert_eq!(discovery.missing_roots, std::slice::from_ref(&missing));
        assert!(matches!(
            Journal::open_dirs(&roots),
            Err(SdJournalError::NotFound)
        ));

        File::create(empty.path().join("candidate.journal")).unwrap();
        let discovery = discover_journal_candidates(&roots, &JournalConfig::default()).unwrap();
        assert_eq!(discovery.candidates.len(), 1);
        assert_eq!(discovery.missing_roots, [missing]);
    }

    #[test]
    fn machine_id_dir_requires_exactly_32_hex_chars() {
        assert!(is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcdef"
        )));
        assert!(!is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcde"
        )));
        assert!(!is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcdeg"
        )));
        assert!(!is_machine_id_dir(OsStr::new("not-a-machine-id")));
    }

    #[test]
    fn discover_journal_paths_finds_root_and_machine_id_files() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let machine_id = root.join("0123456789abcdef0123456789abcdef");
        let ignored_dir = root.join("not-machine-id");

        fs::create_dir(&machine_id).unwrap();
        fs::create_dir(&ignored_dir).unwrap();

        let root_journal = root.join("root.journal");
        let nested_journal = machine_id.join("nested.journal");
        let tilde_journal = root.join("temp.journal~");
        let ignored_journal = ignored_dir.join("ignored.journal");
        let not_journal = root.join("notes.log");

        File::create(&root_journal).unwrap();
        File::create(&nested_journal).unwrap();
        File::create(&tilde_journal).unwrap();
        File::create(&ignored_journal).unwrap();
        File::create(&not_journal).unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&root_journal, root.join("link.journal")).unwrap();

        let mut out = BTreeSet::new();
        discover_journal_paths(root, &JournalConfig::default(), &mut out).unwrap();

        assert_eq!(
            out,
            BTreeSet::from([nested_journal.clone(), root_journal.clone()])
        );

        out.clear();
        discover_journal_paths(
            root,
            &JournalConfig {
                include_journal_tilde: true,
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out,
            BTreeSet::from([nested_journal, root_journal, tilde_journal])
        );
    }

    #[cfg(unix)]
    #[test]
    fn discovery_deduplicates_non_utf8_paths_before_enforcing_file_limit() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let dir = tempdir().unwrap();
        let machine_dir = dir.path().join("0123456789abcdef0123456789abcdef");
        fs::create_dir(&machine_dir).unwrap();
        for byte in [0x80, 0x81] {
            let name = OsString::from_vec([&[byte], b".journal".as_slice()].concat());
            File::create(machine_dir.join(name)).unwrap();
        }
        let config = JournalConfig {
            max_journal_files: 2,
            ..Default::default()
        };
        let discovery =
            discover_journal_candidates(&[dir.path().to_path_buf(), machine_dir], &config).unwrap();
        assert_eq!(discovery.candidates.len(), 2);
    }

    #[test]
    fn discovery_stops_at_the_configured_limit_even_after_finding_valid_candidates() {
        let dir = tempdir().unwrap();
        for idx in 0..10 {
            File::create(dir.path().join(format!("{idx}.journal"))).unwrap();
        }
        let config = JournalConfig {
            max_journal_files: 2,
            ..Default::default()
        };
        let mut candidates = BTreeSet::new();
        assert!(matches!(
            discover_journal_paths(dir.path(), &config, &mut candidates),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::JournalFiles,
                limit: 2
            })
        ));
        assert_eq!(candidates.len(), 3);
        assert!(matches!(
            discover_journal_candidates(&[dir.path().to_path_buf()], &config),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::JournalFiles,
                limit: 2
            })
        ));
    }
}
