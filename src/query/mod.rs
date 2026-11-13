mod cursor;
mod execute;

pub(crate) use self::execute::JournalIter;
use crate::cursor::Cursor;
use crate::entry::{EntryOwned, EntryRef};
#[cfg(test)]
use crate::error::LimitKind;
use crate::error::{Result, SdJournalError};
#[cfg(test)]
use crate::filter::MatchTerm;
use crate::filter::{Filter, OrGroupBuilder};
use crate::journal::Journal;

/// A query builder for reading entries from a [`Journal`].
///
/// `JournalQuery` is mutable and chainable: each builder method updates the query in place and
/// returns `&mut Self`.
///
/// Terms added directly to the query are AND-ed together. Each [`JournalQuery::or_group`] call
/// adds one alternative branch; terms inside that group are also AND-ed together. The full query
/// is therefore:
///
/// `global_terms AND (group_1 OR group_2 OR ...)`
///
/// Validation for field names and query-term limits is intentionally deferred. Builder methods
/// record validation failures internally, and terminal methods such as [`JournalQuery::iter`] and
/// [`JournalQuery::collect_owned`] surface them as [`SdJournalError`] values.
///
/// # Example
///
/// ```no_run
/// use sdjournal::Journal;
///
/// let journal = Journal::open_default()?;
/// let mut query = journal.query();
/// query
///     .match_present("MESSAGE")
///     .or_group(|g| {
///         g.match_exact("_SYSTEMD_UNIT", b"sshd.service");
///     })
///     .or_group(|g| {
///         g.match_exact("_SYSTEMD_UNIT", b"systemd.service");
///     })
///     .limit(10);
///
/// for item in query.iter()? {
///     let entry = item?;
///     println!("{}", entry.realtime_usec());
/// }
/// # Ok::<(), sdjournal::SdJournalError>(())
/// ```
#[derive(Clone)]
pub struct JournalQuery {
    journal: Journal,

    filter: Filter,

    since_realtime: Option<u64>,
    until_realtime: Option<u64>,
    cursor_start: Option<(Cursor, bool)>, // (cursor, inclusive)
    reverse: bool,
    limit: Option<usize>,
}

impl JournalQuery {
    pub(crate) fn new(journal: Journal, filter: Filter) -> Self {
        Self {
            filter,
            journal,
            since_realtime: None,
            until_realtime: None,
            cursor_start: None,
            reverse: false,
            limit: None,
        }
    }

    /// Match entries whose field equals `value` byte-for-byte.
    ///
    /// Multiple exact and presence terms added directly to the query are AND-ed together.
    ///
    /// Field-name validation is deferred until a terminal method is called.
    pub fn match_exact(&mut self, field: &str, value: &[u8]) -> &mut Self {
        self.filter.match_exact(field, value);
        self
    }

    /// Match entries that contain `field`, regardless of its value.
    ///
    /// Multiple exact and presence terms added directly to the query are AND-ed together.
    ///
    /// Field-name validation is deferred until a terminal method is called.
    pub fn match_present(&mut self, field: &str) -> &mut Self {
        self.filter.match_present(field);
        self
    }

    /// Match entries for a specific systemd unit.
    ///
    /// This expands to an OR over common unit fields:
    /// `(_SYSTEMD_UNIT=unit) OR (UNIT=unit) OR (OBJECT_SYSTEMD_UNIT=unit)`.
    ///
    /// Each call is AND-ed with all other query conditions, including OR-groups added before or
    /// after it. It counts as three exact matches toward the query-term limit.
    pub fn match_unit(&mut self, unit: &str) -> &mut Self {
        self.match_unit_bytes(unit.as_bytes())
    }

    /// Same as [`JournalQuery::match_unit`], but accepts the unit name as raw bytes.
    pub fn match_unit_bytes(&mut self, unit: &[u8]) -> &mut Self {
        self.filter.match_unit_bytes(unit);
        self
    }

    /// Add an OR-group to the query.
    ///
    /// Each call creates one OR branch. Terms added inside the closure are AND-ed together within
    /// that branch. Empty groups are ignored.
    ///
    /// For example, the following represents:
    ///
    /// `_SYSTEMD_UNIT=sshd.service AND ((_PID=1 AND _UID=0) OR (PRIORITY=3))`
    ///
    /// ```no_run
    /// # use sdjournal::Journal;
    /// # let journal = Journal::open_default()?;
    /// let mut query = journal.query();
    /// query
    ///     .match_exact("_SYSTEMD_UNIT", b"sshd.service")
    ///     .or_group(|g| {
    ///         g.match_exact("_PID", b"1");
    ///         g.match_exact("_UID", b"0");
    ///     })
    ///     .or_group(|g| {
    ///         g.match_exact("PRIORITY", b"3");
    ///     });
    /// # Ok::<(), sdjournal::SdJournalError>(())
    /// ```
    pub fn or_group<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut OrGroupBuilder),
    {
        self.filter.or_group(f);
        self
    }

    /// Set an inclusive lower realtime bound in microseconds since the Unix epoch.
    pub fn since_realtime(&mut self, usec: u64) -> &mut Self {
        self.since_realtime = Some(usec);
        self
    }

    /// Set an inclusive upper realtime bound in microseconds since the Unix epoch.
    pub fn until_realtime(&mut self, usec: u64) -> &mut Self {
        self.until_realtime = Some(usec);
        self
    }

    /// Resume strictly after `cursor`.
    ///
    /// Unlike [`Journal::seek_cursor`], this excludes the entry identified by `cursor`.
    /// Full entry cursors are preferred for checkpoints. Partial cursors must satisfy
    /// [`Cursor::parse`]'s sequence-identity or realtime requirements.
    pub fn after_cursor(&mut self, cursor: Cursor) -> &mut Self {
        self.cursor_start = Some((cursor, false));
        self
    }

    /// Seek to the start of the journal (oldest entries).
    ///
    /// This clears any cursor-based starting position and disables `reverse`.
    pub fn seek_head(&mut self) -> &mut Self {
        self.cursor_start = None;
        self.reverse = false;
        self
    }

    /// Seek to the end of the journal (newest entries).
    ///
    /// This clears any cursor-based starting position and enables `reverse`.
    pub fn seek_tail(&mut self) -> &mut Self {
        self.cursor_start = None;
        self.reverse = true;
        self
    }

    /// Control whether results are returned newest-first instead of oldest-first.
    ///
    /// `false` is the default.
    pub fn reverse(&mut self, reverse: bool) -> &mut Self {
        self.reverse = reverse;
        self
    }

    /// Limit the number of returned entries.
    ///
    /// Passing `0` produces an empty iterator.
    pub fn limit(&mut self, n: usize) -> &mut Self {
        self.limit = Some(n);
        self
    }

    /// Validate the query and create a streaming iterator of matching entries.
    ///
    /// Multi-file queries merge complete entry metadata before filtering, so adding a filter
    /// does not change cross-file ordering or cursor positions. Exact cursor resumes also scan
    /// metadata through the checkpoint. For these scans, field values are decoded only after
    /// cursor and time bounds have been applied, without skipping metadata using field indexes.
    ///
    /// Logical copies simultaneously at the current position of multiple files are merged once.
    /// This streaming iterator does not retain an unbounded set of previously returned entries:
    /// copies reached at different times in overlapping files can be returned again.
    ///
    /// An entry error does not inherently exhaust the iterator. Callers may continue to read
    /// later entries from sources whose metadata or indexes still allow traversal.
    ///
    /// The iterator yields [`EntryRef`] values, which borrow or share underlying journal storage
    /// when possible. Convert entries with [`EntryRef::to_owned`] if they must outlive the
    /// iterator's immediate processing path.
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<EntryRef>> + use<>> {
        self.validate()?;
        JournalIter::new(self.clone())
    }

    /// Collect all matching entries into owned values.
    ///
    /// This is a convenience wrapper around [`JournalQuery::iter`] plus
    /// [`EntryRef::to_owned`](crate::EntryRef::to_owned). Unless [`JournalQuery::limit`] has been
    /// set, this method may allocate memory proportional to every matching entry. Prefer
    /// [`JournalQuery::collect_owned_limited`] when processing untrusted or potentially large
    /// journals.
    pub fn collect_owned(&self) -> Result<Vec<EntryOwned>> {
        self.iter()?
            .map(|item| item.map(|entry| entry.to_owned()))
            .collect()
    }

    /// Collect at most `max_entries` matching entries into owned values.
    ///
    /// This applies the lower of `max_entries` and any existing [`JournalQuery::limit`] without
    /// mutating the query. Passing `0` returns an empty vector.
    pub fn collect_owned_limited(&self, max_entries: usize) -> Result<Vec<EntryOwned>> {
        let mut query = self.clone();
        query.limit = Some(
            query
                .limit
                .map_or(max_entries, |limit| limit.min(max_entries)),
        );
        query
            .into_iter()?
            .map(|item| item.map(|entry| entry.to_owned()))
            .collect()
    }

    pub(crate) fn set_cursor_start(&mut self, cursor: Cursor, inclusive: bool) {
        self.cursor_start = Some((cursor, inclusive));
    }

    pub(crate) fn into_iter(self) -> Result<JournalIter> {
        self.validate()?;
        JournalIter::new(self)
    }

    fn validate(&self) -> Result<()> {
        self.filter.validate_for(&self.journal.inner.config)?;

        if let (Some(since), Some(until)) = (self.since_realtime, self.until_realtime)
            && since > until
        {
            return Err(SdJournalError::InvalidQuery {
                reason: "since_realtime must be <= until_realtime".to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
fn build_branches(query: &JournalQuery) -> Vec<Vec<&MatchTerm>> {
    query.filter.branches()
}

#[cfg(test)]
fn term_matches(entry: &EntryOwned, term: &MatchTerm) -> bool {
    term.matches(entry.iter_fields())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JournalConfig;
    use crate::journal::JournalInner;
    use std::sync::Arc;

    fn empty_journal_with_config(config: JournalConfig) -> Journal {
        Journal {
            inner: Arc::new(JournalInner {
                config,
                roots: Vec::new(),
                file_infos: Vec::new(),
            }),
        }
    }

    #[test]
    fn invalid_field_name_rejected_on_iter() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.match_exact("BAD=FIELD", b"x");
        match q.iter() {
            Ok(_) => panic!("expected InvalidQuery"),
            Err(err) => assert!(matches!(err, SdJournalError::InvalidQuery { .. })),
        }
    }

    #[test]
    fn empty_field_name_rejected_on_iter() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.match_present("");
        match q.iter() {
            Ok(_) => panic!("expected InvalidQuery"),
            Err(SdJournalError::InvalidQuery { reason }) => {
                assert_eq!(reason, "field name must not be empty");
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[test]
    fn too_many_terms_rejected_on_iter() {
        let cfg = JournalConfig {
            max_query_terms: 1,
            ..Default::default()
        };
        let journal = empty_journal_with_config(cfg);
        let mut q = journal.query();
        q.match_present("A");
        q.match_present("B");
        match q.iter() {
            Ok(_) => panic!("expected QueryTerms limit error"),
            Err(err) => assert!(matches!(
                err,
                SdJournalError::LimitExceeded {
                    kind: LimitKind::QueryTerms,
                    ..
                }
            )),
        }
    }

    #[test]
    fn match_unit_adds_one_condition_with_three_index_alternatives() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.match_present("PRIORITY");
        q.match_unit("sshd.service");

        assert!(q.filter.or_groups.is_empty());
        let branches = build_branches(&q);
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].len(), 2);
        assert!(matches!(&branches[0][0], MatchTerm::Present { field } if field == "PRIORITY"));
        let unit_payloads: std::collections::BTreeSet<&[u8]> = branches[0][1]
            .exact_payloads()
            .iter()
            .map(Vec::as_slice)
            .collect();
        assert_eq!(
            unit_payloads,
            std::collections::BTreeSet::from([
                b"_SYSTEMD_UNIT=sshd.service".as_slice(),
                b"OBJECT_SYSTEMD_UNIT=sshd.service".as_slice(),
                b"UNIT=sshd.service".as_slice(),
            ])
        );
    }

    #[test]
    fn repeated_unit_conditions_do_not_expand_or_branches() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.or_group(|g| {
            g.match_present("A");
        });
        q.or_group(|g| {
            g.match_present("B");
        });
        q.match_unit("foo.service");
        q.match_unit("bar.service");

        assert_eq!(q.filter.or_groups.len(), 2);
        assert_eq!(q.filter.global_terms.len(), 2);
        let branches = build_branches(&q);
        assert_eq!(branches.len(), 2);
        for branch in branches {
            assert_eq!(branch.len(), 3);
            assert!(matches!(branch[0], MatchTerm::Unit { .. }));
            assert!(matches!(branch[1], MatchTerm::Unit { .. }));
            assert!(matches!(branch[2], MatchTerm::Present { .. }));
        }
    }

    #[test]
    fn match_unit_respects_max_query_terms() {
        let cfg = JournalConfig {
            max_query_terms: 2,
            ..Default::default()
        };
        let journal = empty_journal_with_config(cfg);
        let mut q = journal.query();
        q.match_unit("sshd.service");

        assert!(matches!(
            q.iter(),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: 2
            })
        ));
        assert!(q.filter.or_groups.is_empty());
    }

    #[test]
    fn since_realtime_must_not_exceed_until_realtime() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.since_realtime(20);
        q.until_realtime(10);

        match q.iter() {
            Ok(_) => panic!("expected InvalidQuery"),
            Err(err) => assert!(matches!(err, SdJournalError::InvalidQuery { .. })),
        }
    }

    #[test]
    fn seek_head_clears_cursor_start_and_disables_reverse() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.after_cursor(Cursor::parse("t=42").unwrap());
        q.reverse(true);

        q.seek_head();

        assert!(q.cursor_start.is_none());
        assert!(!q.reverse);
    }

    #[test]
    fn seek_tail_clears_cursor_start_and_enables_reverse() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.after_cursor(Cursor::parse("t=42").unwrap());

        q.seek_tail();

        assert!(q.cursor_start.is_none());
        assert!(q.reverse);
    }

    #[test]
    fn limit_zero_produces_empty_iterator() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.limit(0);

        let mut it = q.iter().expect("iter should succeed");
        assert!(it.next().is_none());
    }

    #[test]
    fn or_group_invalid_field_name_rejected_on_iter() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.or_group(|g| {
            g.match_exact("BAD=FIELD", b"x");
        });

        match q.iter() {
            Ok(_) => panic!("expected InvalidQuery"),
            Err(err) => assert!(matches!(err, SdJournalError::InvalidQuery { .. })),
        }
    }

    #[test]
    fn or_group_respects_remaining_term_budget() {
        let cfg = JournalConfig {
            max_query_terms: 1,
            ..Default::default()
        };
        let journal = empty_journal_with_config(cfg);
        let mut q = journal.query();
        q.match_present("A");
        q.or_group(|g| {
            g.match_present("B");
        });

        match q.iter() {
            Ok(_) => panic!("expected QueryTerms limit error"),
            Err(err) => assert!(matches!(
                err,
                SdJournalError::LimitExceeded {
                    kind: LimitKind::QueryTerms,
                    ..
                }
            )),
        }
    }

    #[test]
    fn empty_or_group_is_ignored() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();

        q.or_group(|_| {});

        assert!(q.filter.or_groups.is_empty());
        assert!(q.filter.global_terms.is_empty());
    }

    #[test]
    fn after_cursor_sets_exclusive_start() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        let cursor = Cursor::parse("t=42").unwrap();

        q.after_cursor(cursor.clone());

        match &q.cursor_start {
            Some((saved, inclusive)) => {
                assert_eq!(saved.to_string(), cursor.to_string());
                assert!(!inclusive);
            }
            None => panic!("expected cursor_start to be set"),
        }
    }

    #[test]
    fn build_branches_without_or_groups_uses_global_terms_only() {
        let journal = empty_journal_with_config(JournalConfig::default());
        let mut q = journal.query();
        q.match_present("PRIORITY");

        let branches = build_branches(&q);
        assert_eq!(branches.len(), 1);
        assert!(matches!(
            &branches[0][0],
            MatchTerm::Present { field } if field == "PRIORITY"
        ));
    }

    #[test]
    fn term_matches_handles_exact_and_present_terms() {
        let entry = EntryOwned::new(
            crate::cursor::SdJournalEntryKey {
                file_id: [0x11; 16],
                entry_offset: 7,
                seqnum_id: [0x12; 16],
                seqnum: 9,
                realtime_usec: 11,
                monotonic_usec: 13,
                boot_id: [0x22; 16],
                xor_hash: 15,
            },
            vec![
                ("MESSAGE".to_string(), b"hello".to_vec()),
                ("PRIORITY".to_string(), b"6".to_vec()),
            ],
        );

        assert!(term_matches(
            &entry,
            &MatchTerm::Exact {
                payload: b"MESSAGE=hello".to_vec(),
                eq_pos: 7,
            }
        ));
        assert!(!term_matches(
            &entry,
            &MatchTerm::Exact {
                payload: b"MESSAGE=nope".to_vec(),
                eq_pos: 7,
            }
        ));
        assert!(term_matches(
            &entry,
            &MatchTerm::Present {
                field: "PRIORITY".to_string(),
            }
        ));
        assert!(!term_matches(
            &entry,
            &MatchTerm::Present {
                field: "SYSLOG_IDENTIFIER".to_string(),
            }
        ));
    }

    #[test]
    fn field_name_length_limit_is_inclusive() {
        let cfg = JournalConfig {
            max_field_name_len: 3,
            ..Default::default()
        };
        let journal = empty_journal_with_config(cfg);
        let mut q = journal.query();

        q.match_present("ABC");
        assert!(q.iter().is_ok());

        q.match_present("ABCD");
        assert!(matches!(
            q.iter(),
            Err(SdJournalError::InvalidQuery { reason }) if reason == "field name too long"
        ));
    }
}
