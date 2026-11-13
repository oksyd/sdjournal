use crate::config::JournalConfig;
use crate::entry::EntryRef;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::util::is_ascii_field_name;

const UNIT_FIELDS: [&str; 3] = ["_SYSTEMD_UNIT", "UNIT", "OBJECT_SYSTEMD_UNIT"];

#[derive(Debug, Clone)]
pub(crate) enum MatchTerm {
    Exact { payload: Vec<u8>, eq_pos: usize },
    Unit { payloads: [Vec<u8>; 3] },
    Present { field: String },
}

impl MatchTerm {
    fn exact(field: &str, value: &[u8]) -> Self {
        Self::Exact {
            payload: Self::payload(field, value),
            eq_pos: field.len(),
        }
    }

    fn payload(field: &str, value: &[u8]) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(field.len().saturating_add(1).saturating_add(value.len()));
        payload.extend_from_slice(field.as_bytes());
        payload.push(b'=');
        payload.extend_from_slice(value);
        payload
    }

    fn max_field_name_len(&self) -> usize {
        match self {
            Self::Exact { eq_pos, .. } => *eq_pos,
            Self::Unit { .. } => "OBJECT_SYSTEMD_UNIT".len(),
            Self::Present { field } => field.len(),
        }
    }

    fn term_count(&self) -> usize {
        self.exact_payloads().len().max(1)
    }

    pub(crate) fn exact_payloads(&self) -> &[Vec<u8>] {
        match self {
            Self::Exact { payload, .. } => std::slice::from_ref(payload),
            Self::Unit { payloads } => payloads,
            Self::Present { .. } => &[],
        }
    }

    pub(crate) fn matches<'a>(
        &self,
        mut fields: impl Iterator<Item = (&'a str, &'a [u8])>,
    ) -> bool {
        match self {
            Self::Exact { payload, eq_pos } => {
                let (field, value) = payload.split_at(*eq_pos);
                fields.any(|(name, candidate)| name.as_bytes() == field && candidate == &value[1..])
            }
            Self::Unit { payloads } => fields.any(|(name, candidate)| {
                UNIT_FIELDS.contains(&name) && candidate == &payloads[0][UNIT_FIELDS[0].len() + 1..]
            }),
            Self::Present { field } => fields.any(|(name, _)| name == field),
        }
    }
}

/// Shared filter builder for historical queries and live subscriptions.
///
/// Create one with [`crate::Journal::filter`] or [`crate::LiveJournal::filter`]. Direct terms are
/// AND-ed together; each [`Filter::or_group`] adds an alternative branch of AND-ed terms.
/// Validation is deferred until query execution or subscription registration, where the
/// destination reader's limits are also enforced.
///
/// # Example
///
/// ```no_run
/// use sdjournal::Journal;
///
/// let journal = Journal::open_default()?;
/// let mut filter = journal.filter();
/// filter.match_present("MESSAGE").or_group(|group| {
///     group.match_exact("_SYSTEMD_UNIT", b"sshd.service");
/// });
/// let _recent = journal.query_with_filter(filter.clone()).limit(10).collect_owned()?;
/// let mut live = journal.live()?;
/// let _subscription = live.subscribe(filter)?;
/// # Ok::<(), sdjournal::SdJournalError>(())
/// ```
#[derive(Clone)]
pub struct Filter {
    pub(crate) global_terms: Vec<MatchTerm>,
    pub(crate) or_groups: Vec<Vec<MatchTerm>>,
    invalid_reason: Option<&'static str>,
    too_many_terms: bool,
    max_field_name_len: usize,
    max_query_terms: usize,
}

impl Filter {
    pub(crate) fn new(config: &JournalConfig) -> Self {
        Self {
            global_terms: Vec::new(),
            or_groups: Vec::new(),
            invalid_reason: None,
            too_many_terms: false,
            max_field_name_len: config.max_field_name_len,
            max_query_terms: config.max_query_terms,
        }
    }

    /// Match entries whose field equals `value` byte-for-byte.
    pub fn match_exact(&mut self, field: &str, value: &[u8]) -> &mut Self {
        if self.can_add_term(field) {
            self.global_terms.push(MatchTerm::exact(field, value));
        }
        self
    }

    /// Match entries that contain `field`, regardless of its value.
    pub fn match_present(&mut self, field: &str) -> &mut Self {
        if self.can_add_term(field) {
            self.global_terms.push(MatchTerm::Present {
                field: field.to_string(),
            });
        }
        self
    }

    fn can_add_term(&mut self, field: &str) -> bool {
        if self.invalid_reason.is_some() {
            return false;
        }
        if let Some(reason) = invalid_field_name(field, self.max_field_name_len) {
            self.invalid_reason = Some(reason);
            return false;
        }
        if self.count_terms() >= self.max_query_terms {
            self.too_many_terms = true;
            return false;
        }
        true
    }

    /// Match entries for a systemd unit through `_SYSTEMD_UNIT`, `UNIT`, or `OBJECT_SYSTEMD_UNIT`.
    ///
    /// Each call adds an AND condition, independently of the order in which other terms or
    /// OR-groups are added. It counts as three exact matches toward the query-term limit.
    pub fn match_unit(&mut self, unit: &str) -> &mut Self {
        self.match_unit_bytes(unit.as_bytes())
    }

    /// Same as [`Filter::match_unit`], but accepts the unit name as raw bytes.
    pub fn match_unit_bytes(&mut self, unit: &[u8]) -> &mut Self {
        if self.invalid_reason.is_some() {
            return self;
        }
        for field in UNIT_FIELDS {
            if let Some(reason) = invalid_field_name(field, self.max_field_name_len) {
                self.invalid_reason = Some(reason);
                return self;
            }
        }
        if self.max_query_terms.saturating_sub(self.count_terms()) < UNIT_FIELDS.len() {
            self.too_many_terms = true;
            return self;
        }

        self.global_terms.push(MatchTerm::Unit {
            payloads: UNIT_FIELDS.map(|field| MatchTerm::payload(field, unit)),
        });
        self
    }

    /// Add an alternative branch whose terms are AND-ed together; empty groups are ignored.
    pub fn or_group(&mut self, f: impl FnOnce(&mut OrGroupBuilder)) -> &mut Self {
        if self.invalid_reason.is_some() {
            return self;
        }
        let mut builder = OrGroupBuilder {
            terms: Vec::new(),
            max_field_name_len: self.max_field_name_len,
            invalid_reason: None,
            too_many_terms: false,
            remaining: self.max_query_terms.saturating_sub(self.count_terms()),
        };
        f(&mut builder);
        if let Some(reason) = builder.invalid_reason {
            self.invalid_reason = Some(reason);
        } else if builder.too_many_terms {
            self.too_many_terms = true;
        } else if !builder.terms.is_empty() {
            self.or_groups.push(builder.terms);
        }
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(reason) = &self.invalid_reason {
            return Err(SdJournalError::InvalidQuery {
                reason: reason.to_string(),
            });
        }
        if self.too_many_terms {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: u64::try_from(self.max_query_terms).unwrap_or(u64::MAX),
            });
        }
        Ok(())
    }

    pub(crate) fn validate_for(&self, config: &JournalConfig) -> Result<()> {
        self.validate()?;
        if self.count_terms() > config.max_query_terms {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: u64::try_from(config.max_query_terms).unwrap_or(u64::MAX),
            });
        }
        for term in self
            .global_terms
            .iter()
            .chain(self.or_groups.iter().flatten())
        {
            // Field syntax was validated before storage; only the destination's length limit
            // can differ when a filter is reused with another journal.
            if term.max_field_name_len() > config.max_field_name_len {
                return Err(SdJournalError::InvalidQuery {
                    reason: "field name too long".to_string(),
                });
            }
        }
        Ok(())
    }

    fn count_terms(&self) -> usize {
        self.global_terms
            .iter()
            .chain(self.or_groups.iter().flatten())
            .map(MatchTerm::term_count)
            .sum()
    }

    pub(crate) fn matches(&self, entry: &EntryRef) -> bool {
        self.global_terms
            .iter()
            .all(|term| term.matches(entry.iter_fields()))
            && (self.or_groups.is_empty()
                || self
                    .or_groups
                    .iter()
                    .any(|group| group.iter().all(|term| term.matches(entry.iter_fields()))))
    }

    pub(crate) fn branches(&self) -> Vec<Vec<&MatchTerm>> {
        if self.or_groups.is_empty() {
            return vec![self.global_terms.iter().collect()];
        }
        self.or_groups
            .iter()
            .map(|group| self.global_terms.iter().chain(group).collect())
            .collect()
    }
}

/// Builder used inside historical query and live filter OR-group closures.
///
/// Multiple terms added to the same builder are AND-ed together.
pub struct OrGroupBuilder {
    terms: Vec<MatchTerm>,
    max_field_name_len: usize,
    invalid_reason: Option<&'static str>,
    too_many_terms: bool,
    remaining: usize,
}

impl OrGroupBuilder {
    /// Add an exact field match to this OR-group.
    pub fn match_exact(&mut self, field: &str, value: &[u8]) -> &mut Self {
        if self.can_add_term(field) {
            self.terms.push(MatchTerm::exact(field, value));
        }
        self
    }

    /// Add a field-presence match to this OR-group.
    pub fn match_present(&mut self, field: &str) -> &mut Self {
        if self.can_add_term(field) {
            self.terms.push(MatchTerm::Present {
                field: field.to_string(),
            });
        }
        self
    }

    fn can_add_term(&mut self, field: &str) -> bool {
        if self.invalid_reason.is_some() {
            return false;
        }
        if let Some(reason) = invalid_field_name(field, self.max_field_name_len) {
            self.invalid_reason = Some(reason);
            return false;
        }
        if self.terms.len() >= self.remaining {
            self.too_many_terms = true;
            return false;
        }
        true
    }
}

fn invalid_field_name(field: &str, max_len: usize) -> Option<&'static str> {
    if field.is_empty() {
        Some("field name must not be empty")
    } else if field.len() > max_len {
        Some("field name too long")
    } else if !is_ascii_field_name(field.as_bytes()) {
        Some("field name must be ASCII and must not contain '='")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{EntryRef, FieldRef};
    use crate::error::SdJournalError;
    use crate::filter::MatchTerm;
    use crate::reader::ByteBuf;

    fn sample_entry() -> EntryRef {
        EntryRef::new_parsed(
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
                FieldRef {
                    name: "MESSAGE".into(),
                    payload: ByteBuf::from_vec(b"MESSAGE=hello".to_vec()),
                },
                FieldRef {
                    name: "_SYSTEMD_UNIT".into(),
                    payload: ByteBuf::from_vec(b"_SYSTEMD_UNIT=sshd.service".to_vec()),
                },
                FieldRef {
                    name: "UNIT".into(),
                    payload: ByteBuf::from_vec(b"UNIT=helper.service".to_vec()),
                },
            ],
        )
    }

    #[test]
    fn filter_match_unit_matches_common_unit_fields() {
        let mut filter = Filter::new(&JournalConfig::default());
        filter.match_unit("sshd.service");
        filter.validate().expect("filter should compile");

        assert!(filter.matches(&sample_entry()));
    }

    #[test]
    fn unit_helpers_validate_every_generated_field_name() {
        for max_field_name_len in [0, 4, 13, 18, 19] {
            let mut filter = Filter::new(&JournalConfig {
                max_field_name_len,
                ..Default::default()
            });
            filter.match_unit_bytes(b"sshd.service");
            if max_field_name_len < "OBJECT_SYSTEMD_UNIT".len() {
                assert!(matches!(
                    filter.validate(),
                    Err(SdJournalError::InvalidQuery { .. })
                ));
                assert!(filter.or_groups.is_empty());
                assert!(filter.global_terms.is_empty());
            } else {
                filter.validate().unwrap();
                assert!(filter.or_groups.is_empty());
                assert_eq!(filter.global_terms.len(), 1);
                assert_eq!(filter.count_terms(), 3);
            }
        }
    }

    #[test]
    fn unit_conditions_and_or_groups_match_independently_of_builder_order() {
        for unit_first in [false, true] {
            let mut filter = Filter::new(&JournalConfig::default());
            if unit_first {
                filter.match_unit("other.service");
            }
            filter.or_group(|group| {
                group.match_present("MESSAGE");
            });
            if !unit_first {
                filter.match_unit("other.service");
            }
            assert!(!filter.matches(&sample_entry()));

            let mut filter = Filter::new(&JournalConfig::default());
            filter.match_unit("sshd.service").or_group(|group| {
                group.match_present("ABSENT");
            });
            assert!(!filter.matches(&sample_entry()));
        }
    }

    #[test]
    fn repeated_unit_matches_are_anded_and_use_a_linear_term_budget() {
        let config = JournalConfig {
            max_query_terms: 6,
            ..Default::default()
        };
        let mut filter = Filter::new(&config);
        filter.match_unit("sshd.service").match_unit("sshd.service");
        filter.validate_for(&config).unwrap();
        assert!(filter.matches(&sample_entry()));
        assert_eq!(filter.count_terms(), 6);
        assert!(matches!(
            filter.validate_for(&JournalConfig {
                max_query_terms: 5,
                ..config.clone()
            }),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: 5
            })
        ));
        filter.match_present("MESSAGE");
        assert!(matches!(
            filter.validate(),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: 6
            })
        ));

        let mut filter = Filter::new(&config);
        filter
            .match_unit("sshd.service")
            .match_unit("helper.service");
        filter.validate_for(&config).unwrap();
        assert!(filter.matches(&sample_entry()));

        let mut filter = Filter::new(&config);
        filter
            .match_unit("sshd.service")
            .match_unit("other.service");
        filter.validate_for(&config).unwrap();
        assert!(!filter.matches(&sample_entry()));
    }

    #[test]
    fn destination_validation_preserves_a_reusable_filter() {
        let config = JournalConfig::default();
        let mut filter = Filter::new(&config);
        filter.match_present("MESSAGE").or_group(|group| {
            group.match_exact("PRIORITY", b"3");
        });
        assert!(matches!(
            filter.validate_for(&JournalConfig {
                max_query_terms: 1,
                ..config.clone()
            }),
            Err(SdJournalError::LimitExceeded {
                kind: LimitKind::QueryTerms,
                limit: 1
            })
        ));
        assert!(matches!(
            filter.validate_for(&JournalConfig {
                max_field_name_len: 7,
                ..config.clone()
            }),
            Err(SdJournalError::InvalidQuery { .. })
        ));
        filter.validate_for(&config).unwrap();
        assert_eq!(filter.count_terms(), 2);
    }

    #[test]
    fn filter_rejects_an_empty_field_name() {
        let mut filter = Filter::new(&JournalConfig::default());
        filter.match_present("");
        assert!(matches!(
            filter.validate(),
            Err(SdJournalError::InvalidQuery { reason })
                if reason.contains("field name must not be empty")
        ));
    }

    #[test]
    fn filter_or_group_matches_existing_branch_style() {
        let mut filter = Filter::new(&JournalConfig::default());
        filter.match_present("MESSAGE");
        filter.or_group(|group| {
            group.match_exact("PRIORITY", b"3");
        });
        filter.or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"sshd.service");
        });
        filter.validate().expect("filter should compile");

        assert!(filter.matches(&sample_entry()));
    }

    fn term_matches(entry: &EntryRef, term: &MatchTerm) -> bool {
        term.matches(entry.iter_fields())
    }

    #[test]
    fn term_matches_handles_exact_and_present_terms() {
        let entry = sample_entry();

        assert!(term_matches(
            &entry,
            &MatchTerm::Exact {
                payload: b"MESSAGE=hello".to_vec(),
                eq_pos: 7,
            }
        ));
        assert!(term_matches(
            &entry,
            &MatchTerm::Present {
                field: "_SYSTEMD_UNIT".to_string(),
            }
        ));
        assert!(!term_matches(
            &entry,
            &MatchTerm::Exact {
                payload: b"MESSAGE=missing".to_vec(),
                eq_pos: 7,
            }
        ));
    }
}
