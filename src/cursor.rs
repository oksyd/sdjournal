use crate::error::{Result, SdJournalError};
use crate::util::hex_decode;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SdJournalEntryKey {
    pub(crate) file_id: [u8; 16],
    pub(crate) entry_offset: u64,
    pub(crate) seqnum_id: [u8; 16],
    pub(crate) seqnum: u64,
    pub(crate) boot_id: [u8; 16],
    pub(crate) monotonic_usec: u64,
    pub(crate) realtime_usec: u64,
    pub(crate) xor_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SystemdCursor {
    pub(crate) seqnum_id: Option<[u8; 16]>,
    pub(crate) seqnum: Option<u64>,
    pub(crate) boot_id: Option<[u8; 16]>,
    pub(crate) monotonic_usec: Option<u64>,
    pub(crate) realtime_usec: Option<u64>,
    pub(crate) xor_hash: Option<u64>,
}

impl From<&SdJournalEntryKey> for SystemdCursor {
    fn from(key: &SdJournalEntryKey) -> Self {
        Self {
            seqnum_id: Some(key.seqnum_id),
            seqnum: Some(key.seqnum),
            boot_id: Some(key.boot_id),
            monotonic_usec: Some(key.monotonic_usec),
            realtime_usec: Some(key.realtime_usec),
            xor_hash: Some(key.xor_hash),
        }
    }
}

impl SystemdCursor {
    fn has_supported_position(&self) -> bool {
        (self.seqnum_id.is_some() && self.seqnum.is_some()) || self.realtime_usec.is_some()
    }
}

/// Compare two complete journal locations using systemd's stable merge ordering.
///
/// This intentionally may return [`Ordering::Equal`] for the same logical entry copied into two
/// different files. The conditional relation is not transitive across unrelated sequence/boot
/// streams, so it must only be used for systemd-style pairwise selection or boundary comparison;
/// it must not back [`Ord`], `sort_by`, or a binary heap.
pub(crate) fn compare_entry_keys(left: &SdJournalEntryKey, right: &SdJournalEntryKey) -> Ordering {
    if left.seqnum_id == right.seqnum_id {
        let ord = left.seqnum.cmp(&right.seqnum);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    if left.boot_id == right.boot_id {
        let ord = left.monotonic_usec.cmp(&right.monotonic_usec);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    left.realtime_usec
        .cmp(&right.realtime_usec)
        .then_with(|| left.xor_hash.cmp(&right.xor_hash))
}

pub(crate) fn same_entry(left: &SdJournalEntryKey, right: &SdJournalEntryKey) -> bool {
    left.seqnum_id == right.seqnum_id
        && left.seqnum == right.seqnum
        && left.boot_id == right.boot_id
        && left.monotonic_usec == right.monotonic_usec
        && left.realtime_usec == right.realtime_usec
        && left.xor_hash == right.xor_hash
}

/// Opaque cursor for checkpointing and resuming journal iteration.
///
/// Display and parsing use native systemd cursor text, such as `s=...;i=...;b=...;m=...;t=...;x=...`.
/// Partial cursors must contain both sequence fields (`s` and `i`) or a realtime field (`t`).
/// Unlike systemd, this reader does not support boot-relative cursors containing only `b` and
/// `m`: they provide no boundary for entries from other boots. The retired `SJ1:` format is not
/// accepted.
///
/// Equality and hashing use the logical cursor identity. An in-memory physical location hint
/// accelerates seeking, but does not affect identity and is not persisted.
#[derive(Debug, Clone)]
pub struct Cursor {
    pub(crate) fields: SystemdCursor,
    pub(crate) location_hint: Option<CursorLocation>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CursorLocation {
    pub(crate) file_id: [u8; 16],
    pub(crate) entry_offset: u64,
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for Cursor {}

impl Hash for Cursor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl From<SdJournalEntryKey> for Cursor {
    fn from(key: SdJournalEntryKey) -> Self {
        Self {
            fields: (&key).into(),
            location_hint: Some(CursorLocation {
                file_id: key.file_id,
                entry_offset: key.entry_offset,
            }),
        }
    }
}

impl Cursor {
    /// Parse native systemd cursor text.
    ///
    /// Requires both sequence fields (`s` and `i`) or a realtime field (`t`). Boot-relative
    /// cursors without either return [`SdJournalError::InvalidQuery`]. Full entry cursors and
    /// partial cursors containing `b`, `m`, and `t` are supported.
    ///
    /// The returned cursor can be passed to [`crate::Journal::seek_cursor`] or
    /// [`crate::JournalQuery::after_cursor`].
    pub fn parse(s: &str) -> Result<Self> {
        Ok(Self {
            fields: parse_systemd_cursor(s)?,
            location_hint: None,
        })
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut separator = "";
        if let Some(id) = self.fields.seqnum_id {
            write!(f, "s={:032x}", u128::from_be_bytes(id))?;
            separator = ";";
        }
        if let Some(seqnum) = self.fields.seqnum {
            write!(f, "{separator}i={seqnum:x}")?;
            separator = ";";
        }
        if let Some(id) = self.fields.boot_id {
            write!(f, "{separator}b={:032x}", u128::from_be_bytes(id))?;
            separator = ";";
        }
        if let Some(monotonic) = self.fields.monotonic_usec {
            write!(f, "{separator}m={monotonic:x}")?;
            separator = ";";
        }
        if let Some(realtime) = self.fields.realtime_usec {
            write!(f, "{separator}t={realtime:x}")?;
            separator = ";";
        }
        if let Some(xor_hash) = self.fields.xor_hash {
            write!(f, "{separator}x={xor_hash:x}")?;
        }
        Ok(())
    }
}

fn parse_systemd_cursor(s: &str) -> Result<SystemdCursor> {
    let s = s.trim();
    if s.is_empty() {
        return Err(SdJournalError::InvalidQuery {
            reason: "empty cursor".to_string(),
        });
    }

    fn parse_id128(s: &str, kind: &'static str) -> Result<[u8; 16]> {
        use std::borrow::Cow;

        let (s, urn) = match s.strip_prefix("urn:uuid:") {
            Some(uuid) => (uuid, true),
            None => (s, false),
        };
        let cleaned = if s.len() == 32 && !urn && s.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Cow::Borrowed(s)
        } else if s.len() == 36
            && s.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
        {
            Cow::Owned(s.replace('-', ""))
        } else {
            return Err(SdJournalError::InvalidQuery {
                reason: format!("{kind} must be 32 hex digits, a UUID, or a UUID URN"),
            });
        };

        let bytes = hex_decode(cleaned.as_ref())?;
        let mut out = [0u8; 16];
        out.copy_from_slice(&bytes);
        Ok(out)
    }

    fn parse_hex_u64(s: &str, kind: &'static str) -> Result<u64> {
        u64::from_str_radix(s, 16).map_err(|_| SdJournalError::InvalidQuery {
            reason: format!("{kind} is not valid hex"),
        })
    }

    let mut out = SystemdCursor {
        seqnum_id: None,
        seqnum: None,
        boot_id: None,
        monotonic_usec: None,
        realtime_usec: None,
        xor_hash: None,
    };

    for part in s.split(';') {
        if part.is_empty() {
            return Err(SdJournalError::InvalidQuery {
                reason: "invalid systemd cursor: empty segment".to_string(),
            });
        }
        let bytes = part.as_bytes();
        if bytes.len() < 2 || bytes[1] != b'=' {
            return Err(SdJournalError::InvalidQuery {
                reason: "invalid systemd cursor segment".to_string(),
            });
        }
        let key = bytes[0];
        let value = &part[2..];
        match key {
            b's' => out.seqnum_id = Some(parse_id128(value, "seqnum_id")?),
            b'i' => out.seqnum = Some(parse_hex_u64(value, "seqnum")?),
            b'b' => out.boot_id = Some(parse_id128(value, "boot_id")?),
            b'm' => out.monotonic_usec = Some(parse_hex_u64(value, "monotonic_usec")?),
            b't' => out.realtime_usec = Some(parse_hex_u64(value, "realtime_usec")?),
            b'x' => out.xor_hash = Some(parse_hex_u64(value, "xor_hash")?),
            _ => {}
        }
    }

    if !out.has_supported_position() {
        return Err(SdJournalError::InvalidQuery {
            reason: "cursor requires both sequence fields s and i, or a realtime field t"
                .to_string(),
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::hex_encode;

    #[test]
    fn native_cursor_formats_ids_in_byte_order_with_leading_zeroes() {
        let text = "s=000102030405060708090a0b0c0d0e0f;i=1;b=fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0;t=2";
        let cursor = Cursor::parse(text).unwrap();
        assert_eq!(
            cursor.fields.seqnum_id,
            Some(std::array::from_fn(|i| i as u8))
        );
        assert_eq!(cursor.to_string(), text);

        let zero_ids =
            "s=00000000000000000000000000000000;i=1;b=00000000000000000000000000000000;t=2";
        assert_eq!(Cursor::parse(zero_ids).unwrap().to_string(), zero_ids);
    }

    #[test]
    fn native_cursor_text_roundtrips_supported_field_combinations() {
        for mask in 0..64u8 {
            let fields = SystemdCursor {
                seqnum_id: (mask & 1 != 0).then_some([0x12; 16]),
                seqnum: (mask & 2 != 0).then_some(u64::MAX),
                boot_id: (mask & 4 != 0).then_some([0x34; 16]),
                monotonic_usec: (mask & 8 != 0).then_some(0),
                realtime_usec: (mask & 16 != 0).then_some(0x5678),
                xor_hash: (mask & 32 != 0).then_some(0x9abc),
            };
            let valid = mask & 3 == 3 || mask & 16 != 0;
            assert_eq!(fields.has_supported_position(), valid, "mask {mask}");
            let cursor = Cursor {
                fields,
                location_hint: None,
            };
            let encoded = cursor.to_string();
            assert!(!encoded.starts_with("SJ1:"));
            if mask == 63 {
                assert_eq!(
                    encoded,
                    "s=12121212121212121212121212121212;i=ffffffffffffffff;b=34343434343434343434343434343434;m=0;t=5678;x=9abc"
                );
            }
            if valid {
                let parsed = Cursor::parse(&encoded).unwrap();
                assert_eq!(parsed, cursor, "mask {mask}");
                assert_eq!(parsed.to_string(), encoded, "mask {mask}");
            } else {
                assert!(
                    matches!(
                        Cursor::parse(&encoded),
                        Err(SdJournalError::InvalidQuery { .. })
                    ),
                    "mask {mask}"
                );
            }
        }
    }

    #[test]
    fn cursor_rejects_all_retired_sj1_variants() {
        let raw =
            "s=22222222222222222222222222222222;i=1;b=33333333333333333333333333333333;m=2;t=3;x=4";
        let mut file_offset = vec![1];
        file_offset.extend_from_slice(&[0x11; 16]);
        file_offset.extend_from_slice(&512u64.to_le_bytes());
        let mut raw_systemd = vec![2];
        raw_systemd.extend_from_slice(raw.as_bytes());
        let mut partial_fields = vec![3, 16];
        partial_fields.extend_from_slice(&3u64.to_le_bytes());
        let mut entry_key = vec![4];
        entry_key.extend_from_slice(&[0x11; 16]);
        for value in [512u64, 1, 3] {
            entry_key.extend_from_slice(&value.to_le_bytes());
        }
        let mut location_key = vec![5];
        location_key.extend_from_slice(&[0x11; 16]);
        location_key.extend_from_slice(&512u64.to_le_bytes());
        location_key.extend_from_slice(&[0x22; 16]);
        location_key.extend_from_slice(&1u64.to_le_bytes());
        location_key.extend_from_slice(&[0x33; 16]);
        for value in [2u64, 3, 4] {
            location_key.extend_from_slice(&value.to_le_bytes());
        }

        for bytes in [
            file_offset,
            raw_systemd,
            partial_fields,
            entry_key,
            location_key,
            vec![],
            vec![0xff],
        ] {
            let text = format!("SJ1:{}", hex_encode(&bytes));
            assert!(matches!(
                Cursor::parse(&text),
                Err(SdJournalError::InvalidQuery { .. })
            ));
        }
    }

    #[test]
    fn cursor_systemd_string_is_parsed() {
        let text =
            "s=0123456789abcdef0123456789abcdef;i=1;b=11111111111111111111111111111111;m=2;t=3;x=4";
        let cursor = Cursor::parse(text).unwrap();
        assert_eq!(cursor.to_string(), text);
        assert!(cursor.location_hint.is_none());
        assert_eq!(Cursor::parse(&cursor.to_string()).unwrap(), cursor);
    }

    #[test]
    fn cursor_from_entry_keeps_a_non_persisted_location_hint() {
        let key = SdJournalEntryKey {
            file_id: [0x11; 16],
            entry_offset: 0x1122_3344_5566_7788,
            seqnum_id: [0x22; 16],
            seqnum: 0x99aa_bbcc,
            boot_id: [0x33; 16],
            monotonic_usec: 0x4455,
            realtime_usec: 0x6677,
            xor_hash: 0x8899,
        };
        let cursor = Cursor::from(key);
        let location = cursor.location_hint.unwrap();
        assert_eq!(location.file_id, key.file_id);
        assert_eq!(location.entry_offset, key.entry_offset);
        let encoded = cursor.to_string();
        assert_eq!(
            encoded,
            "s=22222222222222222222222222222222;i=99aabbcc;b=33333333333333333333333333333333;m=4455;t=6677;x=8899"
        );
        let parsed = Cursor::parse(&encoded).unwrap();
        assert_eq!(parsed.fields, SystemdCursor::from(&key));
        assert!(parsed.location_hint.is_none());
        assert_eq!(parsed, cursor);
        assert_eq!(parsed.to_string(), encoded);
    }

    #[test]
    fn cursor_roundtrip_preserves_equality_and_hash_identity() {
        use std::collections::HashSet;

        let key = SdJournalEntryKey {
            file_id: [1; 16],
            entry_offset: 512,
            seqnum_id: [2; 16],
            seqnum: 3,
            boot_id: [4; 16],
            monotonic_usec: 5,
            realtime_usec: 6,
            xor_hash: 7,
        };
        let cursor = Cursor::from(key);
        let parsed = Cursor::parse(&cursor.to_string()).unwrap();
        assert_eq!(cursor, parsed);
        let cursors = HashSet::from([cursor.clone()]);
        assert!(cursors.contains(&parsed));

        let copy = Cursor::from(SdJournalEntryKey {
            file_id: [8; 16],
            entry_offset: 1024,
            ..key
        });
        assert_eq!(copy, cursor);
        assert!(cursors.contains(&copy));
        let different = Cursor::from(SdJournalEntryKey { seqnum: 9, ..key });
        assert_ne!(cursor, different);
        assert!(!cursors.contains(&different));
        assert_ne!(cursor, Cursor::parse("t=6").unwrap());
    }

    #[test]
    fn entry_key_comparison_uses_systemd_priority() {
        let base = SdJournalEntryKey {
            file_id: [1; 16],
            entry_offset: 1,
            seqnum_id: [2; 16],
            seqnum: 10,
            boot_id: [3; 16],
            monotonic_usec: 100,
            realtime_usec: 1_000,
            xor_hash: 9,
        };

        let mut later_sequence = base;
        later_sequence.seqnum = 11;
        later_sequence.realtime_usec = 1;
        assert_eq!(compare_entry_keys(&base, &later_sequence), Ordering::Less);

        let mut later_monotonic = base;
        later_monotonic.seqnum_id = [4; 16];
        later_monotonic.monotonic_usec = 101;
        later_monotonic.realtime_usec = 1;
        assert_eq!(compare_entry_keys(&base, &later_monotonic), Ordering::Less);

        let mut copied = base;
        copied.file_id = [5; 16];
        copied.entry_offset = 99;
        assert_eq!(compare_entry_keys(&base, &copied), Ordering::Equal);
        assert!(same_entry(&base, &copied));
    }

    #[test]
    fn partial_native_cursor_roundtrip() {
        let cursor = Cursor::parse("t=3").unwrap();
        assert_eq!(cursor.to_string(), "t=3");
        assert_eq!(Cursor::parse(&cursor.to_string()).unwrap(), cursor);
    }

    #[test]
    fn boot_relative_cursors_require_sequence_identity_or_realtime() {
        let boot = "b=33333333333333333333333333333333;m=42";
        for suffix in ["", ";x=1234", ";i=1", ";s=22222222222222222222222222222222"] {
            assert!(matches!(
                Cursor::parse(&format!("{boot}{suffix}")),
                Err(SdJournalError::InvalidQuery { .. })
            ));
        }
        for suffix in [";t=0", ";s=22222222222222222222222222222222;i=1"] {
            let cursor = Cursor::parse(&format!("{boot}{suffix}")).unwrap();
            assert_eq!(Cursor::parse(&cursor.to_string()).unwrap(), cursor);
        }
    }

    #[test]
    fn native_cursor_text_is_canonical_and_accepts_unknown_extensions() {
        let cursor = Cursor::parse(
            "s=01234567-89ab-cdef-0123-456789ABCDEF;i=000F;b=00112233-4455-6677-8899-AABBCCDDEEFF;m=00;t=0ABC;x=0001;q=future",
        ).unwrap();
        assert_eq!(
            cursor.to_string(),
            "s=0123456789abcdef0123456789abcdef;i=f;b=00112233445566778899aabbccddeeff;m=0;t=abc;x=1"
        );
        assert_eq!(Cursor::parse(&cursor.to_string()).unwrap(), cursor);
    }

    #[test]
    fn cursor_ids_accept_only_systemd_id128_formats() {
        let canonical =
            "s=0123456789abcdef0123456789abcdef;i=1;b=0123456789abcdef0123456789abcdef;m=2";
        for id in [
            "0123456789abcdef0123456789abcdef",
            "01234567-89ab-cdef-0123-456789ABCDEF",
            "urn:uuid:01234567-89ab-cdef-0123-456789ABCDEF",
        ] {
            let cursor = Cursor::parse(&format!("s={id};i=1;b={id};m=2")).unwrap();
            assert_eq!(cursor.to_string(), canonical);
        }
        for id in [
            "-0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef-",
            "0123456-789ab-cdef-0123-456789abcdef",
            "01234567--89ab-cdef-0123-456789abcdef",
            "urn:uuid:0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef ",
        ] {
            for cursor in [format!("s={id};i=1"), format!("b={id};m=1")] {
                assert!(
                    matches!(
                        Cursor::parse(&cursor),
                        Err(SdJournalError::InvalidQuery { .. })
                    ),
                    "{cursor:?}"
                );
            }
        }
    }

    #[test]
    fn cursor_rejects_invalid_native_fields() {
        for text in [
            "",
            "t=",
            "t=zz",
            "t=10000000000000000",
            "s=00;i=1",
            "b=00;m=1",
            "t=1;",
            "x=1",
            "broken-segment",
        ] {
            assert!(
                matches!(
                    Cursor::parse(text),
                    Err(SdJournalError::InvalidQuery { .. })
                ),
                "{text:?}"
            );
        }
    }
}
