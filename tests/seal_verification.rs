#![cfg(feature = "verify-seal")]

use sdjournal::{Journal, JournalConfig, MmapPolicy, SdJournalError};
use std::fs;

const VERIFICATION_KEY: &str = "000102030405060708090a0b/1-10";
const DATA_HASH: u64 = 0x87dd_eff2_fd1b_d06d;

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_object(bytes: &mut [u8], offset: usize, object_type: u8, size: u64) {
    bytes[offset] = object_type;
    put_u64(bytes, offset + 8, size);
}

fn put_mac(bytes: &mut [u8], offset: usize, mac: &str) {
    assert_eq!(mac.len(), 64);
    for i in 0..32 {
        bytes[offset + i] = u8::from_str_radix(&mac[2 * i..2 * i + 2], 16).unwrap();
    }
}

// The MACs below were computed independently of sdjournal using SHA-256 HMAC
// and the epoch 0/1 keys in systemd's fixed FSPRG vectors:
// https://github.com/systemd/systemd/blob/a7fc55f8654bef6a01c72ae7a6efa3357d1f344b/src/test/test-fsprg.c
// Authenticated byte ranges follow journal_authenticate_put_header/object:
// https://github.com/systemd/systemd/blob/main/src/shared/journal-authenticate.c
// No production hashing, key derivation, or sealing code builds this fixture.
fn sealed_fixture(compact: bool, continuous: bool) -> Vec<u8> {
    let entry = if compact { 536 } else { 528 };
    let payload = if compact { 520 } else { 512 };
    let mut bytes = vec![0; 704];
    bytes[..8].copy_from_slice(b"LPKSHHRH");
    put_u32(&mut bytes, 8, if continuous { 5 } else { 1 });
    put_u32(&mut bytes, 12, if compact { 16 } else { 0 });
    bytes[16] = 2;
    bytes[24..40].fill(0x11);
    bytes[40..56].fill(0x22);
    bytes[56..72].fill(0x33);
    bytes[72..88].fill(0x44);
    for (offset, value) in [
        (88, 272),
        (96, 432),
        (104, 320),
        (112, 16),
        (120, 288),
        (128, 16),
        (136, 640),
        (144, 8),
        (152, 1),
        (160, 1),
        (168, 1),
        (176, 608),
        (184, 37),
        (192, 37),
        (200, 1),
        (208, 1),
        (216, 1),
        (224, 2),
        (232, 1),
        (264, entry as u64),
    ] {
        put_u64(&mut bytes, offset, value);
    }
    put_u32(&mut bytes, 256, 608);
    put_u32(&mut bytes, 260, 1);

    put_object(&mut bytes, 272, 5, 32); // FIELD_HASH_TABLE
    put_u64(&mut bytes, 288, 400);
    put_u64(&mut bytes, 296, 400);
    put_object(&mut bytes, 304, 4, 32); // DATA_HASH_TABLE
    put_u64(&mut bytes, 320, 448);
    put_u64(&mut bytes, 328, 448);
    put_object(&mut bytes, 336, 7, 64); // Initial TAG, before any entries.
    put_u64(&mut bytes, 352, 1);

    put_object(&mut bytes, 400, 2, 47); // FIELD
    put_u64(&mut bytes, 416, 0x8845_60c2_37b1_05c0);
    put_u64(&mut bytes, 432, 448);
    bytes[440..447].copy_from_slice(b"MESSAGE");
    put_object(&mut bytes, 448, 1, if compact { 85 } else { 77 }); // DATA
    put_u64(&mut bytes, 464, DATA_HASH);
    put_u64(&mut bytes, 488, entry as u64);
    put_u64(&mut bytes, 504, 1);
    bytes[payload..payload + 13].copy_from_slice(b"MESSAGE=hello");

    put_object(&mut bytes, entry, 3, if compact { 68 } else { 80 }); // ENTRY
    put_u64(&mut bytes, entry + 16, 1);
    put_u64(&mut bytes, entry + 24, 37);
    put_u64(&mut bytes, entry + 32, 1);
    bytes[entry + 40..entry + 56].fill(0x33);
    put_u64(&mut bytes, entry + 56, DATA_HASH);
    if compact {
        put_u32(&mut bytes, entry + 64, 448);
    } else {
        put_u64(&mut bytes, entry + 64, 448);
        put_u64(&mut bytes, entry + 72, DATA_HASH);
    }

    put_object(&mut bytes, 608, 6, if compact { 28 } else { 32 }); // ENTRY_ARRAY
    if compact {
        put_u32(&mut bytes, 632, entry as u32);
    } else {
        put_u64(&mut bytes, 632, entry as u64);
    }
    put_object(&mut bytes, 640, 7, 64); // TAG covering the entry.
    put_u64(&mut bytes, 656, 2);
    put_u64(&mut bytes, 664, 1);

    let first_mac = match (compact, continuous) {
        (false, false) => "e2ffca747d71e9ca0d3a86cba16fa18dca1d2e5f1d003f0d92e6939916aa0689",
        (false, true) => "626c63a8de895663da21e188f9d8f50ecfe974250c70c10b136d41f3662a0a81",
        (true, false) => "2b34f77031d4b76d2ce850dc0bea161019b584541a1e37cddf12ce4e3bf75594",
        (true, true) => "9ead5a4fa339c970c398c78eb9fada0bdd33cb286a1588f2b401e545fadde65f",
    };
    let last_mac = if compact {
        "3e49cfe7e420eec305a9b3af752900b94691a3ff75446540e86c1f6ca1897827"
    } else {
        "a3064b5b1f469478563adc0184a9f1466f8ee64abe984f7cdf06b50b69a9634c"
    };
    put_mac(&mut bytes, 368, first_mac);
    put_mac(&mut bytes, 672, last_mac);
    bytes
}

fn open_fixture(bytes: &[u8], mmap_policy: MmapPolicy) -> (tempfile::TempDir, Journal) {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("sealed.journal"), bytes).unwrap();
    let journal = Journal::open_dir_with_config(
        root.path(),
        JournalConfig {
            mmap_policy,
            ..Default::default()
        },
    )
    .unwrap();
    (root, journal)
}

#[test]
fn independent_seals_verify_for_regular_compact_and_continuous_files() {
    for compact in [false, true] {
        for continuous in [false, true] {
            for access in [MmapPolicy::Never, MmapPolicy::Auto] {
                let (_root, journal) = open_fixture(&sealed_fixture(compact, continuous), access);
                let entries = journal.query().collect_owned().unwrap();
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].get("MESSAGE"), Some(b"hello".as_slice()));
                journal.verify_seal(VERIFICATION_KEY).unwrap();
            }
        }
    }
}

#[test]
fn allocated_but_uncommitted_arena_bytes_do_not_require_a_tag() {
    let mut bytes = sealed_fixture(false, false);
    bytes.resize(4096, 0xaa);
    put_u64(&mut bytes, 96, 4096 - 272);
    let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
    journal.verify_seal(VERIFICATION_KEY).unwrap();
}

#[test]
fn mutable_index_data_is_not_part_of_the_fss_authentication() {
    let mut bytes = sealed_fixture(false, false);
    // journal-authenticate.c deliberately omits these bytes. Verifying a seal
    // is not a substitute for verifying mutable index/chain consistency.
    for offset in [288, 320, 424, 432, 472, 480, 488, 496, 504, 624, 632] {
        bytes[offset] ^= 1;
    }
    let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
    journal.verify_seal(VERIFICATION_KEY).unwrap();
}

#[test]
fn authenticated_header_objects_and_payloads_reject_tampering() {
    for compact in [false, true] {
        let entry = if compact { 536 } else { 528 };
        let payload = if compact { 520 } else { 512 };
        for offset in [24, 40, 72, 368, 416, 440, 464, payload + 8, entry + 32, 672] {
            let mut bytes = sealed_fixture(compact, false);
            bytes[offset] ^= 1;
            let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
            assert!(
                matches!(
                    journal.verify_seal(VERIFICATION_KEY),
                    Err(SdJournalError::Corrupt { .. })
                ),
                "accepted tampering at {offset} (compact={compact})"
            );
        }
    }
}

#[test]
fn continuous_epochs_are_checked_even_with_valid_macs() {
    for continuous in [false, true] {
        for epoch in [1, 2] {
            let mut bytes = sealed_fixture(false, continuous);
            bytes.resize(768, 0);
            put_u64(&mut bytes, 96, 496);
            put_u64(&mut bytes, 136, 704);
            put_u64(&mut bytes, 144, 9);
            put_u64(&mut bytes, 224, 3);
            put_object(&mut bytes, 704, 7, 64);
            put_u64(&mut bytes, 720, 3);
            put_u64(&mut bytes, 728, epoch);
            let mac = if epoch == 1 {
                "00c44d178d3506900c08462d6dcadb01440221b5ebcb3ce6127670271a1c864d"
            } else {
                "2cfefc10fcad89784de4f3bb750d2bcefae0a3fbdefaa1323f26aa92c2f53cf9"
            };
            put_mac(&mut bytes, 736, mac);
            let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
            let result = journal.verify_seal(VERIFICATION_KEY);
            if continuous && epoch == 1 {
                assert!(
                    matches!(result, Err(SdJournalError::Corrupt { reason, .. }) if reason.contains("epoch sequence not continuous"))
                );
            } else {
                result.unwrap();
            }
        }
    }
}

#[test]
fn clearing_the_sealed_flag_cannot_bypass_authentication() {
    let mut bytes = sealed_fixture(false, false);
    put_u32(&mut bytes, 8, 0);
    let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
    assert!(matches!(
        journal.verify_seal(VERIFICATION_KEY),
        Err(SdJournalError::Unsupported { reason }) if reason.contains("journal is not sealed")
    ));
}

#[test]
fn removing_the_last_tag_cannot_authenticate_the_remaining_entry() {
    let mut bytes = sealed_fixture(false, false);
    bytes.truncate(640);
    put_u64(&mut bytes, 96, 368);
    put_u64(&mut bytes, 136, 608);
    put_u64(&mut bytes, 144, 7);
    put_u64(&mut bytes, 224, 1);
    bytes[520] = b'j';
    let (_root, journal) = open_fixture(&bytes, MmapPolicy::Never);
    assert!(matches!(
        journal.verify_seal(VERIFICATION_KEY),
        Err(SdJournalError::Unsupported { reason }) if reason.contains("unsealed objects after its last TAG")
    ));
}
