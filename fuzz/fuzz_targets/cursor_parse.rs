#![no_main]

use libfuzzer_sys::fuzz_target;
use sdjournal::Cursor;
use std::sync::Once;

const MAX_INPUT_SIZE: usize = 4 * 1024;

fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(2));
    for &b in bytes {
        out.push(LUT[(b >> 4) as usize]);
        out.push(LUT[(b & 0x0f) as usize]);
    }
    String::from_utf8_lossy_owned(out)
}

fn take_wrapped<const N: usize>(data: &[u8], cursor: &mut usize, fallback: u8) -> [u8; N] {
    let mut out = [fallback; N];
    if data.is_empty() {
        *cursor = cursor.saturating_add(N);
        return out;
    }

    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = data
            .get(cursor.saturating_add(idx))
            .copied()
            .unwrap_or_else(|| data[(cursor.saturating_add(idx)) % data.len()]);
    }
    *cursor = cursor.saturating_add(N);
    out
}

fn take_u64(data: &[u8], cursor: &mut usize, fallback: u64) -> u64 {
    if data.is_empty() {
        *cursor = cursor.saturating_add(8);
        return fallback;
    }

    let bytes = take_wrapped::<8>(data, cursor, 0);
    let value = u64::from_le_bytes(bytes);
    if value == 0 { fallback } else { value }
}

fn maybe_hyphenate(hex: &str, enabled: bool) -> String {
    if !enabled {
        return hex.to_string();
    }

    let mut out = String::with_capacity(hex.len() + 4);
    for (idx, ch) in hex.chars().enumerate() {
        if matches!(idx, 8 | 12 | 16 | 20) {
            out.push('-');
        }
        out.push(ch);
    }
    out
}

fn build_systemd_cursor(data: &[u8]) -> String {
    let flags = data.first().copied().unwrap_or(0);
    let corruption = data.get(1).copied().unwrap_or(0);
    let mut cursor = 2usize;
    let mut parts = Vec::new();

    if flags & 0x01 != 0 {
        let id = maybe_hyphenate(
            &hex_encode(&take_wrapped::<16>(data, &mut cursor, 0x11)),
            flags & 0x40 != 0,
        );
        parts.push(format!("s={id}"));
    }
    if flags & 0x02 != 0 {
        parts.push(format!("i={:x}", take_u64(data, &mut cursor, 1)));
    }
    if flags & 0x04 != 0 {
        let id = maybe_hyphenate(
            &hex_encode(&take_wrapped::<16>(data, &mut cursor, 0x22)),
            flags & 0x80 != 0,
        );
        parts.push(format!("b={id}"));
    }
    if flags & 0x08 != 0 {
        parts.push(format!("m={:x}", take_u64(data, &mut cursor, 2)));
    }
    if flags & 0x10 != 0 {
        parts.push(format!("t={:x}", take_u64(data, &mut cursor, 3)));
    }
    if flags & 0x20 != 0 {
        parts.push(format!("x={:x}", take_u64(data, &mut cursor, 4)));
    }

    if parts.is_empty() {
        parts.push(format!("t={:x}", take_u64(data, &mut cursor, 3)));
    }

    if corruption & 0x01 != 0 {
        parts.push(String::new());
    } else if corruption & 0x02 != 0 {
        parts.push("broken".to_string());
    }

    parts.join(";")
}

fn exercise_cursor(s: &str) {
    if let Ok(cursor) = Cursor::parse(s) {
        let roundtrip = cursor.to_string();
        let parsed =
            Cursor::parse(&roundtrip).expect("Display must produce valid native cursor text");
        assert_eq!(parsed, cursor);
        assert_eq!(parsed.to_string(), roundtrip);
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    static CHECK_SEEDS: Once = Once::new();
    CHECK_SEEDS.call_once(|| {
        assert_eq!(
            maybe_hyphenate("0123456789abcdef0123456789abcdef", true),
            "01234567-89ab-cdef-0123-456789abcdef"
        );
        Cursor::parse(&build_systemd_cursor(&[0xff, 0]))
            .expect("hyphenated cursor seed must be valid without corruption flags");
        assert!(Cursor::parse(&build_systemd_cursor(&[0xff, 1])).is_err());
        assert!(Cursor::parse(&build_systemd_cursor(&[0xff, 2])).is_err());
    });

    exercise_cursor(&String::from_utf8_lossy(data));
    exercise_cursor(&build_systemd_cursor(data));
});
