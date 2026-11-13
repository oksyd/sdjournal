#![no_main]

use libfuzzer_sys::fuzz_target;
use std::fs::{File, OpenOptions, create_dir};
use std::io::{Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

const MAX_INPUT_SIZE: usize = 2 * 1024 * 1024;
const HEADER_SIZE: usize = 272;
const DATA_HASH_TABLE_OFFSET: usize = HEADER_SIZE;
const ENTRY_ARRAY_OFFSET: usize = DATA_HASH_TABLE_OFFSET + 32;
const OBJECT_DATA: u8 = 1;
const OBJECT_ENTRY: u8 = 3;
const OBJECT_DATA_HASH_TABLE: u8 = 4;
const OBJECT_ENTRY_ARRAY: u8 = 6;
const HEADER_INCOMPATIBLE_COMPACT: u32 = 1 << 4;
const MACHINE_ID_DIR: &str = "0123456789abcdef0123456789abcdef";
const MESSAGE: &[u8] = b"MESSAGE=hello";
// Jenkins hash of MESSAGE=hello, used by the unkeyed DATA hash table.
const MESSAGE_HASH: u64 = 0x87dd_eff2_fd1b_d06d;

struct ReusableJournalLayout {
    root: TempDir,
    machine_dir: PathBuf,
    root_file: File,
    machine_file: File,
    tilde_file: File,
}

impl ReusableJournalLayout {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create fuzz journal directory");
        let machine_dir = root.path().join(MACHINE_ID_DIR);
        create_dir(&machine_dir).expect("create fuzz machine directory");

        let root_file = open_rw(root.path().join("root.journal"));
        let machine_file = open_rw(machine_dir.join("nested.journal"));
        let tilde_file = open_rw(root.path().join("temp.journal~"));

        Self {
            root,
            machine_dir,
            root_file,
            machine_file,
            tilde_file,
        }
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    fn machine_dir(&self) -> &Path {
        &self.machine_dir
    }

    fn rewrite_all(&mut self, root_bytes: &[u8], machine_bytes: &[u8], tilde_bytes: &[u8]) {
        rewrite_file(&mut self.root_file, root_bytes);
        rewrite_file(&mut self.machine_file, machine_bytes);
        rewrite_file(&mut self.tilde_file, tilde_bytes);
    }
}

fn open_rw(path: PathBuf) -> File {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(path)
        .expect("create fuzz journal file")
}

fn rewrite_file(file: &mut File, bytes: &[u8]) {
    file.rewind().expect("rewind fuzz journal");
    file.write_all(bytes).expect("write fuzz journal");
    file.set_len(bytes.len() as u64)
        .expect("truncate fuzz journal to rewritten length");
}

fn reusable_journal_layout() -> &'static Mutex<ReusableJournalLayout> {
    static SLOT: OnceLock<Mutex<ReusableJournalLayout>> = OnceLock::new();
    SLOT.get_or_init(|| {
        let mut layout = ReusableJournalLayout::new();
        // Cover regular/compact layouts and both indexed (offline/archived) and
        // sequential (online) exact queries before accepting fuzz inputs.
        for seed in [[0, 0], [1, 0], [0, 1], [1, 1], [0, 2], [1, 2]] {
            let bytes = minimal_journal_bytes(&seed);
            layout.rewrite_all(&bytes, &bytes, &bytes);
            let journal = sdjournal::Journal::open_dir(layout.root())
                .expect("normalized journal seed must open");
            let entries = journal
                .query()
                .collect_owned()
                .expect("normalized journal seed must contain a readable entry");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].get("MESSAGE"), Some(b"hello".as_slice()));
            let matches = journal
                .query()
                .match_exact("MESSAGE", b"hello")
                .collect_owned()
                .expect("normalized journal seed must support indexed MESSAGE queries");
            assert_eq!(matches.len(), 1);
        }
        Mutex::new(layout)
    })
}

fn write_le_u32(dst: &mut [u8], offset: usize, v: u32) {
    dst[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
}

fn write_le_u64(dst: &mut [u8], offset: usize, v: u64) {
    dst[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
}

fn fill_bytes(dst: &mut [u8], src: &[u8], start: usize, fallback: u8) {
    if src.is_empty() {
        dst.fill(fallback);
        return;
    }

    for (idx, byte) in dst.iter_mut().enumerate() {
        *byte = src
            .get(start.saturating_add(idx))
            .copied()
            .unwrap_or_else(|| src[idx % src.len()]);
    }
}

fn load_u64(src: &[u8], start: usize, fallback: u64) -> u64 {
    let Some(bytes) = src.get(start..start.saturating_add(8)) else {
        return fallback;
    };
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    u64::from_le_bytes(out)
}

fn variant_bytes(data: &[u8], salt: u8) -> Vec<u8> {
    if data.is_empty() {
        return vec![salt];
    }

    let mut out = data.to_vec();
    let len = out.len();
    let rotate = usize::from(salt) % len;
    out.rotate_left(rotate);
    out[0] ^= salt;
    out.push(salt);
    out
}

fn minimal_journal_bytes(data: &[u8]) -> Vec<u8> {
    let compact = data.first().copied().unwrap_or(0) & 1 != 0;
    let entry_array_size = if compact { 28usize } else { 32usize };
    let data_offset = (ENTRY_ARRAY_OFFSET + entry_array_size).next_multiple_of(8);
    let data_header_size = if compact { 72 } else { 64 };
    let data_size = data_header_size + MESSAGE.len();
    let entry_offset = (data_offset + data_size).next_multiple_of(8);
    let entry_size = if compact { 68 } else { 80 };
    let used_size = (entry_offset + entry_size).next_multiple_of(8);
    let seqnum = load_u64(data, 96, 1);
    let realtime = load_u64(data, 104, 1);
    let monotonic = load_u64(data, 112, 1);

    let mut bytes = vec![0; used_size];

    bytes[0..8].copy_from_slice(b"LPKSHHRH");

    let incompatible = if compact {
        HEADER_INCOMPATIBLE_COMPACT
    } else {
        0
    };
    write_le_u32(&mut bytes, 12, incompatible);
    bytes[16] = data.get(1).copied().unwrap_or(0) % 3;

    fill_bytes(&mut bytes[24..40], data, 24, 0x11);
    fill_bytes(&mut bytes[40..56], data, 40, 0x22);
    fill_bytes(&mut bytes[56..72], data, 56, 0x33);
    fill_bytes(&mut bytes[72..88], data, 72, 0x44);

    write_le_u64(&mut bytes, 88, HEADER_SIZE as u64);
    write_le_u64(&mut bytes, 96, (used_size - HEADER_SIZE) as u64);
    write_le_u64(&mut bytes, 104, (DATA_HASH_TABLE_OFFSET + 16) as u64);
    write_le_u64(&mut bytes, 112, 16);
    write_le_u64(&mut bytes, 136, entry_offset as u64);
    write_le_u64(&mut bytes, 144, 4);
    write_le_u64(&mut bytes, 152, 1);
    write_le_u64(&mut bytes, 160, seqnum);
    write_le_u64(&mut bytes, 168, seqnum);
    write_le_u64(&mut bytes, 176, ENTRY_ARRAY_OFFSET as u64);
    write_le_u64(&mut bytes, 184, realtime);
    write_le_u64(&mut bytes, 192, realtime);
    write_le_u64(&mut bytes, 200, monotonic);
    write_le_u64(&mut bytes, 208, 1);
    write_le_u64(&mut bytes, 224, 1);
    write_le_u64(&mut bytes, 264, entry_offset as u64);

    bytes[DATA_HASH_TABLE_OFFSET] = OBJECT_DATA_HASH_TABLE;
    write_le_u64(&mut bytes, DATA_HASH_TABLE_OFFSET + 8, 32);
    write_le_u64(&mut bytes, DATA_HASH_TABLE_OFFSET + 16, data_offset as u64);
    write_le_u64(&mut bytes, DATA_HASH_TABLE_OFFSET + 24, data_offset as u64);

    bytes[ENTRY_ARRAY_OFFSET] = OBJECT_ENTRY_ARRAY;
    write_le_u64(&mut bytes, ENTRY_ARRAY_OFFSET + 8, entry_array_size as u64);
    if compact {
        write_le_u32(&mut bytes, ENTRY_ARRAY_OFFSET + 24, entry_offset as u32);
    } else {
        write_le_u64(&mut bytes, ENTRY_ARRAY_OFFSET + 24, entry_offset as u64);
    }

    bytes[data_offset] = OBJECT_DATA;
    write_le_u64(&mut bytes, data_offset + 8, data_size as u64);
    write_le_u64(&mut bytes, data_offset + 16, MESSAGE_HASH);
    write_le_u64(&mut bytes, data_offset + 40, entry_offset as u64);
    write_le_u64(&mut bytes, data_offset + 56, 1);
    bytes[data_offset + data_header_size..data_offset + data_size].copy_from_slice(MESSAGE);

    bytes[entry_offset] = OBJECT_ENTRY;
    write_le_u64(&mut bytes, entry_offset + 8, entry_size as u64);
    write_le_u64(&mut bytes, entry_offset + 16, seqnum);
    write_le_u64(&mut bytes, entry_offset + 24, realtime);
    write_le_u64(&mut bytes, entry_offset + 32, monotonic);
    bytes.copy_within(56..72, entry_offset + 40);
    write_le_u64(&mut bytes, entry_offset + 56, MESSAGE_HASH);
    if compact {
        write_le_u32(&mut bytes, entry_offset + 64, data_offset as u32);
    } else {
        write_le_u64(&mut bytes, entry_offset + 64, data_offset as u64);
        write_le_u64(&mut bytes, entry_offset + 72, MESSAGE_HASH);
    }

    bytes
}

fn exercise_queries(journal: &sdjournal::Journal) {
    let _ = journal.query().limit(8).collect_owned();

    let mut head = journal.query();
    head.seek_head().limit(4);
    let _ = head.collect_owned();

    let mut tail = journal.query();
    tail.seek_tail().limit(8);
    let tail_entries = tail.collect_owned();

    let mut bounded = journal.query();
    bounded.since_realtime(0).until_realtime(u64::MAX).limit(4);
    let _ = bounded.collect_owned();

    let mut present = journal.query();
    present.match_present("MESSAGE").limit(4);
    let _ = present.collect_owned();

    let mut exact = journal.query();
    exact.match_exact("MESSAGE", b"hello").limit(4);
    let _ = exact.collect_owned();

    let mut unit = journal.query();
    unit.match_unit("sshd.service").limit(4);
    let _ = unit.collect_owned();

    let mut or_units = journal.query();
    or_units.or_group(|g| {
        g.match_exact("_SYSTEMD_UNIT", b"sshd.service");
    });
    or_units.or_group(|g| {
        g.match_exact("_SYSTEMD_UNIT", b"systemd-journald.service");
    });
    or_units.limit(4);
    let _ = or_units.collect_owned();

    if let Ok(tail_entries) = tail_entries
        && let Some(first) = tail_entries.first()
    {
        let cursor = first.cursor();
        let _ = journal.seek_cursor(&cursor).limit(2).collect_owned();

        let mut after = journal.query();
        after.after_cursor(cursor).limit(2);
        let _ = after.collect_owned();
    }
}

fn exercise_open_paths(layout: &ReusableJournalLayout, include_journal_tilde: bool) {
    let cfg = sdjournal::JournalConfig {
        include_journal_tilde,
        max_object_size_bytes: 4 * 1024 * 1024,
        max_decompressed_bytes: 1024 * 1024,
        ..Default::default()
    };

    if let Ok(journal) = sdjournal::Journal::open_dir_with_config(layout.root(), cfg.clone()) {
        exercise_queries(&journal);
    }
    if let Ok(journal) = sdjournal::Journal::open_dir_with_config(layout.machine_dir(), cfg.clone())
    {
        exercise_queries(&journal);
    }

    let duplicate_roots = vec![layout.root().to_path_buf(), layout.root().to_path_buf()];
    if let Ok(journal) = sdjournal::Journal::open_dirs_with_config(&duplicate_roots, cfg.clone()) {
        exercise_queries(&journal);
    }

    let merged_roots = vec![
        layout.root().to_path_buf(),
        layout.machine_dir().to_path_buf(),
    ];
    if let Ok(journal) = sdjournal::Journal::open_dirs_with_config(&merged_roots, cfg) {
        exercise_queries(&journal);
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    let mut layout = reusable_journal_layout()
        .lock()
        .expect("lock fuzz journal layout");

    // Keep arbitrary inputs intact: normalization must not hide malformed headers,
    // object offsets, compressed payloads, or inconsistent index structures.
    layout.rewrite_all(data, data, data);
    exercise_open_paths(&layout, false);
    exercise_open_paths(&layout, true);

    let root_bytes = minimal_journal_bytes(data);
    let machine_bytes = minimal_journal_bytes(&variant_bytes(data, 0x5a));
    let tilde_bytes = minimal_journal_bytes(&variant_bytes(data, 0xa5));

    layout.rewrite_all(&root_bytes, &machine_bytes, &tilde_bytes);

    exercise_open_paths(&layout, false);
    exercise_open_paths(&layout, true);
});
