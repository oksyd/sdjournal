//! Cursor checkpointing example.
//!
//! This subscribes to a unit's live stream and persists a cursor checkpoint to disk. On restart,
//! it resumes strictly after the last persisted cursor.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use sdjournal::{Cursor, LiveJournal, SubscriptionOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::thread;

    let mut args = std::env::args().skip(1);
    let unit = args.next().unwrap_or_else(|| "sshd.service".to_string());
    let checkpoint_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("sdjournal.cursor"));

    let resume_cursor = match std::fs::read_to_string(&checkpoint_path) {
        Ok(contents) => Some(Cursor::parse(contents.trim())?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let directory = checkpoint_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));

    let mut live = LiveJournal::open_default()?;
    let mut filter = live.filter();
    filter.match_unit(&unit);
    let mut options = SubscriptionOptions::new(filter);
    if let Some(cursor) = resume_cursor {
        options.after_cursor(cursor);
    }

    let subscription = live.subscribe_with_options(options)?;
    let engine = thread::spawn(move || live.run());
    let consumed = (|| -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let entry = subscription.recv()??;
            if let Some(msg) = entry.get("MESSAGE") {
                println!("{}", String::from_utf8_lossy(msg));
            }

            // A unique temporary file in the same directory makes replacement atomic without
            // clobbering an unrelated .tmp path. Persist the data before the rename.
            let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
            writeln!(temporary, "{}", entry.cursor())?;
            temporary.as_file().sync_all()?;
            temporary.persist(&checkpoint_path)?;
            std::fs::File::open(directory)?.sync_all()?;
        }
    })();
    drop(subscription);
    engine.join().map_err(|_| "live engine thread panicked")??;
    consumed
}
