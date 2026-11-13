//! Bridge one live subscription into Tokio.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use sdjournal::LiveJournal;
    use std::thread;

    let unit = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sshd.service".to_string());
    let max_items = std::env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(5usize);

    let mut live = LiveJournal::open_default()?;
    let mut filter = live.filter();
    filter.match_unit(&unit);

    let subscription = live.subscribe(filter)?;
    let mut rx = subscription.into_tokio()?;
    let engine = thread::Builder::new().spawn(move || live.run())?;
    let consumed = (|| -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..max_items {
            let entry = rx.blocking_recv().ok_or("tokio live channel closed")??;
            let msg = entry
                .get("MESSAGE")
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_else(|| "<no MESSAGE>".to_string());
            println!(
                "cursor={} realtime={} message={}",
                entry.cursor(),
                entry.realtime_usec(),
                msg
            );
        }
        Ok(())
    })();
    drop(rx);
    engine.join().map_err(|_| "live engine thread panicked")??;
    consumed
}
