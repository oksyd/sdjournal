//! Share one live engine across multiple unit subscriptions.
//!
//! This is the recommended pattern when you need to tail several units at once. Build one
//! `LiveJournal`, register multiple subscriptions, then drive the engine once in the background.

use sdjournal::LiveJournal;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
struct Args {
    units: Vec<String>,
    count: usize,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = parse_args()?;
    let mut live = LiveJournal::open_default()?;

    let mut subscriptions = Vec::new();
    for unit in &args.units {
        let mut filter = live.filter();
        filter.match_unit(unit);
        let subscription = live.subscribe(filter)?;
        subscriptions.push((unit.clone(), subscription));
    }

    let engine = thread::spawn(move || live.run());

    let workers: Vec<_> = subscriptions
        .into_iter()
        .map(|(unit, subscription)| {
            thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
                for _ in 0..args.count {
                    let entry = subscription.recv_timeout(Duration::from_secs(30))??;
                    let message = entry
                        .get("MESSAGE")
                        .map(|value| String::from_utf8_lossy(value).into_owned())
                        .unwrap_or_else(|| "<no MESSAGE>".to_string());
                    println!(
                        "[{unit}] cursor={} realtime={} message={message}",
                        entry.cursor(),
                        entry.realtime_usec()
                    );
                }
                Ok(())
            })
        })
        .collect();

    // Join every worker before propagating any failure so all subscriptions are released.
    let outcomes: Vec<_> = workers.into_iter().map(|worker| worker.join()).collect();
    engine
        .join()
        .map_err(|_| IoError::other("live engine thread panicked"))??;
    for outcome in outcomes {
        outcome.map_err(|_| IoError::other("subscription worker panicked"))??;
    }

    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn Error + Send + Sync>> {
    let mut units = Vec::new();
    let mut count = 5usize;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--count" {
            let value = args
                .next()
                .ok_or_else(|| usage("`--count` requires a value"))?;
            count = value.parse()?;
            continue;
        }

        units.push(arg);
    }

    if units.is_empty() {
        units.push("sshd.service".to_string());
        units.push("docker.service".to_string());
    }

    Ok(Args { units, count })
}

fn usage(message: &str) -> IoError {
    IoError::new(
        ErrorKind::InvalidInput,
        format!(
            "{message}; usage: cargo run --example live_multi_subscriptions -- [--count N] [unit ...]"
        ),
    )
}
