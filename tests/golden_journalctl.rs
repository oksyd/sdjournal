#![cfg(target_os = "linux")]

use sdjournal::{Cursor, Journal, JournalConfig, LiveJournal};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type NormalizedFields = BTreeMap<String, String>;
type BaselineRow = (u64, u64, String, String, NormalizedFields);
type GotRow = (u64, u64, String, NormalizedFields);

fn cmd_output(cmd: &str, args: &[&str]) -> TestResult<Output> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|error| format!("cannot run {cmd} {args:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{cmd} {args:?} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output)
}

fn run_journalctl(args: &[&str]) -> TestResult<Output> {
    let use_sudo = matches!(
        std::env::var("SDJOURNAL_TEST_USE_SUDO"),
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true")
    );
    if use_sudo {
        let mut sudo_args = vec!["-n", "journalctl"];
        sudo_args.extend_from_slice(args);
        cmd_output("sudo", &sudo_args)
    } else {
        cmd_output("journalctl", args)
    }
}

fn run_logger(tag: &str, message: &str) -> TestResult {
    cmd_output("logger", &["-t", tag, "--", message])?;
    Ok(())
}

fn read_entries_from_journalctl(tag: &str) -> TestResult<Vec<Value>> {
    let output = run_journalctl(&["-o", "json", "--no-pager", "-t", tag])?;
    std::str::from_utf8(&output.stdout)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

fn wait_for_journalctl_entries(tag: &str, count: usize) -> TestResult<Vec<Value>> {
    let started = Instant::now();
    loop {
        let entries = read_entries_from_journalctl(tag)?;
        if entries.len() >= count {
            return Ok(entries);
        }
        if started.elapsed() >= Duration::from_secs(5) {
            return Err(format!(
                "journalctl returned {} of {count} expected entries for {tag} within 5 seconds",
                entries.len()
            )
            .into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn required_string<'a>(entry: &'a Value, field: &str) -> TestResult<&'a str> {
    entry
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("journalctl entry is missing string field {field}: {entry}").into())
}

fn normalize_journalctl_fields(entry: &Value) -> TestResult<NormalizedFields> {
    let object = entry
        .as_object()
        .ok_or("journalctl entry is not a JSON object")?;
    Ok(object
        .iter()
        .filter(|(key, _)| !key.starts_with("__"))
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            (key.clone(), value)
        })
        .collect())
}

fn normalize_sdjournal_fields(entry: &sdjournal::EntryOwned) -> NormalizedFields {
    let mut fields = BTreeMap::new();
    for (name, value) in entry.iter_fields() {
        fields
            .entry(name.to_owned())
            .or_insert_with(|| String::from_utf8_lossy(value).into_owned());
    }
    fields
}

fn message(entry: &sdjournal::EntryOwned) -> TestResult<&str> {
    Ok(std::str::from_utf8(
        entry.get("MESSAGE").ok_or("entry is missing MESSAGE")?,
    )?)
}

#[test]
#[ignore = "requires a running systemd-journald, journalctl, logger, and readable journal files"]
fn golden_match_and_time_filters_against_journalctl() -> TestResult {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let tag = format!("sdjournal-test-{unique}");
    let messages = [
        format!("msg-1-{unique}"),
        format!("msg-2-{unique}"),
        format!("msg-3-{unique}"),
    ];
    for message in &messages {
        run_logger(&tag, message)?;
        thread::sleep(Duration::from_millis(5));
    }

    let mut baseline: Vec<BaselineRow> = wait_for_journalctl_entries(&tag, messages.len())?
        .iter()
        .map(|entry| {
            Ok((
                required_string(entry, "__REALTIME_TIMESTAMP")?.parse()?,
                required_string(entry, "__MONOTONIC_TIMESTAMP")?.parse()?,
                required_string(entry, "__CURSOR")?.to_owned(),
                required_string(entry, "MESSAGE")?.to_owned(),
                normalize_journalctl_fields(entry)?,
            ))
        })
        .collect::<TestResult<_>>()?;
    baseline.sort_by_key(|(realtime, _, _, _, _)| *realtime);
    assert_eq!(
        baseline
            .iter()
            .map(|(_, _, _, message, _)| message)
            .collect::<Vec<_>>(),
        messages.iter().collect::<Vec<_>>()
    );

    let journal = Journal::open_default()?;
    let mut query = journal.query();
    query.match_exact("SYSLOG_IDENTIFIER", tag.as_bytes());
    let mut got: Vec<GotRow> = query
        .collect_owned()?
        .iter()
        .map(|entry| {
            Ok((
                entry.realtime_usec(),
                entry.monotonic_usec(),
                message(entry)?.to_owned(),
                normalize_sdjournal_fields(entry),
            ))
        })
        .collect::<TestResult<_>>()?;
    got.sort_by_key(|(realtime, _, _, _)| *realtime);
    assert_eq!(got.len(), messages.len());
    for (index, (expected, actual)) in baseline.iter().zip(&got).enumerate() {
        assert_eq!(actual.0, expected.0, "timestamp mismatch at index {index}");
        assert_eq!(actual.1, expected.1, "monotonic mismatch at index {index}");
        assert_eq!(actual.2, expected.3, "message mismatch at index {index}");
        assert_eq!(actual.3, expected.4, "field diff at index {index}");
    }

    let second_realtime = baseline[1].0;
    let mut time_query = journal.query();
    time_query
        .match_exact("SYSLOG_IDENTIFIER", tag.as_bytes())
        .since_realtime(second_realtime)
        .until_realtime(second_realtime);
    let at_second = time_query.collect_owned()?;
    assert!(
        !at_second.is_empty(),
        "expected inclusive time window match"
    );
    assert!(
        at_second
            .iter()
            .all(|entry| entry.realtime_usec() == second_realtime),
        "since/until must constrain to the exact realtime_usec"
    );

    let second_cursor = Cursor::parse(&baseline[1].2)?;
    let mut cursor_query = journal.seek_cursor(&second_cursor);
    cursor_query.match_exact("SYSLOG_IDENTIFIER", tag.as_bytes());
    let from_cursor = cursor_query.collect_owned()?;
    assert_eq!(
        message(
            from_cursor
                .first()
                .ok_or("seek_cursor returned no entries")?
        )?,
        baseline[1].3,
        "seek_cursor did not start at expected entry"
    );

    let mut after_query = journal.query();
    after_query
        .match_exact("SYSLOG_IDENTIFIER", tag.as_bytes())
        .after_cursor(second_cursor);
    let after_cursor = after_query.collect_owned()?;
    assert_eq!(
        message(
            after_cursor
                .first()
                .ok_or("after_cursor returned no entries")?
        )?,
        baseline[2].3,
        "after_cursor must be strictly-after"
    );
    Ok(())
}

#[test]
#[ignore = "requires a running systemd-journald, journalctl, logger, and readable journal files"]
fn follow_tails_and_emits_new_entries() -> TestResult {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let tag = format!("sdjournal-follow-{unique}");
    let preexisting = format!("preexisting-follow-msg-{unique}");
    let expected = format!("follow-msg-{unique}");
    run_logger(&tag, &preexisting)?;
    let baseline = wait_for_journalctl_entries(&tag, 1)?;
    assert_eq!(baseline.len(), 1);
    assert_eq!(required_string(&baseline[0], "MESSAGE")?, preexisting);

    // Establish the live tail only after the old message is committed and observable.
    let mut live = LiveJournal::open_default_with_config(JournalConfig {
        poll_interval: Duration::from_millis(100),
        ..Default::default()
    })?;
    let mut filter = live.filter();
    filter.match_exact("SYSLOG_IDENTIFIER", tag.as_bytes());
    let subscription = live.subscribe(filter)?;
    let engine = thread::spawn(move || live.run());

    let received = (|| -> TestResult<_> {
        run_logger(&tag, &expected)?;
        Ok(subscription.recv_timeout(Duration::from_secs(10))??)
    })();
    drop(subscription);
    engine.join().map_err(|_| "live engine thread panicked")??;
    let entry = received?;
    assert_eq!(
        std::str::from_utf8(
            entry
                .get("MESSAGE")
                .ok_or("live entry is missing MESSAGE")?
        )?,
        expected
    );
    Ok(())
}
