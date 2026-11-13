use sdjournal::{EntryOwned, Journal};
use std::collections::BTreeMap;
use std::path::Path;

fn normalize_sdjournal_fields(entry: &EntryOwned) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for (name, value) in entry.iter_fields() {
        fields
            .entry(name.to_owned())
            .or_insert_with(|| String::from_utf8_lossy(value).into_owned());
    }
    fields
}

#[test]
#[ignore = "requires journal/ and expected.json datasets under tests/fixtures/*"]
fn format_fixtures_are_parseable_and_stable() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut fixtures = Vec::new();
    let entries = std::fs::read_dir(&root)
        .map_err(|error| format!("cannot read fixtures {}: {error}", root.display()))?;
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            fixtures.push(entry.path());
        }
    }
    assert!(!fixtures.is_empty(), "no fixtures under {}", root.display());
    fixtures.sort();

    for directory in fixtures {
        let journal_directory = directory.join("journal");
        let expected_path = directory.join("expected.json");
        assert!(
            std::fs::metadata(&journal_directory)?.is_dir(),
            "fixture journal path is not a directory: {}",
            journal_directory.display()
        );
        let expected: Vec<BTreeMap<String, String>> =
            serde_json::from_slice(&std::fs::read(&expected_path)?)?;
        assert!(
            !expected.is_empty(),
            "fixture contains no expected entries: {}",
            expected_path.display()
        );
        let journal = Journal::open_dir(&journal_directory)?;
        let got: Vec<_> = journal
            .query()
            .collect_owned()?
            .iter()
            .map(normalize_sdjournal_fields)
            .collect();
        assert_eq!(got, expected, "fixture mismatch: {}", directory.display());
    }
    Ok(())
}
