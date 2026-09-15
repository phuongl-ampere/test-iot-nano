use std::{fs, path::Path};

const SHARED_LOCK_KEY: &str = "iot_nano:platform-storage-test";
const RESET_HELPER_CALL: &str = "common::reset_timescale_schema";

#[test]
fn every_shared_schema_reset_uses_the_canonical_session_lock() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut resetters = Vec::new();

    for entry in fs::read_dir(&tests_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
            || path.file_name().and_then(|name| name.to_str())
                == Some("schema_reset_lock_invariant.rs")
        {
            continue;
        }

        let source = fs::read_to_string(&path).unwrap();
        if source.contains("DROP SCHEMA") {
            panic!(
                "{} resets the shared schema directly instead of using the common reset helper",
                path.display()
            );
        }
        if source.contains(RESET_HELPER_CALL) {
            assert!(
                source.contains("mod common;"),
                "{} resets the shared schema without importing the common reset helper",
                path.display()
            );
            assert!(
                source.contains(RESET_HELPER_CALL),
                "{} resets the shared schema without using the common reset helper",
                path.display()
            );
            assert!(
                !source.contains("pg_advisory_lock(hashtext('iot_nano:")
                    && !source.contains("pg_advisory_lock(hashtext(\"iot_nano:"),
                "{} contains a non-shared schema-reset advisory lock",
                path.display()
            );
            resetters.push(path);
        }
    }

    assert_eq!(
        resetters.len(),
        19,
        "unexpected shared schema reset inventory"
    );
    let helper_source =
        fs::read_to_string(tests_dir.join("common/mod.rs")).expect("common reset helper exists");
    assert!(helper_source.contains(SHARED_LOCK_KEY));
}
