use std::process::Command;

use serde_json::Value;

#[test]
fn core_package_selects_library_target_without_binary() {
    let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            &manifest_path,
        ])
        .output()
        .expect("cargo metadata should run");

    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata should return JSON");
    let package = metadata["packages"]
        .as_array()
        .and_then(|packages| {
            packages
                .iter()
                .find(|package| package["name"] == "iot-nano-core")
        })
        .expect("iot-nano-core package should be present");
    let targets = package["targets"]
        .as_array()
        .expect("iot-nano-core targets should be present");

    assert!(
        targets.iter().any(|target| target["kind"]
            .as_array()
            .is_some_and(|kinds| { kinds.iter().any(|kind| kind == "lib") })),
        "iot-nano-core must retain a library target"
    );
    assert!(
        targets.iter().all(|target| !target["kind"]
            .as_array()
            .is_some_and(|kinds| { kinds.iter().any(|kind| kind == "bin") })),
        "iot-nano-core must not select a binary target"
    );
}
