use std::process::Command;

#[test]
fn legacy_core_service_binary_exposes_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_iot-nano-core"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Durably ingest MQTT telemetry")
    );
}
