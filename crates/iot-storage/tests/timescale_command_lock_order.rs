#[test]
fn timescale_command_locks_asset_ancestors_before_the_device() {
    let source = include_str!("../src/domain/commands.rs");
    let function = source
        .split("async fn timescale_user_can_issue_command")
        .nth(1)
        .expect("timescale command authorization function must exist");

    let asset_lock = function
        .find("lock_timescale_command_asset_ancestors")
        .expect("command authorization must lock asset ancestors");
    let device_lock = function
        .find("SELECT owner_user_id, asset_id")
        .expect("command authorization must lock the device");

    assert!(
        asset_lock < device_lock,
        "command authorization must lock assets before the device to match asset deletion"
    );
}
