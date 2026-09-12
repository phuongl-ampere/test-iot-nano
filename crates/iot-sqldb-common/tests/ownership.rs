use iot_sqldb_common::open_owned_sqlite_pool;

#[tokio::test]
async fn rejects_a_sqlite_file_owned_by_another_service() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("service.db");

    let first = open_owned_sqlite_pool(&path, 5_000, 0x4150_4931)
        .await
        .unwrap();
    drop(first);

    assert!(
        open_owned_sqlite_pool(&path, 5_000, 0x434F_5231)
            .await
            .is_err()
    );
}
