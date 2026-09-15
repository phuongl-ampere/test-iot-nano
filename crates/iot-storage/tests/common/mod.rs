use sqlx::PgConnection;

pub async fn reset_timescale_schema(connection: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:platform-storage-test'))")
        .execute(&mut *connection)
        .await?;
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut *connection)
        .await?;
    Ok(())
}
