use async_trait::async_trait;
use gesang_plugins::{PinError, PinStore, PinSubject, VersionPin};
use sqlx::{PgConnection, types::Json};
use uuid::Uuid;

use crate::Store;

struct PinRow {
    subject: Json<PinSubject>,
    reference_id: Uuid,
    package: String,
    version: String,
    digest: String,
}

#[async_trait]
impl PinStore for Store {
    async fn pin(&self, pin: VersionPin) -> std::result::Result<VersionPin, PinError> {
        let mut tx = self.begin().await.map_err(storage_error)?;
        let existing = pin_in(&mut tx, pin).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(existing)
    }

    async fn get(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<Option<VersionPin>, PinError> {
        let row = sqlx::query_as!(
            PinRow,
            r#"
            SELECT subject AS "subject: _", reference_id, package, version, digest
            FROM gesang_package_pins
            WHERE subject = $1 AND reference_id = $2 AND package = $3
            "#,
            Json(subject) as _,
            reference_id,
            package,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(storage_error)?;
        Ok(row.map(Into::into))
    }

    async fn list(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
    ) -> std::result::Result<Vec<VersionPin>, PinError> {
        let rows = sqlx::query_as!(
            PinRow,
            r#"
            SELECT subject AS "subject: _", reference_id, package, version, digest
            FROM gesang_package_pins
            WHERE subject = $1 AND reference_id = $2
            ORDER BY package
            "#,
            Json(subject) as _,
            reference_id,
        )
        .fetch_all(self.pool())
        .await
        .map_err(storage_error)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn unpin(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<(), PinError> {
        let mut tx = self.begin().await.map_err(storage_error)?;
        sqlx::query!(
            r#"
            DELETE FROM gesang_package_pins
            WHERE subject = $1 AND reference_id = $2 AND package = $3
            "#,
            Json(subject) as _,
            reference_id,
            package,
        )
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

pub(crate) async fn pin_in(
    connection: &mut PgConnection,
    pin: VersionPin,
) -> std::result::Result<VersionPin, PinError> {
    let row = sqlx::query_as!(
        PinRow,
        r#"
        INSERT INTO gesang_package_pins (subject, reference_id, package, version, digest)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (subject, reference_id, package)
        DO UPDATE SET subject = EXCLUDED.subject
        RETURNING subject AS "subject: _", reference_id, package, version, digest
        "#,
        Json(&pin.subject) as _,
        pin.reference_id,
        &pin.package,
        &pin.version,
        &pin.digest,
    )
    .fetch_one(connection)
    .await
    .map_err(storage_error)?;
    let existing: VersionPin = row.into();
    if existing != pin {
        return Err(PinError::Conflict {
            existing: Box::new(existing),
            requested: Box::new(pin),
        });
    }
    Ok(existing)
}

impl From<PinRow> for VersionPin {
    fn from(row: PinRow) -> Self {
        Self {
            subject: row.subject.0,
            reference_id: row.reference_id,
            package: row.package,
            version: row.version,
            digest: row.digest,
        }
    }
}

fn storage_error(error: impl std::fmt::Display) -> PinError {
    PinError::Storage(error.to_string())
}
