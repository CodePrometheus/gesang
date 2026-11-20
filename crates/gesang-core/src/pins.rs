use async_trait::async_trait;
use gesang_plugins::{PinError, PinStore, PinSubject, VersionPin};
use sqlx::types::Json;
use uuid::Uuid;

use crate::{
    CoordinationTransaction, Store,
    store::database::{Row, query},
};

#[async_trait]
impl PinStore for Store {
    async fn pin(&self, pin: VersionPin) -> std::result::Result<VersionPin, PinError> {
        let mut tx = self.database.begin().await.map_err(storage_error)?;
        let existing = pin_in(&mut tx.coordination(), pin).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(existing)
    }

    async fn get(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<Option<VersionPin>, PinError> {
        query(
            self.database.executor(),
            "SELECT subject, reference_id, package, version, digest
             FROM gesang_package_pins WHERE subject=$1 AND reference_id=$2 AND package=$3",
        )
        .bind(Json(subject))
        .bind(reference_id)
        .bind(package)
        .fetch_optional()
        .await
        .map_err(storage_error)?
        .as_ref()
        .map(read_pin)
        .transpose()
    }

    async fn list(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
    ) -> std::result::Result<Vec<VersionPin>, PinError> {
        query(
            self.database.executor(),
            "SELECT subject, reference_id, package, version, digest
             FROM gesang_package_pins WHERE subject=$1 AND reference_id=$2 ORDER BY package",
        )
        .bind(Json(subject))
        .bind(reference_id)
        .fetch_all()
        .await
        .map_err(storage_error)?
        .iter()
        .map(read_pin)
        .collect()
    }

    async fn unpin(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<(), PinError> {
        let mut tx = self.database.begin().await.map_err(storage_error)?;
        query(
            tx.executor(),
            "DELETE FROM gesang_package_pins WHERE subject=$1 AND reference_id=$2 AND package=$3",
        )
        .bind(Json(subject))
        .bind(reference_id)
        .bind(package)
        .execute()
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

pub(crate) async fn pin_in(
    connection: &mut CoordinationTransaction<'_, '_>,
    pin: VersionPin,
) -> std::result::Result<VersionPin, PinError> {
    let row = query(
        connection.executor(),
        "INSERT INTO gesang_package_pins (subject, reference_id, package, version, digest)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (subject, reference_id, package)
         DO UPDATE SET subject = EXCLUDED.subject
         RETURNING subject, reference_id, package, version, digest",
    )
    .bind(Json(&pin.subject))
    .bind(pin.reference_id)
    .bind(&pin.package)
    .bind(&pin.version)
    .bind(&pin.digest)
    .fetch_one()
    .await
    .map_err(storage_error)?;
    let existing = read_pin(&row)?;
    if existing != pin {
        return Err(PinError::Conflict {
            existing: Box::new(existing),
            requested: Box::new(pin),
        });
    }
    Ok(existing)
}

fn read_pin(row: &Row) -> std::result::Result<VersionPin, PinError> {
    let read = || -> sqlx::Result<VersionPin> {
        Ok(VersionPin {
            subject: row.try_get::<Json<PinSubject>, _>("subject")?.0,
            reference_id: row.try_get("reference_id")?,
            package: row.try_get("package")?,
            version: row.try_get("version")?,
            digest: row.try_get("digest")?,
        })
    };
    read().map_err(storage_error)
}

fn storage_error(error: impl std::fmt::Display) -> PinError {
    PinError::Storage(error.to_string())
}
