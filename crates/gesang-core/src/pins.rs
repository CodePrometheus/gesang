use async_trait::async_trait;
use gesang_plugins::{PinError, PinStore, VersionPin};
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::Store;

#[async_trait]
impl PinStore for Store {
    async fn pin(&self, pin: VersionPin) -> std::result::Result<VersionPin, PinError> {
        let mut tx = self.begin().await.map_err(storage_error)?;
        let row=sqlx::query("INSERT INTO gesang_version_pins(owner_id,reference_id,package,version,digest) VALUES($1,$2,$3,$4,$5) ON CONFLICT(owner_id,reference_id,package) DO UPDATE SET owner_id=EXCLUDED.owner_id RETURNING *")
            .bind(pin.owner_id).bind(pin.reference_id).bind(&pin.package).bind(&pin.version).bind(&pin.digest)
            .fetch_one(&mut *tx).await.map_err(storage_error)?;
        let existing = read_pin(&row).map_err(storage_error)?;
        if existing != pin {
            return Err(PinError::Conflict {
                existing: Box::new(existing),
                requested: Box::new(pin),
            });
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(existing)
    }

    async fn get(
        &self,
        owner_id: Uuid,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<Option<VersionPin>, PinError> {
        sqlx::query("SELECT * FROM gesang_version_pins WHERE owner_id=$1 AND reference_id=$2 AND package=$3")
            .bind(owner_id).bind(reference_id).bind(package).fetch_optional(self.pool()).await.map_err(storage_error)?
            .as_ref().map(read_pin).transpose().map_err(storage_error)
    }

    async fn list(
        &self,
        owner_id: Uuid,
        reference_id: Uuid,
    ) -> std::result::Result<Vec<VersionPin>, PinError> {
        sqlx::query("SELECT * FROM gesang_version_pins WHERE owner_id=$1 AND reference_id=$2 ORDER BY package")
            .bind(owner_id).bind(reference_id).fetch_all(self.pool()).await.map_err(storage_error)?
            .iter().map(read_pin).collect::<std::result::Result<_,_>>().map_err(storage_error)
    }

    async fn unpin(
        &self,
        owner_id: Uuid,
        reference_id: Uuid,
        package: &str,
    ) -> std::result::Result<(), PinError> {
        let mut tx = self.begin().await.map_err(storage_error)?;
        sqlx::query(
            "DELETE FROM gesang_version_pins WHERE owner_id=$1 AND reference_id=$2 AND package=$3",
        )
        .bind(owner_id)
        .bind(reference_id)
        .bind(package)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

fn read_pin(row: &PgRow) -> std::result::Result<VersionPin, sqlx::Error> {
    Ok(VersionPin {
        owner_id: row.try_get("owner_id")?,
        reference_id: row.try_get("reference_id")?,
        package: row.try_get("package")?,
        version: row.try_get("version")?,
        digest: row.try_get("digest")?,
    })
}

fn storage_error(error: impl std::fmt::Display) -> PinError {
    PinError::Storage(error.to_string())
}
