use jiff::Timestamp;
use jiff_sqlx::Timestamp as SqlxTimestamp;
use sqlx::{
    Decode, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
};

/// SQLx mapping for a Jiff instant stored in a native PostgreSQL `TIMESTAMPTZ` column.
#[derive(Clone, Copy, Debug)]
pub struct PostgresTimestamp(pub Timestamp);

impl Type<Postgres> for PostgresTimestamp {
    fn type_info() -> PgTypeInfo {
        <SqlxTimestamp as Type<Postgres>>::type_info()
    }
}

impl Encode<'_, Postgres> for PostgresTimestamp {
    fn encode_by_ref(&self, buffer: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <SqlxTimestamp as Encode<Postgres>>::encode_by_ref(&self.0.into(), buffer)
    }

    fn size_hint(&self) -> usize {
        size_of::<i64>()
    }
}

impl<'r> Decode<'r, Postgres> for PostgresTimestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Self(
            <SqlxTimestamp as Decode<Postgres>>::decode(value)?.to_jiff(),
        ))
    }
}
