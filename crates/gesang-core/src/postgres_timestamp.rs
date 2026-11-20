use jiff::Timestamp;
use sqlx::{
    Decode, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueFormat, PgValueRef, types::Oid},
};

const POSTGRES_EPOCH: Timestamp = Timestamp::constant(946_684_800, 0);

/// SQLx mapping for a Jiff instant stored in a native PostgreSQL `TIMESTAMPTZ` column.
#[derive(Clone, Copy, Debug)]
pub struct PostgresTimestamp(pub Timestamp);

impl Type<Postgres> for PostgresTimestamp {
    fn type_info() -> PgTypeInfo {
        // PostgreSQL's built-in TIMESTAMPTZ OID.
        PgTypeInfo::with_oid(Oid(1184))
    }
}

impl Encode<'_, Postgres> for PostgresTimestamp {
    fn encode_by_ref(&self, buffer: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // The database representation truncates toward zero relative to 2000-01-01.
        let micros = i64::try_from(self.0.duration_since(POSTGRES_EPOCH).as_micros())?;
        Encode::<Postgres>::encode(micros, buffer)
    }

    fn size_hint(&self) -> usize {
        size_of::<i64>()
    }
}

impl<'r> Decode<'r, Postgres> for PostgresTimestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let timestamp = match value.format() {
            PgValueFormat::Binary => {
                let micros = i128::from(<i64 as Decode<Postgres>>::decode(value)?);
                Timestamp::from_nanosecond(POSTGRES_EPOCH.as_nanosecond() + micros * 1_000)?
            }
            PgValueFormat::Text => value.as_str()?.parse()?,
        };
        Ok(Self(timestamp))
    }
}
