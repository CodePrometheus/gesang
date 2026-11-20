//! Timestamp serialization for the platform's existing RFC 3339 fields.

use std::fmt::{self, Display, Formatter};

use jiff::{Timestamp, tz::Offset};
use serde::{Serialize, Serializer};

/// Serializes an instant with UTC's `Z` suffix and whole groups of three fractional digits.
pub fn serialize<S: Serializer>(value: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&Rfc3339(*value, true))
}

/// Serializes an optional instant with the same fractional precision and JSON null semantics.
pub fn serialize_optional<S: Serializer>(
    value: &Option<Timestamp>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.map(|time| Rfc3339(time, true)).serialize(serializer)
}

/// Displays an instant with the native client's numeric UTC offset and fractional precision.
pub fn display_with_utc_offset(value: Timestamp) -> impl Display {
    Rfc3339(value, false)
}

struct Rfc3339(Timestamp, bool);

impl Serialize for Rfc3339 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl Display for Rfc3339 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let Self(value, use_z) = *self;
        let nanos = value.subsec_nanosecond();
        let precision = if nanos == 0 {
            0
        } else if nanos % 1_000_000 == 0 {
            3
        } else if nanos % 1_000 == 0 {
            6
        } else {
            9
        };
        if use_z {
            write!(f, "{value:.precision$}")
        } else {
            write!(f, "{:.precision$}", value.display_with_offset(Offset::UTC))
        }
    }
}
