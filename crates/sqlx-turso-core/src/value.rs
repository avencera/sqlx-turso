use std::{borrow::Cow, sync::Arc};

use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    type_info::TypeInfo,
    types::Type,
    value::Value,
    value::ValueRef,
};

use crate::{Turso, TursoAdapterError, TursoTypeInfo};

/// Owned Turso value
#[derive(Clone, Debug, Default)]
pub struct TursoValue {
    type_info: TursoTypeInfo,
    kind: TursoValueKind,
}

#[derive(Clone, Debug, Default)]
pub(crate) enum TursoValueKind {
    #[default]
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl TursoValue {
    /// Creates a NULL value
    pub fn null() -> Self {
        Self {
            type_info: TursoTypeInfo::NULL,
            kind: TursoValueKind::Null,
        }
    }

    pub(crate) fn from_turso(value: turso::Value) -> Self {
        match value {
            turso::Value::Null => Self::null(),
            turso::Value::Integer(value) => Self {
                type_info: TursoTypeInfo::new("INTEGER"),
                kind: TursoValueKind::Integer(value),
            },
            turso::Value::Real(value) => Self {
                type_info: TursoTypeInfo::new("REAL"),
                kind: TursoValueKind::Real(value),
            },
            turso::Value::Text(value) => Self {
                type_info: TursoTypeInfo::new("TEXT"),
                kind: TursoValueKind::Text(value),
            },
            turso::Value::Blob(value) => Self {
                type_info: TursoTypeInfo::new("BLOB"),
                kind: TursoValueKind::Blob(value),
            },
        }
    }

    pub(crate) fn with_type_info(mut self, type_info: TursoTypeInfo) -> Self {
        if !type_info.is_null() {
            self.type_info = type_info;
        }

        self
    }

    pub(crate) fn integer(value: i64) -> Self {
        Self {
            type_info: TursoTypeInfo::new("INTEGER"),
            kind: TursoValueKind::Integer(value),
        }
    }

    pub(crate) fn real(value: f64) -> Self {
        Self {
            type_info: TursoTypeInfo::new("REAL"),
            kind: TursoValueKind::Real(value),
        }
    }

    pub(crate) fn text(value: impl Into<String>) -> Self {
        Self {
            type_info: TursoTypeInfo::new("TEXT"),
            kind: TursoValueKind::Text(value.into()),
        }
    }

    pub(crate) fn blob(value: impl Into<Vec<u8>>) -> Self {
        Self {
            type_info: TursoTypeInfo::new("BLOB"),
            kind: TursoValueKind::Blob(value.into()),
        }
    }

    pub(crate) fn into_turso(self) -> turso::Value {
        match self.kind {
            TursoValueKind::Null => turso::Value::Null,
            TursoValueKind::Integer(value) => turso::Value::Integer(value),
            TursoValueKind::Real(value) => turso::Value::Real(value),
            TursoValueKind::Text(value) => turso::Value::Text(value),
            TursoValueKind::Blob(value) => turso::Value::Blob(value),
        }
    }
}

impl Value for TursoValue {
    type Database = Turso;

    fn as_ref(&self) -> TursoValueRef<'_> {
        TursoValueRef::new(self)
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        matches!(self.kind, TursoValueKind::Null)
    }
}

/// Borrowed Turso value
#[derive(Clone, Copy, Debug)]
pub struct TursoValueRef<'r> {
    value: &'r TursoValue,
}

impl<'r> TursoValueRef<'r> {
    pub(crate) fn new(value: &'r TursoValue) -> Self {
        Self { value }
    }

    fn integer(&self) -> Result<i64, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Integer(value) => Ok(*value),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-integer Turso value as integer",
            ))),
        }
    }

    fn real(&self) -> Result<f64, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Real(value) => Ok(*value),
            TursoValueKind::Integer(value) => Ok(*value as f64),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-real Turso value as float",
            ))),
        }
    }

    fn text(&self) -> Result<&'r str, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Text(value) => Ok(value),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-text Turso value as text",
            ))),
        }
    }

    fn blob(&self) -> Result<&'r [u8], BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Blob(value) => Ok(value),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-blob Turso value as bytes",
            ))),
        }
    }

    #[cfg(any(feature = "chrono", feature = "time"))]
    fn text_or_integer_or_real(&self) -> Result<TursoTemporalValue<'r>, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Text(value) => Ok(TursoTemporalValue::Text(value)),
            TursoValueKind::Integer(value) => Ok(TursoTemporalValue::Integer(*value)),
            TursoValueKind::Real(value) => Ok(TursoTemporalValue::Real(*value)),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-temporal Turso value as temporal",
            ))),
        }
    }
}

#[cfg(any(feature = "chrono", feature = "time"))]
enum TursoTemporalValue<'r> {
    Text(&'r str),
    Integer(i64),
    Real(f64),
}

impl<'r> ValueRef<'r> for TursoValueRef<'r> {
    type Database = Turso;

    fn to_owned(&self) -> TursoValue {
        self.value.clone()
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        self.value.type_info()
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}

macro_rules! impl_integer_type {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Type<Turso> for $ty {
                fn type_info() -> TursoTypeInfo {
                    TursoTypeInfo::new("INTEGER")
                }

                fn compatible(ty: &TursoTypeInfo) -> bool {
                    ty.has_integer_affinity()
                }
            }

            impl Encode<'_, Turso> for $ty {
                fn encode_by_ref(
                    &self,
                    buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
                ) -> Result<IsNull, BoxDynError> {
                    buf.push(TursoValue::integer((*self).into()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, Turso> for $ty {
                fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.integer()?.try_into()?)
                }
            }
        )+
    };
}

impl_integer_type!(i8, i16, i32, i64);

macro_rules! impl_unsigned_type {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Type<Turso> for $ty {
                fn type_info() -> TursoTypeInfo {
                    TursoTypeInfo::new("INTEGER")
                }

                fn compatible(ty: &TursoTypeInfo) -> bool {
                    ty.has_integer_affinity()
                }
            }

            impl Encode<'_, Turso> for $ty {
                fn encode_by_ref(
                    &self,
                    buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
                ) -> Result<IsNull, BoxDynError> {
                    buf.push(TursoValue::integer(i64::from(*self)));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, Turso> for $ty {
                fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.integer()?.try_into()?)
                }
            }
        )+
    };
}

impl_unsigned_type!(u8, u16, u32);

impl Type<Turso> for u64 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("INTEGER")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_integer_affinity()
    }
}

impl<'r> Decode<'r, Turso> for u64 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.integer()?.try_into()?)
    }
}

impl Type<Turso> for bool {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("BOOLEAN")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_bool_affinity() || ty.has_integer_affinity()
    }
}

impl Encode<'_, Turso> for bool {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::integer(i64::from(*self)));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for bool {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.integer()? != 0)
    }
}

macro_rules! impl_float_type {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Type<Turso> for $ty {
                fn type_info() -> TursoTypeInfo {
                    TursoTypeInfo::new("REAL")
                }

                fn compatible(ty: &TursoTypeInfo) -> bool {
                    ty.has_real_affinity()
                }
            }

            impl Encode<'_, Turso> for $ty {
                fn encode_by_ref(
                    &self,
                    buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
                ) -> Result<IsNull, BoxDynError> {
                    buf.push(TursoValue::real((*self).into()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, Turso> for $ty {
                fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.real()? as $ty)
                }
            }
        )+
    };
}

impl_float_type!(f32, f64);

impl Type<Turso> for str {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TEXT")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for &'_ str {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(*self));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for &'r str {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        value.text()
    }
}

impl Type<Turso> for String {
    fn type_info() -> TursoTypeInfo {
        <str as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <str as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for String {
    fn encode(
        self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self));
        Ok(IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.clone()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for String {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.text()?.to_owned())
    }
}

impl Encode<'_, Turso> for Arc<str> {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.to_string()));
        Ok(IsNull::No)
    }
}

impl Type<Turso> for [u8] {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("BLOB")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_blob_affinity() || ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for &'_ [u8] {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(*self));
        Ok(IsNull::No)
    }
}

impl Type<Turso> for Vec<u8> {
    fn type_info() -> TursoTypeInfo {
        <[u8] as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <[u8] as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for Vec<u8> {
    fn encode(
        self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(self));
        Ok(IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(self.clone()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for Vec<u8> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.blob()?.to_vec())
    }
}

#[cfg(feature = "uuid")]
impl Type<Turso> for uuid::Uuid {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("BLOB")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <[u8] as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "uuid")]
impl Encode<'_, Turso> for uuid::Uuid {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(self.as_bytes().to_vec()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "uuid")]
impl<'r> Decode<'r, Turso> for uuid::Uuid {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        match &value.value.kind {
            TursoValueKind::Blob(_) => Ok(uuid::Uuid::from_slice(value.blob()?)?),
            TursoValueKind::Text(_) => Ok(uuid::Uuid::parse_str(value.text()?)?),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-blob/text Turso value as uuid",
            ))),
        }
    }
}

#[cfg(feature = "uuid")]
impl Type<Turso> for uuid::fmt::Hyphenated {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TEXT")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <str as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "uuid")]
impl Encode<'_, Turso> for uuid::fmt::Hyphenated {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.to_string()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "uuid")]
impl<'r> Decode<'r, Turso> for uuid::fmt::Hyphenated {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(uuid::Uuid::parse_str(value.text()?)?.hyphenated())
    }
}

#[cfg(feature = "uuid")]
impl Type<Turso> for uuid::fmt::Simple {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TEXT")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <str as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "uuid")]
impl Encode<'_, Turso> for uuid::fmt::Simple {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.to_string()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "uuid")]
impl<'r> Decode<'r, Turso> for uuid::fmt::Simple {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(uuid::Uuid::parse_str(value.text()?)?.simple())
    }
}

#[cfg(feature = "json")]
impl<T> Type<Turso> for sqlx_core::types::Json<T> {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TEXT")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <str as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "json")]
impl<T> Encode<'_, Turso> for sqlx_core::types::Json<T>
where
    T: serde::Serialize,
{
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.encode_to_string()?));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "json")]
impl<'r, T> Decode<'r, Turso> for sqlx_core::types::Json<T>
where
    T: 'r + serde::Deserialize<'r>,
{
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        match &value.value.kind {
            TursoValueKind::Text(_) => Self::decode_from_string(value.text()?),
            TursoValueKind::Blob(_) => Self::decode_from_bytes(value.blob()?),
            _ => Err(Box::new(TursoAdapterError::unsupported(
                "decoding non-text/blob Turso value as json",
            ))),
        }
    }
}

#[cfg(feature = "chrono")]
impl<Tz> Type<Turso> for chrono::DateTime<Tz>
where
    Tz: chrono::TimeZone,
{
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <chrono::NaiveDateTime as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "chrono")]
impl<Tz> Encode<'_, Turso> for chrono::DateTime<Tz>
where
    Tz: chrono::TimeZone,
    Tz::Offset: std::fmt::Display,
{
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        use chrono::SecondsFormat;

        buf.push(TursoValue::text(
            self.to_rfc3339_opts(SecondsFormat::AutoSi, false),
        ));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::DateTime<chrono::FixedOffset> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_chrono_datetime(value)
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::DateTime<chrono::Utc> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        use chrono::TimeZone;

        Ok(chrono::Utc.from_utc_datetime(&decode_chrono_datetime(value)?.naive_utc()))
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::DateTime<chrono::Local> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        use chrono::TimeZone;

        Ok(chrono::Local.from_utc_datetime(&decode_chrono_datetime(value)?.naive_utc()))
    }
}

#[cfg(feature = "chrono")]
impl Type<Turso> for chrono::NaiveDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_datetime_affinity()
            || ty.has_text_affinity()
            || ty.has_integer_affinity()
            || ty.has_real_affinity()
    }
}

#[cfg(feature = "chrono")]
impl Encode<'_, Turso> for chrono::NaiveDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%F %T%.f").to_string()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::NaiveDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(decode_chrono_datetime(value)?.naive_local())
    }
}

#[cfg(feature = "chrono")]
impl Type<Turso> for chrono::NaiveDate {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATE")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_date_affinity() || ty.has_text_affinity()
    }
}

#[cfg(feature = "chrono")]
impl Encode<'_, Turso> for chrono::NaiveDate {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%F").to_string()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::NaiveDate {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(chrono::NaiveDate::parse_from_str(value.text()?, "%F")?)
    }
}

#[cfg(feature = "chrono")]
impl Type<Turso> for chrono::NaiveTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_time_affinity() || ty.has_text_affinity()
    }
}

#[cfg(feature = "chrono")]
impl Encode<'_, Turso> for chrono::NaiveTime {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%T%.f").to_string()));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl<'r> Decode<'r, Turso> for chrono::NaiveTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let sqlite_time_formats = ["%T.f", "%T%.f", "%R", "%RZ", "%T%.fZ", "%R%:z", "%T%.f%:z"];

        for format in sqlite_time_formats {
            if let Ok(time) = chrono::NaiveTime::parse_from_str(value.text()?, format) {
                return Ok(time);
            }
        }

        Err(format!("invalid time: {}", value.text()?).into())
    }
}

#[cfg(feature = "chrono")]
fn decode_chrono_datetime(
    value: TursoValueRef<'_>,
) -> Result<chrono::DateTime<chrono::FixedOffset>, BoxDynError> {
    let datetime = match value.text_or_integer_or_real()? {
        TursoTemporalValue::Text(value) => decode_chrono_datetime_from_text(value),
        TursoTemporalValue::Integer(value) => decode_chrono_datetime_from_int(value),
        TursoTemporalValue::Real(value) => decode_chrono_datetime_from_real(value),
    };

    if let Some(datetime) = datetime {
        Ok(datetime)
    } else {
        Err("invalid datetime".into())
    }
}

#[cfg(feature = "chrono")]
fn decode_chrono_datetime_from_text(value: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    use chrono::{Offset, TimeZone};

    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(datetime);
    }

    let sqlite_datetime_formats = [
        "%F %T%.f",
        "%F %R",
        "%F %RZ",
        "%F %R%:z",
        "%F %T%.fZ",
        "%F %T%.f%:z",
        "%FT%R",
        "%FT%RZ",
        "%FT%R%:z",
        "%FT%T%.f",
        "%FT%T%.fZ",
        "%FT%T%.f%:z",
    ];

    for format in sqlite_datetime_formats {
        if let Ok(datetime) = chrono::DateTime::parse_from_str(value, format) {
            return Some(datetime);
        }

        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Some(chrono::Utc.fix().from_utc_datetime(&datetime));
        }
    }

    None
}

#[cfg(feature = "chrono")]
fn decode_chrono_datetime_from_int(value: i64) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    use chrono::{Offset, TimeZone};

    chrono::Utc.fix().timestamp_opt(value, 0).single()
}

#[cfg(feature = "chrono")]
fn decode_chrono_datetime_from_real(value: f64) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    use chrono::{Offset, TimeZone};

    let timestamp = (value - 2_440_587.5) * 86_400.0;
    if !timestamp.is_finite() {
        return None;
    }

    let seconds = timestamp.trunc() as i64;
    let nanos = (timestamp.fract() * 1E9).abs() as u32;
    chrono::Utc.fix().timestamp_opt(seconds, nanos).single()
}

#[cfg(feature = "time")]
impl Type<Turso> for time::OffsetDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <time::PrimitiveDateTime as Type<Turso>>::compatible(ty)
    }
}

#[cfg(feature = "time")]
impl Encode<'_, Turso> for time::OffsetDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        use time::format_description::well_known::Rfc3339;

        buf.push(TursoValue::text(self.format(&Rfc3339)?));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "time")]
impl<'r> Decode<'r, Turso> for time::OffsetDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_time_offset_datetime(value)
    }
}

#[cfg(feature = "time")]
impl Type<Turso> for time::PrimitiveDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_datetime_affinity() || ty.has_text_affinity() || ty.has_integer_affinity()
    }
}

#[cfg(feature = "time")]
impl Encode<'_, Turso> for time::PrimitiveDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        let format = time::macros::format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
        );
        buf.push(TursoValue::text(self.format(&format)?));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "time")]
impl<'r> Decode<'r, Turso> for time::PrimitiveDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_time_primitive_datetime(value)
    }
}

#[cfg(feature = "time")]
impl Type<Turso> for time::Date {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATE")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_date_affinity() || ty.has_text_affinity()
    }
}

#[cfg(feature = "time")]
impl Encode<'_, Turso> for time::Date {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        let format = time::macros::format_description!("[year]-[month]-[day]");
        buf.push(TursoValue::text(self.format(&format)?));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "time")]
impl<'r> Decode<'r, Turso> for time::Date {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let format = time::macros::format_description!("[year]-[month]-[day]");
        Ok(time::Date::parse(value.text()?, &format)?)
    }
}

#[cfg(feature = "time")]
impl Type<Turso> for time::Time {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_time_affinity() || ty.has_text_affinity()
    }
}

#[cfg(feature = "time")]
impl Encode<'_, Turso> for time::Time {
    fn encode_by_ref(
        &self,
        buf: &mut <Turso as sqlx_core::database::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        let format = time::macros::format_description!("[hour]:[minute]:[second].[subsecond]");
        buf.push(TursoValue::text(self.format(&format)?));
        Ok(IsNull::No)
    }
}

#[cfg(feature = "time")]
impl<'r> Decode<'r, Turso> for time::Time {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let sqlite_time_formats = [
            time::macros::format_description!("[hour]:[minute]:[second].[subsecond]"),
            time::macros::format_description!("[hour]:[minute]:[second]"),
            time::macros::format_description!("[hour]:[minute]"),
        ];

        for format in sqlite_time_formats {
            if let Ok(time) = time::Time::parse(value.text()?, &format) {
                return Ok(time);
            }
        }

        Err(format!("invalid time: {}", value.text()?).into())
    }
}

#[cfg(feature = "time")]
fn decode_time_offset_datetime(
    value: TursoValueRef<'_>,
) -> Result<time::OffsetDateTime, BoxDynError> {
    use time::format_description::well_known::Rfc3339;

    let datetime = match value.text_or_integer_or_real()? {
        TursoTemporalValue::Text(value) => time::OffsetDateTime::parse(value, &Rfc3339)
            .or_else(|_| decode_time_primitive_datetime_from_text(value).map(|dt| dt.assume_utc())),
        TursoTemporalValue::Integer(value) => Ok(time::OffsetDateTime::from_unix_timestamp(value)?),
        TursoTemporalValue::Real(_) => Err("REAL datetimes are not supported by sqlx time".into()),
    }?;

    Ok(datetime)
}

#[cfg(feature = "time")]
fn decode_time_primitive_datetime(
    value: TursoValueRef<'_>,
) -> Result<time::PrimitiveDateTime, BoxDynError> {
    let datetime = match value.text_or_integer_or_real()? {
        TursoTemporalValue::Text(value) => decode_time_primitive_datetime_from_text(value),
        TursoTemporalValue::Integer(value) => {
            let parsed = time::OffsetDateTime::from_unix_timestamp(value)?;
            Ok(time::PrimitiveDateTime::new(parsed.date(), parsed.time()))
        }
        TursoTemporalValue::Real(_) => Err("REAL datetimes are not supported by sqlx time".into()),
    }?;

    Ok(datetime)
}

#[cfg(feature = "time")]
fn decode_time_primitive_datetime_from_text(
    value: &str,
) -> Result<time::PrimitiveDateTime, BoxDynError> {
    let default_format = time::macros::format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
    );
    if let Ok(datetime) = time::PrimitiveDateTime::parse(value, &default_format) {
        return Ok(datetime);
    }

    let t_format = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]"
    );
    Ok(time::PrimitiveDateTime::parse(value, &t_format)?)
}

sqlx_core::impl_encode_for_option!(Turso);
