use crate::{Turso, TursoTypeInfo, TursoValue};

use sqlx_core::{
    config::macros::{DateTimeCrate, NumericCrate, PreferredCrates},
    type_checking::{Error as TypeCheckingError, FmtValue, ParamChecking, TypeChecking},
    types::Type,
    value::Value,
};

/// Runtime metadata extension point for checked Turso query macros
pub trait TursoDescribeExt {}

/// Type-checking extension point for checked Turso query macros
pub trait TursoTypeChecking {}

impl TursoDescribeExt for Turso {}
impl TursoTypeChecking for Turso {}

impl TypeChecking for Turso {
    const PARAM_CHECKING: ParamChecking = ParamChecking::Weak;

    fn param_type_for_id(
        info: &TursoTypeInfo,
        preferred_crates: &PreferredCrates,
    ) -> Result<&'static str, TypeCheckingError> {
        type_path_for_id(info, preferred_crates)
    }

    fn return_type_for_id(
        info: &TursoTypeInfo,
        preferred_crates: &PreferredCrates,
    ) -> Result<&'static str, TypeCheckingError> {
        type_path_for_id(info, preferred_crates)
    }

    fn get_feature_gate(_info: &TursoTypeInfo) -> Option<&'static str> {
        None
    }

    fn fmt_value_debug(value: &TursoValue) -> FmtValue<'_, Self> {
        let info = value.type_info();

        #[cfg(feature = "time")]
        {
            if <sqlx_core::types::time::PrimitiveDateTime as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::time::PrimitiveDateTime>(value);
            }

            if <sqlx_core::types::time::Date as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::time::Date>(value);
            }
        }

        #[cfg(feature = "chrono")]
        {
            if <sqlx_core::types::chrono::NaiveDateTime as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::chrono::NaiveDateTime>(value);
            }

            if <sqlx_core::types::chrono::NaiveDate as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::chrono::NaiveDate>(value);
            }
        }

        if <bool as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<bool>(value);
        }

        if <i64 as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<i64>(value);
        }

        if <f64 as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<f64>(value);
        }

        if <String as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<String>(value);
        }

        if <Vec<u8> as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<Vec<u8>>(value);
        }

        FmtValue::unknown(value)
    }
}

fn type_path_for_id(
    info: &TursoTypeInfo,
    preferred_crates: &PreferredCrates,
) -> Result<&'static str, TypeCheckingError> {
    if preferred_crates.numeric == NumericCrate::BigDecimal
        || preferred_crates.numeric == NumericCrate::RustDecimal
    {
        return Err(TypeCheckingError::NumericCrateFeatureNotEnabled);
    }

    if let Some(path) = datetime_type_path(info, preferred_crates) {
        return path;
    }

    if <bool as Type<Turso>>::type_info() == *info {
        return Ok("bool");
    }

    if <i32 as Type<Turso>>::type_info() == *info {
        return Ok("i32");
    }

    if <i64 as Type<Turso>>::type_info() == *info || <i64 as Type<Turso>>::compatible(info) {
        return Ok("i64");
    }

    if <f64 as Type<Turso>>::type_info() == *info || <f64 as Type<Turso>>::compatible(info) {
        return Ok("f64");
    }

    if <String as Type<Turso>>::type_info() == *info || <String as Type<Turso>>::compatible(info) {
        return Ok("String");
    }

    if <Vec<u8> as Type<Turso>>::type_info() == *info || <Vec<u8> as Type<Turso>>::compatible(info)
    {
        return Ok("Vec<u8>");
    }

    #[cfg(feature = "uuid")]
    if <sqlx_core::types::Uuid as Type<Turso>>::type_info() == *info
        || <sqlx_core::types::Uuid as Type<Turso>>::compatible(info)
    {
        return Ok("::sqlx_turso::sqlx::types::Uuid");
    }

    Err(TypeCheckingError::NoMappingFound)
}

fn datetime_type_path(
    info: &TursoTypeInfo,
    preferred_crates: &PreferredCrates,
) -> Option<Result<&'static str, TypeCheckingError>> {
    match preferred_crates.date_time {
        DateTimeCrate::Time => Some(
            time_type_path(info).unwrap_or(Err(TypeCheckingError::DateTimeCrateFeatureNotEnabled)),
        ),
        DateTimeCrate::Chrono => Some(
            chrono_type_path(info)
                .unwrap_or(Err(TypeCheckingError::DateTimeCrateFeatureNotEnabled)),
        ),
        DateTimeCrate::Inferred => {
            #[cfg(feature = "time")]
            if let Some(path) = time_type_path(info) {
                return Some(path);
            }

            #[cfg(feature = "chrono")]
            if let Some(path) = chrono_type_path(info) {
                return Some(path);
            }

            None
        }
    }
}

fn chrono_type_path(_info: &TursoTypeInfo) -> Option<Result<&'static str, TypeCheckingError>> {
    #[cfg(feature = "chrono")]
    {
        if <sqlx_core::types::chrono::NaiveDate as Type<Turso>>::type_info() == *_info
            || _info.has_date_affinity()
        {
            return Some(Ok("::sqlx_turso::sqlx::types::chrono::NaiveDate"));
        }

        if <sqlx_core::types::chrono::NaiveDateTime as Type<Turso>>::type_info() == *_info
            || _info.has_datetime_affinity()
        {
            return Some(Ok("::sqlx_turso::sqlx::types::chrono::NaiveDateTime"));
        }
    }

    None
}

fn time_type_path(_info: &TursoTypeInfo) -> Option<Result<&'static str, TypeCheckingError>> {
    #[cfg(feature = "time")]
    {
        if <sqlx_core::types::time::Date as Type<Turso>>::type_info() == *_info
            || _info.has_date_affinity()
        {
            return Some(Ok("::sqlx_turso::sqlx::types::time::Date"));
        }

        if <sqlx_core::types::time::PrimitiveDateTime as Type<Turso>>::type_info() == *_info
            || _info.has_datetime_affinity()
        {
            return Some(Ok("::sqlx_turso::sqlx::types::time::PrimitiveDateTime"));
        }
    }

    None
}

#[cfg(feature = "macros")]
impl sqlx_macros_core::database::DatabaseExt for Turso {
    const DATABASE_PATH: &'static str = "::sqlx_turso::Turso";
    const ROW_PATH: &'static str = "::sqlx_turso::TursoRow";

    fn describe_blocking(
        query: &str,
        database_url: &str,
        driver_config: &sqlx_core::config::drivers::Config,
    ) -> sqlx_core::Result<sqlx_core::describe::Describe<Self>> {
        use sqlx_macros_core::database::CachingDescribeBlocking;

        static CACHE: CachingDescribeBlocking<Turso> = CachingDescribeBlocking::new();

        CACHE.describe(query, database_url, driver_config)
    }
}
