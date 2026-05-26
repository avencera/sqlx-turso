use std::{borrow::Cow, fmt};

use sqlx_core::type_info::TypeInfo;

/// SQLite-compatible Turso type information
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "offline", derive(serde::Deserialize, serde::Serialize))]
pub struct TursoTypeInfo {
    name: Cow<'static, str>,
}

impl TursoTypeInfo {
    /// NULL storage class
    pub const NULL: Self = Self {
        name: Cow::Borrowed("NULL"),
    };

    /// Creates a type info value from a static type name
    pub const fn new(name: &'static str) -> Self {
        Self {
            name: Cow::Borrowed(name),
        }
    }

    /// Creates a type info value from a dynamic type name
    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            name: Cow::Owned(name.into()),
        }
    }

    pub(crate) fn has_integer_affinity(&self) -> bool {
        let name = self.name.to_ascii_lowercase();
        name == "int4" || name == "int8" || name.contains("int")
    }

    pub(crate) fn has_bool_affinity(&self) -> bool {
        let name = self.name.to_ascii_lowercase();
        name == "boolean" || name == "bool"
    }

    pub(crate) fn has_text_affinity(&self) -> bool {
        let name = self.name.to_ascii_lowercase();
        name.contains("char") || name.contains("clob") || name.contains("text")
    }

    pub(crate) fn has_blob_affinity(&self) -> bool {
        self.name.eq_ignore_ascii_case("blob")
    }

    pub(crate) fn has_real_affinity(&self) -> bool {
        let name = self.name.to_ascii_lowercase();
        name.contains("real") || name.contains("floa") || name.contains("doub")
    }

    #[cfg(any(feature = "chrono", feature = "time"))]
    pub(crate) fn has_date_affinity(&self) -> bool {
        self.name.eq_ignore_ascii_case("date")
    }

    #[cfg(any(feature = "chrono", feature = "time"))]
    pub(crate) fn has_time_affinity(&self) -> bool {
        self.name.eq_ignore_ascii_case("time")
    }

    #[cfg(any(feature = "chrono", feature = "time"))]
    pub(crate) fn has_datetime_affinity(&self) -> bool {
        let name = self.name.to_ascii_lowercase();
        name == "datetime" || name == "timestamp"
    }
}

impl TypeInfo for TursoTypeInfo {
    fn is_null(&self) -> bool {
        self.name == Self::NULL.name
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for TursoTypeInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.name)
    }
}

impl Default for TursoTypeInfo {
    fn default() -> Self {
        Self::NULL
    }
}
