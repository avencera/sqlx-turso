use std::{borrow::Cow, error::Error, fmt};

use sqlx_core::error::{DatabaseError, Error as SqlxError, ErrorKind};

/// Error type for adapter-owned setup and unsupported surfaces
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TursoAdapterError {
    message: String,
}

impl TursoAdapterError {
    /// Creates an error for a surface that is not implemented yet
    pub fn unsupported(surface: impl Into<String>) -> Self {
        let surface = surface.into();
        Self {
            message: format!("{surface} is not supported by sqlx-turso yet"),
        }
    }

    /// Returns the error message
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TursoAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TursoAdapterError {}

pub(crate) fn unsupported_sqlx(surface: impl Into<String>) -> sqlx_core::error::Error {
    let error = TursoAdapterError::unsupported(surface);
    sqlx_core::error::Error::Configuration(Box::new(error))
}

pub(crate) fn unsupported_autovacuum() -> sqlx_core::error::Error {
    sqlx_core::error::Error::Configuration(
        "PRAGMA auto_vacuum is not supported by sqlx-turso yet. Turso keeps autovacuum behind an experimental opt-in because there are still open correctness issues in that code path, and the pinned Rust builder does not expose an autovacuum opt-in. Regular VACUUM is still supported behind TursoExperimentalFeature::Vacuum"
            .into(),
    )
}

/// Database error returned by the Turso engine
#[derive(Debug)]
pub struct TursoDatabaseError {
    code: &'static str,
    message: String,
}

impl TursoDatabaseError {
    pub(crate) fn from_turso(error: turso::Error) -> SqlxError {
        SqlxError::database(Self {
            code: turso_error_code(&error),
            message: error.to_string(),
        })
    }
}

impl fmt::Display for TursoDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TursoDatabaseError {}

impl DatabaseError for TursoDatabaseError {
    fn message(&self) -> &str {
        &self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        sqlx_error_kind(self.code, &self.message)
    }
}

fn turso_error_code(error: &turso::Error) -> &'static str {
    match error {
        turso::Error::ToSqlConversionFailure(_) => "TURSO_TO_SQL_CONVERSION_FAILURE",
        turso::Error::QueryReturnedNoRows => "TURSO_QUERY_RETURNED_NO_ROWS",
        turso::Error::ConversionFailure(_) => "TURSO_CONVERSION_FAILURE",
        turso::Error::Busy(_) => "SQLITE_BUSY",
        turso::Error::BusySnapshot(_) => "SQLITE_BUSY_SNAPSHOT",
        turso::Error::Interrupt(_) => "SQLITE_INTERRUPT",
        turso::Error::Error(_) => "SQLITE_ERROR",
        turso::Error::Misuse(_) => "SQLITE_MISUSE",
        turso::Error::Constraint(_) => "SQLITE_CONSTRAINT",
        turso::Error::Readonly(_) => "SQLITE_READONLY",
        turso::Error::DatabaseFull(_) => "SQLITE_FULL",
        turso::Error::NotAdb(_) => "SQLITE_NOTADB",
        turso::Error::Corrupt(_) => "SQLITE_CORRUPT",
        turso::Error::IoError(_, _) => "SQLITE_IOERR",
    }
}

fn sqlx_error_kind(code: &str, message: &str) -> ErrorKind {
    match code {
        "SQLITE_CONSTRAINT" if constraint_message_contains(message, "unique") => {
            ErrorKind::UniqueViolation
        }
        "SQLITE_CONSTRAINT" if constraint_message_contains(message, "primary") => {
            ErrorKind::UniqueViolation
        }
        "SQLITE_CONSTRAINT" if constraint_message_contains(message, "foreign") => {
            ErrorKind::ForeignKeyViolation
        }
        "SQLITE_CONSTRAINT" if constraint_message_contains(message, "not null") => {
            ErrorKind::NotNullViolation
        }
        "SQLITE_CONSTRAINT" if constraint_message_contains(message, "check") => {
            ErrorKind::CheckViolation
        }
        _ => ErrorKind::Other,
    }
}

fn constraint_message_contains(message: &str, needle: &str) -> bool {
    message.to_ascii_lowercase().contains(needle)
}
