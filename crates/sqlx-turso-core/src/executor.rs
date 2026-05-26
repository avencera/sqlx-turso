use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};
use sqlx_core::{
    column::Column,
    error::Error,
    executor::{Execute, Executor},
    sql_str::SqlStr,
    statement::Statement,
};

use crate::{
    Turso, TursoArguments, TursoColumn, TursoConnection, TursoQueryResult, TursoRow,
    TursoStatement, TursoValue, error::unsupported_sqlx,
};

impl TursoConnection {
    async fn fetch_many_sql(
        &mut self,
        sql: SqlStr,
        persistent: bool,
        arguments: Option<TursoArguments>,
    ) -> Result<Vec<Either<TursoQueryResult, TursoRow>>, Error> {
        self.clear_pending_rollback().await?;

        if may_contain_multiple_statements(sql.as_str()) {
            if arguments.is_some() {
                return Err(unsupported_sqlx("Turso SQLx batch query arguments"));
            }

            return self.execute_batch_sql(sql.as_str()).await;
        }

        if arguments.is_some()
            && let Some(placeholder) = unsupported_named_placeholder(sql.as_str())
        {
            return Err(unsupported_sqlx(format!(
                "Turso SQLx non-integer named placeholder `{placeholder}`"
            )));
        }

        let mut statement = if persistent {
            match self.prepare_sql(sql.clone()).await?.raw() {
                Some(statement) => statement,
                None => self
                    .raw()
                    .prepare(sql.as_str())
                    .await
                    .map_err(map_turso_error)?,
            }
        } else {
            self.raw()
                .prepare(sql.as_str())
                .await
                .map_err(map_turso_error)?
        };
        let columns = turso_columns(statement.columns());
        let mut rows = match arguments {
            Some(arguments) => statement
                .query(arguments.into_turso_values())
                .await
                .map_err(map_turso_error)?,
            None => statement.query(()).await.map_err(map_turso_error)?,
        };
        let mut results = Vec::new();

        while let Some(row) = rows.next().await.map_err(map_turso_error)? {
            let mut values = Vec::with_capacity(columns.len());
            for (index, column) in columns.iter().enumerate() {
                values.push(
                    TursoValue::from_turso(row.get_value(index).map_err(map_turso_error)?)
                        .with_type_info(column.type_info().clone()),
                );
            }
            results.push(Either::Right(TursoRow::new(columns.clone(), values)));
        }

        let rows_affected = statement.n_change();
        results.push(Either::Left(TursoQueryResult::new(rows_affected)));
        Ok(results)
    }

    async fn execute_batch_sql(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Either<TursoQueryResult, TursoRow>>, Error> {
        self.raw()
            .execute_batch(sql)
            .await
            .map_err(map_turso_error)?;
        Ok(vec![Either::Left(TursoQueryResult::default())])
    }

    async fn prepare_sql(&mut self, sql: SqlStr) -> Result<TursoStatement, Error> {
        if let Some(statement) = self.cached_statement(sql.as_str()) {
            return Ok(statement);
        }

        let statement = self
            .raw()
            .prepare(sql.as_str())
            .await
            .map_err(map_turso_error)?;
        let statement = TursoStatement::with_raw(
            sql.as_str().to_owned(),
            turso_columns(statement.columns()),
            statement,
        );
        self.cache_statement(sql.as_str(), statement.clone());
        Ok(statement)
    }
}

impl<'c> Executor<'c> for &'c mut TursoConnection {
    type Database = Turso;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<TursoQueryResult, TursoRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();

        stream::once(async move {
            self.fetch_many_sql(sql, persistent, arguments?)
                .await
                .map(|items| stream::iter(items.into_iter().map(Ok::<_, Error>)).boxed())
        })
        .try_flatten()
        .boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<TursoRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        async move {
            let mut stream = self.fetch_many(query);

            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(row));
                }
            }

            Ok(None)
        }
        .boxed()
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [<Self::Database as sqlx_core::database::Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<TursoStatement, Error>>
    where
        'c: 'e,
    {
        async move { self.prepare_sql(sql).await }.boxed()
    }

    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        async move {
            let statement = self.prepare_sql(sql).await?;
            let columns = statement.columns().to_vec();
            let nullable = vec![None; columns.len()];

            Ok(sqlx_core::describe::Describe {
                columns,
                parameters: None,
                nullable,
            })
        }
        .boxed()
    }
}

fn may_contain_multiple_statements(sql: &str) -> bool {
    let mut saw_statement = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                saw_statement = true;
                while let Some(quoted) = next_char(&mut chars) {
                    if quoted == '\'' && chars.next_if_eq(&'\'').is_none() {
                        break;
                    }
                }
            }
            '"' | '`' => {
                saw_statement = true;
                for quoted in chars.by_ref() {
                    if quoted == ch {
                        break;
                    }
                }
            }
            '[' => {
                saw_statement = true;
                for quoted in chars.by_ref() {
                    if quoted == ']' {
                        break;
                    }
                }
            }
            '-' if chars.next_if_eq(&'-').is_some() => {
                for comment in chars.by_ref() {
                    if comment == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.next_if_eq(&'*').is_some() => {
                let mut previous = '\0';
                for comment in chars.by_ref() {
                    if previous == '*' && comment == '/' {
                        break;
                    }
                    previous = comment;
                }
            }
            ';' if saw_statement && has_remaining_statement_text(chars.clone()) => {
                return true;
            }
            ch if !ch.is_whitespace() => {
                saw_statement = true;
            }
            _ => {}
        }
    }

    false
}

fn unsupported_named_placeholder(sql: &str) -> Option<String> {
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => skip_single_quoted(&mut chars),
            '"' | '`' => skip_quoted(&mut chars, ch),
            '[' => skip_quoted(&mut chars, ']'),
            '-' if chars.next_if_eq(&'-').is_some() => skip_line_comment(&mut chars),
            '/' if chars.next_if_eq(&'*').is_some() => skip_block_comment(&mut chars),
            '$' => {
                let name = read_placeholder_name(&mut chars);
                if name.parse::<usize>().is_err() {
                    return Some(format!("${name}"));
                }
            }
            ':' | '@' => {
                let name = read_placeholder_name(&mut chars);
                if !name.is_empty() {
                    return Some(format!("{ch}{name}"));
                }
            }
            _ => {}
        }
    }

    None
}

fn read_placeholder_name<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut name = String::new();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
            let _ = chars.next();
        } else {
            break;
        }
    }

    name
}

fn skip_single_quoted<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    while let Some(quoted) = next_char(chars) {
        if quoted == '\'' && chars.next_if_eq(&'\'').is_none() {
            break;
        }
    }
}

fn skip_quoted<I>(chars: &mut std::iter::Peekable<I>, quote: char)
where
    I: Iterator<Item = char>,
{
    for quoted in chars.by_ref() {
        if quoted == quote {
            break;
        }
    }
}

fn skip_line_comment<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    for comment in chars.by_ref() {
        if comment == '\n' {
            break;
        }
    }
}

fn skip_block_comment<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    let mut previous = '\0';
    for comment in chars.by_ref() {
        if previous == '*' && comment == '/' {
            break;
        }
        previous = comment;
    }
}

fn next_char<I>(chars: &mut I) -> Option<char>
where
    I: Iterator<Item = char>,
{
    chars.next()
}

fn has_remaining_statement_text<I>(chars: I) -> bool
where
    I: Iterator<Item = char>,
{
    let mut chars = chars.peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '-' if chars.next_if_eq(&'-').is_some() => {
                for comment in chars.by_ref() {
                    if comment == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.next_if_eq(&'*').is_some() => {
                let mut previous = '\0';
                for comment in chars.by_ref() {
                    if previous == '*' && comment == '/' {
                        break;
                    }
                    previous = comment;
                }
            }
            ch if !ch.is_whitespace() && ch != ';' => return true,
            _ => {}
        }
    }

    false
}

fn turso_columns(columns: Vec<turso::Column>) -> Vec<TursoColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| TursoColumn::from_turso(ordinal, column))
        .collect()
}

pub(crate) fn map_turso_error(error: turso::Error) -> Error {
    crate::error::TursoDatabaseError::from_turso(error)
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use sqlx_core::{
        column::Column,
        connection::{ConnectOptions, Connection},
        error::ErrorKind,
        executor::Executor,
        row::Row,
        sql_str::SqlStr,
        value::ValueRef,
    };

    use crate::TursoConnectOptions;

    #[tokio::test]
    async fn executes_literal_sql_and_fetches_rows() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .await?;
        let result = (&mut connection)
            .execute("INSERT INTO test (name) VALUES ('alice')")
            .await?;
        assert_eq!(result.rows_affected(), 1);

        let rows = (&mut connection)
            .fetch_all("SELECT id, name FROM test")
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns()[0].name(), "id");
        assert!(!rows[0].try_get_raw(0)?.is_null());

        Ok(())
    }

    #[tokio::test]
    async fn executes_multi_statement_batch() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT); \
                 INSERT INTO test (name) VALUES ('alice'); \
                 INSERT INTO test (name) VALUES ('bob')",
            )
            .await?;

        let rows = (&mut connection)
            .fetch_all("SELECT name FROM test ORDER BY id")
            .await?;
        assert_eq!(rows.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn optional_fetch_returns_none_for_no_rows() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = (&mut connection)
            .fetch_optional("SELECT 1 WHERE false")
            .await?;
        assert!(row.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn semicolon_inside_string_stays_single_statement() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let rows = (&mut connection).fetch_all("SELECT ';'").await?;
        assert_eq!(rows.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn binds_anonymous_and_numbered_arguments() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = sqlx_core::query::query::<crate::Turso>(
            "SELECT ? AS anonymous, ?2 AS numbered_question, $3 AS numbered_dollar",
        )
        .bind(11_i64)
        .bind(22_i64)
        .bind(33_i64)
        .fetch_one(&mut connection)
        .await?;

        assert_eq!(row.try_get::<i64, _>("anonymous")?, 11);
        assert_eq!(row.try_get::<i64, _>("numbered_question")?, 22);
        assert_eq!(row.try_get::<i64, _>("numbered_dollar")?, 33);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_non_integer_named_placeholders() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let error = sqlx_core::query::query::<crate::Turso>("SELECT $name")
            .bind(11_i64)
            .fetch_one(&mut connection)
            .await
            .expect_err("named placeholder should be rejected");
        assert!(error.to_string().contains("non-integer named placeholder"));

        Ok(())
    }

    #[tokio::test]
    async fn round_trips_basic_storage_classes() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = sqlx_core::query::query::<crate::Turso>(
            "SELECT ? AS int_value, ? AS bool_value, ? AS real_value, \
             ? AS text_value, ? AS blob_value, ? AS null_value",
        )
        .bind(42_i64)
        .bind(true)
        .bind(3.5_f64)
        .bind("hello")
        .bind(vec![1_u8, 2, 3])
        .bind(Option::<i64>::None)
        .fetch_one(&mut connection)
        .await?;

        assert_eq!(row.try_get::<u32, _>("int_value")?, 42);
        assert!(row.try_get::<bool, _>("bool_value")?);
        assert_eq!(row.try_get::<f64, _>("real_value")?, 3.5);
        assert_eq!(row.try_get::<String, _>("text_value")?, "hello");
        assert_eq!(row.try_get::<Vec<u8>, _>("blob_value")?, [1, 2, 3]);
        assert_eq!(row.try_get::<Option<i64>, _>("null_value")?, None);

        Ok(())
    }

    #[tokio::test]
    async fn checked_unsigned_decode_rejects_negative_values() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = (&mut connection).fetch_one("SELECT -1 AS value").await?;
        assert!(row.try_get::<u32, _>("value").is_err());

        Ok(())
    }

    #[tokio::test]
    async fn declared_type_affinity_drives_decode_compatibility() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute(
                "CREATE TABLE test (\
                 id BIGINT, \
                 flag BOOLEAN, \
                 label VARCHAR(20), \
                 payload BLOB, \
                 score DOUBLE PRECISION\
                 )",
            )
            .await?;

        sqlx_core::query::query::<crate::Turso>(
            "INSERT INTO test (id, flag, label, payload, score) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(7_i64)
        .bind(true)
        .bind("alice")
        .bind(vec![9_u8, 8, 7])
        .bind(4.25_f64)
        .execute(&mut connection)
        .await?;

        let row = (&mut connection)
            .fetch_one("SELECT id, flag, label, payload, score FROM test")
            .await?;

        assert_eq!(row.try_get::<i64, _>("id")?, 7);
        assert!(row.try_get::<bool, _>("flag")?);
        assert_eq!(row.try_get::<String, _>("label")?, "alice");
        assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [9, 8, 7]);
        assert_eq!(row.try_get::<f64, _>("score")?, 4.25);

        Ok(())
    }

    #[tokio::test]
    async fn supports_builtin_regexp_operator() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = (&mut connection)
            .fetch_one("SELECT 'alphabet' REGEXP '^alpha' AS matched")
            .await?;
        assert_eq!(row.try_get::<i64, _>("matched")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn maps_turso_database_errors_to_sqlx_database_errors() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let error = (&mut connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await
            .expect_err("duplicate primary key should fail");

        let database_error = error
            .as_database_error()
            .expect("Turso errors should map to SQLx database errors");

        assert_eq!(database_error.code().as_deref(), Some("SQLITE_CONSTRAINT"));
        assert_eq!(database_error.kind(), ErrorKind::UniqueViolation);

        Ok(())
    }

    #[cfg(feature = "offline")]
    #[tokio::test]
    async fn serializes_describe_metadata_for_offline_mode() -> sqlx_core::Result<()> {
        use sqlx_core::sql_str::SqlSafeStr;

        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await?;

        let describe = (&mut connection)
            .describe("SELECT id, name FROM test ORDER BY id".into_sql_str())
            .await?;
        let serialized = serde_json::to_string(&describe).map_err(sqlx_core::Error::config)?;
        let deserialized: sqlx_core::describe::Describe<crate::Turso> =
            serde_json::from_str(&serialized).map_err(sqlx_core::Error::config)?;

        assert_eq!(deserialized.columns().len(), 2);
        assert_eq!(deserialized.columns()[0].name(), "id");
        assert_eq!(
            sqlx_core::type_info::TypeInfo::name(deserialized.columns()[1].type_info()),
            "TEXT"
        );

        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[tokio::test]
    async fn round_trips_uuid_values() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        let id = uuid::Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();

        let row = sqlx_core::query::query::<crate::Turso>("SELECT ? AS id")
            .bind(id)
            .fetch_one(&mut connection)
            .await?;

        assert_eq!(row.try_get::<uuid::Uuid, _>("id")?, id);

        Ok(())
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn round_trips_json_values() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        let value = serde_json::json!({"name": "alice", "count": 2});

        let row = sqlx_core::query::query::<crate::Turso>("SELECT ? AS value")
            .bind(sqlx_core::types::Json(value.clone()))
            .fetch_one(&mut connection)
            .await?;

        let decoded = row.try_get::<sqlx_core::types::Json<serde_json::Value>, _>("value")?;
        assert_eq!(decoded.0, value);

        Ok(())
    }

    #[cfg(feature = "chrono")]
    #[tokio::test]
    async fn round_trips_chrono_values() -> sqlx_core::Result<()> {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

        let mut connection = TursoConnectOptions::new().connect().await?;
        let date = NaiveDate::from_ymd_opt(2026, 5, 26).unwrap();
        let time = NaiveTime::from_hms_micro_opt(14, 30, 15, 123_000).unwrap();
        let datetime = NaiveDateTime::new(date, time);

        let row = sqlx_core::query::query::<crate::Turso>(
            "SELECT ? AS date_value, ? AS time_value, ? AS datetime_value",
        )
        .bind(date)
        .bind(time)
        .bind(datetime)
        .fetch_one(&mut connection)
        .await?;

        assert_eq!(row.try_get::<NaiveDate, _>("date_value")?, date);
        assert_eq!(row.try_get::<NaiveTime, _>("time_value")?, time);
        assert_eq!(row.try_get::<NaiveDateTime, _>("datetime_value")?, datetime);

        Ok(())
    }

    #[cfg(feature = "time")]
    #[tokio::test]
    async fn round_trips_time_values() -> sqlx_core::Result<()> {
        use time::{Date, Month, PrimitiveDateTime, Time};

        let mut connection = TursoConnectOptions::new().connect().await?;
        let date = Date::from_calendar_date(2026, Month::May, 26).unwrap();
        let time = Time::from_hms_micro(14, 30, 15, 123_000).unwrap();
        let datetime = PrimitiveDateTime::new(date, time);

        let row = sqlx_core::query::query::<crate::Turso>(
            "SELECT ? AS date_value, ? AS time_value, ? AS datetime_value",
        )
        .bind(date)
        .bind(time)
        .bind(datetime)
        .fetch_one(&mut connection)
        .await?;

        assert_eq!(row.try_get::<Date, _>("date_value")?, date);
        assert_eq!(row.try_get::<Time, _>("time_value")?, time);
        assert_eq!(
            row.try_get::<PrimitiveDateTime, _>("datetime_value")?,
            datetime
        );

        Ok(())
    }

    #[tokio::test]
    async fn dropped_stream_allows_connection_reuse() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        (&mut connection)
            .execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY); \
                 INSERT INTO test (id) VALUES (1); \
                 INSERT INTO test (id) VALUES (2)",
            )
            .await?;

        let mut rows = (&mut connection).fetch("SELECT id FROM test ORDER BY id");
        assert!(rows.try_next().await?.is_some());
        drop(rows);

        let row = (&mut connection).fetch_one("SELECT 3").await?;
        assert!(!row.try_get_raw(0)?.is_null());

        Ok(())
    }

    #[tokio::test]
    async fn dropped_unpolled_stream_does_not_execute_sql() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let rows = (&mut connection).fetch("CREATE TABLE test (id INTEGER PRIMARY KEY)");
        drop(rows);

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn bounds_and_clears_statement_cache() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .statement_cache_capacity(2)
            .connect()
            .await?;

        (&mut connection)
            .prepare(SqlStr::from_static("SELECT 1"))
            .await?;
        (&mut connection)
            .prepare(SqlStr::from_static("SELECT 2"))
            .await?;
        assert_eq!(connection.cached_statements_size(), 2);

        (&mut connection)
            .prepare(SqlStr::from_static("SELECT 3"))
            .await?;
        assert_eq!(connection.cached_statements_size(), 2);

        connection.clear_cached_statements().await?;
        assert_eq!(connection.cached_statements_size(), 0);

        Ok(())
    }
}
