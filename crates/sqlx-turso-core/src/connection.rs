use std::{collections::VecDeque, fmt, future};

use sqlx_core::{
    common::StatementCache, connection::Connection, error::Error, transaction::Transaction,
};

use crate::{Turso, TursoStatement, driver::TursoDriverConnection, options::TursoConnectOptions};

/// SQLx connection handle for Turso databases
pub struct TursoConnection {
    options: TursoConnectOptions,
    raw: turso::Connection,
    #[cfg(feature = "sync")]
    sync: Option<turso::sync::Database>,
    statements: StatementCache<TursoStatement>,
    transaction_depth: usize,
    pending_rollback_depths: VecDeque<usize>,
    rollback_failed: bool,
}

impl fmt::Debug for TursoConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoConnection")
            .field("options", &self.options)
            .field("transaction_depth", &self.transaction_depth)
            .field("pending_rollback_depths", &self.pending_rollback_depths)
            .field("rollback_failed", &self.rollback_failed)
            .finish_non_exhaustive()
    }
}

impl TursoConnection {
    pub(crate) fn new(options: TursoConnectOptions, connection: TursoDriverConnection) -> Self {
        Self {
            statements: StatementCache::new(options.get_statement_cache_capacity()),
            options,
            raw: connection.raw,
            #[cfg(feature = "sync")]
            sync: connection.sync,
            transaction_depth: 0,
            pending_rollback_depths: VecDeque::new(),
            rollback_failed: false,
        }
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_depth
    }

    pub(crate) fn increment_transaction_depth(&mut self) {
        self.transaction_depth += 1;
    }

    pub(crate) fn decrement_transaction_depth(&mut self) {
        self.transaction_depth = self.transaction_depth.saturating_sub(1);
    }

    pub(crate) fn mark_rollback_needed(&mut self) {
        if self.transaction_depth > 0 {
            self.pending_rollback_depths
                .push_back(self.transaction_depth);
            self.decrement_transaction_depth();
        }
    }

    pub(crate) async fn clear_pending_rollback(&mut self) -> Result<(), Error> {
        if self.rollback_failed {
            return Err(Error::WorkerCrashed);
        }

        while let Some(depth) = self.pending_rollback_depths.pop_front() {
            let sql = sqlx_core::transaction::rollback_ansi_transaction_sql(depth);
            if let Err(error) = self.raw().execute(sql.as_str(), ()).await {
                if depth == 1 && rollback_error_is_inactive_transaction(&error) {
                    continue;
                }

                self.rollback_failed = true;
                self.pending_rollback_depths.clear();
                return Err(crate::executor::map_turso_error(error));
            }
        }

        Ok(())
    }

    /// Returns the options used to create this connection
    pub fn options(&self) -> &TursoConnectOptions {
        &self.options
    }

    pub(crate) fn raw(&self) -> &turso::Connection {
        &self.raw
    }

    /// Pushes local sync changes to the configured remote
    #[cfg(feature = "sync")]
    pub async fn sync_push(&self) -> Result<(), Error> {
        self.sync_database()?
            .push()
            .await
            .map_err(crate::executor::map_turso_error)
    }

    /// Pulls remote sync changes and applies them locally
    #[cfg(feature = "sync")]
    pub async fn sync_pull(&self) -> Result<bool, Error> {
        self.sync_database()?
            .pull()
            .await
            .map_err(crate::executor::map_turso_error)
    }

    /// Checkpoints the synced database WAL
    #[cfg(feature = "sync")]
    pub async fn sync_checkpoint(&self) -> Result<(), Error> {
        self.sync_database()?
            .checkpoint()
            .await
            .map_err(crate::executor::map_turso_error)
    }

    /// Returns synced database statistics
    #[cfg(feature = "sync")]
    pub async fn sync_stats(&self) -> Result<turso::sync::DatabaseSyncStats, Error> {
        self.sync_database()?
            .stats()
            .await
            .map_err(crate::executor::map_turso_error)
    }

    #[cfg(feature = "sync")]
    fn sync_database(&self) -> Result<&turso::sync::Database, Error> {
        self.sync
            .as_ref()
            .ok_or_else(|| crate::error::unsupported_sqlx("non-sync Turso connections"))
    }

    pub(crate) fn cached_statement(&mut self, sql: &str) -> Option<TursoStatement> {
        self.statements.get_mut(sql).cloned()
    }

    pub(crate) fn cache_statement(&mut self, sql: &str, statement: TursoStatement) {
        if self.statements.is_enabled() {
            self.statements.insert(sql, statement);
        }
    }
}

fn rollback_error_is_inactive_transaction(error: &turso::Error) -> bool {
    error
        .to_string()
        .contains("cannot rollback - no transaction is active")
}

impl Connection for TursoConnection {
    type Database = Turso;
    type Options = TursoConnectOptions;

    async fn close(self) -> Result<(), Error> {
        Ok(())
    }

    async fn close_hard(self) -> Result<(), Error> {
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), Error> {
        let _ = self.raw();
        Ok(())
    }

    async fn begin(&mut self) -> Result<Transaction<'_, Self::Database>, Error> {
        Transaction::begin(self, None).await
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn should_flush(&self) -> bool {
        false
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.statements.len()
    }

    fn clear_cached_statements(&mut self) -> impl Future<Output = Result<(), Error>> + Send + '_
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.statements.clear();
        future::ready(Ok(()))
    }
}

impl AsRef<TursoConnectOptions> for TursoConnection {
    fn as_ref(&self) -> &TursoConnectOptions {
        &self.options
    }
}
