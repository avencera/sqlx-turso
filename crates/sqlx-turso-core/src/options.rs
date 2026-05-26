use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use percent_encoding::percent_decode_str;
use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    error::Error,
};
use url::Url;

use crate::{connection::TursoConnection, driver::TursoDriver};

const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 100;
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

static IN_MEMORY_DB_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Target database location for Turso connections
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TursoDatabaseTarget {
    Memory { name: Arc<str> },
    File(PathBuf),
}

/// Local database encryption settings
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TursoEncryptionOptions {
    cipher: String,
    hexkey: String,
}

/// Remote sync connection settings
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TursoSyncOptions {
    remote_url: String,
    auth_token: Option<String>,
    client_name: Option<String>,
    long_poll_timeout: Option<Duration>,
    bootstrap_if_empty: bool,
}

/// Custom Turso IO backend
#[derive(Clone)]
pub struct TursoIo(Arc<dyn turso::core::IO>);

impl TursoIo {
    /// Creates a custom IO backend wrapper
    pub fn new(io: Arc<dyn turso::core::IO>) -> Self {
        Self(io)
    }

    /// Returns a clone of the wrapped Turso IO backend
    pub fn to_turso(&self) -> Arc<dyn turso::core::IO> {
        self.0.clone()
    }
}

impl std::fmt::Debug for TursoIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TursoIo").field(&"<custom>").finish()
    }
}

/// Experimental Turso builder features
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TursoExperimentalFeature {
    Attach,
    CustomTypes,
    GeneratedColumns,
    IndexMethod,
    MaterializedViews,
    MultiprocessWal,
    Vacuum,
    WithoutRowid,
}

/// Enabled experimental Turso builder features
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TursoExperimentalFeatures {
    attach: bool,
    custom_types: bool,
    generated_columns: bool,
    index_method: bool,
    materialized_views: bool,
    multiprocess_wal: bool,
    vacuum: bool,
    without_rowid: bool,
}

/// Typed connection options for Turso databases
#[derive(Clone, Debug)]
pub struct TursoConnectOptions(Arc<TursoConnectOptionsInner>);

#[derive(Clone, Debug)]
struct TursoConnectOptionsInner {
    target: TursoDatabaseTarget,
    open_mode: TursoOpenMode,
    cache_mode: TursoCacheMode,
    immutable: bool,
    busy_timeout: Duration,
    statement_cache_capacity: usize,
    vfs: Option<String>,
    custom_io: Option<TursoIo>,
    encryption: Option<TursoEncryptionOptions>,
    sync: Option<TursoSyncOptions>,
    experimental_features: TursoExperimentalFeatures,
    pragmas: Vec<(String, Option<String>)>,
    log_settings: LogSettings,
    memory_state: Arc<TursoMemoryState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TursoOpenMode {
    ReadWrite,
    CreateIfMissing,
    ReadOnly,
}

impl TursoOpenMode {
    fn from_read_only(read_only: bool, current: Self) -> Self {
        if read_only {
            return Self::ReadOnly;
        }

        if current == Self::ReadOnly {
            return Self::ReadWrite;
        }

        current
    }

    fn from_create_if_missing(create_if_missing: bool, current: Self) -> Self {
        if create_if_missing {
            return Self::CreateIfMissing;
        }

        if current == Self::CreateIfMissing {
            return Self::ReadWrite;
        }

        current
    }

    fn is_read_only(self) -> bool {
        self == Self::ReadOnly
    }

    fn create_if_missing(self) -> bool {
        self == Self::CreateIfMissing
    }

    fn url_mode(self, target: &TursoDatabaseTarget) -> &'static str {
        if matches!(target, TursoDatabaseTarget::Memory { .. }) {
            return "memory";
        }

        match self {
            Self::ReadWrite => "rw",
            Self::CreateIfMissing => "rwc",
            Self::ReadOnly => "ro",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TursoCacheMode {
    Private,
    Shared,
}

impl TursoCacheMode {
    fn from_shared(shared_cache: bool) -> Self {
        if shared_cache {
            Self::Shared
        } else {
            Self::Private
        }
    }

    fn is_shared(self) -> bool {
        self == Self::Shared
    }

    fn url_value(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Shared => "shared",
        }
    }
}

#[derive(Debug)]
pub(crate) struct TursoMemoryState {
    pub(crate) database: tokio::sync::Mutex<Option<turso::Database>>,
}

impl TursoMemoryState {
    fn new() -> Self {
        Self {
            database: tokio::sync::Mutex::new(None),
        }
    }
}

impl TursoConnectOptions {
    /// Creates options with SQLx SQLite-compatible defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the target database location
    pub fn target(&self) -> &TursoDatabaseTarget {
        &self.0.target
    }

    /// Returns the file path for file-backed databases
    pub fn get_filename(&self) -> Option<&Path> {
        match &self.0.target {
            TursoDatabaseTarget::Memory { .. } => None,
            TursoDatabaseTarget::File(path) => Some(path),
        }
    }

    /// Returns whether the target is an in-memory database
    pub fn is_in_memory(&self) -> bool {
        matches!(self.0.target, TursoDatabaseTarget::Memory { .. })
    }

    /// Returns whether the database should be opened read-only
    pub fn is_read_only(&self) -> bool {
        self.0.open_mode.is_read_only()
    }

    /// Returns whether a missing file-backed database should be created
    pub fn get_create_if_missing(&self) -> bool {
        self.0.open_mode.create_if_missing()
    }

    /// Returns whether Turso should use a shared cache for this database
    pub fn get_shared_cache(&self) -> bool {
        self.0.cache_mode.is_shared()
    }

    /// Returns whether the database should be opened as immutable
    pub fn get_immutable(&self) -> bool {
        self.0.immutable
    }

    /// Returns the configured busy timeout
    pub fn get_busy_timeout(&self) -> Duration {
        self.0.busy_timeout
    }

    /// Returns the SQLx-facing statement cache capacity
    pub fn get_statement_cache_capacity(&self) -> usize {
        self.0.statement_cache_capacity
    }

    /// Returns the configured VFS name
    pub fn get_vfs(&self) -> Option<&str> {
        self.0.vfs.as_deref()
    }

    /// Returns the configured custom IO backend
    pub fn get_custom_io(&self) -> Option<&TursoIo> {
        self.0.custom_io.as_ref()
    }

    /// Returns local encryption settings
    pub fn encryption(&self) -> Option<&TursoEncryptionOptions> {
        self.0.encryption.as_ref()
    }

    /// Returns remote sync settings
    pub fn sync_options(&self) -> Option<&TursoSyncOptions> {
        self.0.sync.as_ref()
    }

    /// Returns enabled Turso experimental features
    pub fn experimental_features(&self) -> TursoExperimentalFeatures {
        self.0.experimental_features
    }

    /// Returns whether MVCC journal mode is requested
    pub fn get_mvcc(&self) -> bool {
        self.0
            .pragmas
            .iter()
            .find(|(name, _)| name == "journal_mode")
            .and_then(|(_, value)| value.as_deref())
            .map(normalized_pragma_value)
            .is_some_and(|value| value.eq_ignore_ascii_case("mvcc"))
    }

    /// Returns configured PRAGMAs in execution order
    pub fn pragmas(&self) -> &[(String, Option<String>)] {
        &self.0.pragmas
    }

    /// Returns SQLx statement logging settings
    pub fn log_settings(&self) -> &LogSettings {
        &self.0.log_settings
    }

    pub(crate) fn memory_state(&self) -> &Arc<TursoMemoryState> {
        &self.0.memory_state
    }

    /// Sets the target database file
    pub fn filename(mut self, filename: impl AsRef<Path>) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.target = TursoDatabaseTarget::File(filename.as_ref().to_path_buf());
        inner.cache_mode = TursoCacheMode::Private;
        self
    }

    /// Sets whether the target is an in-memory database
    pub fn in_memory(mut self, in_memory: bool) -> Self {
        let inner = Arc::make_mut(&mut self.0);

        if in_memory {
            inner.target = new_memory_target();
            inner.cache_mode = TursoCacheMode::Shared;
            inner.memory_state = Arc::new(TursoMemoryState::new());
            return self;
        }

        if matches!(inner.target, TursoDatabaseTarget::Memory { .. }) {
            inner.target = TursoDatabaseTarget::File(PathBuf::from(":memory:"));
            inner.cache_mode = TursoCacheMode::Private;
        }

        self
    }

    /// Sets whether the database should be opened read-only
    pub fn read_only(mut self, read_only: bool) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.open_mode = TursoOpenMode::from_read_only(read_only, inner.open_mode);
        self
    }

    /// Sets whether a missing file-backed database should be created
    pub fn create_if_missing(mut self, create_if_missing: bool) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.open_mode = TursoOpenMode::from_create_if_missing(create_if_missing, inner.open_mode);
        self
    }

    /// Sets whether Turso should use a shared cache for this database
    pub fn shared_cache(mut self, shared_cache: bool) -> Self {
        Arc::make_mut(&mut self.0).cache_mode = TursoCacheMode::from_shared(shared_cache);
        self
    }

    /// Sets whether the database should be opened as immutable
    pub fn immutable(mut self, immutable: bool) -> Self {
        Arc::make_mut(&mut self.0).immutable = immutable;
        self
    }

    /// Sets the busy timeout
    pub fn busy_timeout(mut self, busy_timeout: Duration) -> Self {
        Arc::make_mut(&mut self.0).busy_timeout = busy_timeout;
        self
    }

    /// Sets the SQLx-facing statement cache capacity
    pub fn statement_cache_capacity(mut self, statement_cache_capacity: usize) -> Self {
        Arc::make_mut(&mut self.0).statement_cache_capacity = statement_cache_capacity;
        self
    }

    /// Sets the VFS name
    pub fn vfs(mut self, vfs: impl Into<String>) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.vfs = Some(vfs.into());
        inner.custom_io = None;
        self
    }

    /// Sets a custom IO backend
    pub fn custom_io(mut self, io: Arc<dyn turso::core::IO>) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.custom_io = Some(TursoIo::new(io));
        inner.vfs = None;
        self
    }

    /// Clears the custom IO backend
    pub fn clear_custom_io(mut self) -> Self {
        Arc::make_mut(&mut self.0).custom_io = None;
        self
    }

    /// Sets local database encryption settings
    pub fn encryption_options(mut self, encryption: TursoEncryptionOptions) -> Self {
        Arc::make_mut(&mut self.0).encryption = Some(encryption);
        self
    }

    /// Clears local database encryption settings
    pub fn clear_encryption(mut self) -> Self {
        Arc::make_mut(&mut self.0).encryption = None;
        self
    }

    /// Sets remote sync settings
    pub fn with_sync_options(mut self, sync: TursoSyncOptions) -> Self {
        Arc::make_mut(&mut self.0).sync = Some(sync);
        self
    }

    /// Clears remote sync settings
    pub fn clear_sync_options(mut self) -> Self {
        Arc::make_mut(&mut self.0).sync = None;
        self
    }

    /// Enables or disables an experimental Turso builder feature
    pub fn experimental_feature(
        mut self,
        feature: TursoExperimentalFeature,
        enabled: bool,
    ) -> Self {
        Arc::make_mut(&mut self.0)
            .experimental_features
            .set(feature, enabled);
        self
    }

    /// Sets whether Turso should use MVCC journal mode
    pub fn mvcc(self, enabled: bool) -> Self {
        if enabled {
            return self.pragma("journal_mode", Some("'mvcc'".to_owned()));
        }

        self.pragma("journal_mode", None)
    }

    /// Adds or replaces a PRAGMA value while preserving default ordering
    pub fn pragma(mut self, key: impl Into<String>, value: impl Into<Option<String>>) -> Self {
        let key = key.into();
        let value = value.into();
        let inner = Arc::make_mut(&mut self.0);

        if let Some((_, existing)) = inner.pragmas.iter_mut().find(|(name, _)| name == &key) {
            *existing = value;
            return self;
        }

        inner.pragmas.push((key, value));
        self
    }

    /// Sets regular statement logging level
    pub fn log_statements(mut self, level: log::LevelFilter) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_statements(level);
        self
    }

    /// Sets slow statement logging level and threshold
    pub fn log_slow_statements(mut self, level: log::LevelFilter, duration: Duration) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_slow_statements(level, duration);
        self
    }
}

impl Default for TursoConnectOptions {
    fn default() -> Self {
        Self(Arc::new(TursoConnectOptionsInner {
            target: new_memory_target(),
            open_mode: TursoOpenMode::ReadWrite,
            cache_mode: TursoCacheMode::Shared,
            immutable: false,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            statement_cache_capacity: DEFAULT_STATEMENT_CACHE_CAPACITY,
            vfs: None,
            custom_io: None,
            encryption: None,
            sync: None,
            experimental_features: TursoExperimentalFeatures::default(),
            pragmas: default_pragmas(),
            log_settings: LogSettings::default(),
            memory_state: Arc::new(TursoMemoryState::new()),
        }))
    }
}

impl FromStr for TursoConnectOptions {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let url = url
            .strip_prefix("turso://")
            .or_else(|| url.strip_prefix("turso:"))
            .ok_or_else(|| config_error("Turso URLs must use the `turso:` scheme"))?;

        let mut database_and_params = url.splitn(2, '?');
        let database = database_and_params.next().unwrap_or_default();
        let params = database_and_params.next();

        Self::from_db_and_params(database, params)
    }
}

impl ConnectOptions for TursoConnectOptions {
    type Connection = TursoConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::from_str(url.as_str())
    }

    fn to_url_lossy(&self) -> Url {
        self.build_url()
    }

    async fn connect(&self) -> Result<Self::Connection, Error>
    where
        Self::Connection: Sized,
    {
        let connection = TursoDriver::connect(self).await?;
        Ok(TursoConnection::new(self.clone(), connection))
    }

    fn log_statements(mut self, level: log::LevelFilter) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: log::LevelFilter, duration: Duration) -> Self {
        Arc::make_mut(&mut self.0)
            .log_settings
            .log_slow_statements(level, duration);
        self
    }
}

impl TursoConnectOptions {
    fn from_db_and_params(database: &str, params: Option<&str>) -> Result<Self, Error> {
        let mut options = Self::default();

        if database.is_empty() {
            options = options.filename("");
        } else if database == ":memory:" {
            options = options.in_memory(true);
        } else {
            let decoded = percent_decode_str(database)
                .decode_utf8()
                .map_err(Error::config)?;
            options = options.filename(PathBuf::from(decoded.as_ref()));
        }

        if let Some(params) = params {
            options = options.apply_query_params(params)?;
        }

        Ok(options)
    }

    fn apply_query_params(mut self, params: &str) -> Result<Self, Error> {
        let mut encryption_cipher = self
            .0
            .encryption
            .as_ref()
            .map(|encryption| encryption.cipher().to_owned());
        let mut encryption_hexkey = self
            .0
            .encryption
            .as_ref()
            .map(|encryption| encryption.hexkey().to_owned());
        let mut sync = self.0.sync.clone();

        for (key, value) in url::form_urlencoded::parse(params.as_bytes()) {
            match &*key {
                "mode" => match &*value {
                    "ro" => {
                        self = self.read_only(true);
                    }
                    "rw" => {}
                    "rwc" => {
                        self = self.create_if_missing(true);
                    }
                    "memory" => {
                        self = self.in_memory(true);
                    }
                    _ => return Err(config_error(format!("unknown value {value:?} for `mode`"))),
                },
                "cache" => match &*value {
                    "private" => {
                        self = self.shared_cache(false);
                    }
                    "shared" => {
                        self = self.shared_cache(true);
                    }
                    _ => return Err(config_error(format!("unknown value {value:?} for `cache`"))),
                },
                "immutable" => match &*value {
                    "true" | "1" => {
                        self = self.immutable(true);
                    }
                    "false" | "0" => {
                        self = self.immutable(false);
                    }
                    _ => {
                        return Err(config_error(format!(
                            "unknown value {value:?} for `immutable`"
                        )));
                    }
                },
                "vfs" => {
                    self = self.vfs(value.into_owned());
                }
                "encryption_cipher" => {
                    encryption_cipher = Some(value.into_owned());
                }
                "encryption_hexkey" => {
                    encryption_hexkey = Some(value.into_owned());
                }
                "journal_mode" => {
                    if normalized_pragma_value(&value).eq_ignore_ascii_case("mvcc") {
                        self = self.mvcc(true);
                    } else {
                        self = self.pragma("journal_mode", Some(value.into_owned()));
                    }
                }
                "experimental" => {
                    for feature in value.split(',').filter(|feature| !feature.is_empty()) {
                        self =
                            self.experimental_feature(parse_experimental_feature(feature)?, true);
                    }
                }
                "sync_remote_url" => {
                    sync = Some(TursoSyncOptions::new(value.into_owned()));
                }
                "sync_auth_token" => {
                    sync = Some(sync.unwrap_or_default().with_auth_token(value.into_owned()));
                }
                "sync_client_name" => {
                    sync = Some(
                        sync.unwrap_or_default()
                            .with_client_name(value.into_owned()),
                    );
                }
                "sync_long_poll_timeout_ms" => {
                    let millis = value.parse::<u64>().map_err(Error::config)?;
                    sync = Some(
                        sync.unwrap_or_default()
                            .with_long_poll_timeout(Duration::from_millis(millis)),
                    );
                }
                "sync_bootstrap_if_empty" => {
                    sync =
                        Some(sync.unwrap_or_default().with_bootstrap_if_empty(parse_bool(
                            "sync_bootstrap_if_empty",
                            &value,
                        )?));
                }
                _ => {
                    return Err(config_error(format!(
                        "unknown query parameter `{key}` while parsing Turso connection URL"
                    )));
                }
            }
        }

        match (encryption_cipher, encryption_hexkey) {
            (Some(cipher), Some(hexkey)) => {
                self = self.encryption_options(TursoEncryptionOptions::new(cipher, hexkey)?);
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(config_error(
                    "`encryption_cipher` and `encryption_hexkey` must be provided together",
                ));
            }
            (None, None) => {}
        }

        if let Some(sync) = sync {
            if sync.remote_url().is_empty() {
                return Err(config_error(
                    "`sync_remote_url` must be provided when sync options are used",
                ));
            }

            self = self.with_sync_options(sync);
        }

        Ok(self)
    }

    fn build_url(&self) -> Url {
        let path = match &self.0.target {
            TursoDatabaseTarget::Memory { .. } => ":memory:".to_owned(),
            TursoDatabaseTarget::File(path) => path.to_string_lossy().into_owned(),
        };

        let mut url = Url::parse(&format!("turso://{}", path.trim_start_matches('/')))
            .expect("generated Turso URL must parse");

        if matches!(&self.0.target, TursoDatabaseTarget::File(path) if path.is_absolute()) {
            url.set_path(&path);
        }

        url.query_pairs_mut()
            .append_pair("mode", self.0.open_mode.url_mode(&self.0.target));
        url.query_pairs_mut()
            .append_pair("cache", self.0.cache_mode.url_value());

        if self.0.immutable {
            url.query_pairs_mut().append_pair("immutable", "true");
        }

        if let Some(vfs) = &self.0.vfs {
            url.query_pairs_mut().append_pair("vfs", vfs);
        }

        if let Some(encryption) = &self.0.encryption {
            url.query_pairs_mut()
                .append_pair("encryption_cipher", encryption.cipher());
            url.query_pairs_mut()
                .append_pair("encryption_hexkey", encryption.hexkey());
        }

        if let Some(sync) = &self.0.sync {
            url.query_pairs_mut()
                .append_pair("sync_remote_url", sync.remote_url());

            if let Some(auth_token) = sync.auth_token() {
                url.query_pairs_mut()
                    .append_pair("sync_auth_token", auth_token);
            }

            if let Some(client_name) = sync.client_name() {
                url.query_pairs_mut()
                    .append_pair("sync_client_name", client_name);
            }

            if let Some(timeout) = sync.long_poll_timeout() {
                url.query_pairs_mut().append_pair(
                    "sync_long_poll_timeout_ms",
                    &timeout.as_millis().to_string(),
                );
            }

            if !sync.bootstrap_if_empty() {
                url.query_pairs_mut()
                    .append_pair("sync_bootstrap_if_empty", "false");
            }
        }

        let experimental = self.0.experimental_features.to_query_value();
        if !experimental.is_empty() {
            url.query_pairs_mut()
                .append_pair("experimental", &experimental);
        }

        url
    }
}

impl Default for TursoSyncOptions {
    fn default() -> Self {
        Self {
            remote_url: String::new(),
            auth_token: None,
            client_name: None,
            long_poll_timeout: None,
            bootstrap_if_empty: true,
        }
    }
}

impl TursoSyncOptions {
    /// Creates remote sync settings
    pub fn new(remote_url: impl Into<String>) -> Self {
        Self {
            remote_url: remote_url.into(),
            ..Self::default()
        }
    }

    /// Returns the remote sync URL
    pub fn remote_url(&self) -> &str {
        &self.remote_url
    }

    /// Returns the static sync auth token
    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }

    /// Returns the sync client name
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// Returns the sync long-poll timeout
    pub fn long_poll_timeout(&self) -> Option<Duration> {
        self.long_poll_timeout
    }

    /// Returns whether empty local databases should be bootstrapped
    pub fn bootstrap_if_empty(&self) -> bool {
        self.bootstrap_if_empty
    }

    /// Sets the static sync auth token
    pub fn with_auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    /// Sets the sync client name
    pub fn with_client_name(mut self, client_name: impl Into<String>) -> Self {
        self.client_name = Some(client_name.into());
        self
    }

    /// Sets the sync long-poll timeout
    pub fn with_long_poll_timeout(mut self, timeout: Duration) -> Self {
        self.long_poll_timeout = Some(timeout);
        self
    }

    /// Sets whether empty local databases should be bootstrapped
    pub fn with_bootstrap_if_empty(mut self, bootstrap_if_empty: bool) -> Self {
        self.bootstrap_if_empty = bootstrap_if_empty;
        self
    }
}

impl TursoEncryptionOptions {
    /// Creates local encryption settings
    pub fn new(cipher: impl Into<String>, hexkey: impl Into<String>) -> Result<Self, Error> {
        let cipher = cipher.into();
        let hexkey = hexkey.into();

        if cipher.is_empty() {
            return Err(config_error("Turso encryption cipher cannot be empty"));
        }

        if hexkey.is_empty() {
            return Err(config_error("Turso encryption hex key cannot be empty"));
        }

        if hexkey.len() % 2 != 0 || !hexkey.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(config_error(
                "Turso encryption hex key must contain even-length hexadecimal",
            ));
        }

        Ok(Self { cipher, hexkey })
    }

    /// Returns the Turso encryption cipher name
    pub fn cipher(&self) -> &str {
        &self.cipher
    }

    /// Returns the Turso encryption key as hexadecimal
    pub fn hexkey(&self) -> &str {
        &self.hexkey
    }

    pub(crate) fn to_turso(&self) -> turso::EncryptionOpts {
        turso::EncryptionOpts {
            cipher: self.cipher.clone(),
            hexkey: self.hexkey.clone(),
        }
    }
}

impl TursoExperimentalFeatures {
    /// Returns whether an experimental feature is enabled
    pub fn is_enabled(self, feature: TursoExperimentalFeature) -> bool {
        match feature {
            TursoExperimentalFeature::Attach => self.attach,
            TursoExperimentalFeature::CustomTypes => self.custom_types,
            TursoExperimentalFeature::GeneratedColumns => self.generated_columns,
            TursoExperimentalFeature::IndexMethod => self.index_method,
            TursoExperimentalFeature::MaterializedViews => self.materialized_views,
            TursoExperimentalFeature::MultiprocessWal => self.multiprocess_wal,
            TursoExperimentalFeature::Vacuum => self.vacuum,
            TursoExperimentalFeature::WithoutRowid => self.without_rowid,
        }
    }

    fn set(&mut self, feature: TursoExperimentalFeature, enabled: bool) {
        match feature {
            TursoExperimentalFeature::Attach => self.attach = enabled,
            TursoExperimentalFeature::CustomTypes => self.custom_types = enabled,
            TursoExperimentalFeature::GeneratedColumns => self.generated_columns = enabled,
            TursoExperimentalFeature::IndexMethod => self.index_method = enabled,
            TursoExperimentalFeature::MaterializedViews => self.materialized_views = enabled,
            TursoExperimentalFeature::MultiprocessWal => self.multiprocess_wal = enabled,
            TursoExperimentalFeature::Vacuum => self.vacuum = enabled,
            TursoExperimentalFeature::WithoutRowid => self.without_rowid = enabled,
        }
    }

    fn to_query_value(self) -> String {
        [
            (TursoExperimentalFeature::Attach, "attach"),
            (TursoExperimentalFeature::CustomTypes, "custom_types"),
            (
                TursoExperimentalFeature::GeneratedColumns,
                "generated_columns",
            ),
            (TursoExperimentalFeature::IndexMethod, "index_method"),
            (TursoExperimentalFeature::MaterializedViews, "views"),
            (
                TursoExperimentalFeature::MultiprocessWal,
                "multiprocess_wal",
            ),
            (TursoExperimentalFeature::Vacuum, "vacuum"),
            (TursoExperimentalFeature::WithoutRowid, "without_rowid"),
        ]
        .into_iter()
        .filter_map(|(feature, value)| self.is_enabled(feature).then_some(value))
        .collect::<Vec<_>>()
        .join(",")
    }
}

fn new_memory_target() -> TursoDatabaseTarget {
    let seqno = IN_MEMORY_DB_SEQ.fetch_add(1, Ordering::Relaxed);
    TursoDatabaseTarget::Memory {
        name: Arc::from(format!("sqlx-turso-in-memory-{seqno}")),
    }
}

fn default_pragmas() -> Vec<(String, Option<String>)> {
    [
        ("key", None),
        ("cipher_plaintext_header_size", None),
        ("cipher_salt", None),
        ("kdf_iter", None),
        ("cipher_kdf_algorithm", None),
        ("cipher_use_hmac", None),
        ("cipher_compatibility", None),
        ("cipher_page_size", None),
        ("cipher_hmac_algorithm", None),
        ("page_size", None),
        ("locking_mode", None),
        ("auto_vacuum", None),
        ("journal_mode", None),
        ("foreign_keys", Some("ON")),
        ("synchronous", None),
        ("analysis_limit", None),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.map(str::to_owned)))
    .collect()
}

fn config_error(message: impl Into<String>) -> Error {
    Error::Configuration(message.into().into())
}

fn normalized_pragma_value(value: &str) -> &str {
    value.trim_matches(|ch| matches!(ch, '\'' | '"'))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, Error> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(config_error(format!(
            "unknown value {value:?} for `{name}`"
        ))),
    }
}

fn parse_experimental_feature(feature: &str) -> Result<TursoExperimentalFeature, Error> {
    match feature {
        "attach" => Ok(TursoExperimentalFeature::Attach),
        "custom_types" => Ok(TursoExperimentalFeature::CustomTypes),
        "generated_columns" => Ok(TursoExperimentalFeature::GeneratedColumns),
        "index_method" => Ok(TursoExperimentalFeature::IndexMethod),
        "views" | "materialized_views" => Ok(TursoExperimentalFeature::MaterializedViews),
        "multiprocess_wal" => Ok(TursoExperimentalFeature::MultiprocessWal),
        "vacuum" => Ok(TursoExperimentalFeature::Vacuum),
        "without_rowid" => Ok(TursoExperimentalFeature::WithoutRowid),
        _ => Err(config_error(format!(
            "unknown Turso experimental feature `{feature}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TursoConnectOptions, TursoDatabaseTarget, TursoEncryptionOptions, TursoExperimentalFeature,
    };
    use sqlx_core::{
        connection::{ConnectOptions, Connection},
        error::Error,
        executor::Executor,
    };
    use std::{path::Path, str::FromStr, sync::Arc, time::Duration};
    use url::Url;

    #[test]
    fn parses_memory_urls_with_shared_cache() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso::memory:")?;
        assert!(options.is_in_memory());
        assert!(options.get_shared_cache());

        let options = TursoConnectOptions::from_str("turso://?mode=memory")?;
        assert!(options.is_in_memory());
        assert!(options.get_shared_cache());

        let options = TursoConnectOptions::from_str("turso://:memory:?cache=private")?;
        assert!(options.is_in_memory());
        assert!(!options.get_shared_cache());

        Ok(())
    }

    #[test]
    fn parses_file_urls_and_modes() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso:data.db?mode=ro")?;
        assert_eq!(options.get_filename(), Some(Path::new("data.db")));
        assert!(options.is_read_only());
        assert!(!options.get_create_if_missing());

        let options = TursoConnectOptions::from_str("turso://data.db?mode=rwc")?;
        assert_eq!(options.get_filename(), Some(Path::new("data.db")));
        assert!(options.get_create_if_missing());

        let options = TursoConnectOptions::from_str("turso:///tmp/data.db")?;
        assert_eq!(options.get_filename(), Some(Path::new("/tmp/data.db")));

        Ok(())
    }

    #[test]
    fn preserves_memory_identity_across_clones_and_builders() {
        let options = TursoConnectOptions::new().in_memory(true);
        let clone = options.clone().statement_cache_capacity(4);

        match (options.target(), clone.target()) {
            (
                TursoDatabaseTarget::Memory { name },
                TursoDatabaseTarget::Memory { name: clone_name },
            ) => assert!(Arc::ptr_eq(name, clone_name)),
            _ => panic!("expected in-memory targets"),
        }
    }

    #[test]
    fn in_memory_resets_memory_identity() {
        let options = TursoConnectOptions::new();
        let reset = options.clone().in_memory(true);

        match (options.target(), reset.target()) {
            (
                TursoDatabaseTarget::Memory { name },
                TursoDatabaseTarget::Memory { name: reset_name },
            ) => assert!(!Arc::ptr_eq(name, reset_name)),
            _ => panic!("expected in-memory targets"),
        }
    }

    #[test]
    fn applies_sqlite_compatible_defaults_and_pragmas() {
        let options = TursoConnectOptions::new();
        assert_eq!(options.get_statement_cache_capacity(), 100);
        assert_eq!(options.get_busy_timeout(), Duration::from_secs(5));
        assert_eq!(
            options
                .pragmas()
                .iter()
                .find(|(name, _)| name == "foreign_keys")
                .and_then(|(_, value)| value.as_deref()),
            Some("ON")
        );

        let options = options.pragma("foreign_keys", Some("OFF".to_owned()));
        assert_eq!(
            options
                .pragmas()
                .iter()
                .find(|(name, _)| name == "foreign_keys")
                .and_then(|(_, value)| value.as_deref()),
            Some("OFF")
        );
    }

    #[test]
    fn rejects_unsupported_schemes_and_options() {
        assert!(TursoConnectOptions::from_str("sqlite://data.db").is_err());
        assert!(TursoConnectOptions::from_str("turso://data.db?mode=bad").is_err());
        assert!(TursoConnectOptions::from_str("turso://data.db?unknown=true").is_err());
        assert!(TursoConnectOptions::from_str("turso://data.db?experimental=bad").is_err());
        assert!(
            TursoConnectOptions::from_str("turso://data.db?encryption_hexkey=not-hex").is_err()
        );
    }

    #[test]
    fn implements_sqlx_connect_options_url_hooks() -> sqlx_core::Result<()> {
        let url = Url::parse("turso://data.db?mode=ro&cache=shared").unwrap();
        let options = <TursoConnectOptions as ConnectOptions>::from_url(&url)?;

        assert_eq!(options.get_filename(), Some(Path::new("data.db")));
        assert!(options.is_read_only());
        assert!(options.get_shared_cache());
        assert_eq!(
            options.to_url_lossy().as_str(),
            "turso://data.db?mode=ro&cache=shared"
        );

        Ok(())
    }

    #[test]
    fn parses_encryption_mvcc_and_experimental_options() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str(
            "turso://data.db?encryption_cipher=aegis256&encryption_hexkey=b1bb&journal_mode=mvcc&experimental=attach,index_method,views",
        )?;

        let encryption = options.encryption().expect("expected encryption options");
        assert_eq!(encryption.cipher(), "aegis256");
        assert_eq!(encryption.hexkey(), "b1bb");
        assert!(options.get_mvcc());
        assert!(
            options
                .experimental_features()
                .is_enabled(TursoExperimentalFeature::Attach)
        );
        assert!(
            options
                .experimental_features()
                .is_enabled(TursoExperimentalFeature::IndexMethod)
        );
        assert!(
            options
                .experimental_features()
                .is_enabled(TursoExperimentalFeature::MaterializedViews)
        );

        Ok(())
    }

    #[test]
    fn parses_sync_options_without_connecting() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str(
            "turso://local.db?sync_remote_url=https%3A%2F%2Fexample.turso.io&sync_auth_token=secret&sync_client_name=sqlx-turso&sync_long_poll_timeout_ms=250&sync_bootstrap_if_empty=false",
        )?;

        let sync = options.sync_options().expect("expected sync options");
        assert_eq!(sync.remote_url(), "https://example.turso.io");
        assert_eq!(sync.auth_token(), Some("secret"));
        assert_eq!(sync.client_name(), Some("sqlx-turso"));
        assert_eq!(sync.long_poll_timeout(), Some(Duration::from_millis(250)));
        assert!(!sync.bootstrap_if_empty());

        Ok(())
    }

    #[tokio::test]
    async fn connect_opens_memory_database() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso::memory:")?;
        let mut connection = options.connect().await?;

        connection.ping().await?;
        assert!(connection.options().is_in_memory());

        Ok(())
    }

    #[tokio::test]
    async fn connect_opens_file_database_with_create_if_missing() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-connect-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let url = format!("turso://{}?mode=rwc", path.display());
        let options = TursoConnectOptions::from_str(&url)?;
        let mut connection = options.connect().await?;

        connection.ping().await?;
        assert_eq!(connection.options().get_filename(), Some(path.as_path()));
        assert!(path.exists());

        drop(connection);
        let _ = std::fs::remove_file(&path);

        Ok(())
    }

    #[tokio::test]
    async fn read_write_file_mode_rejects_missing_database() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-missing-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let options = TursoConnectOptions::new().filename(&path);
        let error = options.connect().await.unwrap_err();

        assert!(matches!(
            error,
            Error::Io(ref io_error) if io_error.kind() == std::io::ErrorKind::NotFound
        ));

        Ok(())
    }

    #[tokio::test]
    async fn private_memory_cache_uses_distinct_databases() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::new()
            .in_memory(true)
            .shared_cache(false);
        let mut first = options.connect().await?;
        let mut second = options.connect().await?;

        first
            .execute("CREATE TABLE private_cache_test (id INTEGER PRIMARY KEY)")
            .await?;

        let error = second
            .execute("INSERT INTO private_cache_test (id) VALUES (1)")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("private_cache_test"));

        Ok(())
    }

    #[tokio::test]
    async fn connect_rejects_read_only_until_mapped() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso://data.db?mode=ro")?;
        let error = options.connect().await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("read-only Turso connections is not supported")
        );

        Ok(())
    }

    #[cfg(not(feature = "sync"))]
    #[tokio::test]
    async fn connect_rejects_sync_without_feature() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::new()
            .with_sync_options(super::TursoSyncOptions::new("https://example.turso.io"));
        let error = options.connect().await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Turso sync connections require the `sync` feature")
        );

        Ok(())
    }

    #[tokio::test]
    async fn connect_maps_encryption_options() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-encrypted-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let options = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .encryption_options(TursoEncryptionOptions::new(
                "aegis256",
                "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            )?);
        let connection = options.connect().await?;

        connection
            .raw()
            .execute("CREATE TABLE test (value TEXT)", ())
            .await
            .map_err(Error::config)?;
        connection
            .raw()
            .execute("INSERT INTO test (value) VALUES ('secret_data')", ())
            .await
            .map_err(Error::config)?;
        let mut rows = connection
            .raw()
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .map_err(Error::config)?;
        while rows.next().await.map_err(Error::config)?.is_some() {}

        drop(connection);

        let content = std::fs::read(&path).map_err(Error::config)?;
        assert!(!content.windows(11).any(|window| window == b"secret_data"));

        let _ = std::fs::remove_file(&path);

        Ok(())
    }

    #[tokio::test]
    async fn connect_applies_mvcc_and_experimental_options() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-mvcc-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let options = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .mvcc(true)
            .experimental_feature(TursoExperimentalFeature::Attach, true)
            .experimental_feature(TursoExperimentalFeature::IndexMethod, true);
        let connection = options.connect().await?;

        connection
            .raw()
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .await
            .map_err(Error::config)?;

        drop(connection);
        let _ = std::fs::remove_file(&path);

        Ok(())
    }
}
