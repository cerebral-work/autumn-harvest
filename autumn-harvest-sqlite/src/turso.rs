//! Turso (`libsql://` / Hrana `/v2/pipeline`) distributed `SQLite` and embedded
//! replica synchronization for [`SqliteRuntime`](crate::SqliteRuntime).
//!
//! This module enables a single-writer [`SqliteRuntime`](crate::SqliteRuntime)
//! edge node to replicate its durable workflow state (`harvest_executions`,
//! `harvest_events`, `harvest_tasks`, `harvest_timers`, and `harvest_signals`)
//! to a remote **Turso** (`libsql://` / `https://`) database over the Hrana v2
//! HTTP pipeline (`/v2/pipeline`) and/or to a local `SQLite` embedded replica
//! file without introducing conflicting native `libsqlite3` symbols alongside
//! `rusqlite` (`bundled`).
//!
//! # Security Invariants
//!
//! - `auth_token` (`TURSO_AUTH_TOKEN`) is **never** formatted in `Debug` output
//!   or error strings; [`TursoReplicaConfig`]'s custom `fmt::Debug` prints
//!   `"[REDACTED]"` whenever a token is present.
//! - Query parameters containing `authToken=`, `jwt=`, or `token=` are masked
//!   by [`redact_turso_url`] before appearing in any diagnostic or receipt.

use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::error::{SqliteError, SqliteResult};
use crate::runtime::SqliteRuntime;
use crate::schema;

/// Synchronization mode for Turso persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TursoSyncMode {
    /// Push state to the remote Turso `/v2/pipeline` HTTP endpoint only.
    RemoteOnly,
    /// Mirror state to the local `SQLite` embedded replica file only.
    LocalReplicaOnly,
    /// Synchronize both the local `SQLite` embedded replica and the remote
    /// Turso `/v2/pipeline` endpoint.
    EmbeddedReplicaSync,
}

/// Configuration for replicating [`SqliteRuntime`] state to Turso and/or a
/// local embedded replica file.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TursoReplicaConfig {
    /// Remote Turso database URL (`libsql://<db>-<org>.turso.io`, `turso://...`,
    /// or `http(s)://...`).
    pub database_url: String,
    /// Optional Bearer JWT token (`TURSO_AUTH_TOKEN`). Redacted in `Debug`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    /// Optional local filesystem path for an embedded `SQLite` replica file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_replica_path: Option<PathBuf>,
    /// Replication mode.
    pub sync_mode: TursoSyncMode,
}

impl fmt::Debug for TursoReplicaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoReplicaConfig")
            .field("database_url", &redact_turso_url(&self.database_url))
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("local_replica_path", &self.local_replica_path)
            .field("sync_mode", &self.sync_mode)
            .finish()
    }
}

/// Redact `authToken`, `jwt`, or `token` query parameters from a Turso URL.
#[must_use]
pub fn redact_turso_url(url: &str) -> String {
    let Some((base, query)) = url.split_once('?') else {
        return url.to_string();
    };
    let redacted_pairs: Vec<String> = query
        .split('&')
        .map(|pair| {
            if let Some((k, _)) = pair.split_once('=') {
                let lower = k.to_ascii_lowercase();
                if lower.contains("token")
                    || lower.contains("jwt")
                    || lower.contains("secret")
                    || lower.contains("key")
                    || lower.contains("password")
                {
                    return format!("{k}=[REDACTED]");
                }
            }
            pair.to_string()
        })
        .collect();
    format!("{base}?{}", redacted_pairs.join("&"))
}

impl TursoReplicaConfig {
    /// Construct a [`TursoReplicaConfig`] from environment variables:
    /// - `TURSO_DATABASE_URL` (required)
    /// - `TURSO_AUTH_TOKEN` (optional)
    /// - `TURSO_LOCAL_REPLICA_PATH` (optional)
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Turso`] if `TURSO_DATABASE_URL` is unset or blank.
    pub fn from_env() -> SqliteResult<Self> {
        let database_url = std::env::var("TURSO_DATABASE_URL")
            .map_err(|_| SqliteError::Turso("TURSO_DATABASE_URL is not set".to_string()))?;
        if database_url.trim().is_empty() {
            return Err(SqliteError::Turso(
                "TURSO_DATABASE_URL must not be blank".to_string(),
            ));
        }
        let auth_token = std::env::var("TURSO_AUTH_TOKEN")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let local_replica_path = std::env::var("TURSO_LOCAL_REPLICA_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from);
        let sync_mode = if local_replica_path.is_some() {
            TursoSyncMode::EmbeddedReplicaSync
        } else {
            TursoSyncMode::RemoteOnly
        };
        Ok(Self {
            database_url,
            auth_token,
            local_replica_path,
            sync_mode,
        })
    }

    /// Normalize the configured `database_url` into a Turso Hrana v2
    /// `/v2/pipeline` HTTP endpoint (`https://.../v2/pipeline` or
    /// `http://.../v2/pipeline`).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Turso`] if the URL is blank or uses an
    /// unsupported scheme.
    pub fn pipeline_endpoint(&self) -> SqliteResult<String> {
        let trimmed = self.database_url.trim();
        if trimmed.is_empty() {
            return Err(SqliteError::Turso(
                "database_url must not be empty".to_string(),
            ));
        }
        let without_query = trimmed.split('?').next().unwrap_or(trimmed);
        let normalized = if let Some(rest) = without_query.strip_prefix("libsql://") {
            if rest.starts_with("127.0.0.1")
                || rest.starts_with("localhost")
                || rest.starts_with("[::1]")
            {
                format!("http://{rest}")
            } else {
                format!("https://{rest}")
            }
        } else if let Some(rest) = without_query.strip_prefix("turso://") {
            if rest.starts_with("127.0.0.1")
                || rest.starts_with("localhost")
                || rest.starts_with("[::1]")
            {
                format!("http://{rest}")
            } else {
                format!("https://{rest}")
            }
        } else if without_query.starts_with("https://") || without_query.starts_with("http://") {
            without_query.to_string()
        } else {
            return Err(SqliteError::Turso(format!(
                "unsupported Turso URL scheme in {}",
                redact_turso_url(trimmed)
            )));
        };
        let base = normalized.trim_end_matches('/');
        if base.ends_with("/v2/pipeline") {
            Ok(base.to_string())
        } else {
            Ok(format!("{base}/v2/pipeline"))
        }
    }
}

/// Typed value in a Turso Hrana v2 SQL statement parameter list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TursoArg {
    /// Text parameter.
    Text {
        /// String value.
        value: String,
    },
    /// 64-bit signed integer parameter (serialized as string per Hrana v2 spec).
    Integer {
        /// Base-10 integer string.
        value: String,
    },
    /// SQL NULL parameter.
    Null,
}

impl TursoArg {
    /// Construct a `TEXT` argument.
    #[must_use]
    pub fn text(v: impl Into<String>) -> Self {
        Self::Text { value: v.into() }
    }

    /// Construct an `INTEGER` argument.
    #[must_use]
    pub fn integer(v: i64) -> Self {
        Self::Integer {
            value: v.to_string(),
        }
    }

    /// Construct a `TEXT` or `NULL` argument from an `Option<String>`.
    #[must_use]
    pub fn opt_text(v: Option<String>) -> Self {
        v.map_or(Self::Null, |s| Self::Text { value: s })
    }

    /// Construct an `INTEGER` or `NULL` argument from an `Option<i64>`.
    #[must_use]
    pub fn opt_integer(v: Option<i64>) -> Self {
        v.map_or(Self::Null, |n| Self::Integer {
            value: n.to_string(),
        })
    }
}

/// A single parameterized SQL statement in a Turso Hrana v2 pipeline batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TursoStmt {
    /// SQL text using `?1`, `?2`, ... positional placeholders.
    pub sql: String,
    /// Positional arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<TursoArg>,
}

/// Request item inside a Turso `/v2/pipeline` batch payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TursoPipelineRequestItem {
    /// Execute a single SQL statement.
    Execute {
        /// Statement to execute.
        stmt: TursoStmt,
    },
    /// Close the interactive stream at the end of the pipeline batch.
    Close,
}

/// Complete JSON request body for `POST /v2/pipeline`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TursoPipelineRequest {
    /// Ordered pipeline requests ending with [`TursoPipelineRequestItem::Close`].
    pub requests: Vec<TursoPipelineRequestItem>,
}

impl TursoPipelineRequest {
    /// Wrap a list of SQL statements into a closed Hrana v2 pipeline batch.
    #[must_use]
    pub fn from_statements(stmts: Vec<TursoStmt>) -> Self {
        let mut requests: Vec<TursoPipelineRequestItem> = stmts
            .into_iter()
            .map(|stmt| TursoPipelineRequestItem::Execute { stmt })
            .collect();
        requests.push(TursoPipelineRequestItem::Close);
        Self { requests }
    }
}

/// Receipt returned by [`SqliteRuntime::sync_to_turso`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TursoSyncReceipt {
    /// Number of `harvest_executions` rows synchronized.
    pub executions_synced: usize,
    /// Number of `harvest_events` rows synchronized.
    pub events_synced: usize,
    /// Number of `harvest_tasks` rows synchronized.
    pub tasks_synced: usize,
    /// Number of `harvest_timers` rows synchronized.
    pub timers_synced: usize,
    /// Number of `harvest_signals` rows synchronized.
    pub signals_synced: usize,
    /// Whether the remote `/v2/pipeline` request succeeded.
    pub remote_pipeline_ok: bool,
    /// Whether the local `SQLite` embedded replica file was updated.
    pub local_replica_ok: bool,
    /// Redacted endpoint URL used for synchronization.
    pub endpoint: String,
}

/// Snapshot of a single `harvest_executions` row for replication.
#[derive(Debug, Clone)]
struct ExecutionRow {
    exec_id: String,
    workflow_name: String,
    workflow_id: String,
    input_json: String,
    state: String,
    output_json: Option<String>,
    error: Option<String>,
    cancel_reason: Option<String>,
    cancel_from_seq: Option<i64>,
    started_at: Option<i64>,
    closed_at: Option<i64>,
    search_attrs: Option<String>,
    memo: Option<String>,
    execution_timeout_ms: Option<i64>,
    deadline_at: Option<i64>,
    start_at: Option<i64>,
    workflow_attempt: i64,
    retry_policy_json: Option<String>,
    retry_of_exec_id: Option<String>,
    local_retry_at: Option<i64>,
}

/// Snapshot of a single `harvest_events` row for replication.
#[derive(Debug, Clone)]
struct EventRow {
    exec_id: String,
    seq: i64,
    event_json: String,
}

/// Snapshot of a single `harvest_tasks` row for replication.
#[derive(Debug, Clone)]
struct TaskRow {
    task_id: String,
    exec_id: String,
    activity_id: String,
    name: String,
    input_json: String,
    queue: String,
    state: String,
    attempt: i64,
    run_at: i64,
    seq: i64,
    max_attempts: Option<i64>,
    retry_policy_json: Option<String>,
    start_to_close_ms: Option<i64>,
    scheduled_at: i64,
    schedule_to_close_at: Option<i64>,
    defaults_frozen: i64,
    heartbeat_details: Option<String>,
    last_error: Option<String>,
}

/// Snapshot of a single `harvest_timers` row for replication.
#[derive(Debug, Clone)]
struct TimerRow {
    timer_id: String,
    exec_id: String,
    fire_at: i64,
    fired: i64,
    arm_seq: i64,
}

/// Snapshot of a single `harvest_signals` row for replication.
#[derive(Debug, Clone)]
struct SignalRow {
    signal_seq: i64,
    exec_id: String,
    name: String,
    payload_json: String,
    delivered: i64,
    received_at: i64,
    idempotency_key: Option<String>,
}

impl SqliteRuntime {
    /// Replicate durable workflow state (`harvest_executions`, `harvest_events`,
    /// `harvest_tasks`, `harvest_timers`, and `harvest_signals`) to a Turso
    /// `/v2/pipeline` endpoint and/or a local `SQLite` embedded replica file.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if reading the primary database or
    /// writing the local replica fails, or [`SqliteError::Turso`] if the
    /// remote Hrana `/v2/pipeline` request fails.
    #[allow(clippy::too_many_lines)]
    pub fn sync_to_turso(&self, config: &TursoReplicaConfig) -> SqliteResult<TursoSyncReceipt> {
        let executions = read_executions(&self.conn)?;
        let events = read_events(&self.conn)?;
        let tasks = read_tasks(&self.conn)?;
        let timers = read_timers(&self.conn)?;
        let signals = read_signals(&self.conn)?;

        let local_replica_ok = if matches!(
            config.sync_mode,
            TursoSyncMode::LocalReplicaOnly | TursoSyncMode::EmbeddedReplicaSync
        ) && let Some(replica_path) = &config.local_replica_path
        {
            sync_local_sqlite_replica(
                replica_path,
                &executions,
                &events,
                &tasks,
                &timers,
                &signals,
            )?;
            true
        } else {
            false
        };

        let (remote_pipeline_ok, endpoint) = if matches!(
            config.sync_mode,
            TursoSyncMode::RemoteOnly | TursoSyncMode::EmbeddedReplicaSync
        ) {
            let ep = config.pipeline_endpoint()?;
            let stmts =
                build_hrana_sync_statements(&executions, &events, &tasks, &timers, &signals);
            let req = TursoPipelineRequest::from_statements(stmts);
            send_hrana_pipeline(&ep, config.auth_token.as_deref(), &req)?;
            (true, redact_turso_url(&ep))
        } else {
            (false, redact_turso_url(&config.database_url))
        };

        Ok(TursoSyncReceipt {
            executions_synced: executions.len(),
            events_synced: events.len(),
            tasks_synced: tasks.len(),
            timers_synced: timers.len(),
            signals_synced: signals.len(),
            remote_pipeline_ok,
            local_replica_ok,
            endpoint,
        })
    }
}

fn read_executions(conn: &Connection) -> SqliteResult<Vec<ExecutionRow>> {
    let mut stmt = conn.prepare(
        "SELECT exec_id, workflow_name, workflow_id, input_json, state, output_json, error, \
                cancel_reason, cancel_from_seq, started_at, closed_at, search_attrs, memo, \
                execution_timeout_ms, deadline_at, start_at, workflow_attempt, \
                retry_policy_json, retry_of_exec_id, local_retry_at \
           FROM harvest_executions ORDER BY exec_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(ExecutionRow {
                exec_id: r.get(0)?,
                workflow_name: r.get(1)?,
                workflow_id: r.get(2)?,
                input_json: r.get(3)?,
                state: r.get(4)?,
                output_json: r.get(5)?,
                error: r.get(6)?,
                cancel_reason: r.get(7)?,
                cancel_from_seq: r.get(8)?,
                started_at: r.get(9)?,
                closed_at: r.get(10)?,
                search_attrs: r.get(11)?,
                memo: r.get(12)?,
                execution_timeout_ms: r.get(13)?,
                deadline_at: r.get(14)?,
                start_at: r.get(15)?,
                workflow_attempt: r.get(16)?,
                retry_policy_json: r.get(17)?,
                retry_of_exec_id: r.get(18)?,
                local_retry_at: r.get(19)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn read_events(conn: &Connection) -> SqliteResult<Vec<EventRow>> {
    let mut stmt =
        conn.prepare("SELECT exec_id, seq, event_json FROM harvest_events ORDER BY exec_id, seq")?;
    let rows = stmt
        .query_map([], |r| {
            Ok(EventRow {
                exec_id: r.get(0)?,
                seq: r.get(1)?,
                event_json: r.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn read_tasks(conn: &Connection) -> SqliteResult<Vec<TaskRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, exec_id, activity_id, name, input_json, queue, state, attempt, \
                run_at, seq, max_attempts, retry_policy_json, start_to_close_ms, \
                scheduled_at, schedule_to_close_at, defaults_frozen, heartbeat_details, last_error \
           FROM harvest_tasks ORDER BY seq, task_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(TaskRow {
                task_id: r.get(0)?,
                exec_id: r.get(1)?,
                activity_id: r.get(2)?,
                name: r.get(3)?,
                input_json: r.get(4)?,
                queue: r.get(5)?,
                state: r.get(6)?,
                attempt: r.get(7)?,
                run_at: r.get(8)?,
                seq: r.get(9)?,
                max_attempts: r.get(10)?,
                retry_policy_json: r.get(11)?,
                start_to_close_ms: r.get(12)?,
                scheduled_at: r.get(13)?,
                schedule_to_close_at: r.get(14)?,
                defaults_frozen: r.get(15)?,
                heartbeat_details: r.get(16)?,
                last_error: r.get(17)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn read_timers(conn: &Connection) -> SqliteResult<Vec<TimerRow>> {
    let mut stmt = conn.prepare(
        "SELECT timer_id, exec_id, fire_at, fired, arm_seq \
           FROM harvest_timers ORDER BY exec_id, arm_seq, timer_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(TimerRow {
                timer_id: r.get(0)?,
                exec_id: r.get(1)?,
                fire_at: r.get(2)?,
                fired: r.get(3)?,
                arm_seq: r.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn read_signals(conn: &Connection) -> SqliteResult<Vec<SignalRow>> {
    let mut stmt = conn.prepare(
        "SELECT signal_seq, exec_id, name, payload_json, delivered, received_at, idempotency_key \
           FROM harvest_signals ORDER BY signal_seq",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(SignalRow {
                signal_seq: r.get(0)?,
                exec_id: r.get(1)?,
                name: r.get(2)?,
                payload_json: r.get(3)?,
                delivered: r.get(4)?,
                received_at: r.get(5)?,
                idempotency_key: r.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[allow(clippy::too_many_lines)]
fn sync_local_sqlite_replica(
    replica_path: &Path,
    executions: &[ExecutionRow],
    events: &[EventRow],
    tasks: &[TaskRow],
    timers: &[TimerRow],
    signals: &[SignalRow],
) -> SqliteResult<()> {
    if let Some(parent) = replica_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|source| SqliteError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let replica = Connection::open(replica_path)?;
    replica.execute_batch(schema::SCHEMA)?;
    schema::migrate(&replica)?;

    let tx = replica.unchecked_transaction()?;
    for row in executions {
        tx.execute(
            "INSERT OR REPLACE INTO harvest_executions \
             (exec_id, workflow_name, workflow_id, input_json, state, output_json, error, \
              cancel_reason, cancel_from_seq, started_at, closed_at, search_attrs, memo, \
              execution_timeout_ms, deadline_at, start_at, workflow_attempt, \
              retry_policy_json, retry_of_exec_id, local_retry_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
            rusqlite::params![
                row.exec_id,
                row.workflow_name,
                row.workflow_id,
                row.input_json,
                row.state,
                row.output_json,
                row.error,
                row.cancel_reason,
                row.cancel_from_seq,
                row.started_at,
                row.closed_at,
                row.search_attrs,
                row.memo,
                row.execution_timeout_ms,
                row.deadline_at,
                row.start_at,
                row.workflow_attempt,
                row.retry_policy_json,
                row.retry_of_exec_id,
                row.local_retry_at,
            ],
        )?;
    }
    for row in events {
        tx.execute(
            "INSERT OR REPLACE INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![row.exec_id, row.seq, row.event_json],
        )?;
    }
    for row in tasks {
        tx.execute(
            "INSERT OR REPLACE INTO harvest_tasks \
             (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, \
              run_at, seq, max_attempts, retry_policy_json, start_to_close_ms, \
              scheduled_at, schedule_to_close_at, defaults_frozen, heartbeat_details, last_error) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                row.task_id,
                row.exec_id,
                row.activity_id,
                row.name,
                row.input_json,
                row.queue,
                row.state,
                row.attempt,
                row.run_at,
                row.seq,
                row.max_attempts,
                row.retry_policy_json,
                row.start_to_close_ms,
                row.scheduled_at,
                row.schedule_to_close_at,
                row.defaults_frozen,
                row.heartbeat_details,
                row.last_error,
            ],
        )?;
    }
    for row in timers {
        tx.execute(
            "INSERT OR REPLACE INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                row.timer_id,
                row.exec_id,
                row.fire_at,
                row.fired,
                row.arm_seq
            ],
        )?;
    }
    for row in signals {
        tx.execute(
            "INSERT OR REPLACE INTO harvest_signals \
             (signal_seq, exec_id, name, payload_json, delivered, received_at, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                row.signal_seq,
                row.exec_id,
                row.name,
                row.payload_json,
                row.delivered,
                row.received_at,
                row.idempotency_key,
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_hrana_sync_statements(
    executions: &[ExecutionRow],
    events: &[EventRow],
    tasks: &[TaskRow],
    timers: &[TimerRow],
    signals: &[SignalRow],
) -> Vec<TursoStmt> {
    let mut stmts = Vec::new();
    for raw_ddl in schema::SCHEMA.split(';') {
        let trimmed = raw_ddl.trim();
        if !trimmed.is_empty() {
            stmts.push(TursoStmt {
                sql: format!("{trimmed};"),
                args: Vec::new(),
            });
        }
    }

    for row in executions {
        stmts.push(TursoStmt {
            sql: "INSERT OR REPLACE INTO harvest_executions \
                  (exec_id, workflow_name, workflow_id, input_json, state, output_json, error, \
                   cancel_reason, cancel_from_seq, started_at, closed_at, search_attrs, memo, \
                   execution_timeout_ms, deadline_at, start_at, workflow_attempt, \
                   retry_policy_json, retry_of_exec_id, local_retry_at) \
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20);"
                .to_string(),
            args: vec![
                TursoArg::text(row.exec_id.clone()),
                TursoArg::text(row.workflow_name.clone()),
                TursoArg::text(row.workflow_id.clone()),
                TursoArg::text(row.input_json.clone()),
                TursoArg::text(row.state.clone()),
                TursoArg::opt_text(row.output_json.clone()),
                TursoArg::opt_text(row.error.clone()),
                TursoArg::opt_text(row.cancel_reason.clone()),
                TursoArg::opt_integer(row.cancel_from_seq),
                TursoArg::opt_integer(row.started_at),
                TursoArg::opt_integer(row.closed_at),
                TursoArg::opt_text(row.search_attrs.clone()),
                TursoArg::opt_text(row.memo.clone()),
                TursoArg::opt_integer(row.execution_timeout_ms),
                TursoArg::opt_integer(row.deadline_at),
                TursoArg::opt_integer(row.start_at),
                TursoArg::integer(row.workflow_attempt),
                TursoArg::opt_text(row.retry_policy_json.clone()),
                TursoArg::opt_text(row.retry_of_exec_id.clone()),
                TursoArg::opt_integer(row.local_retry_at),
            ],
        });
    }

    for row in events {
        stmts.push(TursoStmt {
            sql: "INSERT OR REPLACE INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3);"
                .to_string(),
            args: vec![
                TursoArg::text(row.exec_id.clone()),
                TursoArg::integer(row.seq),
                TursoArg::text(row.event_json.clone()),
            ],
        });
    }

    for row in tasks {
        stmts.push(TursoStmt {
            sql: "INSERT OR REPLACE INTO harvest_tasks \
                  (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, \
                   run_at, seq, max_attempts, retry_policy_json, start_to_close_ms, \
                   scheduled_at, schedule_to_close_at, defaults_frozen, heartbeat_details, last_error) \
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18);"
                .to_string(),
            args: vec![
                TursoArg::text(row.task_id.clone()),
                TursoArg::text(row.exec_id.clone()),
                TursoArg::text(row.activity_id.clone()),
                TursoArg::text(row.name.clone()),
                TursoArg::text(row.input_json.clone()),
                TursoArg::text(row.queue.clone()),
                TursoArg::text(row.state.clone()),
                TursoArg::integer(row.attempt),
                TursoArg::integer(row.run_at),
                TursoArg::integer(row.seq),
                TursoArg::opt_integer(row.max_attempts),
                TursoArg::opt_text(row.retry_policy_json.clone()),
                TursoArg::opt_integer(row.start_to_close_ms),
                TursoArg::integer(row.scheduled_at),
                TursoArg::opt_integer(row.schedule_to_close_at),
                TursoArg::integer(row.defaults_frozen),
                TursoArg::opt_text(row.heartbeat_details.clone()),
                TursoArg::opt_text(row.last_error.clone()),
            ],
        });
    }

    for row in timers {
        stmts.push(TursoStmt {
            sql: "INSERT OR REPLACE INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
                  VALUES (?1, ?2, ?3, ?4, ?5);"
                .to_string(),
            args: vec![
                TursoArg::text(row.timer_id.clone()),
                TursoArg::text(row.exec_id.clone()),
                TursoArg::integer(row.fire_at),
                TursoArg::integer(row.fired),
                TursoArg::integer(row.arm_seq),
            ],
        });
    }

    for row in signals {
        stmts.push(TursoStmt {
            sql: "INSERT OR REPLACE INTO harvest_signals \
                  (signal_seq, exec_id, name, payload_json, delivered, received_at, idempotency_key) \
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);"
                .to_string(),
            args: vec![
                TursoArg::integer(row.signal_seq),
                TursoArg::text(row.exec_id.clone()),
                TursoArg::text(row.name.clone()),
                TursoArg::text(row.payload_json.clone()),
                TursoArg::integer(row.delivered),
                TursoArg::integer(row.received_at),
                TursoArg::opt_text(row.idempotency_key.clone()),
            ],
        });
    }

    stmts
}

fn send_hrana_pipeline(
    endpoint: &str,
    auth_token: Option<&str>,
    req: &TursoPipelineRequest,
) -> SqliteResult<()> {
    let body = serde_json::to_string(req)?;
    let Some(rest) = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
    else {
        return Err(SqliteError::Turso(format!(
            "unsupported endpoint scheme: {}",
            redact_turso_url(endpoint)
        )));
    };
    let (host_port, path) = rest.split_once('/').map_or_else(
        || (rest, "/v2/pipeline".to_string()),
        |(h, p)| (h, format!("/{p}")),
    );
    let socket_target = if host_port.contains(':') {
        host_port.to_string()
    } else if endpoint.starts_with("https://") {
        format!("{host_port}:443")
    } else {
        format!("{host_port}:80")
    };

    let addr = socket_target
        .to_socket_addrs()
        .map_err(|e| {
            SqliteError::Turso(format!(
                "failed to resolve Turso host {}: {e}",
                redact_turso_url(host_port)
            ))
        })?
        .next()
        .ok_or_else(|| {
            SqliteError::Turso(format!(
                "no socket addresses resolved for {}",
                redact_turso_url(host_port)
            ))
        })?;

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).map_err(|e| {
        SqliteError::Turso(format!(
            "connect timeout to Turso {}: {e}",
            redact_turso_url(host_port)
        ))
    })?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

    let auth_header = match auth_token {
        Some(token) if !token.trim().is_empty() => {
            format!("Authorization: Bearer {}\r\n", token.trim())
        }
        _ => String::new(),
    };
    let http_req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host_port}\r\n{auth_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(http_req.as_bytes())
        .map_err(|e| SqliteError::Turso(format!("failed to write to Turso pipeline: {e}")))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| SqliteError::Turso(format!("failed to read from Turso pipeline: {e}")))?;

    let (headers, resp_body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        SqliteError::Turso("malformed HTTP response from Turso pipeline".to_string())
    })?;
    let status_line = headers.lines().next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| {
            SqliteError::Turso(format!(
                "invalid HTTP status line from Turso: {status_line}"
            ))
        })?;
    if !(200..300).contains(&status) {
        return Err(SqliteError::Turso(format!(
            "Turso pipeline returned HTTP {status}: {resp_body}"
        )));
    }
    if resp_body.contains("\"type\":\"error\"") {
        return Err(SqliteError::Turso(format!(
            "Turso pipeline statement error: {resp_body}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use autumn_harvest::prelude::*;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::{RunState, SqliteReader, Stored};

    #[workflow]
    async fn turso_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
        let step: i64 = ctx
            .execute_activity(&double_it_info(), n)
            .await
            .map_err(|e| e.to_string())?;
        Ok(step)
    }

    #[activity]
    #[allow(clippy::unused_async)]
    async fn double_it(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
        Ok(n * 2)
    }

    #[test]
    fn turso_config_redacts_tokens_in_debug_and_url() {
        let cfg = TursoReplicaConfig {
            database_url:
                "libsql://autumn-harvest-cerebral.turso.io?authToken=eyJhbGciOiJFZERTQSI6InRvcC1zZWNyZXQifQ"
                    .to_string(),
            auth_token: Some("eyJhbGciOiJFZERTQSI6InRvcC1zZWNyZXQifQ".to_string()),
            local_replica_path: None,
            sync_mode: TursoSyncMode::RemoteOnly,
        };
        let debug_str = format!("{cfg:?}");
        assert!(!debug_str.contains("eyJhbGciOiJFZERTQSI"));
        assert!(debug_str.contains("[REDACTED]"));
        assert_eq!(
            cfg.pipeline_endpoint().unwrap(),
            "https://autumn-harvest-cerebral.turso.io/v2/pipeline"
        );
    }

    #[tokio::test]
    async fn sqlite_runtime_syncs_workflow_state_to_turso_pipeline_and_embedded_replica() {
        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary.db");
        let replica_path = dir.path().join("replica.db");

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = vec![0_u8; 65536];
            let mut total = 0;
            loop {
                let n = stream.read(&mut buf[total..]).unwrap();
                if n == 0 {
                    break;
                }
                total += n;
                let current = String::from_utf8_lossy(&buf[..total]);
                if let Some((hdrs, body)) = current.split_once("\r\n\r\n")
                    && let Some(cl_line) = hdrs
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                    && let Some(cl) = cl_line
                        .split(':')
                        .nth(1)
                        .and_then(|v| v.trim().parse::<usize>().ok())
                    && body.len() >= cl
                {
                    break;
                }
            }
            let req_str = String::from_utf8_lossy(&buf[..total]).to_string();
            assert!(req_str.starts_with("POST /v2/pipeline HTTP/1.1"));
            assert!(req_str.contains("Authorization: Bearer turso-secret-token-99"));
            assert!(req_str.contains("INSERT OR REPLACE INTO harvest_executions"));
            assert!(req_str.contains("INSERT OR REPLACE INTO harvest_events"));

            let body = r#"{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":1,"last_insert_rowid":null}}},{"type":"ok","response":{"type":"close"}}]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
        });

        let mut rt = SqliteRuntime::open(&primary_path).unwrap();
        rt.register_workflow(&turso_wf_info());
        rt.register_activity(&double_it_info());

        let exec_id = rt.start_workflow("turso_wf", json!(21)).unwrap();
        let state = rt.run_until_blocked(exec_id).await.unwrap();
        assert!(matches!(state, RunState::Completed(ref v) if *v == json!(42)));

        let cfg = TursoReplicaConfig {
            database_url: format!("libsql://{addr}"),
            auth_token: Some("turso-secret-token-99".to_string()),
            local_replica_path: Some(replica_path.clone()),
            sync_mode: TursoSyncMode::EmbeddedReplicaSync,
        };

        let receipt = rt.sync_to_turso(&cfg).unwrap();
        server.join().unwrap();

        assert!(receipt.remote_pipeline_ok);
        assert!(receipt.local_replica_ok);
        assert_eq!(receipt.executions_synced, 1);
        assert!(receipt.events_synced >= 4);
        assert_eq!(receipt.tasks_synced, 1);

        // Verify the replicated local SQLite embedded replica can be opened by
        // SqliteReader and describes the completed execution byte-identically.
        let reader = SqliteReader::open(&replica_path).unwrap();
        let desc = reader.describe(exec_id).unwrap();
        assert_eq!(
            desc.summary.workflow_name,
            Stored::Value("turso_wf".to_string())
        );
        assert_eq!(desc.summary.state, Stored::Value("COMPLETED".to_string()));
        assert_eq!(desc.output_json, Stored::Value("42".to_string()));
    }
}
