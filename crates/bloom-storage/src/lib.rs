pub mod models;

pub use models::{EventRecord, JobRecord, JobState};

use bloom_core::{JobId, ModelId, NodeId};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("value {field} exceeds SQLite's supported integer range")]
    ValueOutOfRange { field: &'static str },
}

pub type Result<T> = core::result::Result<T, StorageError>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    node TEXT NOT NULL,
    model TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    output_chars INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    meta TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    data TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs (created_at_ms DESC);
"#;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let path: PathBuf = path.to_path_buf();
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn create_job(&self, record: &JobRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO jobs (id, node, model, kind, state, created_at_ms, updated_at_ms, output_chars, error, meta)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.id.as_str())
        .bind(record.node.as_str())
        .bind(record.model.as_str())
        .bind(&record.kind)
        .bind(record.state.as_str())
        .bind(sqlite_i64(record.created_at_ms, "created_at_ms")?)
        .bind(sqlite_i64(record.updated_at_ms, "updated_at_ms")?)
        .bind(sqlite_i64(record.output_chars, "output_chars")?)
        .bind(record.error.as_deref())
        .bind(record.meta.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_job(&self, id: &JobId, state: JobState, error: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE jobs SET state = ?, error = ?, updated_at_ms = ? WHERE id = ?")
            .bind(state.as_str())
            .bind(error)
            .bind(bloom_core::capacity::unix_millis() as i64)
            .bind(id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn finish_job(
        &self,
        id: &JobId,
        state: JobState,
        output_chars: u64,
        error: Option<&str>,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE jobs SET state = ?, output_chars = ?, error = ?, updated_at_ms = ? WHERE id = ? AND state IN ('queued', 'running')",
        )
        .bind(state.as_str())
        .bind(sqlite_i64(output_chars, "output_chars")?)
        .bind(error)
        .bind(bloom_core::capacity::unix_millis() as i64)
        .bind(id.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Marks jobs left queued or running by a previous process as interrupted.
    /// Call this only after the process has acquired its exclusive data-directory lock.
    pub async fn interrupt_unfinished(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE jobs SET state = ?, error = ?, updated_at_ms = ? WHERE state IN (?, ?)",
        )
        .bind(JobState::Interrupted.as_str())
        .bind("daemon restarted while job was unfinished")
        .bind(bloom_core::capacity::unix_millis() as i64)
        .bind(JobState::Queued.as_str())
        .bind(JobState::Running.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn job(&self, id: &JobId) -> Result<Option<JobRecord>> {
        let row = sqlx::query_as::<_, (String, String, String, String, String, i64, i64, i64, Option<String>, Option<String>)>(
            "SELECT id, node, model, kind, state, created_at_ms, updated_at_ms, output_chars, error, meta FROM jobs WHERE id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| job_from_row(&r)))
    }

    pub async fn list_jobs(&self, limit: u32) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, i64, i64, i64, Option<String>, Option<String>)>(
            "SELECT id, node, model, kind, state, created_at_ms, updated_at_ms, output_chars, error, meta FROM jobs ORDER BY created_at_ms DESC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(job_from_row).collect())
    }

    pub async fn append_event(&self, record: &EventRecord) -> Result<()> {
        sqlx::query("INSERT INTO events (at_ms, kind, message, data) VALUES (?, ?, ?, ?)")
            .bind(record.at_ms as i64)
            .bind(&record.kind)
            .bind(&record.message)
            .bind(record.data.as_deref())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn recent_events(&self, limit: u32) -> Result<Vec<EventRecord>> {
        let rows = sqlx::query_as::<_, (i64, String, String, Option<String>)>(
            "SELECT at_ms, kind, message, data FROM events ORDER BY id DESC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(at_ms, kind, message, data)| EventRecord {
                at_ms: at_ms as u64,
                kind,
                message,
                data,
            })
            .collect())
    }

    pub async fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO kv (key, value, updated_at_ms) VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(key)
        .bind(value)
        .bind(bloom_core::capacity::unix_millis() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM kv WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(value,)| value))
    }
}

type JobRow = (
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    Option<String>,
    Option<String>,
);

fn sqlite_i64(value: u64, field: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| StorageError::ValueOutOfRange { field })
}

fn job_from_row(row: &JobRow) -> JobRecord {
    JobRecord {
        id: JobId::new(row.0.clone()),
        node: NodeId::new(row.1.clone()),
        model: ModelId::new(row.2.clone()),
        kind: row.3.clone(),
        state: JobState::parse(&row.4).unwrap_or(JobState::Failed),
        created_at_ms: row.5 as u64,
        updated_at_ms: row.6 as u64,
        output_chars: row.7 as u64,
        error: row.8.clone(),
        meta: row.9.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bloom_core::capacity::unix_millis;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "bloom-test-{}-{}.db",
            name,
            bloom_core::capacity::unix_millis()
        ))
    }

    #[tokio::test]
    async fn job_lifecycle_roundtrip() {
        let store = Store::open(&temp_db("jobs")).await.unwrap();
        let id = JobId::generate();
        let record = JobRecord {
            id: id.clone(),
            node: NodeId::new("node"),
            model: ModelId::new("m"),
            kind: "chat".into(),
            state: JobState::Queued,
            created_at_ms: unix_millis(),
            updated_at_ms: unix_millis(),
            output_chars: 0,
            error: None,
            meta: None,
        };
        store.create_job(&record).await.unwrap();
        store
            .update_job(&id, JobState::Running, None)
            .await
            .unwrap();
        store
            .finish_job(&id, JobState::Completed, 42, None)
            .await
            .unwrap();
        let jobs = store.list_jobs(10).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, JobState::Completed);
        assert_eq!(jobs[0].output_chars, 42);
        let fetched = store.job(&id).await.unwrap().unwrap();
        assert_eq!(fetched.id, id);
    }

    #[tokio::test]
    async fn events_and_kv_roundtrip() {
        let store = Store::open(&temp_db("kv")).await.unwrap();
        store
            .append_event(&EventRecord {
                at_ms: unix_millis(),
                kind: "job".into(),
                message: "started".into(),
                data: None,
            })
            .await
            .unwrap();
        let events = store.recent_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "job");

        store.kv_set("node_id", "abc").await.unwrap();
        store.kv_set("node_id", "def").await.unwrap();
        assert_eq!(
            store.kv_get("node_id").await.unwrap().as_deref(),
            Some("def")
        );
        assert!(store.kv_get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restart_interrupts_only_unfinished_jobs() {
        let store = Store::open(&temp_db("restart")).await.unwrap();
        let queued_id = JobId::generate();
        let running_id = JobId::generate();
        let completed_id = JobId::generate();
        let now = unix_millis();
        for (id, state) in [
            (&queued_id, JobState::Queued),
            (&running_id, JobState::Running),
            (&completed_id, JobState::Completed),
        ] {
            store
                .create_job(&JobRecord {
                    id: id.clone(),
                    node: NodeId::new("node"),
                    model: ModelId::new("model"),
                    kind: "chat".into(),
                    state,
                    created_at_ms: now,
                    updated_at_ms: now,
                    output_chars: 0,
                    error: None,
                    meta: None,
                })
                .await
                .unwrap();
        }

        assert_eq!(store.interrupt_unfinished().await.unwrap(), 2);
        for id in [&queued_id, &running_id] {
            let job = store.job(id).await.unwrap().unwrap();
            assert_eq!(job.state, JobState::Interrupted);
            assert_eq!(
                job.error.as_deref(),
                Some("daemon restarted while job was unfinished")
            );
        }
        assert_eq!(
            store.job(&completed_id).await.unwrap().unwrap().state,
            JobState::Completed
        );
        assert_eq!(store.interrupt_unfinished().await.unwrap(), 0);
    }
}
