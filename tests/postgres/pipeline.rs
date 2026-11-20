//! Unnamed statements in one round trip (`statement_cache_capacity(0)`), the shared type
//! cache, first-sight column type resolution, `test_on_release(false)`, the acquire yield
//! and `timezone(None)`.
//!
//! Every test works in its own schema, so they can run in parallel on one database.

use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::TryStreamExt;
use sqlx::postgres::{
    PgConnectOptions, PgConnection, PgPoolOptions, PgRow, PgSslMode, PgTypeCache, Postgres,
};
use sqlx::types::Json;
use sqlx::{Connection, Executor, Row, SqlSafeStr, Statement};
use sqlx_core::io::ReadBuf;
use sqlx_core::net::Socket;
use sqlx_core::sql_str::AssertSqlSafe;
use tokio::net::TcpStream;

fn base_options() -> PgConnectOptions {
    sqlx_test::setup_if_needed();
    PgConnectOptions::from_str(&std::env::var("DATABASE_URL").unwrap()).unwrap()
}

/// The options under test: no named statements, a shared type cache.
fn options(cache: &Arc<PgTypeCache>) -> PgConnectOptions {
    base_options()
        .statement_cache_capacity(0)
        .close_unnamed_portal(false)
        .shared_type_cache(cache.clone())
}

async fn connect(cache: &Arc<PgTypeCache>) -> PgConnection {
    PgConnection::connect_with(&options(cache)).await.unwrap()
}

/// A fresh schema with two enums of the same labels, `mood` and `feeling`, and a table.
async fn schema(name: &str) {
    let mut conn = PgConnection::connect_with(&base_options()).await.unwrap();
    conn.execute(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {name} CASCADE;
         CREATE SCHEMA {name};
         CREATE TYPE {name}.mood AS ENUM ('sad', 'ok', 'happy');
         CREATE TYPE {name}.feeling AS ENUM ('sad', 'ok', 'happy');
         CREATE TABLE {name}.t (
             id int PRIMARY KEY,
             mood {name}.mood NOT NULL,
             moods {name}.mood[] NOT NULL,
             doc json,
             docb jsonb
         );
         INSERT INTO {name}.t VALUES
             (1, 'sad', '{{sad,ok}}', '{{}}', '{{}}'),
             (2, 'happy', '{{happy}}', '[]', '[]');"
    )))
    .await
    .unwrap();
}

/// The test's schema is on `search_path`, so `mood` resolves to its type.
#[derive(Debug, PartialEq, Clone, Copy, sqlx::Type)]
#[sqlx(type_name = "mood", rename_all = "lowercase")]
#[allow(dead_code)]
enum Mood {
    Sad,
    Ok,
    Happy,
}

/// Same labels as [`Mood`]: only the column type check tells them apart.
#[derive(Debug, PartialEq, Clone, Copy, sqlx::Type)]
#[sqlx(type_name = "feeling", rename_all = "lowercase")]
#[allow(dead_code)]
enum Feeling {
    Sad,
    Ok,
    Happy,
}

/// Counts what goes over the wire: round trips (a write, then the first bytes of the
/// reply), type lookups (simple queries on `pg_type`) and `Parse` messages.
#[derive(Default)]
struct Wire {
    sockets: AtomicU32,
    round_trips: AtomicU32,
    type_lookups: AtomicU32,
}

struct CountingSocket {
    inner: TcpStream,
    wire: Arc<Wire>,
    awaiting: bool,
}

impl Socket for CountingSocket {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let n = Socket::try_read(&mut self.inner, buf)?;
        if n > 0 && std::mem::take(&mut self.awaiting) {
            self.wire.round_trips.fetch_add(1, Ordering::Relaxed);
        }
        Ok(n)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = Socket::try_write(&mut self.inner, buf)?;
        if buf[..n].windows(18).any(|w| w == b"pg_catalog.pg_type") {
            self.wire.type_lookups.fetch_add(1, Ordering::Relaxed);
        }
        self.awaiting |= n > 0;
        Ok(n)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_shutdown(cx)
    }
}

fn counted(options: PgConnectOptions) -> (PgConnectOptions, Arc<Wire>) {
    let wire = Arc::new(Wire::default());
    let (host, port) = (options.get_host().to_owned(), options.get_port());
    let factory_wire = wire.clone();
    let options = options
        .ssl_mode(PgSslMode::Disable)
        .socket_factory(move || {
            let (host, port, wire) = (host.clone(), port, factory_wire.clone());
            async move {
                wire.sockets.fetch_add(1, Ordering::Relaxed);
                Ok(CountingSocket {
                    inner: TcpStream::connect((host.as_str(), port)).await?,
                    wire,
                    awaiting: false,
                })
            }
        });
    (options, wire)
}

fn get(counter: &AtomicU32) -> u32 {
    counter.load(Ordering::Relaxed)
}

#[sqlx_macros::test]
async fn enum_parameter_and_column_one_round_trip_when_warm() -> anyhow::Result<()> {
    schema("pl_enum").await;
    let cache = Arc::new(PgTypeCache::new());
    let (options, wire) = counted(options(&cache));
    let mut conn = PgConnection::connect_with(&options).await?;
    conn.execute("SET search_path TO pl_enum").await?;

    let sql = "SELECT id, mood FROM t WHERE mood = $1";
    let row: (i32, Mood) = sqlx::query_as(sql)
        .bind(Mood::Happy)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row, (2, Mood::Happy));
    // the bind type was resolved before the batch, so the column type was known
    assert_eq!(get(&wire.type_lookups), 1);

    let before = get(&wire.round_trips);
    let row: (i32, Mood) = sqlx::query_as(sql)
        .bind(Mood::Sad)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row, (1, Mood::Sad));
    assert_eq!(get(&wire.round_trips) - before, 1);
    assert_eq!(get(&wire.type_lookups), 1);
    Ok(())
}

#[sqlx_macros::test]
async fn column_type_check_is_kept_for_first_sight_columns() -> anyhow::Result<()> {
    schema("pl_check").await;

    let cache = Arc::new(PgTypeCache::new());
    let (options, wire) = counted(options(&cache));
    let mut conn = PgConnection::connect_with(&options).await?;
    conn.execute("SET search_path TO pl_check").await?;

    // `mood` is never bound, so the first sight of it is the row description
    let row: PgRow = sqlx::query("SELECT mood FROM t WHERE id = $1")
        .bind(1)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(get(&wire.type_lookups), 1);
    assert_eq!(row.try_get::<Mood, _>(0)?, Mood::Sad);
    // a decode into another enum with the same labels is refused, as with a described
    // statement (an unresolved column type would accept it)
    let error = row.try_get::<Feeling, _>(0).unwrap_err();
    assert!(
        matches!(error, sqlx::Error::ColumnDecode { .. }),
        "{error:?}"
    );

    // known now: no lookup, and the rows stream as usual
    let rows: Vec<PgRow> = sqlx::query("SELECT mood FROM t WHERE id > $1 ORDER BY id")
        .bind(0)
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(get(&wire.type_lookups), 1);
    Ok(())
}

#[sqlx_macros::test]
async fn enum_arrays_as_column_and_bind() -> anyhow::Result<()> {
    schema("pl_array").await;
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_array").await?;

    let moods: Vec<Mood> = sqlx::query_scalar("SELECT moods FROM t WHERE id = $1")
        .bind(1)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(moods, vec![Mood::Sad, Mood::Ok]);

    let ids: Vec<i32> =
        sqlx::query_scalar("SELECT id FROM t WHERE moods && $1 ORDER BY id")
            .bind(vec![Mood::Happy, Mood::Ok])
            .fetch_all(&mut conn)
            .await?;
    assert_eq!(ids, vec![1, 2]);

    // the element OID inside an array of a custom type is a hole in the argument buffer
    let echoed: Vec<Mood> = sqlx::query_scalar("SELECT $1::mood[]")
        .bind(vec![Mood::Happy])
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(echoed, vec![Mood::Happy]);
    Ok(())
}

#[sqlx_macros::test]
async fn json_into_json_and_jsonb_and_builtin_arrays() -> anyhow::Result<()> {
    schema("pl_json").await;
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;

    let value = serde_json::json!({"a": [1, 2]});
    sqlx::query("UPDATE pl_json.t SET doc = $1, docb = $2 WHERE id = 1")
        .bind(Json(&value))
        .bind(Json(&value))
        .execute(&mut conn)
        .await?;
    let (doc, docb): (Json<serde_json::Value>, Json<serde_json::Value>) =
        sqlx::query_as("SELECT doc, docb FROM pl_json.t WHERE id = $1")
            .bind(1)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!((doc.0, docb.0), (value.clone(), value));

    let (ints, texts): (Vec<i32>, Vec<String>) = sqlx::query_as("SELECT $1::int4[], $2::text[]")
        .bind(vec![1, 2, 3])
        .bind(vec!["x".to_owned()])
        .fetch_one(&mut conn)
        .await?;
    assert_eq!((ints, texts), (vec![1, 2, 3], vec!["x".to_owned()]));
    Ok(())
}

/// Claims `int4` but encodes three bytes: the server rejects it at `Bind`.
struct BadInt;

impl sqlx::Type<Postgres> for BadInt {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i32 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for BadInt {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        buf.extend_from_slice(&[0, 0, 1]);
        Ok(sqlx::encode::IsNull::No)
    }
}

#[sqlx_macros::test]
async fn parse_and_bind_errors_leave_the_connection_usable() -> anyhow::Result<()> {
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;

    let error = sqlx::query("SELEC $1::int4")
        .bind(1)
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().unwrap(), "42601");
    let one: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(1)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(one, 1);

    let error = sqlx::query("SELECT $1::int4")
        .bind(BadInt)
        .execute(&mut conn)
        .await
        .unwrap_err();
    // 08P01: three bytes where `int4` needs four
    let code = error.as_database_error().unwrap().code().unwrap().into_owned();
    assert!(matches!(code.as_str(), "08P01" | "22P03"), "{error:?}");
    let two: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(2)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(two, 2);
    Ok(())
}

#[sqlx_macros::test]
async fn first_sight_columns_with_no_rows_and_with_an_error_after_the_description(
) -> anyhow::Result<()> {
    schema("pl_first").await;
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_first").await?;

    let rows: Vec<PgRow> = sqlx::query("SELECT mood FROM t WHERE id = $1")
        .bind(-1)
        .fetch_all(&mut conn)
        .await?;
    assert!(rows.is_empty());

    // a fresh cache, so `feeling` is unknown when the error arrives after its description
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_first").await?;
    let error = sqlx::query("SELECT 'ok'::feeling, 1 / (id - $1) FROM t")
        .bind(1)
        .fetch_all(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().unwrap(), "22012");
    let mood: Mood = sqlx::query_scalar("SELECT mood FROM t WHERE id = $1")
        .bind(2)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(mood, Mood::Happy);
    Ok(())
}

#[sqlx_macros::test]
async fn transactions_dropped_streams_and_dropped_transactions() -> anyhow::Result<()> {
    schema("pl_tx").await;
    let cache = Arc::new(PgTypeCache::new());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .test_on_release(false)
        .connect_with(options(&cache))
        .await?;

    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE pl_tx.t SET id = id + 10 WHERE id = $1")
        .bind(1)
        .execute(&mut *tx)
        .await?;
    let seen: i64 = sqlx::query_scalar("SELECT count(*) FROM pl_tx.t WHERE id = $1")
        .bind(11)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(seen, 1);
    tx.commit().await?;

    // dropped mid-rows: the release drains it
    {
        let mut conn = pool.acquire().await?;
        let mut rows = sqlx::query("SELECT g FROM generate_series(1, $1) g")
            .bind(10_000)
            .fetch(&mut *conn);
        rows.try_next().await?;
    }
    let n: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(7)
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 7);

    // dropped transaction: the release sends its ROLLBACK
    {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM pl_tx.t WHERE id = $1")
            .bind(2)
            .execute(&mut *tx)
            .await?;
    }
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM pl_tx.t WHERE id = $1")
        .bind(2)
        .fetch_one(&pool)
        .await?;
    assert_eq!(left, 1);
    Ok(())
}

#[sqlx_macros::test]
async fn no_named_statements_with_the_cache_disabled() -> anyhow::Result<()> {
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    for i in 0..5 {
        let _: i32 = sqlx::query_scalar("SELECT $1::int4 + 1")
            .bind(i)
            .fetch_one(&mut conn)
            .await?;
    }
    // a prepared statement object, executed later, is unnamed too
    let statement = conn.prepare("SELECT $1::int4".into_sql_str()).await?;
    let one: i32 = statement.query_scalar().bind(1).fetch_one(&mut conn).await?;
    assert_eq!(one, 1);

    let named: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_prepared_statements")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(named, 0);
    Ok(())
}

#[sqlx_macros::test]
async fn describe_still_works_with_the_cache_disabled() -> anyhow::Result<()> {
    schema("pl_describe").await;
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    let describe = conn
        .describe(AssertSqlSafe("SELECT t.id, u.id FROM pl_describe.t LEFT JOIN pl_describe.t u ON u.id = t.id + $1").into_sql_str())
        .await?;
    assert_eq!(describe.nullable, vec![Some(false), Some(true)]);
    let named: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_prepared_statements")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(named, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sequential_queries_reuse_one_connection_on_a_current_thread_runtime(
) -> anyhow::Result<()> {
    let cache = Arc::new(PgTypeCache::new());
    let (options, wire) = counted(options(&cache));
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .test_before_acquire(false)
        .test_on_release(false)
        .connect_lazy_with(options);

    let before_warm = {
        let _: i32 = sqlx::query_scalar("SELECT $1::int4")
            .bind(1)
            .fetch_one(&pool)
            .await?;
        get(&wire.round_trips)
    };
    for i in 0..10 {
        let _: i32 = sqlx::query_scalar("SELECT $1::int4")
            .bind(i)
            .fetch_one(&pool)
            .await?;
    }
    assert_eq!(pool.size(), 1);
    assert_eq!(get(&wire.sockets), 1);
    // one round trip per query, no ping on release
    assert_eq!(get(&wire.round_trips) - before_warm, 10);
    Ok(())
}

#[sqlx_macros::test]
async fn second_connection_and_second_pool_are_seeded_from_the_shared_cache(
) -> anyhow::Result<()> {
    schema("pl_shared").await;
    let cache = Arc::new(PgTypeCache::new());

    let (first, first_wire) = counted(options(&cache));
    let mut conn = PgConnection::connect_with(&first).await?;
    conn.execute("SET search_path TO pl_shared").await?;
    let _: Vec<Mood> = sqlx::query_scalar("SELECT moods FROM t WHERE mood = $1")
        .bind(Mood::Sad)
        .fetch_one(&mut conn)
        .await?;
    assert!(get(&first_wire.type_lookups) >= 1);

    let (second, second_wire) = counted(options(&cache));
    let mut conn = PgConnection::connect_with(&second).await?;
    conn.execute("SET search_path TO pl_shared").await?;
    let _: Vec<Mood> = sqlx::query_scalar("SELECT moods FROM t WHERE mood = $1")
        .bind(Mood::Sad)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(get(&second_wire.type_lookups), 0);

    let (third, third_wire) = counted(options(&cache));
    let pool = PgPoolOptions::new()
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET search_path TO pl_shared").await?;
                Ok(())
            })
        })
        .connect_lazy_with(third);
    let moods: Vec<Mood> = sqlx::query_scalar("SELECT moods FROM t WHERE mood = $1")
        .bind(Mood::Happy)
        .fetch_one(&pool)
        .await?;
    assert_eq!(moods, vec![Mood::Happy]);
    assert_eq!(get(&third_wire.type_lookups), 0);
    Ok(())
}

#[sqlx_macros::test]
async fn a_stale_type_oid_fails_at_parse() -> anyhow::Result<()> {
    schema("pl_stale").await;
    let cache = Arc::new(PgTypeCache::new());
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_stale").await?;
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM t WHERE mood = $1")
        .bind(Mood::Ok)
        .fetch_one(&mut conn)
        .await?;

    conn.execute(
        "ALTER TABLE t DROP COLUMN mood, DROP COLUMN moods; DROP TYPE mood;
         CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
         ALTER TABLE t ADD COLUMN mood mood NOT NULL DEFAULT 'ok';",
    )
    .await?;

    // a new connection seeded with the old OID: refused before anything runs
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_stale").await?;
    let error = sqlx::query("UPDATE t SET id = id WHERE mood = $1")
        .bind(Mood::Ok)
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert!(error.as_database_error().is_some(), "{error:?}");

    cache.clear();
    let mut conn = connect(&cache).await;
    conn.execute("SET search_path TO pl_stale").await?;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM t WHERE mood = $1")
        .bind(Mood::Ok)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(n, 2);
    Ok(())
}

#[sqlx_macros::test]
async fn timezone_none_keeps_the_database_default() -> anyhow::Result<()> {
    let mut admin = PgConnection::connect_with(&base_options()).await?;
    admin
        .execute("DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET TimeZone = %L', current_database(), 'America/Los_Angeles'); END $$")
        .await?;

    let cache = Arc::new(PgTypeCache::new());
    let mut conn = PgConnection::connect_with(&options(&cache).timezone(None::<&str>)).await?;
    let tz: String = sqlx::query_scalar("SELECT current_setting('TimeZone')")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(tz, "America/Los_Angeles");

    let mut conn = connect(&cache).await;
    let tz: String = sqlx::query_scalar("SELECT current_setting('TimeZone')")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(tz, "UTC");
    Ok(())
}
