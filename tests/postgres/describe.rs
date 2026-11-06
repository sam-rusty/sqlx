use sqlx::postgres::{PgConnection, Postgres};
use sqlx::{AssertSqlSafe, Column, Executor, SqlSafeStr, TypeInfo};
use sqlx_test::new;

#[sqlx_macros::test]
async fn it_describes_simple() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;

    let d = conn.describe("SELECT * FROM tweet".into_sql_str()).await?;

    assert_eq!(d.columns()[0].name(), "id");
    assert_eq!(d.columns()[1].name(), "created_at");
    assert_eq!(d.columns()[2].name(), "text");
    assert_eq!(d.columns()[3].name(), "owner_id");

    assert_eq!(d.nullable(0), Some(false));
    assert_eq!(d.nullable(1), Some(false));
    assert_eq!(d.nullable(2), Some(false));
    assert_eq!(d.nullable(3), Some(true));

    assert_eq!(d.columns()[0].type_info().name(), "INT8");
    assert_eq!(d.columns()[1].type_info().name(), "TIMESTAMPTZ");
    assert_eq!(d.columns()[2].type_info().name(), "TEXT");
    assert_eq!(d.columns()[3].type_info().name(), "INT8");

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_expression() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;

    let d = conn.describe("SELECT 1::int8 + 10".into_sql_str()).await?;

    // ?column? will cause the macro to emit an error ad ask the user to explicitly name the type
    assert_eq!(d.columns()[0].name(), "?column?");

    // postgres cannot infer nullability from an expression
    // this will cause the macro to emit `Option<_>`
    assert_eq!(d.nullable(0), None);
    assert_eq!(d.columns()[0].type_info().name(), "INT8");

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_enum() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;

    let d = conn
        .describe("SELECT 'open'::status as _1".into_sql_str())
        .await?;

    assert_eq!(d.columns()[0].name(), "_1");

    let ty = d.columns()[0].type_info();

    assert_eq!(ty.name(), "status");

    assert_eq!(
        format!("{:?}", ty.kind()),
        r#"Enum(["new", "open", "closed"])"#
    );

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_record() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;

    let d = conn
        .describe("SELECT (true, 10::int2)".into_sql_str())
        .await?;

    let ty = d.columns()[0].type_info();
    assert_eq!(ty.name(), "RECORD");

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_composite() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;

    let d = conn
        .describe("SELECT ROW('name',10,500)::inventory_item".into_sql_str())
        .await?;

    let ty = d.columns()[0].type_info();

    assert_eq!(ty.name(), "inventory_item");

    assert_eq!(
        format!("{:?}", ty.kind()),
        r#"Composite([("name", PgTypeInfo(Text)), ("supplier_id", PgTypeInfo(Int4)), ("price", PgTypeInfo(Int8))])"#
    );

    Ok(())
}

/// A one-row parent and a thousand-row child. With nested loops and merge joins off, the planner
/// hashes whichever side is smaller, so the side of the join that gets hashed is under our control.
async fn outer_join_tables(conn: &mut PgConnection) -> anyhow::Result<()> {
    sqlx::raw_sql(
        "CREATE TEMPORARY TABLE nullability_parent (id INT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TEMPORARY TABLE nullability_child (parent_id INT NOT NULL, name TEXT NOT NULL);
         INSERT INTO nullability_parent VALUES (1, 'parent');
         INSERT INTO nullability_child SELECT 1, 'child' FROM generate_series(1, 1000);
         ANALYZE nullability_parent;
         ANALYZE nullability_child;
         SET enable_nestloop = off;
         SET enable_mergejoin = off;",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn explain(conn: &mut PgConnection, sql: &str) -> anyhow::Result<String> {
    let lines = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!("EXPLAIN {sql}")))
        .fetch_all(&mut *conn)
        .await?;

    Ok(lines.join("\n"))
}

#[sqlx_macros::test]
async fn it_describes_left_join_planned_as_left_join() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;
    outer_join_tables(&mut conn).await?;

    let sql = "SELECT c.name, p.name FROM nullability_child c LEFT JOIN nullability_parent p ON p.id = c.parent_id";

    assert!(explain(&mut conn, sql).await?.contains("Hash Left Join"));

    let d = conn.describe(sql.into_sql_str()).await?;

    assert_eq!(d.nullable(0), Some(false));
    assert_eq!(d.nullable(1), Some(true));

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_left_join_planned_as_right_join() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;
    outer_join_tables(&mut conn).await?;

    // the planner hashes the smaller, preserved side, so the null-extended side becomes `Outer`
    let sql = "SELECT p.id, p.name, c.name FROM nullability_parent p LEFT JOIN nullability_child c ON c.parent_id = p.id";

    assert!(explain(&mut conn, sql).await?.contains("Hash Right Join"));

    let d = conn.describe(sql.into_sql_str()).await?;

    assert_eq!(d.nullable(0), Some(false));
    assert_eq!(d.nullable(1), Some(false));
    assert_eq!(d.nullable(2), Some(true));

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_outer_join_under_another_node() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;
    outer_join_tables(&mut conn).await?;

    let sql = "SELECT c.name, p.name FROM nullability_child c LEFT JOIN nullability_parent p ON p.id = c.parent_id LIMIT 1";

    assert!(explain(&mut conn, sql).await?.starts_with("Limit"));

    let d = conn.describe(sql.into_sql_str()).await?;

    assert_eq!(d.nullable(0), Some(false));
    assert_eq!(d.nullable(1), Some(true));

    Ok(())
}

#[sqlx_macros::test]
async fn it_describes_full_join() -> anyhow::Result<()> {
    let mut conn = new::<Postgres>().await?;
    outer_join_tables(&mut conn).await?;

    let sql = "SELECT p.name, c.name FROM nullability_parent p FULL JOIN nullability_child c ON c.parent_id = p.id";

    assert!(explain(&mut conn, sql).await?.contains("Full Join"));

    let d = conn.describe(sql.into_sql_str()).await?;

    assert_eq!(d.nullable(0), Some(true));
    assert_eq!(d.nullable(1), Some(true));

    Ok(())
}
