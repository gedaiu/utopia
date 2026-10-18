//! The recompute plans each statement for the base it is given (#1298: a generic plan of the
//! source cleanup ran for hours on a production base where the custom plan takes two seconds).
use super::materialize_in_tx;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn a_recompute_plans_its_statements_for_the_base_it_is_given() -> anyhow::Result<()> {
    let Some(url) = crate::test_db::url() else {
        return Ok(());
    };
    let pool = PgPool::connect(&url).await?;
    let mut tx = pool.begin().await?;

    materialize_in_tx(&mut tx, Uuid::now_v7()).await?;
    let mode: String = sqlx::query_scalar("SELECT current_setting('plan_cache_mode')")
        .fetch_one(&mut *tx)
        .await?;
    tx.rollback().await?;

    assert_eq!(mode, "force_custom_plan");
    Ok(())
}
