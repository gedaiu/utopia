//! 推送来的名字是客户端给的身份（#1293、#1296）：`statements` 来源的文档里一个有名字的实体，
//! 只按名字字面归并或新建，不再拿名字向量去找相近的名字、给裁决器排 `name_vector|` 一对。
//! 机器起的名字共用一个前缀（`owner/repo:…`），向量上个个相近，排出来的对永远不是一个；
//! 而每个名字一次全库近邻查询，是推送时数据库的主要开销。
//!
//! 真库、脚本化的嵌入端点：所有名字的向量都一样，所以只要走了名字向量通道就一定排出一对。
//! 没有 `UTOPIA_DATABASE_URL` 时跳过而不是失败。自建自拆，绝不碰已有的库。
use super::*;
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn a_pushed_name_is_never_proposed_by_name_vector() -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&url).await?;
    utopia_store::db::migrate(&pool).await?;
    let (org, ws, kb, source, doc, chunk, known) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    let payload: Value = json!({
        "external_id": "commit:b",
        "e": [["re-cinq/lore:src/b.ts", "SourceFile", true]],
        "s": [[null, "re-cinq/lore:src/b.ts", "path", null, "src/b.ts", null, null, null]],
        "n": []
    });
    let text = serde_json::to_string_pretty(&payload)?;
    sqlx::raw_sql(&format!(
        "INSERT INTO organizations(id,name) VALUES ('{org}','pushed-names');
         INSERT INTO workspaces(id,org_id,name) VALUES ('{ws}','{org}','pushed-names');
         INSERT INTO knowledge_bases(id,workspace_id,name) VALUES ('{kb}','{ws}','pushed-names');
         INSERT INTO sources(id,kb_id,kind,name) VALUES ('{source}','{kb}','statements','lore');
         INSERT INTO documents(id,kb_id,filename,sha256,source_id) VALUES
             ('{doc}','{kb}','commit-b.json','x{doc}','{source}');
         INSERT INTO chunks(id,kb_id,document_id,seq,text) VALUES
             ('{chunk}','{kb}','{doc}',0,$${text}$$);
         INSERT INTO entities(id,kb_id,canonical_name) VALUES
             ('{known}','{kb}','re-cinq/lore:src/a.ts');"
    ))
    .execute(&pool)
    .await?;
    let fact = utopia_store::names::record(&pool, kb, known, "re-cinq/lore:src/a.ts", None, None)
        .await?
        .expect("a named entity gets a name fact");
    utopia_store::name_vectors::set(&pool, kb, &[(fact, known, vec![1.0, 0.0, 0.0, 0.0])]).await?;

    // 每个名字都是同一条向量：走了名字向量通道，a.ts 就一定是 b.ts 的近邻
    let router = Router::new().route(
        "/embeddings",
        post(|Json(body): Json<Value>| async move {
            let data: Vec<Value> = body["input"]
                .as_array()
                .expect("embedding input is an array")
                .iter()
                .map(|_| json!({"embedding": [1.0, 0.0, 0.0, 0.0]}))
                .collect();
            Json(json!({"data": data}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    utopia_store::settings::upsert(
        &pool,
        ws,
        None,
        None,
        None,
        Some(&base),
        None,
        Some("fake-embed"),
        Some(4),
    )
    .await?;

    let dir = tempfile::tempdir()?;
    let cfg = utopia_core::config::AppConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        ..Default::default()
    };
    let search = Arc::new(utopia_search::SearchIndex::open(
        &dir.path().join("search"),
    )?);
    let state = AppState::new(pool.clone(), &cfg, search, "test-only".into());
    crate::extraction::extract_document(&state, doc, utopia_core::models::Proposer::default())
        .await?;

    let proposed: Vec<(String,)> = sqlx::query_as(
        "SELECT reason FROM resolution_reviews WHERE kb_id = $1 AND reason LIKE 'name_vector|%'",
    )
    .bind(kb)
    .fetch_all(&pool)
    .await?;
    let b_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM entities WHERE kb_id = $1 AND canonical_name = 're-cinq/lore:src/b.ts')",
    )
    .bind(kb)
    .fetch_one(&pool)
    .await?;

    server.abort();
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(&pool)
        .await?;
    assert!(b_exists, "the pushed name is still resolved into an entity");
    assert!(
        proposed.is_empty(),
        "a pushed name proposes no name_vector pair: {proposed:?}"
    );
    Ok(())
}
