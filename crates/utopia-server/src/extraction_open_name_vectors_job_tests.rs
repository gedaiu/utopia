//! 名字向量补在一份按库去重的任务里（#1296）：从前每篇文档抽完都在线扫一遍全库还没有向量的
//! 名字事实，一阵推送几百篇就是几百遍全库扫描。现在一阵推送只排一份 `embed_names`，
//! 它跑一遍补上一批；没补完就再排一份，直到补完。
//!
//! 真库、脚本化的嵌入端点。没有 `UTOPIA_DATABASE_URL` 时跳过而不是失败。自建自拆。
use super::*;
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn two_pushed_documents_queue_one_embed_names_job_that_fills_the_vectors(
) -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&url).await?;
    utopia_store::db::migrate(&pool).await?;
    let (org, ws, kb, source) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    sqlx::raw_sql(&format!(
        "INSERT INTO organizations(id,name) VALUES ('{org}','name-vectors-job');
         INSERT INTO workspaces(id,org_id,name) VALUES ('{ws}','{org}','name-vectors-job');
         INSERT INTO knowledge_bases(id,workspace_id,name) VALUES ('{kb}','{ws}','name-vectors-job');
         INSERT INTO sources(id,kb_id,kind,name) VALUES ('{source}','{kb}','statements','lore');"
    ))
    .execute(&pool)
    .await?;
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

    for file in ["a", "b"] {
        let payload = json!({
            "external_id": format!("commit:{file}"),
            "e": [[format!("re-cinq/lore:src/{file}.ts"), "SourceFile", true]],
            "s": [[null, format!("re-cinq/lore:src/{file}.ts"), "path", null, format!("src/{file}.ts"), null, null, null]],
            "n": []
        });
        let (doc, chunk) = (Uuid::now_v7(), Uuid::now_v7());
        sqlx::query(
            "INSERT INTO documents(id,kb_id,filename,sha256,source_id) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(doc)
        .bind(kb)
        .bind(format!("{file}.json"))
        .bind(format!("x{doc}"))
        .bind(source)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO chunks(id,kb_id,document_id,seq,text) VALUES ($1,$2,$3,0,$4)")
            .bind(chunk)
            .bind(kb)
            .bind(doc)
            .bind(serde_json::to_string_pretty(&payload)?)
            .execute(&pool)
            .await?;
        crate::extraction::extract_document(&state, doc, utopia_core::models::Proposer::default())
            .await?;
    }
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='embed_names' AND status='queued' AND payload->>'kb_id'=$1",
    )
    .bind(kb.to_string())
    .fetch_one(&pool)
    .await?;
    let before = utopia_store::name_vectors::pending(&pool, kb, 100)
        .await?
        .len();
    embed_names_job(&state, kb).await?;
    let after = utopia_store::name_vectors::pending(&pool, kb, 100)
        .await?
        .len();

    server.abort();
    sqlx::query("DELETE FROM jobs WHERE payload->>'kb_id'=$1")
        .bind(kb.to_string())
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(&pool)
        .await?;
    assert_eq!((queued, before, after), (1, 2, 0));
    Ok(())
}
