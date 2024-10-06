use datafusion::prelude::*;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use subframe_rs;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let table = subframe_rs::table(
        vec![
            ("a".to_string(), "i64".to_string()),
            ("b".to_string(), "i64".to_string()),
            ("c".to_string(), "i64".to_string()),
        ],
        String::from("example"),
    )
    .select(vec!["a".to_string(), "c".to_string()])
    .select(vec!["c".to_string()]);

    let plan = table.to_substrait();

    ctx.register_csv("example", "example.csv", CsvReadOptions::new())
        .await?;

    let logical_plan = from_substrait_plan(&ctx, &plan).await?;

    let df = ctx.execute_logical_plan(logical_plan).await?;

    // let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

    df.show().await?;
    Ok(())
}
