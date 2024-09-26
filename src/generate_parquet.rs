use datafusion::arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use rand::Rng;
use std::sync::Arc;

pub async fn write() -> Result<()> {
    let ctx = SessionContext::new();

    let mut delivery_ids = vec![];
    let mut user_ids = vec![];
    let mut package_weights = vec![];

    let mut rng = rand::thread_rng();
    // generate 1m records
    for i in 0..(10usize.pow(6)) {
        delivery_ids.push(i as u64);

        let user_id: u64 = rng.gen_range(0..10);
        user_ids.push(user_id);

        let package_weight: u32 = rng.gen_range(150..8000);
        package_weights.push(package_weight)
    }

    let data = RecordBatch::try_from_iter(vec![
        (
            "delivery_id",
            Arc::new(UInt64Array::from(delivery_ids)) as ArrayRef,
        ),
        ("user_id", Arc::new(UInt64Array::from(user_ids)) as ArrayRef),
        ("package_weight", Arc::new(UInt32Array::from(package_weights))),
    ])?;

    ctx.register_batch("deliveries", data)?;

    let df = ctx.sql("SELECT * FROM deliveries").await?;
    df.write_parquet(
        "data/deliveries.parquet",
        DataFrameWriteOptions::new(),
        None, // writer_options
    )
    .await?;

    Ok(())
}
