use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, BooleanBuilder, PrimitiveArray, PrimitiveBuilder, RecordBatch, StringBuilder,
        },
        datatypes::{
            ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, Schema, SchemaBuilder,
            SchemaRef,
        },
        util::pretty,
    },
    catalog::{CatalogProvider, SchemaProvider},
    common::project_schema,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, ExecutionMode, ExecutionPlan, PlanProperties,
    },
    prelude::*,
};
use itertools::Itertools;
use serde::{Deserialize, Deserializer};
use tokio_postgres::{types::FromSql, Row};

mod generate_parquet;

const PICODATA_CONNSTR: &str = "postgres://alice:Admin1234@127.0.0.1:5432";

async fn make_conn() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(PICODATA_CONNSTR, tokio_postgres::NoTls)
        .await
        .expect("cant connect to picodata");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

#[derive(Debug)]
struct PicodataExec {
    table_name: String,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl PicodataExec {
    fn new(
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
        schema: &SchemaRef,
        table_name: String,
    ) -> PicodataExec {
        let projected_schema = project_schema(&schema, projection).expect("cant project schema");

        let eq_properties = EquivalenceProperties::new(projected_schema.clone());

        Self {
            properties: PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            projected_schema,
            table_name,
        }
    }
}

fn build_query(table_name: &str, schema: &Schema) -> String {
    // TODO this is full of extra allocations
    let fields = schema
        .fields
        .into_iter()
        .map(|f| format!("\"{}\"", f.name()))
        .join(",");

    format!("SELECT {} FROM \"{}\"", fields, table_name)
}

impl ExecutionPlan for PicodataExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let projected_schema = self.projected_schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                // TODO translate datafusion exprs to sql: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.expr_to_sql.html
                // will be very helpful for pushdown materialization
                let query = build_query(&table_name, &projected_schema);
                // TODO we're spawning a connection without join handle, issues with cancellation, etc
                let client = make_conn().await;

                // TODO copy out?
                let rows = client
                    .query(&query, &[])
                    .await
                    .expect("picodata query failed");

                let mut cols: Vec<Arc<dyn Array>> =
                    Vec::with_capacity(projected_schema.fields().len());

                for (idx, field) in projected_schema.fields().into_iter().enumerate() {
                    // TODO handle nullability
                    match field.data_type() {
                        DataType::Boolean => {
                            let mut builder = BooleanBuilder::with_capacity(rows.len());
                            for row in &rows {
                                builder.append_value(row.get(idx))
                            }
                            cols.push(Arc::new(builder.finish()))
                        }
                        DataType::Int64 => {
                            cols.push(Arc::new(build_array::<Int64Type>(&rows, idx)))
                        }
                        DataType::Float64 => {
                            cols.push(Arc::new(build_array::<Float64Type>(&rows, idx)))
                        }
                        DataType::Utf8 => {
                            // TODO: with capacity here also requires total capacity of all strings in bytes
                            let mut builder = StringBuilder::new();
                            for row in &rows {
                                let value: &str = row.get(idx);
                                builder.append_value(value)
                            }
                            cols.push(Arc::new(builder.finish()))
                        }
                        ty @ _ => panic!("unsupported type while decoding query result: {}", ty),
                    }
                }

                RecordBatch::try_new(projected_schema, cols)
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        )))
    }
}

fn build_array<'a, A: ArrowPrimitiveType>(rows: &'a [Row], idx: usize) -> PrimitiveArray<A>
where
    A: ArrowPrimitiveType,
    A::Native: FromSql<'a>,
{
    // Alternative: use ArrayBuilder, but it mandates downcasting on each value
    let mut builder = PrimitiveBuilder::<A>::with_capacity(rows.len());

    for row in rows {
        let value: A::Native = row.get(idx);
        builder.append_value(value)
    }

    builder.finish()
}

impl DisplayAs for PicodataExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

struct PicodataTableProvider {
    table_name: String,
    schema: SchemaRef,
}

impl PicodataTableProvider {
    fn new(table_name: String, schema: Schema) -> Self {
        Self {
            table_name,
            schema: Arc::new(schema),
        }
    }
}

#[async_trait]
impl TableProvider for PicodataTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PicodataExec::new(
            projection,
            filters,
            limit,
            &self.schema,
            self.table_name.clone(),
        )))
    }
}

#[derive(Debug, Deserialize)]
enum PicodataType {
    Bool,
    Integer,
    Unsigned,
    Decimal,
    Number,
    Double,
    String,
    Datetime,
    Uuid,
    Map,
    Array,
    Any,
}

fn picodata_type_from_str<'de, D>(deserializer: D) -> Result<PicodataType, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer).and_then(|string| match &*string {
        "boolean" => Ok(PicodataType::Bool),
        "integer" => Ok(PicodataType::Integer),
        "unsigned" => Ok(PicodataType::Unsigned),
        "decimal" => Ok(PicodataType::Decimal),
        "number" => Ok(PicodataType::Number),
        "double" => Ok(PicodataType::Double),
        "string" => Ok(PicodataType::String),
        "datetime" => Ok(PicodataType::Datetime),
        "uuid" => Ok(PicodataType::Uuid),
        "map" => Ok(PicodataType::Map),
        "array" => Ok(PicodataType::Array),
        "any" => Ok(PicodataType::Any),
        ty @ _ => Err(Error::custom(format!("unknown type: {ty}"))),
    })
}

#[derive(Debug, Deserialize)]
struct PicodataField {
    name: String,
    #[serde(deserialize_with = "picodata_type_from_str")]
    field_type: PicodataType,
    is_nullable: bool,
}

impl From<PicodataField> for Field {
    fn from(value: PicodataField) -> Self {
        let arrow_ty = match value.field_type {
            PicodataType::Bool => DataType::Boolean,
            PicodataType::Integer => DataType::Int64,
            // Note: using Int64 for unsigned since postgres doesnt
            // have U64 type.
            PicodataType::Unsigned => DataType::Int64,
            PicodataType::Decimal => unimplemented!(),
            PicodataType::Number => DataType::Float32, // TODO verify
            PicodataType::Double => DataType::Float64,
            PicodataType::String => DataType::Utf8,
            PicodataType::Datetime => unimplemented!(),
            PicodataType::Uuid => unimplemented!(),
            // arrow has maps and arrays, for now let them be just strings
            PicodataType::Map => DataType::Utf8,
            PicodataType::Array => DataType::Utf8,
            // no idea what to do with any
            PicodataType::Any => DataType::Utf8,
        };

        Field::new(value.name, arrow_ty, value.is_nullable)
    }
}

struct PicodataSchemaProvider {
    tables: HashMap<String, Arc<PicodataTableProvider>>,
}

impl PicodataSchemaProvider {
    async fn new() -> Self {
        let mut tables = HashMap::new();

        let client = make_conn().await;

        let picodata_tables = client
            .query("SELECT * FROM \"_pico_table\"", &[])
            .await
            .expect("cant query _pico_table");

        for picodata_table in picodata_tables {
            let table_name: String = picodata_table.get(1);
            let table_format: tokio_postgres::types::Json<Vec<PicodataField>> =
                picodata_table.try_get(3).unwrap();

            let mut schema_builder = SchemaBuilder::new();
            for field_format in table_format.0 {
                schema_builder.push(Arc::new(Field::from(field_format)))
            }

            tables.insert(
                table_name.clone(),
                Arc::new(PicodataTableProvider::new(
                    table_name,
                    schema_builder.finish(),
                )),
            );
        }

        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for PicodataSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|s| s.to_owned()).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = match self.tables.get(name) {
            Some(table) => table,
            None => return Ok(None),
        };

        Ok(Some(Arc::clone(&table) as _))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

struct PicodataCatalogProvider {
    demo_schema: Arc<PicodataSchemaProvider>,
}

impl PicodataCatalogProvider {
    async fn new() -> Self {
        PicodataCatalogProvider {
            demo_schema: Arc::new(PicodataSchemaProvider::new().await),
        }
    }
}

impl CatalogProvider for PicodataCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["demo".to_owned()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "demo" => Some(Arc::clone(&self.demo_schema) as _),
            _ => None,
        }
    }
}

async fn exec(query: &str) {
    let ctx = SessionContext::new();
    let picodata_catalog = Arc::new(PicodataCatalogProvider::new().await);

    ctx.register_catalog("picodata", picodata_catalog.clone());
    ctx.register_parquet(
        "deliveries",
        "data/deliveries.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .expect("failed to register parquet");

    let df = ctx.sql(query).await.expect("query failed");

    let result = df.collect().await;
    match result {
        Ok(batches) => {
            log::info!("query completed");
            pretty::print_batches(&batches).unwrap();
        }
        Err(e) => {
            log::error!("query failed due to {e}");
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    match std::env::args().nth(1) {
        Some(value) => match &*value {
            "exec" => {
                let query = tokio::fs::read_to_string(std::env::args().nth(2).unwrap())
                    .await
                    .expect("cant read query");
                exec(&query).await
            }
            "seed" => generate_parquet::write()
                .await
                .expect("failed to write parquet"),
            c @ _ => eprintln!("unkown command: {c}, choices: exec, seed"),
        },
        None => todo!(),
    }
}
