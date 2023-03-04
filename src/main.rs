use std::sync::Arc;
use std::time::Instant;

use bench_arrow_sort::{make_sort_exprs, sort_batch};
use clap::{Arg, Parser};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::RowConverter;
use datafusion::physical_expr::PhysicalSortExpr;
use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Normal};
lazy_static! {
    static ref DIST: Normal<f64> = Normal::new(0.0, 10_000.0).unwrap();
}

#[derive(Debug, clap::Parser)]
struct Opt {
    /// how many rows to increment batch size by every time
    #[arg(short, long, required = true)]
    step_size: usize,
    /// how many rows to start with
    #[arg(short, long, required = true)]
    begin: usize,
    /// end after we've tested batches with this number of rows
    #[arg(short, long, required = true)]
    end: usize,
    #[arg(short, long, default_value = "50")]
    /// how many iterations to run. raise this to decrease variance
    iters: u128,
}

#[tokio::main]
async fn main() {
    let opts = Opt::parse();
    let batch = DataGen { batch_size: 1 }.generate_batch();
    let schemaref = batch.schema();
    let sortexpr = make_sort_exprs(&schemaref);
    // let default_lines = vec![];

    for batch_size in (opts.begin..opts.end + 1).step_by(opts.step_size) {
        println!("starting batch size {batch_size}");
        let gen = DataGen { batch_size };
        let mut runtimes_default = Vec::with_capacity(opts.iters as usize);
        let mut runtimes_row = Vec::with_capacity(opts.iters as usize);
        for _ in 0..opts.iters {
            let schema = schemaref.clone();
            let batch = gen.generate_batch();
            let now = Instant::now();
            sort_batch(batch, schema, &sortexpr, false).unwrap();
            let elapsed = now.elapsed();
            runtimes_default.push(elapsed);

            let schema = schemaref.clone();
            let batch = gen.generate_batch();
            let now = Instant::now();
            sort_batch(batch, schema, &sortexpr, true).unwrap();
            let elapsed = now.elapsed();
            runtimes_row.push(elapsed);
        }
        let default_total = runtimes_default.iter().sum::<std::time::Duration>();
        let defavg = default_total.as_micros() / opts.iters;
        let rows_total = runtimes_row.iter().sum::<std::time::Duration>();
        let rowsavg = rows_total.as_micros() / opts.iters;
        dbg!(defavg);
        dbg!(rowsavg);
    }
}

struct DataGen {
    batch_size: usize,
}
impl DataGen {
    /// gen batch of random data
    fn generate_batch(&self) -> RecordBatch {
        let mut rng = thread_rng();
        let ints = (0..self.batch_size)
            .map(|_| DIST.sample(&mut rng) as i64)
            .collect::<Int64Array>();
        let floats = (0..self.batch_size)
            .map(|_| DIST.sample(&mut rng))
            .collect::<Float64Array>();
        // random hex strings
        let strings = (0..self.batch_size)
            .map(|_| {
                let rand_bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
                Some(hex::encode(rand_bytes))
            })
            .collect::<StringArray>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Utf8, false),
            ])),
            vec![Arc::new(ints), Arc::new(floats), Arc::new(strings)],
        )
        .unwrap()
    }
}
