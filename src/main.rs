use clap::{Parser, ValueEnum};
use inspect_arrow_sort::{make_sort_exprs, plot, sort_batch, Case, DataPoint};
use std::time::Instant;

#[derive(Debug, clap::Parser)]
struct Opt {
    #[arg(value_enum, long, short, default_value = "mixed-tuple")]
    case: Case,
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
    let caseval = opts.case.to_possible_value().unwrap();
    let casename = caseval.get_name();
    let gen = opts.case;
    let batch = gen.generate_batch(1);
    // let batch = DataGen { batch_size: 1 }.generate_batch();
    let schemaref = batch.schema();
    let sortexpr = make_sort_exprs(&schemaref);
    let mut data = vec![];
    // // let default_lines = vec![];

    for batch_size in (opts.begin..opts.end + 1).step_by(opts.step_size) {
        println!("starting batch size {batch_size}");
        let mut runtimes_default = Vec::with_capacity(opts.iters as usize);
        let mut runtimes_row = Vec::with_capacity(opts.iters as usize);
        for _ in 0..opts.iters {
            // generate batch of data for this iteration. used by both sorts
            let batch = gen.generate_batch(batch_size);
            let batch1 = batch.clone();
            let schema = schemaref.clone();
            // measure sorting batch without row encoding
            let now = Instant::now();
            sort_batch(batch1, schema, &sortexpr, false).unwrap();
            let elapsed = now.elapsed();
            runtimes_default.push(elapsed);

            let schema = schemaref.clone();
            let now = Instant::now();
            sort_batch(batch, schema, &sortexpr, true).unwrap();
            let elapsed = now.elapsed();
            runtimes_row.push(elapsed);
        }
        let default_total = runtimes_default.iter().sum::<std::time::Duration>();
        let dp = DataPoint {
            batch_size,
            runtime_micros: default_total.as_micros() / opts.iters,
            used_row_encoding: false,
        };
        data.push(dp);
        let rows_total = runtimes_row.iter().sum::<std::time::Duration>();
        let dp = DataPoint {
            batch_size,
            runtime_micros: rows_total.as_micros() / opts.iters,
            used_row_encoding: true,
        };
        data.push(dp);
    }
    plot(data, casename).unwrap();
}
