use std::sync::Arc;

use clap::Arg;
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::RowConverter;
use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Normal};
lazy_static! {
    static ref DIST: Normal<f64> = Normal::new(0.0, 10_000.0).unwrap();
}

// #[derive(Debug, Arg)]
// struct Opt {}

#[tokio::main]
async fn main() {
    // let sizes =
}

struct DataGen {
    batch_count: usize,
    batch_size: usize,
}
impl DataGen {
    fn with_total_rows(total_rows: usize, batch_count: usize) -> Self {
        let batch_size = total_rows / batch_count;
        Self::new(batch_count, batch_size)
    }

    fn new(batch_count: usize, batch_size: usize) -> Self {
        Self {
            batch_count,
            batch_size,
        }
    }
    fn get_data(&self) -> Vec<RecordBatch> {
        (0..self.batch_count)
            .map(|_| self.generate_batch())
            .collect()
    }
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
