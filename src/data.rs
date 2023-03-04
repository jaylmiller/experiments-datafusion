//! Data generation code lifted from datafusion sort benchmark

use std::sync::Arc;

use datafusion::arrow::{
    array::{DictionaryArray, Float64Array, Int64Array, StringArray, UInt64Array},
    compute::{self, TakeOptions},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Case {
    Utf8Tuple,
    MixedTuple,
    DictionaryTuple,
    MixedDictionaryTuple,
}
impl Case {
    pub fn generate_batch(&self, size: usize) -> RecordBatch {
        match self {
            Case::Utf8Tuple => utf8_tuple_streams(size),
            Case::MixedTuple => mixed_tuple_streams(size),
            Case::DictionaryTuple => dictionary_tuple_streams(size),
            Case::MixedDictionaryTuple => mixed_dictionary_tuple_streams(size),
        }
    }
}

/// Create streams of random low cardinality utf8 values
pub fn utf8_low_cardinality_streams(size: usize) -> RecordBatch {
    let array: StringArray = DataGenerator::new(size)
        .utf8_low_cardinality_values()
        .into_iter()
        .collect();

    let batch = RecordBatch::try_from_iter(vec![("utf_low", Arc::new(array) as _)]).unwrap();

    batch
}

/// Create streams of high  cardinality (~ no duplicates) utf8 values
pub fn utf8_high_cardinality_streams(size: usize) -> RecordBatch {
    let array: StringArray = DataGenerator::new(size)
        .utf8_high_cardinality_values()
        .into_iter()
        .collect();

    let batch = RecordBatch::try_from_iter(vec![("utf_high", Arc::new(array) as _)]).unwrap();

    batch
}

/// Create a batch of (utf8_low, utf8_low, utf8_high)
pub fn utf8_tuple_streams(size: usize) -> RecordBatch {
    let mut gen = DataGenerator::new(size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.utf8_high_cardinality_values().into_iter())
        .collect();

    tuples.sort_unstable();

    let (tuples, utf8_high): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (utf8_low1, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let utf8_high: StringArray = utf8_high.into_iter().collect();
    let utf8_low1: StringArray = utf8_low1.into_iter().collect();
    let utf8_low2: StringArray = utf8_low2.into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("utf_low1", Arc::new(utf8_low1) as _),
        ("utf_low2", Arc::new(utf8_low2) as _),
        ("utf_high", Arc::new(utf8_high) as _),
    ])
    .unwrap();

    batch
}

/// Create a batch of (f64, utf8_low, utf8_low, i64)
pub fn mixed_tuple_streams(size: usize) -> RecordBatch {
    let mut gen = DataGenerator::new(size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = gen
        .i64_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.i64_values().into_iter())
        .collect();
    tuples.sort_unstable();

    let (tuples, i64_values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (tuples, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (f64_values, utf8_low1): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let f64_values: Float64Array = f64_values.into_iter().map(|v| v as f64).collect();
    let utf8_low1: StringArray = utf8_low1.into_iter().collect();
    let utf8_low2: StringArray = utf8_low2.into_iter().collect();
    let i64_values: Int64Array = i64_values.into_iter().collect();

    RecordBatch::try_from_iter(vec![
        ("f64", Arc::new(f64_values) as _),
        ("utf_low1", Arc::new(utf8_low1) as _),
        ("utf_low2", Arc::new(utf8_low2) as _),
        ("i64", Arc::new(i64_values) as _),
    ])
    .unwrap()
}

/// Create a batch of (utf8_dict)
pub fn dictionary_streams(size: usize) -> RecordBatch {
    let mut gen = DataGenerator::new(size);
    let values = gen.utf8_low_cardinality_values();
    let dictionary: DictionaryArray<Int32Type> = values.iter().map(Option::as_deref).collect();

    let batch = RecordBatch::try_from_iter(vec![("dict", Arc::new(dictionary) as _)]).unwrap();
    batch
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict)
pub fn dictionary_tuple_streams(size: usize) -> RecordBatch {
    let mut gen = DataGenerator::new(size);
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.utf8_low_cardinality_values())
        .collect();
    tuples.sort_unstable();

    let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
    let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
    let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();

    RecordBatch::try_from_iter(vec![
        ("a", Arc::new(a) as _),
        ("b", Arc::new(b) as _),
        ("c", Arc::new(c) as _),
    ])
    .unwrap()
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict, i64)
pub fn mixed_dictionary_tuple_streams(size: usize) -> RecordBatch {
    let mut gen = DataGenerator::new(size);
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.i64_values())
        .collect();
    tuples.sort_unstable();

    let (tuples, d): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
    let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
    let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();
    let d: Int64Array = d.into_iter().collect();

    RecordBatch::try_from_iter(vec![
        ("a", Arc::new(a) as _),
        ("b", Arc::new(b) as _),
        ("c", Arc::new(c) as _),
        ("d", Arc::new(d) as _),
    ])
    .unwrap()
}

/// Encapsulates creating data for this test
struct DataGenerator {
    rng: StdRng,
    row_count: usize,
}

impl DataGenerator {
    fn new(row_count: usize) -> Self {
        Self {
            rng: StdRng::seed_from_u64(42),
            row_count,
        }
    }

    /// Create an array of i64 unsorted values (where approximately 1/3 values is repeated)
    fn i64_values(&mut self) -> Vec<i64> {
        (0..self.row_count)
            .map(|_| self.rng.gen_range(0..self.row_count as i64))
            .collect()
    }

    /// Create an array of f64 sorted values (with same distribution of `i64_values`)
    fn f64_values(&mut self) -> Vec<f64> {
        self.i64_values().into_iter().map(|v| v as f64).collect()
    }

    /// array of low cardinality (100 distinct) values
    fn utf8_low_cardinality_values(&mut self) -> Vec<Option<Arc<str>>> {
        let strings = (0..100)
            .map(|s| format!("value{s}").into())
            .collect::<Vec<_>>();

        // pick from the 100 strings randomly
        (0..self.row_count)
            .map(|_| {
                let idx = self.rng.gen_range(0..strings.len());
                let s = Arc::clone(&strings[idx]);
                Some(s)
            })
            .collect::<Vec<_>>()
    }

    /// Create values of high  cardinality (~ no duplicates) utf8 values
    fn utf8_high_cardinality_values(&mut self) -> Vec<Option<String>> {
        // make random strings
        (0..self.row_count)
            .map(|_| Some(self.random_string()))
            .collect::<Vec<_>>()
    }

    fn random_string(&mut self) -> String {
        let rng = &mut self.rng;
        rng.sample_iter(rand::distributions::Alphanumeric)
            .filter(|c| c.is_ascii_alphabetic())
            .take(20)
            .map(char::from)
            .collect::<String>()
    }
}

/// Splits the `input_batch` randomly into `NUM_STREAMS` approximately evenly sorted streams
fn split_batch(input_batch: RecordBatch, num_streams: u64) -> Vec<Vec<RecordBatch>> {
    // figure out which inputs go where
    let mut rng = StdRng::seed_from_u64(1337);

    // randomly assign rows to streams
    let stream_assignments = (0..input_batch.num_rows())
        .map(|_| rng.gen_range(0..num_streams))
        .collect();

    // split the inputs into streams
    (0..num_streams)
        .map(|stream| {
            // make a "stream" of 1 record batch
            vec![take_columns(&input_batch, &stream_assignments, stream)]
        })
        .collect::<Vec<_>>()
}

/// returns a record batch that contains all there values where
/// stream_assignment[i] = stream (aka this is the equivalent of
/// calling take(indicies) where indicies[i] == stream_index)
fn take_columns(
    input_batch: &RecordBatch,
    stream_assignments: &UInt64Array,
    stream: u64,
) -> RecordBatch {
    // find just the indicies needed from record batches to extract
    let stream_indices: UInt64Array = stream_assignments
        .iter()
        .enumerate()
        .filter_map(|(idx, stream_idx)| {
            if stream_idx.unwrap() == stream {
                Some(idx as u64)
            } else {
                None
            }
        })
        .collect();

    let options = Some(TakeOptions { check_bounds: true });

    // now, get the columns from each array
    let new_columns = input_batch
        .columns()
        .iter()
        .map(|array| compute::take(array, &stream_indices, options.clone()).unwrap())
        .collect();

    RecordBatch::try_new(input_batch.schema(), new_columns).unwrap()
}
