#![allow(dead_code)]
use datafusion::{
    arrow::{
        array::{ArrayRef, UInt32Array},
        compute::{lexsort_to_indices, take, SortColumn, SortOptions, TakeOptions},
        datatypes::{Schema, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
        row::{Row, RowConverter, SortField},
    },
    error::Result,
    physical_expr::PhysicalSortExpr,
    physical_plan::expressions::col,
};
use lazy_static::lazy_static;
use plotters::prelude::*;
use std::path::PathBuf;

mod data;
pub use data::*;

// Sort batch code lifted from SortExec row format PR (https://github.com/apache/arrow-datafusion/pull/5292)
pub fn sort_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    expr: &[PhysicalSortExpr],
    use_row_encoding: bool,
) -> Result<RecordBatch> {
    let sort_columns = expr
        .iter()
        .map(|e| e.evaluate_to_sort_column(&batch))
        .collect::<Result<Vec<SortColumn>>>()?;
    let indices = match use_row_encoding {
        false => lexsort_to_indices(&sort_columns, None)?,
        _ => {
            let sort_fields = sort_columns
                .iter()
                .map(|c| {
                    let datatype = c.values.data_type().to_owned();
                    SortField::new_with_options(datatype, c.options.unwrap_or_default())
                })
                .collect::<Vec<_>>();
            let arrays: Vec<ArrayRef> = sort_columns.iter().map(|c| c.values.clone()).collect();
            let mut row_converter = RowConverter::new(sort_fields)?;
            let rows = row_converter.convert_columns(&arrays)?;

            let mut to_sort: Vec<(usize, Row)> = rows.into_iter().enumerate().collect();
            to_sort.sort_unstable_by(|(_, row_a), (_, row_b)| row_a.cmp(row_b));
            let sorted_indices = to_sort.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();

            UInt32Array::from_iter(sorted_indices.iter().map(|i| *i as u32))
        }
    };

    // reorder all rows based on sorted indices
    let sorted_batch = RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .map(|column| {
                take(
                    column.as_ref(),
                    &indices,
                    // disable bound check overhead since indices are already generated from
                    // the same record batch
                    Some(TakeOptions {
                        check_bounds: false,
                    }),
                )
            })
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?,
    )?;
    Ok(sorted_batch)
}

/// Make sort exprs for each column in `schema`
pub fn make_sort_exprs(schema: &Schema) -> Vec<PhysicalSortExpr> {
    schema
        .fields()
        .iter()
        .map(|f| PhysicalSortExpr {
            expr: col(f.name(), schema).unwrap(),
            options: SortOptions::default(),
        })
        .collect()
}
#[derive(Debug, Clone, Copy)]
pub struct DataPoint {
    pub batch_size: usize,
    pub runtime_micros: u128,
    pub used_row_encoding: bool,
}
lazy_static! {
    static ref OUTDIR: PathBuf = PathBuf::new().join(env!("CARGO_MANIFEST_DIR")).join("img");
}
pub fn plot(data: Vec<DataPoint>, case_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !OUTDIR.exists() {
        std::fs::create_dir(OUTDIR.as_path()).unwrap();
    }
    let outfile = PathBuf::new()
        .join(OUTDIR.as_path())
        .join(format!("{case_name}.png"));
    let root_area = BitMapBackend::new(outfile.as_path(), (1024, 500)).into_drawing_area();
    root_area.fill(&WHITE)?;

    let (upper, _lower) = root_area.split_vertically(500);
    let xvals = data
        .iter()
        .step_by(2)
        .map(|d| d.batch_size as f64)
        .collect::<Vec<_>>();
    let xmin = *xvals.iter().next().unwrap();
    let xmax = *xvals.iter().last().unwrap();
    let yvals = data
        .iter()
        .map(|v| v.runtime_micros as f64)
        .collect::<Vec<_>>();
    let ymin = yvals.clone().into_iter().reduce(f64::min).unwrap();
    let ymax = yvals.into_iter().reduce(f64::max).unwrap();
    let mut cc = ChartBuilder::on(&upper)
        .margin(5)
        .set_all_label_area_size(50)
        .caption(case_name, ("sans-serif", 40))
        .build_cartesian_2d(xmin..xmax, ymin..ymax)?;

    cc.configure_mesh()
        .x_labels(10)
        .y_labels(3)
        .disable_mesh()
        .x_desc("batch size (row count)")
        .y_desc("avg runtime (microsecs)")
        .x_label_formatter(&|v| format!("{:.0}", v))
        .y_label_formatter(&|v| format!("{:.0}", v))
        .draw()?;

    cc.draw_series(LineSeries::new(
        data.iter()
            .filter(|v| !v.used_row_encoding)
            .map(|v| (v.batch_size as f64, v.runtime_micros as f64)),
        &RED,
    ))?
    .label("DynComparator sort")
    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

    cc.draw_series(LineSeries::new(
        data.iter()
            .filter(|v| v.used_row_encoding)
            .map(|v| (v.batch_size as f64, v.runtime_micros as f64)),
        &BLUE,
    ))?
    .label("Rows format sort")
    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE));

    cc.configure_series_labels().border_style(&BLACK).draw()?;

    // To avoid the IO failure being ignored silently, we manually call the present function
    root_area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
    Ok(())
}
