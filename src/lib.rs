use datafusion::{
    arrow::{
        array::{ArrayRef, UInt32Array},
        compute::{lexsort_to_indices, take, SortColumn, TakeOptions},
        datatypes::SchemaRef,
        error::ArrowError,
        record_batch::RecordBatch,
        row::{Row, RowConverter, SortField},
    },
    error::Result,
    physical_expr::PhysicalSortExpr,
};

fn sort_batch(
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
