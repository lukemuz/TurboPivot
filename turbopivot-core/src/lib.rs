use polars::prelude::*;
use polars::lazy::dsl::Expr;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use thiserror::Error;
use polars_ops::pivot::{pivot, PivotAgg};

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Failed to read file: {0}")]
    ReadError(String),
    #[error("Failed to process data: {0}")]
    ProcessingError(String),
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AggregationType {
    Sum,
    Mean,
    Count,
    Min,
    Max,
    First,
    Last,
    Median,
    Std,
    Var,
}

impl AggregationType {
    pub fn snake(&self) -> &'static str {
        match self {
            AggregationType::Sum => "sum",
            AggregationType::Mean => "mean",
            AggregationType::Count => "count",
            AggregationType::Min => "min",
            AggregationType::Max => "max",
            AggregationType::First => "first",
            AggregationType::Last => "last",
            AggregationType::Median => "median",
            AggregationType::Std => "std",
            AggregationType::Var => "var",
        }
    }

    pub fn display(&self) -> &'static str {
        match self {
            AggregationType::Sum => "Sum",
            AggregationType::Mean => "Mean",
            AggregationType::Count => "Count",
            AggregationType::Min => "Min",
            AggregationType::Max => "Max",
            AggregationType::First => "First",
            AggregationType::Last => "Last",
            AggregationType::Median => "Median",
            AggregationType::Std => "Std",
            AggregationType::Var => "Var",
        }
    }

    pub fn apply(&self, e: Expr) -> Expr {
        match self {
            AggregationType::Sum => e.sum(),
            AggregationType::Mean => e.mean(),
            AggregationType::Count => e.count(),
            AggregationType::Min => e.min(),
            AggregationType::Max => e.max(),
            AggregationType::First => e.first(),
            AggregationType::Last => e.last(),
            AggregationType::Median => e.median(),
            AggregationType::Std => e.std(1),
            AggregationType::Var => e.var(1),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ValueWithAggregation {
    pub field: String,
    pub aggregation: AggregationType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SortConfig {
    pub column: String,
    pub order: SortOrder,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotRequest {
    pub data_path: String,
    pub rows: Vec<String>,
    pub columns: Vec<String>,
    pub values: Vec<ValueWithAggregation>,
    pub filters: Option<Vec<FilterCondition>>,
    pub sort: Option<SortConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FilterCondition {
    pub column: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    In,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnHeader {
    pub key: String,
    pub label: String,
    pub agg_label: String,
    pub column_values: Vec<String>,
    pub value_field: String,
    pub aggregation: AggregationType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotResult {
    pub data: Vec<HashMap<String, serde_json::Value>>,
    pub column_headers: Vec<ColumnHeader>,
    pub row_headers: Vec<String>,
    pub grand_total: Option<HashMap<String, serde_json::Value>>,
}

pub fn read_data(file_path: &str) -> Result<LazyFrame, DataError> {
    let path = Path::new(file_path);
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| DataError::UnsupportedFormat("File has no extension".to_string()))?;

    match extension.to_lowercase().as_str() {
        "csv" => LazyCsvReader::new(file_path)
            .with_has_header(true)
            .finish()
            .map_err(|e| DataError::ReadError(e.to_string())),
        "parquet" => LazyFrame::scan_parquet(file_path, Default::default())
            .map_err(|e| DataError::ReadError(e.to_string())),
        _ => Err(DataError::UnsupportedFormat(format!(
            "Unsupported file format: {}",
            extension
        ))),
    }
}

pub fn get_column_names(file_path: &str) -> Result<Vec<String>, DataError> {
    let mut lf = read_data(file_path)?;
    let schema = lf
        .schema()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    Ok(schema.iter_names().map(|name| name.to_string()).collect())
}

fn validate_pivot_request(request: &PivotRequest) -> Result<(), DataError> {
    if request.rows.is_empty() && request.columns.is_empty() {
        return Err(DataError::ProcessingError(
            "At least one row or column field must be specified".to_string(),
        ));
    }

    if request.values.is_empty() {
        return Err(DataError::ProcessingError(
            "At least one value field must be specified".to_string(),
        ));
    }

    for val in &request.values {
        if val.field.is_empty() {
            return Err(DataError::ProcessingError(
                "Value field name cannot be empty".to_string(),
            ));
        }
    }

    let mut seen = HashSet::new();
    for field in request.rows.iter().chain(request.columns.iter()) {
        if !seen.insert(field) {
            return Err(DataError::ProcessingError(format!(
                "Duplicate field in rows/columns: {}",
                field
            )));
        }
    }

    Ok(())
}

fn validate_columns_exist(
    request: &PivotRequest,
    available_columns: &[String],
) -> Result<(), DataError> {
    let available_set: HashSet<_> = available_columns.iter().collect();

    for field in &request.rows {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(format!(
                "Row field '{}' does not exist in the dataset. Available columns: {}",
                field,
                available_columns.join(", ")
            )));
        }
    }

    for field in &request.columns {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(format!(
                "Column field '{}' does not exist in the dataset. Available columns: {}",
                field,
                available_columns.join(", ")
            )));
        }
    }

    for val in &request.values {
        if !available_set.contains(&val.field) {
            return Err(DataError::ProcessingError(format!(
                "Value field '{}' does not exist in the dataset. Available columns: {}",
                val.field,
                available_columns.join(", ")
            )));
        }
    }

    if let Some(filters) = &request.filters {
        for filter in filters {
            if !available_set.contains(&filter.column) {
                return Err(DataError::ProcessingError(format!(
                    "Filter column '{}' does not exist in the dataset. Available columns: {}",
                    filter.column,
                    available_columns.join(", ")
                )));
            }
        }
    }

    Ok(())
}

fn apply_filter(lf: LazyFrame, filter: &FilterCondition) -> Result<LazyFrame, DataError> {
    let col_expr = col(&filter.column);

    let filter_expr = match &filter.operator {
        FilterOperator::Equal => match &filter.value {
            serde_json::Value::String(s) => col_expr.eq(lit(s.clone())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    col_expr.eq(lit(i))
                } else if let Some(f) = n.as_f64() {
                    col_expr.eq(lit(f))
                } else {
                    return Err(DataError::ProcessingError("Invalid number".to_string()));
                }
            }
            serde_json::Value::Bool(b) => col_expr.eq(lit(*b)),
            _ => return Err(DataError::ProcessingError("Unsupported value type".to_string())),
        },
        FilterOperator::NotEqual => match &filter.value {
            serde_json::Value::String(s) => col_expr.neq(lit(s.clone())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    col_expr.neq(lit(i))
                } else if let Some(f) = n.as_f64() {
                    col_expr.neq(lit(f))
                } else {
                    return Err(DataError::ProcessingError("Invalid number".to_string()));
                }
            }
            serde_json::Value::Bool(b) => col_expr.neq(lit(*b)),
            _ => return Err(DataError::ProcessingError("Unsupported value type".to_string())),
        },
        FilterOperator::GreaterThan => numeric_filter(col_expr, &filter.value, |c, v| c.gt(v))?,
        FilterOperator::LessThan => numeric_filter(col_expr, &filter.value, |c, v| c.lt(v))?,
        FilterOperator::GreaterThanOrEqual => {
            numeric_filter(col_expr, &filter.value, |c, v| c.gt_eq(v))?
        }
        FilterOperator::LessThanOrEqual => {
            numeric_filter(col_expr, &filter.value, |c, v| c.lt_eq(v))?
        }
        FilterOperator::In => {
            let arr = match &filter.value {
                serde_json::Value::Array(arr) => arr,
                _ => {
                    return Err(DataError::ProcessingError(
                        "Value must be an array".to_string(),
                    ))
                }
            };
            if arr.is_empty() {
                return Err(DataError::ProcessingError(
                    "Empty array in IN filter".to_string(),
                ));
            }

            let mut expr_list = Vec::new();
            for val in arr {
                match val {
                    serde_json::Value::String(s) => {
                        expr_list.push(col_expr.clone().eq(lit(s.clone())));
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            expr_list.push(col_expr.clone().eq(lit(i)));
                        } else if let Some(f) = n.as_f64() {
                            expr_list.push(col_expr.clone().eq(lit(f)));
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        expr_list.push(col_expr.clone().eq(lit(*b)));
                    }
                    _ => continue,
                }
            }

            if expr_list.is_empty() {
                return Err(DataError::ProcessingError(
                    "No valid values in IN filter".to_string(),
                ));
            }

            let mut final_expr = expr_list.remove(0);
            for expr in expr_list {
                final_expr = final_expr.or(expr);
            }
            final_expr
        }
    };

    Ok(lf.filter(filter_expr))
}

fn numeric_filter<F>(
    col_expr: Expr,
    value: &serde_json::Value,
    f: F,
) -> Result<Expr, DataError>
where
    F: FnOnce(Expr, Expr) -> Expr,
{
    let lit_expr = match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                lit(i)
            } else if let Some(fv) = n.as_f64() {
                lit(fv)
            } else {
                return Err(DataError::ProcessingError("Invalid number".to_string()));
            }
        }
        _ => {
            return Err(DataError::ProcessingError(
                "Value must be a number".to_string(),
            ))
        }
    };
    Ok(f(col_expr, lit_expr))
}

fn apply_sorting(df: DataFrame, sort_config: &SortConfig) -> Result<DataFrame, DataError> {
    if df.column(&sort_config.column).is_err() {
        return Err(DataError::ProcessingError(format!(
            "Sort column '{}' does not exist in the result",
            sort_config.column
        )));
    }

    let descending = matches!(sort_config.order, SortOrder::Descending);
    df.sort(
        [&sort_config.column],
        SortMultipleOptions::default().with_order_descending(descending),
    )
    .map_err(|e| DataError::ProcessingError(format!("Sort error: {}", e)))
}

fn anyvalue_to_json(v: AnyValue<'_>) -> serde_json::Value {
    match v {
        AnyValue::Null => serde_json::Value::Null,
        AnyValue::Boolean(b) => serde_json::Value::Bool(b),
        AnyValue::String(s) => serde_json::Value::String(s.to_string()),
        AnyValue::StringOwned(s) => serde_json::Value::String(s.to_string()),
        AnyValue::Int8(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::Int16(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::Int32(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::Int64(v) => int64_to_json(v),
        AnyValue::UInt8(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::UInt16(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::UInt32(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        AnyValue::UInt64(v) => {
            if v <= (1u64 << 53) {
                serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
            } else {
                serde_json::Value::String(v.to_string())
            }
        }
        AnyValue::Float32(v) => f64_to_json(v as f64),
        AnyValue::Float64(v) => f64_to_json(v),
        other => serde_json::Value::String(format!("{}", other)),
    }
}

fn int64_to_json(v: i64) -> serde_json::Value {
    let max = 1i64 << 53;
    if v > max || v < -max {
        serde_json::Value::String(v.to_string())
    } else {
        serde_json::Value::Number(serde_json::Number::from(v))
    }
}

fn f64_to_json(v: f64) -> serde_json::Value {
    if v.is_nan() || v.is_infinite() {
        serde_json::Value::Null
    } else if let Some(num) = serde_json::Number::from_f64(v) {
        serde_json::Value::Number(num)
    } else {
        serde_json::Value::Null
    }
}

fn anyvalue_to_display_string(v: &AnyValue<'_>) -> String {
    match v {
        AnyValue::Null => "".to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.to_string(),
        other => format!("{}", other),
    }
}

fn df_to_json_rows(df: &DataFrame) -> Result<Vec<HashMap<String, serde_json::Value>>, DataError> {
    let mut result = Vec::with_capacity(df.height());
    let cols = df.get_columns();
    for i in 0..df.height() {
        let mut row_map = HashMap::with_capacity(cols.len());
        for c in cols {
            let val = c
                .get(i)
                .map_err(|e| DataError::ProcessingError(e.to_string()))?;
            row_map.insert(c.name().to_string(), anyvalue_to_json(val));
        }
        result.push(row_map);
    }
    Ok(result)
}

fn build_value_agg_exprs(values: &[ValueWithAggregation]) -> Vec<Expr> {
    values
        .iter()
        .map(|v| {
            let alias = format!("{}_{}", v.aggregation.snake(), v.field);
            v.aggregation.apply(col(&v.field)).alias(&alias)
        })
        .collect()
}

pub fn generate_pivot(request: PivotRequest) -> Result<PivotResult, DataError> {
    validate_pivot_request(&request)?;

    let mut lf = read_data(&request.data_path)?;

    let schema = lf
        .schema()
        .map_err(|e| DataError::ProcessingError(format!("Failed to get schema: {}", e)))?;
    let available_columns: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    validate_columns_exist(&request, &available_columns)?;

    if let Some(filters) = &request.filters {
        for filter in filters {
            lf = apply_filter(lf, filter)?;
        }
    }

    if request.columns.is_empty() {
        generate_pivot_rows_only(lf, request)
    } else {
        generate_pivot_with_columns(lf, request)
    }
}

fn generate_pivot_rows_only(
    lf: LazyFrame,
    request: PivotRequest,
) -> Result<PivotResult, DataError> {
    let group_exprs: Vec<Expr> = request.rows.iter().map(|s| col(s)).collect();
    let agg_exprs = build_value_agg_exprs(&request.values);

    let mut result_df = lf
        .clone()
        .group_by(group_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;

    if let Some(sort_config) = &request.sort {
        result_df = apply_sorting(result_df, sort_config)?;
    }

    let column_headers: Vec<ColumnHeader> = request
        .values
        .iter()
        .map(|v| {
            let key = format!("{}_{}", v.aggregation.snake(), v.field);
            ColumnHeader {
                key: key.clone(),
                label: key,
                agg_label: format!("{} of {}", v.aggregation.display(), v.field),
                column_values: vec![],
                value_field: v.field.clone(),
                aggregation: v.aggregation.clone(),
            }
        })
        .collect();

    let data = df_to_json_rows(&result_df)?;
    let grand_total = compute_grand_total(lf, &request, &column_headers)?;

    Ok(PivotResult {
        data,
        column_headers,
        row_headers: request.rows,
        grand_total: Some(grand_total),
    })
}

fn generate_pivot_with_columns(
    lf: LazyFrame,
    request: PivotRequest,
) -> Result<PivotResult, DataError> {
    let mut group_cols: Vec<String> = request.rows.clone();
    group_cols.extend(request.columns.clone());

    let group_exprs: Vec<Expr> = group_cols.iter().map(|s| col(s)).collect();
    let agg_exprs = build_value_agg_exprs(&request.values);

    let agg_df = lf
        .clone()
        .group_by(group_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;

    let row_set: HashSet<&str> = request.rows.iter().map(|s| s.as_str()).collect();
    let mut merged_df: Option<DataFrame> = None;
    let mut column_headers: Vec<ColumnHeader> = Vec::new();

    for v in &request.values {
        let agg_col_name = format!("{}_{}", v.aggregation.snake(), v.field);

        // Polars 0.41 pivot signature: pivot(df, on, index, values, ...).
        // `on` becomes the new columns, `index` stays as rows.
        // Since each (rows, columns) tuple appears once in agg_df after group_by,
        // PivotAgg::First is exact for every aggregation type.
        let mut pivoted = pivot(
            &agg_df,
            request.columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            Some(request.rows.iter().map(|s| s.as_str()).collect::<Vec<&str>>()),
            Some(vec![agg_col_name.as_str()]),
            false,
            Some(PivotAgg::First),
            None,
        )
        .map_err(|e| DataError::ProcessingError(format!("Pivot error: {}", e)))?;

        let col_names: Vec<String> = pivoted
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Sort pivoted value columns by their column-value label for stable output.
        let mut value_col_labels: Vec<String> = col_names
            .iter()
            .filter(|n| !row_set.contains(n.as_str()))
            .cloned()
            .collect();
        value_col_labels.sort();

        for col_name in &value_col_labels {
            let key = format!("{}_{}_{}", v.aggregation.snake(), v.field, col_name);
            pivoted
                .rename(col_name, &key)
                .map_err(|e| DataError::ProcessingError(format!("Rename error: {}", e)))?;

            let column_values = if request.columns.len() == 1 {
                vec![col_name.clone()]
            } else {
                col_name.split('_').map(|s| s.to_string()).collect()
            };

            column_headers.push(ColumnHeader {
                key,
                label: col_name.clone(),
                agg_label: format!("{} of {}", v.aggregation.display(), v.field),
                column_values,
                value_field: v.field.clone(),
                aggregation: v.aggregation.clone(),
            });
        }

        merged_df = Some(match merged_df {
            None => pivoted,
            Some(prev) => {
                let join_cols: Vec<&str> = request.rows.iter().map(|s| s.as_str()).collect();
                prev.join(
                    &pivoted,
                    &join_cols,
                    &join_cols,
                    JoinArgs::new(JoinType::Left),
                )
                .map_err(|e| DataError::ProcessingError(format!("Join error: {}", e)))?
            }
        });
    }

    let mut merged_df = merged_df.expect("at least one value field guaranteed");

    if let Some(sort_config) = &request.sort {
        merged_df = apply_sorting(merged_df, sort_config)?;
    }

    let data = df_to_json_rows(&merged_df)?;
    let grand_total = compute_grand_total(lf, &request, &column_headers)?;

    Ok(PivotResult {
        data,
        column_headers,
        row_headers: request.rows,
        grand_total: Some(grand_total),
    })
}

/// Compute grand totals by re-aggregating the filtered data grouped only by the
/// `columns` dimension (or with no grouping when there are no column fields).
/// This respects each value field's aggregation type — the grand total of a mean
/// is the true mean across the underlying rows, not the mean of means.
fn compute_grand_total(
    lf: LazyFrame,
    request: &PivotRequest,
    column_headers: &[ColumnHeader],
) -> Result<HashMap<String, serde_json::Value>, DataError> {
    let agg_exprs = build_value_agg_exprs(&request.values);

    let totals_df = if request.columns.is_empty() {
        lf.select(agg_exprs)
            .collect()
            .map_err(|e| DataError::ProcessingError(format!("Grand total error: {}", e)))?
    } else {
        let group_exprs: Vec<Expr> = request.columns.iter().map(|s| col(s)).collect();
        lf.group_by(group_exprs)
            .agg(agg_exprs)
            .collect()
            .map_err(|e| DataError::ProcessingError(format!("Grand total error: {}", e)))?
    };

    let mut totals: HashMap<String, serde_json::Value> = HashMap::new();
    totals.insert(
        "__total_label__".to_string(),
        serde_json::Value::String("Grand Total".to_string()),
    );

    if request.columns.is_empty() {
        // One row: every column header maps directly to its alias in totals_df.
        for ch in column_headers {
            let alias = format!("{}_{}", ch.aggregation.snake(), ch.value_field);
            if let Ok(c) = totals_df.column(&alias) {
                if let Ok(v) = c.get(0) {
                    totals.insert(ch.key.clone(), anyvalue_to_json(v));
                }
            }
        }
    } else {
        // One row per unique column-field value tuple. Build a key matching the
        // pivoted column names by joining stringified column values with "_".
        for i in 0..totals_df.height() {
            let mut col_value_parts: Vec<String> = Vec::with_capacity(request.columns.len());
            for col_field in &request.columns {
                let c = totals_df
                    .column(col_field)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                let av = c
                    .get(i)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                col_value_parts.push(anyvalue_to_display_string(&av));
            }
            let col_key_part = col_value_parts.join("_");

            for v in &request.values {
                let alias = format!("{}_{}", v.aggregation.snake(), v.field);
                let key = format!("{}_{}_{}", v.aggregation.snake(), v.field, col_key_part);

                let c = match totals_df.column(&alias) {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                if let Ok(av) = c.get(i) {
                    totals.insert(key, anyvalue_to_json(av));
                }
            }
        }
    }

    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_csv(name: &str, body: &str) -> String {
        let path = format!("/tmp/{}", name);
        fs::write(&path, body).unwrap();
        path
    }

    fn standard_csv() -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        write_csv(
            &format!("tp_standard_{}_{}.csv", std::process::id(), n),
            "Country,Product,Sales,Quantity\nUSA,Widget,1000,10\nUSA,Gadget,2000,20\nCanada,Widget,1500,15\nCanada,Gadget,2500,25\n",
        )
    }

    fn cell(row: &HashMap<String, serde_json::Value>, key: &str) -> f64 {
        match row.get(key) {
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap(),
            other => panic!("expected number at {}, got {:?}", key, other),
        }
    }

    fn total(gt: &HashMap<String, serde_json::Value>, key: &str) -> f64 {
        match gt.get(key) {
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap(),
            other => panic!("expected number total at {}, got {:?}", key, other),
        }
    }

    fn find_row<'a>(
        data: &'a [HashMap<String, serde_json::Value>],
        col: &str,
        value: &str,
    ) -> &'a HashMap<String, serde_json::Value> {
        data.iter()
            .find(|r| match r.get(col) {
                Some(serde_json::Value::String(s)) => s == value,
                _ => false,
            })
            .unwrap_or_else(|| panic!("row {}={} not found", col, value))
    }

    #[test]
    fn test_read_csv_data() {
        let path = standard_csv();
        assert!(read_data(&path).is_ok());
    }

    #[test]
    fn test_get_column_names() {
        let path = standard_csv();
        let cols = get_column_names(&path).unwrap();
        assert_eq!(cols, vec!["Country", "Product", "Sales", "Quantity"]);
    }

    #[test]
    fn test_validate_pivot_request_no_rows_or_columns() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec![],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: None,
        };
        let err = validate_pivot_request(&request).unwrap_err();
        assert!(err.to_string().contains("At least one row or column"));
    }

    #[test]
    fn test_validate_pivot_request_no_values() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![],
            filters: None,
            sort: None,
        };
        let err = validate_pivot_request(&request).unwrap_err();
        assert!(err.to_string().contains("At least one value field"));
    }

    #[test]
    fn test_validate_pivot_request_duplicate_fields() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec!["Country".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: None,
        };
        let err = validate_pivot_request(&request).unwrap_err();
        assert!(err.to_string().contains("Duplicate field"));
    }

    #[test]
    fn test_validate_columns_exist_invalid_row() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["InvalidColumn".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: None,
        };
        let available = vec!["Country".to_string(), "Sales".to_string()];
        let err = validate_columns_exist(&request, &available).unwrap_err();
        assert!(err.to_string().contains("InvalidColumn"));
    }

    #[test]
    fn test_simple_pivot_rows_only() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: None,
        };
        let result = generate_pivot(request).unwrap();
        assert_eq!(result.data.len(), 2);
        assert_eq!(result.row_headers, vec!["Country"]);
        assert_eq!(result.column_headers.len(), 1);
        assert_eq!(result.column_headers[0].key, "sum_Sales");

        let usa = find_row(&result.data, "Country", "USA");
        assert_eq!(cell(usa, "sum_Sales"), 3000.0);
        let canada = find_row(&result.data, "Country", "Canada");
        assert_eq!(cell(canada, "sum_Sales"), 4000.0);

        let gt = result.grand_total.unwrap();
        assert_eq!(total(&gt, "sum_Sales"), 7000.0);
    }

    #[test]
    fn test_pivot_with_columns_cell_values() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: None,
        };
        let result = generate_pivot(request).unwrap();

        // 2 rows for Country
        assert_eq!(result.data.len(), 2);
        // 2 value columns: Widget and Gadget
        assert_eq!(result.column_headers.len(), 2);

        let headers_by_label: HashMap<&str, &ColumnHeader> =
            result.column_headers.iter().map(|h| (h.label.as_str(), h)).collect();
        let widget_key = &headers_by_label["Widget"].key;
        let gadget_key = &headers_by_label["Gadget"].key;
        assert_eq!(widget_key, "sum_Sales_Widget");
        assert_eq!(gadget_key, "sum_Sales_Gadget");

        let usa = find_row(&result.data, "Country", "USA");
        assert_eq!(cell(usa, widget_key), 1000.0);
        assert_eq!(cell(usa, gadget_key), 2000.0);

        let canada = find_row(&result.data, "Country", "Canada");
        assert_eq!(cell(canada, widget_key), 1500.0);
        assert_eq!(cell(canada, gadget_key), 2500.0);

        let gt = result.grand_total.unwrap();
        assert_eq!(total(&gt, widget_key), 2500.0);
        assert_eq!(total(&gt, gadget_key), 4500.0);
    }

    #[test]
    fn test_multi_value_pivot_cell_values() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![
                ValueWithAggregation {
                    field: "Sales".to_string(),
                    aggregation: AggregationType::Sum,
                },
                ValueWithAggregation {
                    field: "Quantity".to_string(),
                    aggregation: AggregationType::Mean,
                },
            ],
            filters: None,
            sort: None,
        };
        let result = generate_pivot(request).unwrap();

        assert_eq!(result.data.len(), 2);
        assert_eq!(result.column_headers.len(), 4);

        let by_key: HashMap<&str, &ColumnHeader> = result
            .column_headers
            .iter()
            .map(|h| (h.key.as_str(), h))
            .collect();
        assert!(by_key.contains_key("sum_Sales_Widget"));
        assert!(by_key.contains_key("sum_Sales_Gadget"));
        assert!(by_key.contains_key("mean_Quantity_Widget"));
        assert!(by_key.contains_key("mean_Quantity_Gadget"));

        let usa = find_row(&result.data, "Country", "USA");
        assert_eq!(cell(usa, "sum_Sales_Widget"), 1000.0);
        assert_eq!(cell(usa, "sum_Sales_Gadget"), 2000.0);
        assert_eq!(cell(usa, "mean_Quantity_Widget"), 10.0);
        assert_eq!(cell(usa, "mean_Quantity_Gadget"), 20.0);

        let canada = find_row(&result.data, "Country", "Canada");
        assert_eq!(cell(canada, "sum_Sales_Widget"), 1500.0);
        assert_eq!(cell(canada, "sum_Sales_Gadget"), 2500.0);
        assert_eq!(cell(canada, "mean_Quantity_Widget"), 15.0);
        assert_eq!(cell(canada, "mean_Quantity_Gadget"), 25.0);

        let gt = result.grand_total.unwrap();
        assert_eq!(total(&gt, "sum_Sales_Widget"), 2500.0);
        assert_eq!(total(&gt, "sum_Sales_Gadget"), 4500.0);
        // Mean of [10, 15] for Widget; [20, 25] for Gadget.
        assert_eq!(total(&gt, "mean_Quantity_Widget"), 12.5);
        assert_eq!(total(&gt, "mean_Quantity_Gadget"), 22.5);
    }

    #[test]
    fn test_grand_total_mean_is_not_mean_of_means() {
        // Group sizes unequal so mean-of-means differs from true mean.
        let path = write_csv(
            "tp_uneven.csv",
            "Country,Product,Sales\nUSA,Widget,10\nUSA,Widget,20\nUSA,Widget,30\nCanada,Widget,100\n",
        );
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Mean,
            }],
            filters: None,
            sort: None,
        };
        let result = generate_pivot(request).unwrap();

        let usa = find_row(&result.data, "Country", "USA");
        assert_eq!(cell(usa, "mean_Sales_Widget"), 20.0);
        let canada = find_row(&result.data, "Country", "Canada");
        assert_eq!(cell(canada, "mean_Sales_Widget"), 100.0);

        let gt = result.grand_total.unwrap();
        // True mean of [10, 20, 30, 100] = 40.0; mean-of-means would be 60.0.
        assert_eq!(total(&gt, "mean_Sales_Widget"), 40.0);
    }

    #[test]
    fn test_count_with_columns_returns_actual_count() {
        let path = write_csv(
            "tp_count.csv",
            "Country,Product,Sales\nUSA,Widget,1\nUSA,Widget,2\nUSA,Gadget,3\nCanada,Widget,4\nCanada,Gadget,5\nCanada,Gadget,6\n",
        );
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Count,
            }],
            filters: None,
            sort: None,
        };
        let result = generate_pivot(request).unwrap();

        let usa = find_row(&result.data, "Country", "USA");
        assert_eq!(cell(usa, "count_Sales_Widget"), 2.0);
        assert_eq!(cell(usa, "count_Sales_Gadget"), 1.0);
        let canada = find_row(&result.data, "Country", "Canada");
        assert_eq!(cell(canada, "count_Sales_Widget"), 1.0);
        assert_eq!(cell(canada, "count_Sales_Gadget"), 2.0);

        let gt = result.grand_total.unwrap();
        assert_eq!(total(&gt, "count_Sales_Widget"), 3.0);
        assert_eq!(total(&gt, "count_Sales_Gadget"), 3.0);
    }

    #[test]
    fn test_min_max_aggregations_with_columns() {
        let path = write_csv(
            "tp_minmax.csv",
            "Country,Product,Sales\nUSA,Widget,5\nUSA,Widget,15\nUSA,Gadget,10\nCanada,Widget,20\nCanada,Gadget,30\nCanada,Gadget,1\n",
        );

        let req = |agg: AggregationType| PivotRequest {
            data_path: path.clone(),
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: agg,
            }],
            filters: None,
            sort: None,
        };

        let min_result = generate_pivot(req(AggregationType::Min)).unwrap();
        let usa = find_row(&min_result.data, "Country", "USA");
        assert_eq!(cell(usa, "min_Sales_Widget"), 5.0);
        let canada = find_row(&min_result.data, "Country", "Canada");
        assert_eq!(cell(canada, "min_Sales_Gadget"), 1.0);
        let gt = min_result.grand_total.unwrap();
        assert_eq!(total(&gt, "min_Sales_Gadget"), 1.0);

        let max_result = generate_pivot(req(AggregationType::Max)).unwrap();
        let usa = find_row(&max_result.data, "Country", "USA");
        assert_eq!(cell(usa, "max_Sales_Widget"), 15.0);
        let canada = find_row(&max_result.data, "Country", "Canada");
        assert_eq!(cell(canada, "max_Sales_Gadget"), 30.0);
    }

    #[test]
    fn test_pivot_with_filter() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: Some(vec![FilterCondition {
                column: "Country".to_string(),
                operator: FilterOperator::Equal,
                value: serde_json::Value::String("USA".to_string()),
            }]),
            sort: None,
        };
        let result = generate_pivot(request).unwrap();
        assert_eq!(result.data.len(), 1);
        let row = &result.data[0];
        assert_eq!(row.get("Country"), Some(&serde_json::Value::String("USA".to_string())));
        assert_eq!(cell(row, "sum_Sales"), 3000.0);
    }

    #[test]
    fn test_sort_descending_on_value_column() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: Some(SortConfig {
                column: "sum_Sales".to_string(),
                order: SortOrder::Descending,
            }),
        };
        let result = generate_pivot(request).unwrap();
        assert_eq!(result.data.len(), 2);
        // Canada (4000) > USA (3000)
        assert_eq!(
            result.data[0].get("Country"),
            Some(&serde_json::Value::String("Canada".to_string()))
        );
        assert_eq!(
            result.data[1].get("Country"),
            Some(&serde_json::Value::String("USA".to_string()))
        );
    }

    #[test]
    fn test_sort_on_pivoted_value_column() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
            sort: Some(SortConfig {
                column: "sum_Sales_Widget".to_string(),
                order: SortOrder::Descending,
            }),
        };
        let result = generate_pivot(request).unwrap();
        // Canada Widget 1500 > USA Widget 1000
        assert_eq!(
            result.data[0].get("Country"),
            Some(&serde_json::Value::String("Canada".to_string()))
        );
    }

    #[test]
    fn test_in_filter() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: Some(vec![FilterCondition {
                column: "Country".to_string(),
                operator: FilterOperator::In,
                value: serde_json::json!(["USA", "Canada"]),
            }]),
            sort: None,
        };
        let result = generate_pivot(request).unwrap();
        assert_eq!(result.data.len(), 2);
    }

    #[test]
    fn test_greater_than_filter() {
        let path = standard_csv();
        let request = PivotRequest {
            data_path: path,
            rows: vec!["Product".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: Some(vec![FilterCondition {
                column: "Sales".to_string(),
                operator: FilterOperator::GreaterThan,
                value: serde_json::Value::Number(serde_json::Number::from(1200)),
            }]),
            sort: None,
        };
        let result = generate_pivot(request).unwrap();
        // Surviving rows: USA Gadget 2000, Canada Widget 1500, Canada Gadget 2500
        let widget = find_row(&result.data, "Product", "Widget");
        assert_eq!(cell(widget, "sum_Sales"), 1500.0);
        let gadget = find_row(&result.data, "Product", "Gadget");
        assert_eq!(cell(gadget, "sum_Sales"), 4500.0);
    }
}
