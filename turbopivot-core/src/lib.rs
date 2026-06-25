use polars::prelude::*;
use polars::lazy::dsl::Expr;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use polars_ops::pivot::{pivot, PivotAgg};

mod data_source;
mod export;

pub use data_source::{ColumnInfo, ColumnDtype, DataSource, load_data_source, describe_columns};
pub use export::{pivot_result_to_csv, pivot_result_to_tsv};

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Failed to read file: {0}")]
    ReadError(String),
    #[error("Failed to process data: {0}")]
    ProcessingError(String),
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DateGranularity {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    YearMonth,
    YearQuarter,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DateGroup {
    /// Existing date / datetime column the virtual field is derived from.
    pub source_field: String,
    pub granularity: DateGranularity,
    /// Name of the new virtual column that user can place in rows/columns.
    pub alias: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotRequest {
    /// Legacy field — when `source` is None this is treated as a file path.
    #[serde(default)]
    pub data_path: String,
    #[serde(default)]
    pub source: Option<DataSource>,
    pub rows: Vec<String>,
    pub columns: Vec<String>,
    pub values: Vec<ValueWithAggregation>,
    pub filters: Option<Vec<FilterCondition>>,
    pub sort: Option<SortConfig>,
    #[serde(default)]
    pub date_groups: Vec<DateGroup>,
    #[serde(default)]
    pub subtotals: bool,
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
    NotIn,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
    Between,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RowKind {
    Data,
    Subtotal,
    GrandTotal,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotResult {
    pub data: Vec<HashMap<String, serde_json::Value>>,
    /// Parallel to `data`: kind + subtotal level (None for Data and GrandTotal).
    pub row_meta: Vec<RowMetaEntry>,
    pub column_headers: Vec<ColumnHeader>,
    pub row_headers: Vec<String>,
    pub grand_total: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RowMetaEntry {
    pub kind: RowKind,
    /// For Subtotal rows, which level of the row hierarchy this subtotal aggregates.
    /// 0 means subtotal across the outermost row field, etc.
    pub level: Option<usize>,
}

// ----- Legacy file-only entry points kept for back-compat -----

pub fn read_data(file_path: &str) -> Result<LazyFrame, DataError> {
    data_source::read_file_path(file_path)
}

pub fn get_column_names(file_path: &str) -> Result<Vec<String>, DataError> {
    let info = describe_columns(&DataSource::File {
        path: file_path.to_string(),
    })?;
    Ok(info.into_iter().map(|c| c.name).collect())
}

// ----- Pivot machinery -----

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

    for dg in &request.date_groups {
        if dg.alias.is_empty() {
            return Err(DataError::ProcessingError(
                "Date group alias cannot be empty".to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_columns_exist(
    request: &PivotRequest,
    available_columns: &[String],
) -> Result<(), DataError> {
    let mut available_set: HashSet<String> = available_columns.iter().cloned().collect();
    // Date-group aliases are virtual columns that will be created before the pivot.
    for dg in &request.date_groups {
        available_set.insert(dg.alias.clone());
    }

    for field in &request.rows {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(format!(
                "Row field '{}' does not exist in the dataset",
                field
            )));
        }
    }

    for field in &request.columns {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(format!(
                "Column field '{}' does not exist in the dataset",
                field
            )));
        }
    }

    for val in &request.values {
        if !available_set.contains(&val.field) {
            return Err(DataError::ProcessingError(format!(
                "Value field '{}' does not exist in the dataset",
                val.field
            )));
        }
    }

    if let Some(filters) = &request.filters {
        for filter in filters {
            if !available_set.contains(&filter.column) {
                return Err(DataError::ProcessingError(format!(
                    "Filter column '{}' does not exist in the dataset",
                    filter.column
                )));
            }
        }
    }

    for dg in &request.date_groups {
        if !available_columns.iter().any(|c| c == &dg.source_field) {
            return Err(DataError::ProcessingError(format!(
                "Date group source '{}' does not exist in the dataset",
                dg.source_field
            )));
        }
    }

    Ok(())
}

fn apply_filter(lf: LazyFrame, filter: &FilterCondition) -> Result<LazyFrame, DataError> {
    let col_expr = col(&filter.column);

    let filter_expr = match &filter.operator {
        FilterOperator::Equal => match &filter.value {
            serde_json::Value::String(s) => col_expr.eq(lit(s.clone())),
            serde_json::Value::Number(n) => numeric_lit_eq(col_expr, n)?,
            serde_json::Value::Bool(b) => col_expr.eq(lit(*b)),
            _ => return Err(DataError::ProcessingError("Unsupported value type".to_string())),
        },
        FilterOperator::NotEqual => match &filter.value {
            serde_json::Value::String(s) => col_expr.neq(lit(s.clone())),
            serde_json::Value::Number(n) => numeric_lit_neq(col_expr, n)?,
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
        FilterOperator::In => disjunctive_match(col_expr, &filter.value, true)?,
        FilterOperator::NotIn => disjunctive_match(col_expr, &filter.value, false)?,
        FilterOperator::Contains => string_match(col_expr, &filter.value, StringMatch::Contains)?,
        FilterOperator::StartsWith => {
            string_match(col_expr, &filter.value, StringMatch::StartsWith)?
        }
        FilterOperator::EndsWith => string_match(col_expr, &filter.value, StringMatch::EndsWith)?,
        FilterOperator::IsNull => col_expr.is_null(),
        FilterOperator::IsNotNull => col_expr.is_not_null(),
        FilterOperator::Between => {
            let arr = match &filter.value {
                serde_json::Value::Array(arr) if arr.len() == 2 => arr,
                _ => {
                    return Err(DataError::ProcessingError(
                        "Between requires an array [low, high]".to_string(),
                    ))
                }
            };
            let low = json_to_numeric_lit(&arr[0])?;
            let high = json_to_numeric_lit(&arr[1])?;
            col_expr.clone().gt_eq(low).and(col_expr.lt_eq(high))
        }
    };

    Ok(lf.filter(filter_expr))
}

enum StringMatch {
    Contains,
    StartsWith,
    EndsWith,
}

fn string_match(
    col_expr: Expr,
    value: &serde_json::Value,
    kind: StringMatch,
) -> Result<Expr, DataError> {
    let s = match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Ok(match kind {
        StringMatch::Contains => col_expr.str().contains_literal(lit(s)),
        StringMatch::StartsWith => col_expr.str().starts_with(lit(s)),
        StringMatch::EndsWith => col_expr.str().ends_with(lit(s)),
    })
}

fn disjunctive_match(
    col_expr: Expr,
    value: &serde_json::Value,
    include: bool,
) -> Result<Expr, DataError> {
    let arr = match value {
        serde_json::Value::Array(arr) => arr,
        _ => {
            return Err(DataError::ProcessingError(
                "Value must be an array".to_string(),
            ))
        }
    };
    if arr.is_empty() {
        return Err(DataError::ProcessingError(
            "Empty array in In / NotIn filter".to_string(),
        ));
    }

    let mut expr_list: Vec<Expr> = Vec::new();
    for v in arr {
        match v {
            serde_json::Value::String(s) => expr_list.push(col_expr.clone().eq(lit(s.clone()))),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    expr_list.push(col_expr.clone().eq(lit(i)));
                } else if let Some(f) = n.as_f64() {
                    expr_list.push(col_expr.clone().eq(lit(f)));
                }
            }
            serde_json::Value::Bool(b) => expr_list.push(col_expr.clone().eq(lit(*b))),
            _ => continue,
        }
    }
    if expr_list.is_empty() {
        return Err(DataError::ProcessingError(
            "No valid values in In / NotIn filter".to_string(),
        ));
    }
    let mut final_expr = expr_list.remove(0);
    for e in expr_list {
        final_expr = final_expr.or(e);
    }
    if include {
        Ok(final_expr)
    } else {
        Ok(final_expr.not())
    }
}

fn json_to_numeric_lit(value: &serde_json::Value) -> Result<Expr, DataError> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(lit(i))
            } else if let Some(f) = n.as_f64() {
                Ok(lit(f))
            } else {
                Err(DataError::ProcessingError("Invalid number".to_string()))
            }
        }
        _ => Err(DataError::ProcessingError(
            "Value must be a number".to_string(),
        )),
    }
}

fn numeric_lit_eq(col_expr: Expr, n: &serde_json::Number) -> Result<Expr, DataError> {
    if let Some(i) = n.as_i64() {
        Ok(col_expr.eq(lit(i)))
    } else if let Some(f) = n.as_f64() {
        Ok(col_expr.eq(lit(f)))
    } else {
        Err(DataError::ProcessingError("Invalid number".to_string()))
    }
}

fn numeric_lit_neq(col_expr: Expr, n: &serde_json::Number) -> Result<Expr, DataError> {
    if let Some(i) = n.as_i64() {
        Ok(col_expr.neq(lit(i)))
    } else if let Some(f) = n.as_f64() {
        Ok(col_expr.neq(lit(f)))
    } else {
        Err(DataError::ProcessingError("Invalid number".to_string()))
    }
}

fn numeric_filter<F>(col_expr: Expr, value: &serde_json::Value, f: F) -> Result<Expr, DataError>
where
    F: FnOnce(Expr, Expr) -> Expr,
{
    let lit_expr = json_to_numeric_lit(value)?;
    Ok(f(col_expr, lit_expr))
}

fn apply_date_groups(mut lf: LazyFrame, groups: &[DateGroup]) -> LazyFrame {
    for g in groups {
        let src = col(&g.source_field);
        let expr = match g.granularity {
            DateGranularity::Year => src.dt().year().cast(DataType::String),
            DateGranularity::Quarter => concat_str(
                [
                    src.clone().dt().year().cast(DataType::String),
                    lit("-Q"),
                    src.dt().quarter().cast(DataType::String),
                ],
                "",
                false,
            ),
            DateGranularity::Month => src.dt().month().cast(DataType::String),
            DateGranularity::Week => src.dt().week().cast(DataType::String),
            DateGranularity::Day => src.dt().day().cast(DataType::String),
            DateGranularity::YearMonth => concat_str(
                [
                    src.clone().dt().year().cast(DataType::String),
                    lit("-"),
                    src.dt().strftime("%m"),
                ],
                "",
                false,
            ),
            DateGranularity::YearQuarter => concat_str(
                [
                    src.clone().dt().year().cast(DataType::String),
                    lit("-Q"),
                    src.dt().quarter().cast(DataType::String),
                ],
                "",
                false,
            ),
        };
        lf = lf.with_column(expr.alias(&g.alias));
    }
    lf
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

fn resolve_source(request: &PivotRequest) -> DataSource {
    if let Some(s) = &request.source {
        return s.clone();
    }
    DataSource::File {
        path: request.data_path.clone(),
    }
}

pub fn generate_pivot(request: PivotRequest) -> Result<PivotResult, DataError> {
    validate_pivot_request(&request)?;

    let source = resolve_source(&request);
    let mut lf = load_data_source(&source)?;

    let schema = lf
        .schema()
        .map_err(|e| DataError::ProcessingError(format!("Failed to get schema: {}", e)))?;
    let available_columns: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    validate_columns_exist(&request, &available_columns)?;

    lf = apply_date_groups(lf, &request.date_groups);

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
        .agg(agg_exprs.clone())
        .collect()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;

    if let Some(sort_config) = &request.sort {
        result_df = apply_sorting(result_df, sort_config)?;
    } else if request.rows.len() > 1 {
        // Stable order so subtotals can interleave correctly.
        let sort_cols: Vec<&str> = request.rows.iter().map(|s| s.as_str()).collect();
        result_df = result_df
            .sort(sort_cols, SortMultipleOptions::default())
            .map_err(|e| DataError::ProcessingError(format!("Sort error: {}", e)))?;
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

    let mut data: Vec<HashMap<String, serde_json::Value>> = df_to_json_rows(&result_df)?;
    let mut row_meta: Vec<RowMetaEntry> = data
        .iter()
        .map(|_| RowMetaEntry {
            kind: RowKind::Data,
            level: None,
        })
        .collect();

    if request.subtotals && request.rows.len() > 1 {
        let (subtotal_data, subtotal_meta) =
            compute_subtotals_rows_only(lf.clone(), &request, &column_headers)?;
        (data, row_meta) = interleave_subtotals(data, row_meta, subtotal_data, subtotal_meta, &request.rows);
    }

    let grand_total = compute_grand_total(lf, &request, &column_headers)?;

    Ok(PivotResult {
        data,
        row_meta,
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
    } else if request.rows.len() > 1 {
        let sort_cols: Vec<&str> = request.rows.iter().map(|s| s.as_str()).collect();
        merged_df = merged_df
            .sort(sort_cols, SortMultipleOptions::default())
            .map_err(|e| DataError::ProcessingError(format!("Sort error: {}", e)))?;
    }

    let mut data = df_to_json_rows(&merged_df)?;
    let mut row_meta: Vec<RowMetaEntry> = data
        .iter()
        .map(|_| RowMetaEntry {
            kind: RowKind::Data,
            level: None,
        })
        .collect();

    if request.subtotals && request.rows.len() > 1 {
        let (subtotal_data, subtotal_meta) =
            compute_subtotals_with_columns(lf.clone(), &request, &column_headers)?;
        (data, row_meta) = interleave_subtotals(data, row_meta, subtotal_data, subtotal_meta, &request.rows);
    }

    let grand_total = compute_grand_total(lf, &request, &column_headers)?;

    Ok(PivotResult {
        data,
        row_meta,
        column_headers,
        row_headers: request.rows,
        grand_total: Some(grand_total),
    })
}

/// Subtotals for the rows-only case: for each prefix level L in 0..rows.len()-1,
/// re-aggregate by rows[0..=L]. Returns (rows, metas) parallel arrays.
fn compute_subtotals_rows_only(
    lf: LazyFrame,
    request: &PivotRequest,
    column_headers: &[ColumnHeader],
) -> Result<(Vec<HashMap<String, serde_json::Value>>, Vec<RowMetaEntry>), DataError> {
    let mut all_rows = Vec::new();
    let mut all_meta = Vec::new();

    for level in 0..request.rows.len().saturating_sub(1) {
        let group_fields: Vec<Expr> = request.rows[0..=level].iter().map(|s| col(s)).collect();
        let agg_exprs = build_value_agg_exprs(&request.values);

        let df = lf
            .clone()
            .group_by(group_fields)
            .agg(agg_exprs)
            .collect()
            .map_err(|e| DataError::ProcessingError(format!("Subtotal error: {}", e)))?;

        for i in 0..df.height() {
            let mut row = HashMap::new();
            for (idx, rf) in request.rows.iter().enumerate() {
                if idx <= level {
                    let c = df
                        .column(rf)
                        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                    let av = c
                        .get(i)
                        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                    row.insert(rf.clone(), anyvalue_to_json(av));
                } else {
                    row.insert(rf.clone(), serde_json::Value::Null);
                }
            }
            for ch in column_headers {
                let alias = format!("{}_{}", ch.aggregation.snake(), ch.value_field);
                if let Ok(c) = df.column(&alias) {
                    if let Ok(av) = c.get(i) {
                        row.insert(ch.key.clone(), anyvalue_to_json(av));
                    }
                }
            }
            all_rows.push(row);
            all_meta.push(RowMetaEntry {
                kind: RowKind::Subtotal,
                level: Some(level),
            });
        }
    }
    Ok((all_rows, all_meta))
}

fn compute_subtotals_with_columns(
    lf: LazyFrame,
    request: &PivotRequest,
    column_headers: &[ColumnHeader],
) -> Result<(Vec<HashMap<String, serde_json::Value>>, Vec<RowMetaEntry>), DataError> {
    let mut all_rows = Vec::new();
    let mut all_meta = Vec::new();

    for level in 0..request.rows.len().saturating_sub(1) {
        let prefix: Vec<String> = request.rows[0..=level].to_vec();
        let mut group_fields: Vec<Expr> = prefix.iter().map(|s| col(s)).collect();
        group_fields.extend(request.columns.iter().map(|s| col(s)));
        let agg_exprs = build_value_agg_exprs(&request.values);

        let df = lf
            .clone()
            .group_by(group_fields)
            .agg(agg_exprs)
            .collect()
            .map_err(|e| DataError::ProcessingError(format!("Subtotal error: {}", e)))?;

        // For each (prefix tuple), build a row by pivoting columns manually.
        // Build a key: prefix_values -> per-(value_field, col_values) value.
        // Simpler: iterate df rows, write into a flat map keyed by prefix.
        let mut by_prefix: HashMap<Vec<String>, HashMap<String, serde_json::Value>> = HashMap::new();

        for i in 0..df.height() {
            let mut prefix_key: Vec<String> = Vec::with_capacity(prefix.len());
            for pf in &prefix {
                let c = df
                    .column(pf)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                let av = c
                    .get(i)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                prefix_key.push(anyvalue_to_display_string(&av));
            }

            let mut col_value_parts: Vec<String> = Vec::with_capacity(request.columns.len());
            for cf in &request.columns {
                let c = df
                    .column(cf)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                let av = c
                    .get(i)
                    .map_err(|e| DataError::ProcessingError(e.to_string()))?;
                col_value_parts.push(anyvalue_to_display_string(&av));
            }
            let col_str = col_value_parts.join("_");

            let entry = by_prefix.entry(prefix_key.clone()).or_insert_with(|| {
                let mut h = HashMap::new();
                for (idx, rf) in request.rows.iter().enumerate() {
                    if idx <= level {
                        h.insert(rf.clone(), serde_json::Value::String(prefix_key[idx].clone()));
                    } else {
                        h.insert(rf.clone(), serde_json::Value::Null);
                    }
                }
                h
            });

            for v in &request.values {
                let alias = format!("{}_{}", v.aggregation.snake(), v.field);
                let key = format!("{}_{}_{}", v.aggregation.snake(), v.field, col_str);
                if let Ok(c) = df.column(&alias) {
                    if let Ok(av) = c.get(i) {
                        entry.insert(key, anyvalue_to_json(av));
                    }
                }
            }
        }

        // Restore the original-typed prefix values (since we string-ified above
        // for keying purposes only).
        for (_prefix_strs, mut row) in by_prefix {
            // Best-effort: try to coerce string-typed prefix entries back to numbers.
            for (idx, rf) in request.rows.iter().enumerate() {
                if idx > level {
                    continue;
                }
                if let Some(serde_json::Value::String(s)) = row.get(rf) {
                    if let Ok(n) = s.parse::<i64>() {
                        row.insert(rf.clone(), serde_json::Value::Number(serde_json::Number::from(n)));
                    } else if let Ok(f) = s.parse::<f64>() {
                        if let Some(num) = serde_json::Number::from_f64(f) {
                            row.insert(rf.clone(), serde_json::Value::Number(num));
                        }
                    }
                }
            }
            // Fill any missing value-column keys with null for stability.
            for ch in column_headers {
                row.entry(ch.key.clone()).or_insert(serde_json::Value::Null);
            }
            all_rows.push(row);
            all_meta.push(RowMetaEntry {
                kind: RowKind::Subtotal,
                level: Some(level),
            });
        }
    }
    Ok((all_rows, all_meta))
}

/// Interleave subtotals after the last data row that shares the same row-field
/// prefix. Order: data rows for prefix → subtotal for that prefix at the
/// deepest non-leaf level, going outward.
fn interleave_subtotals(
    data: Vec<HashMap<String, serde_json::Value>>,
    meta: Vec<RowMetaEntry>,
    subtotals: Vec<HashMap<String, serde_json::Value>>,
    sub_meta: Vec<RowMetaEntry>,
    row_fields: &[String],
) -> (Vec<HashMap<String, serde_json::Value>>, Vec<RowMetaEntry>) {
    fn prefix_at(
        row: &HashMap<String, serde_json::Value>,
        fields: &[String],
        level: usize,
    ) -> Vec<String> {
        fields[0..=level]
            .iter()
            .map(|f| value_to_string(row.get(f)))
            .collect()
    }

    fn value_to_string(v: Option<&serde_json::Value>) -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            Some(serde_json::Value::Bool(b)) => b.to_string(),
            Some(serde_json::Value::Null) | None => "".to_string(),
            Some(other) => other.to_string(),
        }
    }

    // For each data row, find which subtotals to emit after it: a subtotal at
    // level L is emitted just before the data row whose row[fields[L]] differs
    // from the next row's. We do this by tracking the previous prefix and
    // emitting subtotal rows whose prefix matches the just-completed group.
    let mut out_data = Vec::with_capacity(data.len() + subtotals.len());
    let mut out_meta = Vec::with_capacity(meta.len() + sub_meta.len());

    let n = data.len();
    for i in 0..n {
        out_data.push(data[i].clone());
        out_meta.push(meta[i].clone());

        let current_prefixes: Vec<Vec<String>> = (0..row_fields.len().saturating_sub(1))
            .map(|lvl| prefix_at(&data[i], row_fields, lvl))
            .collect();
        let next_prefixes: Vec<Vec<String>> = if i + 1 < n {
            (0..row_fields.len().saturating_sub(1))
                .map(|lvl| prefix_at(&data[i + 1], row_fields, lvl))
                .collect()
        } else {
            (0..row_fields.len().saturating_sub(1))
                .map(|_| vec!["__end__".to_string()])
                .collect()
        };

        // Emit subtotals from deepest level (largest L) to outermost (L=0) when
        // the prefix at that level changes between current and next row.
        for lvl in (0..row_fields.len().saturating_sub(1)).rev() {
            if current_prefixes[lvl] != next_prefixes[lvl] {
                // Find the matching subtotal row(s).
                for (s_idx, s_meta) in sub_meta.iter().enumerate() {
                    if s_meta.level != Some(lvl) {
                        continue;
                    }
                    let s_pref = prefix_at(&subtotals[s_idx], row_fields, lvl);
                    if s_pref == current_prefixes[lvl] {
                        out_data.push(subtotals[s_idx].clone());
                        out_meta.push(s_meta.clone());
                    }
                }
            }
        }
    }

    (out_data, out_meta)
}

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
        for ch in column_headers {
            let alias = format!("{}_{}", ch.aggregation.snake(), ch.value_field);
            if let Ok(c) = totals_df.column(&alias) {
                if let Ok(v) = c.get(0) {
                    totals.insert(ch.key.clone(), anyvalue_to_json(v));
                }
            }
        }
    } else {
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
mod tests;
