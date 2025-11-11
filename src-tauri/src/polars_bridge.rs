use polars::prelude::*;
use polars::lazy::dsl::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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

impl From<&AggregationType> for Expr {
    fn from(agg_type: &AggregationType) -> Self {
        match agg_type {
            AggregationType::Sum => col("").sum(),
            AggregationType::Mean => col("").mean(),
            AggregationType::Count => col("").count(),
            AggregationType::Min => col("").min(),
            AggregationType::Max => col("").max(),
            AggregationType::First => col("").first(),
            AggregationType::Last => col("").last(),
            AggregationType::Median => col("").median(),
            AggregationType::Std => col("").std(1),
            AggregationType::Var => col("").var(1),
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
    pub column: String,  // Column name to sort by (can be row field or value column)
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

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotResult {
    pub data: Vec<HashMap<String, serde_json::Value>>,
    pub column_headers: Vec<Vec<String>>, // Multi-level column headers
    pub row_headers: Vec<String>,
    pub grand_total: Option<HashMap<String, serde_json::Value>>, // Grand totals row
}

pub fn read_data(file_path: &str) -> Result<LazyFrame, DataError> {
    let path = Path::new(file_path);
    let extension = path.extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| DataError::UnsupportedFormat("File has no extension".to_string()))?;

    match extension.to_lowercase().as_str() {
        "csv" => {
            // LazyCsvReader is in the prelude
            LazyCsvReader::new(file_path)
                .with_has_header(true)
                .finish()
                .map_err(|e| DataError::ReadError(e.to_string()))
        },
        "parquet" => {
            LazyFrame::scan_parquet(file_path, Default::default())
                .map_err(|e| DataError::ReadError(e.to_string()))
        },
        _ => Err(DataError::UnsupportedFormat(format!("Unsupported file format: {}", extension))),
    }
}

pub fn get_column_names(file_path: &str) -> Result<Vec<String>, DataError> {
    let path = Path::new(file_path);
    // Use underscore to ignore unused variable
    let _extension = path.extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| DataError::UnsupportedFormat("File has no extension".to_string()))?;
        
    // Make lf mutable
    let mut lf = read_data(file_path)?;
    
    // Then fetch just the schema
    let schema = lf.schema()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    
    // Extract field names from the schema
    Ok(schema.iter_names().map(|name| name.to_string()).collect())
}

fn validate_pivot_request(request: &PivotRequest) -> Result<(), DataError> {
    // Check that at least one of rows or columns is specified
    if request.rows.is_empty() && request.columns.is_empty() {
        return Err(DataError::ProcessingError(
            "At least one row or column field must be specified".to_string()
        ));
    }

    // Check that at least one value field is specified
    if request.values.is_empty() {
        return Err(DataError::ProcessingError(
            "At least one value field must be specified".to_string()
        ));
    }

    // Check that value field names are not empty
    for val in &request.values {
        if val.field.is_empty() {
            return Err(DataError::ProcessingError(
                "Value field name cannot be empty".to_string()
            ));
        }
    }

    // Check that no duplicate fields across rows, columns, and values
    let mut all_fields = std::collections::HashSet::new();
    for field in request.rows.iter().chain(request.columns.iter()) {
        if !all_fields.insert(field) {
            return Err(DataError::ProcessingError(
                format!("Duplicate field in rows/columns: {}", field)
            ));
        }
    }

    Ok(())
}

fn validate_columns_exist(request: &PivotRequest, available_columns: &[String]) -> Result<(), DataError> {
    let available_set: std::collections::HashSet<_> = available_columns.iter().collect();

    // Check row fields
    for field in &request.rows {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(
                format!("Row field '{}' does not exist in the dataset. Available columns: {}",
                    field, available_columns.join(", "))
            ));
        }
    }

    // Check column fields
    for field in &request.columns {
        if !available_set.contains(field) {
            return Err(DataError::ProcessingError(
                format!("Column field '{}' does not exist in the dataset. Available columns: {}",
                    field, available_columns.join(", "))
            ));
        }
    }

    // Check value fields
    for val in &request.values {
        if !available_set.contains(&val.field) {
            return Err(DataError::ProcessingError(
                format!("Value field '{}' does not exist in the dataset. Available columns: {}",
                    val.field, available_columns.join(", "))
            ));
        }
    }

    // Check filter columns
    if let Some(filters) = &request.filters {
        for filter in filters {
            if !available_set.contains(&filter.column) {
                return Err(DataError::ProcessingError(
                    format!("Filter column '{}' does not exist in the dataset. Available columns: {}",
                        filter.column, available_columns.join(", "))
                ));
            }
        }
    }

    Ok(())
}

fn apply_filter(lf: LazyFrame, filter: &FilterCondition) -> Result<LazyFrame, DataError> {
    let col_expr = col(&filter.column);
    
    let filter_expr = match &filter.operator {
        FilterOperator::Equal => {
            match &filter.value {
                serde_json::Value::String(s) => col_expr.eq(lit(s.clone())),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.eq(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.eq(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                serde_json::Value::Bool(b) => col_expr.eq(lit(*b)),
                _ => return Err(DataError::ProcessingError("Unsupported value type".to_string())),
            }
        },
        FilterOperator::NotEqual => {
            match &filter.value {
                serde_json::Value::String(s) => col_expr.neq(lit(s.clone())),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.neq(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.neq(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                serde_json::Value::Bool(b) => col_expr.neq(lit(*b)),
                _ => return Err(DataError::ProcessingError("Unsupported value type".to_string())),
            }
        },
        FilterOperator::GreaterThan => {
            match &filter.value {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.gt(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.gt(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                _ => return Err(DataError::ProcessingError("Value must be a number".to_string())),
            }
        },
        FilterOperator::LessThan => {
            match &filter.value {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.lt(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.lt(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                _ => return Err(DataError::ProcessingError("Value must be a number".to_string())),
            }
        },
        FilterOperator::GreaterThanOrEqual => {
            match &filter.value {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.gt_eq(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.gt_eq(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                _ => return Err(DataError::ProcessingError("Value must be a number".to_string())),
            }
        },
        FilterOperator::LessThanOrEqual => {
            match &filter.value {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        col_expr.lt_eq(lit(i))
                    } else if let Some(f) = n.as_f64() {
                        col_expr.lt_eq(lit(f))
                    } else {
                        return Err(DataError::ProcessingError("Invalid number".to_string()));
                    }
                },
                _ => return Err(DataError::ProcessingError("Value must be a number".to_string())),
            }
        },
        FilterOperator::In => {
            match &filter.value {
                serde_json::Value::Array(arr) => {
                    if arr.is_empty() {
                        return Err(DataError::ProcessingError("Empty array in IN filter".to_string()));
                    }
                    
                    // Create a disjunction of equality expressions
                    let mut expr_list = Vec::new();
                    
                    for val in arr {
                        match val {
                            serde_json::Value::String(s) => {
                                expr_list.push(col_expr.clone().eq(lit(s.clone())));
                            },
                            serde_json::Value::Number(n) => {
                                if n.is_i64() {
                                    if let Some(num) = n.as_i64() {
                                        expr_list.push(col_expr.clone().eq(lit(num)));
                                    }
                                } else if let Some(num) = n.as_f64() {
                                    expr_list.push(col_expr.clone().eq(lit(num)));
                                }
                            },
                            serde_json::Value::Bool(b) => {
                                expr_list.push(col_expr.clone().eq(lit(*b)));
                            },
                            _ => continue, // Skip non-primitive values
                        }
                    }
                    
                    if expr_list.is_empty() {
                        return Err(DataError::ProcessingError("No valid values in IN filter".to_string()));
                    }
                    
                    // Combine all equality expressions with OR
                    let mut final_expr = expr_list.remove(0);
                    for expr in expr_list {
                        final_expr = final_expr.or(expr);
                    }
                    
                    final_expr
                },
                _ => return Err(DataError::ProcessingError("Value must be an array".to_string())),
            }
        },
    };
    
    Ok(lf.filter(filter_expr))
}

fn calculate_grand_totals(df: &DataFrame, value_columns: &[String]) -> HashMap<String, serde_json::Value> {
    let mut totals = HashMap::new();

    for col_name in value_columns {
        if let Ok(col) = df.column(col_name) {
            // Try to sum numeric columns
            match col.dtype() {
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {
                    if let Ok(sum_series) = col.sum_reduce() {
                        let total_value = match sum_series {
                            polars::prelude::AnyValue::Int32(v) => serde_json::Value::Number(serde_json::Number::from(v)),
                            polars::prelude::AnyValue::Int64(v) => {
                                if v > i64::pow(2, 53) || v < -i64::pow(2, 53) {
                                    serde_json::Value::String(v.to_string())
                                } else {
                                    serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                                }
                            },
                            polars::prelude::AnyValue::Float64(v) => {
                                if let Some(num) = serde_json::Number::from_f64(v) {
                                    serde_json::Value::Number(num)
                                } else {
                                    serde_json::Value::Null
                                }
                            },
                            polars::prelude::AnyValue::Float32(v) => {
                                if let Some(num) = serde_json::Number::from_f64(v as f64) {
                                    serde_json::Value::Number(num)
                                } else {
                                    serde_json::Value::Null
                                }
                            },
                            _ => serde_json::Value::Null,
                        };
                        totals.insert(col_name.clone(), total_value);
                    }
                },
                _ => {
                    // Non-numeric column, skip
                }
            }
        }
    }

    // Add a label for the grand total row
    totals.insert("__total_label__".to_string(), serde_json::Value::String("Grand Total".to_string()));

    totals
}

fn apply_sorting(mut df: DataFrame, sort_config: &SortConfig) -> Result<DataFrame, DataError> {
    // Check if the sort column exists
    if df.column(&sort_config.column).is_err() {
        return Err(DataError::ProcessingError(
            format!("Sort column '{}' does not exist in the result", sort_config.column)
        ));
    }

    // Sort the dataframe
    let descending = matches!(sort_config.order, SortOrder::Descending);

    df = df.sort(
        [&sort_config.column],
        SortMultipleOptions::default()
            .with_order_descending(descending)
    )
    .map_err(|e| DataError::ProcessingError(format!("Sort error: {}", e)))?;

    Ok(df)
}

pub fn generate_pivot(request: PivotRequest) -> Result<PivotResult, DataError> {
    // Validate request
    validate_pivot_request(&request)?;

    // Read the data as a LazyFrame
    let mut lf = read_data(&request.data_path)?;

    // Validate that all requested columns exist in the data
    let schema = lf.schema()
        .map_err(|e| DataError::ProcessingError(format!("Failed to get schema: {}", e)))?;
    let available_columns: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    validate_columns_exist(&request, &available_columns)?;

    // Apply filters if they exist
    if let Some(filters) = &request.filters {
        for filter in filters {
            lf = apply_filter(lf, filter)?;
        }
    }
    
    // Combine rows and columns for groupby
    let mut group_cols = request.rows.clone();
    group_cols.extend(request.columns.clone());
    
    // Create groupby expressions and aggregation expressions
    let group_exprs: Vec<Expr> = group_cols.iter().map(|s| col(s)).collect();
    let agg_exprs: Vec<Expr> = request.values
        .iter()
        .map(|val_with_agg| {
            let field_col = col(&val_with_agg.field);
            let agg_name = format!(
                "{}_{}",
                match val_with_agg.aggregation {
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
                },
                val_with_agg.field
            );
            
            match val_with_agg.aggregation {
                AggregationType::Sum => field_col.sum().alias(&agg_name),
                AggregationType::Mean => field_col.mean().alias(&agg_name),
                AggregationType::Count => field_col.count().alias(&agg_name),
                AggregationType::Min => field_col.min().alias(&agg_name),
                AggregationType::Max => field_col.max().alias(&agg_name),
                AggregationType::First => field_col.first().alias(&agg_name),
                AggregationType::Last => field_col.last().alias(&agg_name),
                AggregationType::Median => field_col.median().alias(&agg_name),
                AggregationType::Std => field_col.std(1).alias(&agg_name),
                AggregationType::Var => field_col.var(1).alias(&agg_name),
            }
        })
        .collect();
    
    // Execute the query to get the initial aggregated DataFrame
    let agg_df = lf
        .group_by(group_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    
    println!("Aggregated DataFrame: {:?}", agg_df);
    
    // Transform the data using the actual pivot functionality
    if request.columns.is_empty() {
        // No need to pivot if there are no column fields
        // Apply sorting if configured
        let mut final_df = agg_df;
        if let Some(sort_config) = &request.sort {
            final_df = apply_sorting(final_df, sort_config)?;
        }

        let data = df_to_json_rows(final_df).map_err(|e| DataError::ProcessingError(e.to_string()))?;
        
        let value_headers = request.values.iter()
            .map(|v| format!("{}_{}", 
                match v.aggregation {
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
                }, 
                v.field
            ))
            .collect::<Vec<String>>();

        // Calculate grand totals
        let grand_total = Some(calculate_grand_totals(&final_df, &value_headers));

        Ok(PivotResult {
            data,
            column_headers: vec![value_headers],
            row_headers: request.rows,
            grand_total,
        })
    } else {
        // We need to pivot the DataFrame
        // Process each value field separately and merge results
        let mut pivoted_dfs: Vec<(DataFrame, ValueWithAggregation)> = Vec::new();

        for val_with_agg in &request.values {
            let agg_col_name = format!(
                "{}_{}",
                match val_with_agg.aggregation {
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
                },
                val_with_agg.field
            );

            // Map our aggregation type to PivotAgg
            let pivot_agg = match val_with_agg.aggregation {
                AggregationType::Sum => PivotAgg::Sum,
                AggregationType::Mean => PivotAgg::Mean,
                AggregationType::Count => PivotAgg::Count,
                AggregationType::Min => PivotAgg::Min,
                AggregationType::Max => PivotAgg::Max,
                AggregationType::First => PivotAgg::First,
                AggregationType::Last => PivotAgg::Last,
                AggregationType::Median => PivotAgg::Median,
                // For Std and Var, we already calculated them in the aggregation phase
                // The pivot will use First to just take the pre-calculated value
                AggregationType::Std => PivotAgg::First,
                AggregationType::Var => PivotAgg::First,
            };

            // REVERSED pivot parameters:
            let pivoted = pivot(
                &agg_df,
                // Use columns (processing methods) as the index instead of rows
                request.columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                // Use rows (countries) as the columns instead of columns
                Some(request.rows.iter().map(|s| s.as_str()).collect::<Vec<&str>>()),
                Some(vec![agg_col_name.as_str()]), // values
                false, // maintain_order
                Some(pivot_agg),
                None,  // separator
            )
            .map_err(|e| DataError::ProcessingError(format!("Pivot error: {}", e)))?;

            pivoted_dfs.push((pivoted, val_with_agg.clone()));
        }

        // Merge all pivoted dataframes on row columns and rename columns to avoid conflicts
        let mut merged_df = pivoted_dfs[0].0.clone();
        let mut value_column_mapping: Vec<(String, String, ValueWithAggregation)> = Vec::new();

        // Rename columns from first pivot for consistency
        let row_columns_set: std::collections::HashSet<_> = request.rows.iter().map(|s| s.as_str()).collect();
        let val_agg = &pivoted_dfs[0].1;
        for col_name in merged_df.get_column_names() {
            if !row_columns_set.contains(col_name) {
                let new_name = format!("{}_{}_{}",
                    match val_agg.aggregation {
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
                    },
                    val_agg.field,
                    col_name
                );
                merged_df = merged_df.rename(col_name, &new_name)
                    .map_err(|e| DataError::ProcessingError(format!("Rename error: {}", e)))?;
                value_column_mapping.push((new_name, col_name.to_string(), val_agg.clone()));
            }
        }

        // Join remaining pivots and track their columns
        for i in 1..pivoted_dfs.len() {
            let (mut df, val_agg) = pivoted_dfs[i].clone();

            // Rename value columns to avoid conflicts
            for col_name in df.get_column_names() {
                if !row_columns_set.contains(col_name) {
                    let new_name = format!("{}_{}_{}",
                        match val_agg.aggregation {
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
                        },
                        val_agg.field,
                        col_name
                    );
                    df = df.rename(col_name, &new_name)
                        .map_err(|e| DataError::ProcessingError(format!("Rename error: {}", e)))?;
                    value_column_mapping.push((new_name.clone(), col_name.to_string(), val_agg.clone()));
                }
            }

            // Join on row columns
            let join_cols: Vec<&str> = request.rows.iter().map(|s| s.as_str()).collect();
            merged_df = merged_df.join(
                &df,
                &join_cols,
                &join_cols,
                JoinArgs::new(JoinType::Left)
            )
            .map_err(|e| DataError::ProcessingError(format!("Join error: {}", e)))?;
        }

        let mut pivoted = merged_df;

        // Apply sorting if configured
        if let Some(sort_config) = &request.sort {
            pivoted = apply_sorting(pivoted, sort_config)?;
        }

        println!("Pivoted DataFrame: {:?}", pivoted);

        // Extract column headers from the pivoted DataFrame
        let all_columns = pivoted.get_column_names();
        println!("All columns: {:?}", all_columns);

        // We know the row identifier column(s) from the request
        let row_columns = request.rows.clone();

        // Extract value column names for headers (just the renamed column names)
        let value_columns: Vec<String> = value_column_mapping.iter()
            .map(|(renamed, _, _)| renamed.clone())
            .collect();

        println!("Row columns: {:?}", row_columns);
        println!("Value columns: {:?}", value_columns);

        // Create column headers structure for frontend
        let column_headers = vec![value_columns.clone()];
        
        // Now we need to convert the pivoted DataFrame to rows
        let mut data = Vec::new();
        
        // Each row in the DataFrame represents one entry by row values
        for i in 0..pivoted.height() {
            let mut row_map = HashMap::new();
            
            // First, add the row identifier columns
            for row_col in &row_columns {
                if let Ok(col) = pivoted.column(row_col) {
                    let value = match col.get(i) {
                        Ok(AnyValue::String(s)) => serde_json::Value::String(s.to_string()),
                        Ok(AnyValue::Int32(v)) => serde_json::Value::Number(serde_json::Number::from(v)),
                        Ok(AnyValue::Int64(v)) => {
                            if v > i64::pow(2, 53) || v < -i64::pow(2, 53) {
                                serde_json::Value::String(v.to_string())
                            } else {
                                serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                            }
                        },
                        Ok(AnyValue::Float64(v)) => {
                            if let Some(num) = serde_json::Number::from_f64(v) {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::Null
                            }
                        },
                        _ => serde_json::Value::String(format!("{:?}", col.get(i))),
                    };
                    
                    row_map.insert(row_col.clone(), value);
                }
            }
            
            // Then, add all value columns
            for value_col in &value_columns {
                if let Ok(col) = pivoted.column(value_col) {
                    let value = match col.get(i) {
                        Ok(AnyValue::Float64(v)) => {
                            if let Some(num) = serde_json::Number::from_f64(v) {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::Null
                            }
                        },
                        Ok(AnyValue::Int32(v)) => serde_json::Value::Number(serde_json::Number::from(v)),
                        Ok(AnyValue::Int64(v)) => {
                            if v > i64::pow(2, 53) || v < -i64::pow(2, 53) {
                                serde_json::Value::String(v.to_string())
                            } else {
                                serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                            }
                        },
                        Ok(AnyValue::Null) => serde_json::Value::Null,
                        _ => serde_json::Value::String(format!("{:?}", col.get(i))),
                    };

                    row_map.insert(value_col.clone(), value);
                }
            }
            
            data.push(row_map);
        }
        
        println!("Final data (rows: {}): {:?}", data.len(), data);

        // Calculate grand totals
        let grand_total = Some(calculate_grand_totals(&pivoted, &value_columns));

        // Correct structure for frontend
        Ok(PivotResult {
            data,
            column_headers,
            row_headers: request.rows,
            grand_total,
        })
    }
}

fn df_to_json_rows(df: DataFrame) -> Result<Vec<HashMap<String, serde_json::Value>>, polars::error::PolarsError> {
    let mut result = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let mut row_map = HashMap::new();
        
        for col in df.get_columns() {
            let col_name = col.name().to_string();
            let value = match col.dtype() {
                DataType::Int32 => {
                    let s = col.i32()?;
                    if let Some(v) = s.get(i) {
                        serde_json::Value::Number(serde_json::Number::from(v))
                    } else {
                        serde_json::Value::Null
                    }
                },
                DataType::Int64 => {
                    let s = col.i64()?;
                    if let Some(v) = s.get(i) {
                        // Note: serde_json can't represent i64 outside of i53 range
                        if v > i64::pow(2, 53) || v < -i64::pow(2, 53) {
                            serde_json::Value::String(v.to_string())
                        } else {
                            serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                        }
                    } else {
                        serde_json::Value::Null
                    }
                },
                DataType::Float32 | DataType::Float64 => {
                    let s = col.f64()?;
                    if let Some(v) = s.get(i) {
                        if let Some(num) = serde_json::Number::from_f64(v as f64) {
                            serde_json::Value::Number(num)
                        } else {
                            serde_json::Value::String(v.to_string())
                        }
                    } else {
                        serde_json::Value::Null
                    }
                },
                DataType::String => {
                    let s = col.str()?;
                    if let Some(v) = s.get(i) {
                        serde_json::Value::String(v.to_string())
                    } else {
                        serde_json::Value::Null
                    }
                },
                DataType::Boolean => {
                    let s = col.bool()?;
                    if let Some(v) = s.get(i) {
                        serde_json::Value::Bool(v)
                    } else {
                        serde_json::Value::Null
                    }
                },
                _ => serde_json::Value::String(format!("{:?}", col.get(i))),
            };
            
            row_map.insert(col_name, value);
        }
        
        result.push(row_map);
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // Helper function to create test CSV file
    fn create_test_csv() -> String {
        let test_data = "Country,Product,Sales,Quantity\nUSA,Widget,1000,10\nUSA,Gadget,2000,20\nCanada,Widget,1500,15\nCanada,Gadget,2500,25\n";
        let test_file = "/tmp/test_data.csv";
        fs::write(test_file, test_data).unwrap();
        test_file.to_string()
    }

    // Helper function to create test CSV with nulls
    fn create_test_csv_with_nulls() -> String {
        let test_data = "Country,Product,Sales,Quantity\nUSA,Widget,1000,10\nUSA,Gadget,,20\nCanada,Widget,1500,\nCanada,Gadget,2500,25\n";
        let test_file = "/tmp/test_data_nulls.csv";
        fs::write(test_file, test_data).unwrap();
        test_file.to_string()
    }

    #[test]
    fn test_read_csv_data() {
        let file_path = create_test_csv();
        let result = read_data(&file_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_column_names() {
        let file_path = create_test_csv();
        let result = get_column_names(&file_path);
        assert!(result.is_ok());
        let columns = result.unwrap();
        assert_eq!(columns, vec!["Country", "Product", "Sales", "Quantity"]);
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
        };
        let result = validate_pivot_request(&request);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("At least one row or column"));
    }

    #[test]
    fn test_validate_pivot_request_no_values() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![],
            filters: None,
        };
        let result = validate_pivot_request(&request);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("At least one value field"));
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
        };
        let result = validate_pivot_request(&request);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate field"));
    }

    #[test]
    fn test_validate_pivot_request_valid() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
        };
        let result = validate_pivot_request(&request);
        assert!(result.is_ok());
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
        };
        let available = vec!["Country".to_string(), "Sales".to_string()];
        let result = validate_columns_exist(&request, &available);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("InvalidColumn"));
    }

    #[test]
    fn test_validate_columns_exist_invalid_value() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "InvalidValue".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
        };
        let available = vec!["Country".to_string(), "Sales".to_string()];
        let result = validate_columns_exist(&request, &available);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("InvalidValue"));
    }

    #[test]
    fn test_validate_columns_exist_valid() {
        let request = PivotRequest {
            data_path: "test.csv".to_string(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
        };
        let available = vec!["Country".to_string(), "Sales".to_string()];
        let result = validate_columns_exist(&request, &available);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_pivot_rows_only() {
        let file_path = create_test_csv();
        let request = PivotRequest {
            data_path: file_path.clone(),
            rows: vec!["Country".to_string()],
            columns: vec![],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
        };
        let result = generate_pivot(request);
        assert!(result.is_ok());
        let pivot_result = result.unwrap();

        // Should have 2 rows (USA, Canada)
        assert_eq!(pivot_result.data.len(), 2);

        // Check that we have row headers
        assert_eq!(pivot_result.row_headers, vec!["Country"]);

        // Check that we have column headers for the aggregation
        assert_eq!(pivot_result.column_headers.len(), 1);
        assert!(pivot_result.column_headers[0].contains(&"sum_Sales".to_string()));
    }

    #[test]
    fn test_simple_pivot_with_columns() {
        let file_path = create_test_csv();
        let request = PivotRequest {
            data_path: file_path.clone(),
            rows: vec!["Country".to_string()],
            columns: vec!["Product".to_string()],
            values: vec![ValueWithAggregation {
                field: "Sales".to_string(),
                aggregation: AggregationType::Sum,
            }],
            filters: None,
        };
        let result = generate_pivot(request);
        assert!(result.is_ok());
        let pivot_result = result.unwrap();

        // Should have 2 rows (USA, Canada)
        assert_eq!(pivot_result.data.len(), 2);

        // Check row headers
        assert_eq!(pivot_result.row_headers, vec!["Country"]);

        // Should have column headers for each product
        assert!(pivot_result.column_headers[0].len() > 0);
    }

    #[test]
    fn test_multi_value_pivot() {
        let file_path = create_test_csv();
        let request = PivotRequest {
            data_path: file_path.clone(),
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
        };
        let result = generate_pivot(request);
        assert!(result.is_ok());
        let pivot_result = result.unwrap();

        // Should have 2 rows (USA, Canada)
        assert_eq!(pivot_result.data.len(), 2);

        // Should have column headers for both aggregations
        let all_headers = &pivot_result.column_headers[0];

        // Check that we have headers for both Sales and Quantity
        let has_sales = all_headers.iter().any(|h| h.contains("Sales"));
        let has_quantity = all_headers.iter().any(|h| h.contains("Quantity"));
        assert!(has_sales, "Should have Sales columns");
        assert!(has_quantity, "Should have Quantity columns");
    }

    #[test]
    fn test_pivot_with_filter() {
        let file_path = create_test_csv();
        let request = PivotRequest {
            data_path: file_path.clone(),
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
        };
        let result = generate_pivot(request);
        assert!(result.is_ok());
        let pivot_result = result.unwrap();

        // Should have only 1 row (USA) after filtering
        assert_eq!(pivot_result.data.len(), 1);

        // Check that the row is USA
        let first_row = &pivot_result.data[0];
        assert_eq!(first_row.get("Country"), Some(&serde_json::Value::String("USA".to_string())));
    }

    #[test]
    fn test_aggregation_types() {
        let file_path = create_test_csv();

        // Test different aggregation types
        let agg_types = vec![
            AggregationType::Sum,
            AggregationType::Mean,
            AggregationType::Count,
            AggregationType::Min,
            AggregationType::Max,
        ];

        for agg_type in agg_types {
            let request = PivotRequest {
                data_path: file_path.clone(),
                rows: vec!["Country".to_string()],
                columns: vec![],
                values: vec![ValueWithAggregation {
                    field: "Sales".to_string(),
                    aggregation: agg_type.clone(),
                }],
                filters: None,
            };
            let result = generate_pivot(request);
            assert!(result.is_ok(), "Failed for aggregation type: {:?}", agg_type);
        }
    }

    #[test]
    fn test_filter_operators() {
        let file_path = create_test_csv();

        // Test Greater Than filter
        let request = PivotRequest {
            data_path: file_path.clone(),
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
        };
        let result = generate_pivot(request);
        assert!(result.is_ok());

        // Test In filter
        let request2 = PivotRequest {
            data_path: file_path.clone(),
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
        };
        let result2 = generate_pivot(request2);
        assert!(result2.is_ok());
    }
}