use polars::prelude::*;
use polars::lazy::dsl::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

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

#[derive(Serialize, Deserialize, Debug)]
pub struct PivotRequest {
    pub data_path: String,
    pub rows: Vec<String>,
    pub columns: Vec<String>,
    pub values: Vec<ValueWithAggregation>,
    pub filters: Option<Vec<FilterCondition>>,
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

pub fn generate_pivot(request: PivotRequest) -> Result<PivotResult, DataError> {
    // Read the data as a LazyFrame
    let mut lf = read_data(&request.data_path)?;
    
    // Apply filters if they exist
    if let Some(filters) = &request.filters {
        for filter in filters {
            lf = apply_filter(lf, filter)?;
        }
    }
    
    // Combine rows and columns for groupby
    let mut group_cols = request.rows.clone();
    group_cols.extend(request.columns.clone());
    
    // Create groupby expressions
    let group_exprs: Vec<Expr> = group_cols.iter().map(|s| col(s)).collect();
    
    // Create aggregation expressions
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
    
    // Execute the query to get the result DataFrame
    let result_df = lf
        .group_by(group_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    
    // Generate column headers for the result
    let mut column_headers = Vec::new();
    
    // Add value fields with aggregation as headers
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
        .collect();
    column_headers.push(value_headers);
    
    // Convert to serde_json format for the frontend
    let data = df_to_json_rows(result_df)
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    
    Ok(PivotResult {
        data,
        column_headers,
        row_headers: request.rows,
    })
}

fn extract_column_values(df: &DataFrame, columns: &[String]) -> Result<Vec<String>, DataError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    // Handle only the first column dimension for now (can be extended for multi-level)
    let col_name = &columns[0];
    let unique_series = df.column(col_name)
        .map_err(|e| DataError::ProcessingError(e.to_string()))?
        .unique()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?;
    
    let unique_strings = unique_series.cast(&DataType::String)
        .map_err(|e| DataError::ProcessingError(e.to_string()))?
        .str()
        .map_err(|e| DataError::ProcessingError(e.to_string()))?
        .into_iter()
        .filter_map(|opt_str| opt_str.map(|s| s.to_string()))
        .collect();
    
    Ok(unique_strings)
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
                        if let Some(num) = serde_json::Number::from_f64(v) {
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