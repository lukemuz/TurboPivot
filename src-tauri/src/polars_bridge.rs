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
        let data = df_to_json_rows(agg_df).map_err(|e| DataError::ProcessingError(e.to_string()))?;
        
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
        
        Ok(PivotResult {
            data,
            column_headers: vec![value_headers],
            row_headers: request.rows,
        })
    } else {
        // We need to pivot the DataFrame
        let val_with_agg = &request.values[0]; // Using just the first value for simplicity
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
            // For Std and Var, use First since they don't have direct equivalents
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
        
        println!("Pivoted DataFrame: {:?}", pivoted);
        
        // Extract column headers from the pivoted DataFrame
        let all_columns = pivoted.get_column_names();
        println!("All columns: {:?}", all_columns);
        
        // We know the row identifier column(s) from the request
        let row_columns = request.rows.clone();
        
        // The remaining columns in the pivoted dataframe are the "value" columns
        // These will typically be combinations of the column values
        let value_columns: Vec<String> = all_columns.iter()
            .filter(|&name| !row_columns.contains(&name.to_string()))
            .map(|s| s.to_string())
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
                    
                    // Use the aggregation type from the request to form the key prefix
                    let agg_prefix = match &request.values[0].aggregation {
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
                    };
                    
                    // When we have column features, the frontend is still expecting the
                    // aggregation prefix in the key
                    let key = format!("{}_{}", agg_prefix, value_col);
                    row_map.insert(key, value);
                }
            }
            
            data.push(row_map);
        }
        
        println!("Final data (rows: {}): {:?}", data.len(), data);
        
        // Correct structure for frontend
        Ok(PivotResult {
            data,
            column_headers,
            row_headers: request.rows,
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