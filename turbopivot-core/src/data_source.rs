use crate::DataError;
use calamine::{open_workbook_auto, Data, Reader};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
pub enum DataSource {
    File {
        path: String,
    },
    Sqlite {
        path: String,
        query: String,
    },
    Postgres {
        connection_string: String,
        query: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColumnDtype {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    Datetime,
    Other,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: ColumnDtype,
}

impl ColumnDtype {
    fn from_polars(dtype: &DataType) -> Self {
        match dtype {
            DataType::String => ColumnDtype::String,
            DataType::Boolean => ColumnDtype::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => ColumnDtype::Integer,
            DataType::Float32 | DataType::Float64 => ColumnDtype::Float,
            DataType::Date => ColumnDtype::Date,
            DataType::Datetime(_, _) => ColumnDtype::Datetime,
            _ => ColumnDtype::Other,
        }
    }
}

pub fn load_data_source(source: &DataSource) -> Result<LazyFrame, DataError> {
    match source {
        DataSource::File { path } => read_file_path(path),
        DataSource::Sqlite { path, query } => read_sqlite(path, query),
        DataSource::Postgres {
            connection_string,
            query,
        } => read_postgres(connection_string, query),
    }
}

pub fn describe_columns(source: &DataSource) -> Result<Vec<ColumnInfo>, DataError> {
    let mut lf = load_data_source(source)?;
    let schema = lf
        .schema()
        .map_err(|e| DataError::ProcessingError(format!("Failed to get schema: {}", e)))?;
    Ok(schema
        .iter()
        .map(|(name, dtype)| ColumnInfo {
            name: name.to_string(),
            dtype: ColumnDtype::from_polars(dtype),
        })
        .collect())
}

pub fn read_file_path(file_path: &str) -> Result<LazyFrame, DataError> {
    let path = Path::new(file_path);
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| DataError::UnsupportedFormat("File has no extension".to_string()))?;

    match extension.to_lowercase().as_str() {
        "csv" => LazyCsvReader::new(file_path)
            .with_has_header(true)
            .with_try_parse_dates(true)
            .finish()
            .map_err(|e| DataError::ReadError(e.to_string())),
        "parquet" => LazyFrame::scan_parquet(file_path, Default::default())
            .map_err(|e| DataError::ReadError(e.to_string())),
        "xlsx" | "xls" | "xlsm" | "xlsb" | "ods" => read_excel(file_path),
        _ => Err(DataError::UnsupportedFormat(format!(
            "Unsupported file format: {}",
            extension
        ))),
    }
}

fn read_excel(path: &str) -> Result<LazyFrame, DataError> {
    let mut workbook = open_workbook_auto(path)
        .map_err(|e| DataError::ReadError(format!("Failed to open workbook: {}", e)))?;

    let sheet_name = workbook
        .sheet_names()
        .into_iter()
        .next()
        .ok_or_else(|| DataError::ReadError("Workbook has no sheets".to_string()))?;

    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| DataError::ReadError(format!("Failed to read sheet: {}", e)))?;

    if range.is_empty() {
        return Err(DataError::ReadError("Sheet is empty".to_string()));
    }

    let rows: Vec<Vec<&Data>> = range.rows().map(|r| r.iter().collect()).collect();
    let header_row = rows
        .first()
        .ok_or_else(|| DataError::ReadError("Sheet has no rows".to_string()))?;

    let headers: Vec<String> = header_row
        .iter()
        .enumerate()
        .map(|(idx, c)| match c {
            Data::String(s) if !s.is_empty() => s.clone(),
            Data::Empty => format!("column_{}", idx + 1),
            other => other.to_string(),
        })
        .collect();

    let n_cols = headers.len();
    let n_data_rows = rows.len() - 1;
    let mut columns: Vec<Vec<Data>> = (0..n_cols).map(|_| Vec::with_capacity(n_data_rows)).collect();
    for row in rows.iter().skip(1) {
        for c in 0..n_cols {
            columns[c].push(row.get(c).copied().cloned().unwrap_or(Data::Empty));
        }
    }

    let series_vec: Vec<Series> = headers
        .iter()
        .zip(columns.into_iter())
        .map(|(name, col)| excel_column_to_series(name, &col))
        .collect();

    let df = DataFrame::new(series_vec)
        .map_err(|e| DataError::ReadError(format!("DataFrame error: {}", e)))?;
    Ok(df.lazy())
}

fn excel_column_to_series(name: &str, col: &[Data]) -> Series {
    let mut all_int = true;
    let mut all_float = true;
    let mut all_bool = true;
    let mut all_empty = true;
    for v in col {
        match v {
            Data::Empty => {}
            Data::Int(_) => {
                all_bool = false;
                all_empty = false;
            }
            Data::Float(f) => {
                all_bool = false;
                all_empty = false;
                if f.fract() != 0.0 {
                    all_int = false;
                }
            }
            Data::Bool(_) => {
                all_int = false;
                all_float = false;
                all_empty = false;
            }
            _ => {
                all_int = false;
                all_float = false;
                all_bool = false;
                all_empty = false;
            }
        }
    }

    if all_empty {
        return Series::new(name, vec![None::<String>; col.len()]);
    }
    if all_bool {
        let vals: Vec<Option<bool>> = col
            .iter()
            .map(|v| match v {
                Data::Bool(b) => Some(*b),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    if all_int {
        let vals: Vec<Option<i64>> = col
            .iter()
            .map(|v| match v {
                Data::Int(i) => Some(*i),
                Data::Float(f) => Some(*f as i64),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    if all_float {
        let vals: Vec<Option<f64>> = col
            .iter()
            .map(|v| match v {
                Data::Int(i) => Some(*i as f64),
                Data::Float(f) => Some(*f),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    let vals: Vec<Option<String>> = col
        .iter()
        .map(|v| match v {
            Data::Empty => None,
            Data::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        })
        .collect();
    Series::new(name, vals)
}

fn read_sqlite(path: &str, query: &str) -> Result<LazyFrame, DataError> {
    use rusqlite::{types::ValueRef, Connection};

    let conn = Connection::open(path)
        .map_err(|e| DataError::DatabaseError(format!("SQLite open: {}", e)))?;
    let mut stmt = conn
        .prepare(query)
        .map_err(|e| DataError::DatabaseError(format!("SQLite prepare: {}", e)))?;
    let column_names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let n_cols = column_names.len();

    let mut columns: Vec<Vec<SqlValue>> = (0..n_cols).map(|_| Vec::new()).collect();

    let mut rows = stmt
        .query([])
        .map_err(|e| DataError::DatabaseError(format!("SQLite query: {}", e)))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| DataError::DatabaseError(format!("SQLite next: {}", e)))?
    {
        for c in 0..n_cols {
            let v = row
                .get_ref(c)
                .map_err(|e| DataError::DatabaseError(e.to_string()))?;
            columns[c].push(match v {
                ValueRef::Null => SqlValue::Null,
                ValueRef::Integer(i) => SqlValue::Int(i),
                ValueRef::Real(f) => SqlValue::Float(f),
                ValueRef::Text(b) => SqlValue::Text(String::from_utf8_lossy(b).to_string()),
                ValueRef::Blob(_) => SqlValue::Null,
            });
        }
    }

    let series_vec: Vec<Series> = column_names
        .iter()
        .zip(columns.into_iter())
        .map(|(name, col)| sql_column_to_series(name, &col))
        .collect();
    let df = DataFrame::new(series_vec)
        .map_err(|e| DataError::DatabaseError(format!("DataFrame error: {}", e)))?;
    Ok(df.lazy())
}

fn read_postgres(connection_string: &str, query: &str) -> Result<LazyFrame, DataError> {
    use postgres::types::Type;
    use postgres::{Client, NoTls};

    let mut client = Client::connect(connection_string, NoTls)
        .map_err(|e| DataError::DatabaseError(format!("Postgres connect: {}", e)))?;

    let rows = client
        .query(query, &[])
        .map_err(|e| DataError::DatabaseError(format!("Postgres query: {}", e)))?;

    if rows.is_empty() {
        return Err(DataError::DatabaseError(
            "Postgres query returned 0 rows".to_string(),
        ));
    }

    let columns = rows[0].columns();
    let column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();
    let column_types: Vec<Type> = columns.iter().map(|c| c.type_().clone()).collect();
    let n_cols = column_names.len();

    let mut col_values: Vec<Vec<SqlValue>> = (0..n_cols).map(|_| Vec::with_capacity(rows.len())).collect();

    for row in &rows {
        for (idx, ty) in column_types.iter().enumerate() {
            let val: SqlValue = match *ty {
                Type::BOOL => row
                    .get::<_, Option<bool>>(idx)
                    .map(SqlValue::Bool)
                    .unwrap_or(SqlValue::Null),
                Type::INT2 => row
                    .get::<_, Option<i16>>(idx)
                    .map(|v| SqlValue::Int(v as i64))
                    .unwrap_or(SqlValue::Null),
                Type::INT4 => row
                    .get::<_, Option<i32>>(idx)
                    .map(|v| SqlValue::Int(v as i64))
                    .unwrap_or(SqlValue::Null),
                Type::INT8 => row
                    .get::<_, Option<i64>>(idx)
                    .map(SqlValue::Int)
                    .unwrap_or(SqlValue::Null),
                Type::FLOAT4 => row
                    .get::<_, Option<f32>>(idx)
                    .map(|v| SqlValue::Float(v as f64))
                    .unwrap_or(SqlValue::Null),
                Type::FLOAT8 => row
                    .get::<_, Option<f64>>(idx)
                    .map(SqlValue::Float)
                    .unwrap_or(SqlValue::Null),
                Type::NUMERIC => row
                    .try_get::<_, Option<f64>>(idx)
                    .ok()
                    .flatten()
                    .map(SqlValue::Float)
                    .unwrap_or(SqlValue::Null),
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
                    .get::<_, Option<String>>(idx)
                    .map(SqlValue::Text)
                    .unwrap_or(SqlValue::Null),
                _ => row
                    .try_get::<_, Option<String>>(idx)
                    .ok()
                    .flatten()
                    .map(SqlValue::Text)
                    .unwrap_or(SqlValue::Null),
            };
            col_values[idx].push(val);
        }
    }

    let series_vec: Vec<Series> = column_names
        .iter()
        .zip(col_values.into_iter())
        .map(|(name, col)| sql_column_to_series(name, &col))
        .collect();
    let df = DataFrame::new(series_vec)
        .map_err(|e| DataError::DatabaseError(format!("DataFrame error: {}", e)))?;
    Ok(df.lazy())
}

#[derive(Clone, Debug)]
enum SqlValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

fn sql_column_to_series(name: &str, col: &[SqlValue]) -> Series {
    let mut has_int = false;
    let mut has_float = false;
    let mut has_text = false;
    let mut has_bool = false;
    for v in col {
        match v {
            SqlValue::Int(_) => has_int = true,
            SqlValue::Float(_) => has_float = true,
            SqlValue::Text(_) => has_text = true,
            SqlValue::Bool(_) => has_bool = true,
            SqlValue::Null => {}
        }
    }

    if has_text || (has_bool && (has_int || has_float)) {
        let vals: Vec<Option<String>> = col
            .iter()
            .map(|v| match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some(i.to_string()),
                SqlValue::Float(f) => Some(f.to_string()),
                SqlValue::Bool(b) => Some(b.to_string()),
                SqlValue::Text(s) => Some(s.clone()),
            })
            .collect();
        return Series::new(name, vals);
    }
    if has_bool {
        let vals: Vec<Option<bool>> = col
            .iter()
            .map(|v| match v {
                SqlValue::Bool(b) => Some(*b),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    if has_float {
        let vals: Vec<Option<f64>> = col
            .iter()
            .map(|v| match v {
                SqlValue::Float(f) => Some(*f),
                SqlValue::Int(i) => Some(*i as f64),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    if has_int {
        let vals: Vec<Option<i64>> = col
            .iter()
            .map(|v| match v {
                SqlValue::Int(i) => Some(*i),
                _ => None,
            })
            .collect();
        return Series::new(name, vals);
    }
    Series::new(name, vec![None::<String>; col.len()])
}
