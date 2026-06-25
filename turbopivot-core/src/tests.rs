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

fn basic_request(path: &str) -> PivotRequest {
    PivotRequest {
        data_path: path.to_string(),
        source: None,
        rows: vec!["Country".to_string()],
        columns: vec![],
        values: vec![ValueWithAggregation {
            field: "Sales".to_string(),
            aggregation: AggregationType::Sum,
        }],
        filters: None,
        sort: None,
        date_groups: vec![],
        subtotals: false,
    }
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
fn test_describe_columns_returns_dtypes() {
    let path = standard_csv();
    let info = describe_columns(&DataSource::File { path }).unwrap();
    let by_name: HashMap<String, ColumnDtype> =
        info.into_iter().map(|c| (c.name, c.dtype)).collect();
    assert_eq!(by_name["Country"], ColumnDtype::String);
    assert_eq!(by_name["Sales"], ColumnDtype::Integer);
}

#[test]
fn test_validate_pivot_request_no_rows_or_columns() {
    let mut req = basic_request("test.csv");
    req.rows.clear();
    req.columns.clear();
    let err = validate_pivot_request(&req).unwrap_err();
    assert!(err.to_string().contains("At least one row or column"));
}

#[test]
fn test_validate_pivot_request_no_values() {
    let mut req = basic_request("test.csv");
    req.values.clear();
    let err = validate_pivot_request(&req).unwrap_err();
    assert!(err.to_string().contains("At least one value field"));
}

#[test]
fn test_validate_pivot_request_duplicate_fields() {
    let mut req = basic_request("test.csv");
    req.columns = vec!["Country".to_string()];
    let err = validate_pivot_request(&req).unwrap_err();
    assert!(err.to_string().contains("Duplicate field"));
}

#[test]
fn test_simple_pivot_rows_only() {
    let path = standard_csv();
    let result = generate_pivot(basic_request(&path)).unwrap();
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
    let mut req = basic_request(&path);
    req.columns = vec!["Product".to_string()];
    let result = generate_pivot(req).unwrap();

    assert_eq!(result.data.len(), 2);
    assert_eq!(result.column_headers.len(), 2);

    let usa = find_row(&result.data, "Country", "USA");
    assert_eq!(cell(usa, "sum_Sales_Widget"), 1000.0);
    assert_eq!(cell(usa, "sum_Sales_Gadget"), 2000.0);
    let canada = find_row(&result.data, "Country", "Canada");
    assert_eq!(cell(canada, "sum_Sales_Widget"), 1500.0);
    assert_eq!(cell(canada, "sum_Sales_Gadget"), 2500.0);

    let gt = result.grand_total.unwrap();
    assert_eq!(total(&gt, "sum_Sales_Widget"), 2500.0);
    assert_eq!(total(&gt, "sum_Sales_Gadget"), 4500.0);
}

#[test]
fn test_multi_value_pivot_cell_values() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.columns = vec!["Product".to_string()];
    req.values = vec![
        ValueWithAggregation {
            field: "Sales".to_string(),
            aggregation: AggregationType::Sum,
        },
        ValueWithAggregation {
            field: "Quantity".to_string(),
            aggregation: AggregationType::Mean,
        },
    ];
    let result = generate_pivot(req).unwrap();

    let usa = find_row(&result.data, "Country", "USA");
    assert_eq!(cell(usa, "sum_Sales_Widget"), 1000.0);
    assert_eq!(cell(usa, "mean_Quantity_Widget"), 10.0);
    let canada = find_row(&result.data, "Country", "Canada");
    assert_eq!(cell(canada, "mean_Quantity_Gadget"), 25.0);

    let gt = result.grand_total.unwrap();
    assert_eq!(total(&gt, "mean_Quantity_Widget"), 12.5);
}

#[test]
fn test_grand_total_mean_is_not_mean_of_means() {
    let path = write_csv(
        "tp_uneven.csv",
        "Country,Product,Sales\nUSA,Widget,10\nUSA,Widget,20\nUSA,Widget,30\nCanada,Widget,100\n",
    );
    let mut req = basic_request(&path);
    req.columns = vec!["Product".to_string()];
    req.values = vec![ValueWithAggregation {
        field: "Sales".to_string(),
        aggregation: AggregationType::Mean,
    }];
    let result = generate_pivot(req).unwrap();

    let gt = result.grand_total.unwrap();
    assert_eq!(total(&gt, "mean_Sales_Widget"), 40.0);
}

#[test]
fn test_count_with_columns_returns_actual_count() {
    let path = write_csv(
        "tp_count.csv",
        "Country,Product,Sales\nUSA,Widget,1\nUSA,Widget,2\nUSA,Gadget,3\nCanada,Widget,4\nCanada,Gadget,5\nCanada,Gadget,6\n",
    );
    let mut req = basic_request(&path);
    req.columns = vec!["Product".to_string()];
    req.values = vec![ValueWithAggregation {
        field: "Sales".to_string(),
        aggregation: AggregationType::Count,
    }];
    let result = generate_pivot(req).unwrap();

    let usa = find_row(&result.data, "Country", "USA");
    assert_eq!(cell(usa, "count_Sales_Widget"), 2.0);
    let canada = find_row(&result.data, "Country", "Canada");
    assert_eq!(cell(canada, "count_Sales_Gadget"), 2.0);

    let gt = result.grand_total.unwrap();
    assert_eq!(total(&gt, "count_Sales_Widget"), 3.0);
}

#[test]
fn test_in_filter() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.filters = Some(vec![FilterCondition {
        column: "Country".to_string(),
        operator: FilterOperator::In,
        value: serde_json::json!(["USA", "Canada"]),
    }]);
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 2);
}

#[test]
fn test_not_in_filter() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.filters = Some(vec![FilterCondition {
        column: "Country".to_string(),
        operator: FilterOperator::NotIn,
        value: serde_json::json!(["Canada"]),
    }]);
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 1);
    let row = &result.data[0];
    assert_eq!(row.get("Country"), Some(&serde_json::Value::String("USA".to_string())));
}

#[test]
fn test_contains_filter() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.filters = Some(vec![FilterCondition {
        column: "Product".to_string(),
        operator: FilterOperator::Contains,
        value: serde_json::Value::String("idge".to_string()),
    }]);
    req.rows = vec!["Product".to_string()];
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 1);
    let row = &result.data[0];
    assert_eq!(row.get("Product"), Some(&serde_json::Value::String("Widget".to_string())));
}

#[test]
fn test_starts_with_filter() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.filters = Some(vec![FilterCondition {
        column: "Country".to_string(),
        operator: FilterOperator::StartsWith,
        value: serde_json::Value::String("Can".to_string()),
    }]);
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 1);
}

#[test]
fn test_between_filter() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.rows = vec!["Product".to_string()];
    req.filters = Some(vec![FilterCondition {
        column: "Sales".to_string(),
        operator: FilterOperator::Between,
        value: serde_json::json!([1500, 2500]),
    }]);
    let result = generate_pivot(req).unwrap();
    // Filtered rows: USA/Gadget/2000, Canada/Widget/1500, Canada/Gadget/2500
    let widget = find_row(&result.data, "Product", "Widget");
    assert_eq!(cell(widget, "sum_Sales"), 1500.0);
    let gadget = find_row(&result.data, "Product", "Gadget");
    assert_eq!(cell(gadget, "sum_Sales"), 4500.0);
}

#[test]
fn test_is_null_filter() {
    let path = write_csv(
        "tp_nulls.csv",
        "Country,Product,Sales\nUSA,Widget,1000\nUSA,Gadget,\nCanada,Widget,1500\n",
    );
    let mut req = basic_request(&path);
    req.rows = vec!["Country".to_string()];
    req.filters = Some(vec![FilterCondition {
        column: "Sales".to_string(),
        operator: FilterOperator::IsNull,
        value: serde_json::Value::Null,
    }]);
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 1);
}

#[test]
fn test_sort_descending_on_value_column() {
    let path = standard_csv();
    let mut req = basic_request(&path);
    req.sort = Some(SortConfig {
        column: "sum_Sales".to_string(),
        order: SortOrder::Descending,
    });
    let result = generate_pivot(req).unwrap();
    assert_eq!(
        result.data[0].get("Country"),
        Some(&serde_json::Value::String("Canada".to_string()))
    );
}

#[test]
fn test_subtotals_with_two_row_fields() {
    let path = write_csv(
        "tp_subtotals.csv",
        "Country,Region,Sales\nUSA,East,100\nUSA,West,200\nCanada,East,50\nCanada,West,75\n",
    );
    let mut req = basic_request(&path);
    req.rows = vec!["Country".to_string(), "Region".to_string()];
    req.subtotals = true;
    let result = generate_pivot(req).unwrap();

    // 4 data rows + 2 subtotal rows = 6 total
    assert_eq!(result.data.len(), 6);
    assert_eq!(result.row_meta.len(), 6);

    let subtotal_rows: Vec<_> = result
        .row_meta
        .iter()
        .enumerate()
        .filter(|(_, m)| m.kind == RowKind::Subtotal)
        .map(|(i, _)| &result.data[i])
        .collect();
    assert_eq!(subtotal_rows.len(), 2);

    // USA subtotal = 300, Canada subtotal = 125
    let totals: Vec<f64> = subtotal_rows.iter().map(|r| cell(r, "sum_Sales")).collect();
    assert!(totals.contains(&300.0));
    assert!(totals.contains(&125.0));

    let gt = result.grand_total.unwrap();
    assert_eq!(total(&gt, "sum_Sales"), 425.0);
}

#[test]
fn test_date_group_year() {
    let path = write_csv(
        "tp_dates.csv",
        "date,Sales\n2023-01-15,100\n2023-06-20,200\n2024-02-10,300\n2024-09-05,400\n",
    );
    let mut req = basic_request(&path);
    req.rows = vec!["DateYear".to_string()];
    req.date_groups = vec![DateGroup {
        source_field: "date".to_string(),
        granularity: DateGranularity::Year,
        alias: "DateYear".to_string(),
    }];
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 2);
    let r2023 = find_row(&result.data, "DateYear", "2023");
    assert_eq!(cell(r2023, "sum_Sales"), 300.0);
    let r2024 = find_row(&result.data, "DateYear", "2024");
    assert_eq!(cell(r2024, "sum_Sales"), 700.0);
}

#[test]
fn test_csv_export() {
    let path = standard_csv();
    let result = generate_pivot(basic_request(&path)).unwrap();
    let csv = pivot_result_to_csv(&result);
    assert!(csv.starts_with("Country,Sum of Sales\n"));
    assert!(csv.contains("USA,3000\n"));
    assert!(csv.contains("Canada,4000\n"));
    assert!(csv.contains("Grand Total"));
}

#[test]
fn test_tsv_export() {
    let path = standard_csv();
    let result = generate_pivot(basic_request(&path)).unwrap();
    let tsv = pivot_result_to_tsv(&result);
    assert!(tsv.starts_with("Country\tSum of Sales\n"));
}

#[test]
fn test_sqlite_source() {
    use rusqlite::Connection;
    let db_path = format!("/tmp/tp_sqlite_{}.db", std::process::id());
    let _ = std::fs::remove_file(&db_path);
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "CREATE TABLE sales (country TEXT, product TEXT, amount INTEGER)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO sales VALUES ('USA', 'Widget', 100)", [])
        .unwrap();
    conn.execute("INSERT INTO sales VALUES ('USA', 'Gadget', 200)", [])
        .unwrap();
    conn.execute("INSERT INTO sales VALUES ('Canada', 'Widget', 50)", [])
        .unwrap();
    drop(conn);

    let req = PivotRequest {
        data_path: String::new(),
        source: Some(DataSource::Sqlite {
            path: db_path.clone(),
            query: "SELECT * FROM sales".to_string(),
        }),
        rows: vec!["country".to_string()],
        columns: vec![],
        values: vec![ValueWithAggregation {
            field: "amount".to_string(),
            aggregation: AggregationType::Sum,
        }],
        filters: None,
        sort: None,
        date_groups: vec![],
        subtotals: false,
    };
    let result = generate_pivot(req).unwrap();
    assert_eq!(result.data.len(), 2);
    let usa = find_row(&result.data, "country", "USA");
    assert_eq!(cell(usa, "sum_amount"), 300.0);

    let _ = std::fs::remove_file(&db_path);
}
