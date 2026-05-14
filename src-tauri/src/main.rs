// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use turbopivot::{
    describe_columns, generate_pivot, get_column_names, pivot_result_to_csv, pivot_result_to_tsv,
    ColumnInfo, DataSource, PivotRequest, PivotResult,
};

#[tauri::command]
fn get_csv_columns(file_path: String) -> Result<Vec<String>, String> {
    get_column_names(&file_path).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_columns(source: DataSource) -> Result<Vec<ColumnInfo>, String> {
    describe_columns(&source).map_err(|e| e.to_string())
}

#[tauri::command]
fn run_pivot(request: PivotRequest) -> Result<PivotResult, String> {
    generate_pivot(request).map_err(|e| e.to_string())
}

#[tauri::command]
fn export_csv(result: PivotResult) -> Result<String, String> {
    Ok(pivot_result_to_csv(&result))
}

#[tauri::command]
fn export_tsv(result: PivotResult) -> Result<String, String> {
    Ok(pivot_result_to_tsv(&result))
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_fs::init())
        .invoke_handler(tauri::generate_handler![
            get_csv_columns,
            get_columns,
            run_pivot,
            export_csv,
            export_tsv,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
