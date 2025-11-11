// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// Use the library crate
use turbopivot::{get_column_names, generate_pivot, PivotRequest, PivotResult};

// Greet command from the original lib.rs
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
fn get_csv_columns(file_path: String) -> Result<Vec<String>, String> {
    get_column_names(&file_path)
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn run_pivot(request: PivotRequest) -> Result<PivotResult, String> {
    generate_pivot(request)
        .map_err(|e| e.to_string())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_fs::init()) 
        .invoke_handler(tauri::generate_handler![
            greet,
            get_csv_columns,
            run_pivot
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
