import { useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { invoke } from "@tauri-apps/api/core";

interface FileSelectorProps {
  onFileSelected: (path: string, columns: string[]) => void;
  isLoading: boolean;
}

export default function FileSelector({ onFileSelected, isLoading }: FileSelectorProps) {
  const [error, setError] = useState<string | null>(null);

  async function selectFile() {
    try {
      // Open file dialog for selecting CSV or Parquet files
      const selected = await open({
        multiple: false,
        filters: [{
          name: "Data Files",
          extensions: ["csv", "parquet"]
        }]
      });

      if (selected && !Array.isArray(selected)) {
        // Call Rust backend to get columns
        const columns = await invoke("get_csv_columns", { filePath: selected }) as string[];
        onFileSelected(selected, columns);
        setError(null);
      }
    } catch (err) {
      console.error("Error selecting file:", err);
      setError(`Error loading file: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  return (
    <div className="file-selector">
      <button 
        onClick={selectFile} 
        disabled={isLoading}
        className="primary-button"
      >
        {isLoading ? "Loading..." : "Select CSV or Parquet File"}
      </button>
      
      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
    </div>
  );
} 