import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import "./App.css";
import FileSelector from "./components/FileSelector";
import PivotConfigurator from "./components/PivotConfigurator";
import FilterConfigurator from "./components/FilterConfigurator";
import SortConfigurator from "./components/SortConfigurator";
import PivotTable from "./components/PivotTable";
import { FilterCondition, PivotRequest, PivotResult, SortConfig, ValueWithAggregation } from "./components/types";

function App() {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [rowFields, setRowFields] = useState<string[]>([]);
  const [columnFields, setColumnFields] = useState<string[]>([]);
  const [valueFields, setValueFields] = useState<ValueWithAggregation[]>([]);
  const [filters, setFilters] = useState<FilterCondition[]>([]);
  const [sortConfig, setSortConfig] = useState<SortConfig | null>(null);
  const [pivotResult, setPivotResult] = useState<PivotResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Handle file selection
  const handleFileSelected = (path: string, columns: string[]) => {
    setFilePath(path);
    setColumns(columns);
    setPivotResult(null);
    setError(null);
    setFilters([]);
  };

  // Handle pivot configuration changes
  const handleConfigChange = (
    rows: string[], 
    columns: string[], 
    values: ValueWithAggregation[]
  ) => {
    setRowFields(rows);
    setColumnFields(columns);
    setValueFields(values);
  };

  // Handle filter changes
  const handleFiltersChange = (newFilters: FilterCondition[]) => {
    setFilters(newFilters);
  };

  // Handle sort changes
  const handleSortChange = (newSort: SortConfig | null) => {
    setSortConfig(newSort);
  };

  // Generate pivot table
  const generatePivot = async () => {
    if (!filePath) {
      setError("Please select a file first");
      return;
    }

    if (rowFields.length === 0 && columnFields.length === 0) {
      setError("Please select at least one row or column field");
      return;
    }

    if (valueFields.length === 0) {
      setError("Please select at least one value field with aggregation");
      return;
    }

    // Check for duplicate fields in rows and columns
    const duplicates = rowFields.filter(f => columnFields.includes(f));
    if (duplicates.length > 0) {
      setError(`Field(s) cannot be in both rows and columns: ${duplicates.join(", ")}`);
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const request: PivotRequest = {
        data_path: filePath,
        rows: rowFields,
        columns: columnFields,
        values: valueFields,
        filters: filters.length > 0 ? filters : undefined,
        sort: sortConfig || undefined
      };

      console.log("Sending request:", request);
      
      const result = await invoke("run_pivot", { request }) as PivotResult;
      setPivotResult(result);
    } catch (err) {
      console.error("Error generating pivot:", err);
      // Extract more user-friendly error messages
      let errorMessage = "Error generating pivot";
      if (err instanceof Error) {
        errorMessage = err.message;
      } else if (typeof err === 'string') {
        errorMessage = err;
      } else {
        errorMessage = String(err);
      }

      // Make error message more user-friendly
      if (errorMessage.includes("does not exist in the dataset")) {
        setError(`❌ ${errorMessage}`);
      } else if (errorMessage.includes("At least one")) {
        setError(`⚠️ Configuration Error: ${errorMessage}`);
      } else {
        setError(`❌ ${errorMessage}`);
      }
      setPivotResult(null);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="container">
      <h1>TurboPivot</h1>
      <p className="tagline">Lightning fast pivot tables powered by Rust & Polars</p>

      <div className="app-layout">
        <div className="sidebar">
          <FileSelector 
            onFileSelected={handleFileSelected}
            isLoading={isLoading}
          />
          
          {columns.length > 0 && (
            <>
              <PivotConfigurator 
                columns={columns}
                onConfigChange={handleConfigChange}
              />
              
              <FilterConfigurator
                columns={columns}
                onFiltersChange={handleFiltersChange}
              />

              <SortConfigurator
                columns={columns}
                rowFields={rowFields}
                columnFields={columnFields}
                valueFields={valueFields.map(v => `${v.field} (${v.aggregation})`)}
                onSortChange={handleSortChange}
              />

              <button 
                className="generate-button"
                onClick={generatePivot}
                disabled={isLoading || valueFields.length === 0}
              >
                Generate Pivot Table
              </button>
            </>
          )}
          
          {error && (
            <div className="error-message">
              {error}
            </div>
          )}
        </div>
        
        <div className="main-content">
          <PivotTable 
            result={pivotResult}
            isLoading={isLoading}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
