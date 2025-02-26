import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import "./App.css";
import FileSelector from "./components/FileSelector";
import PivotConfigurator from "./components/PivotConfigurator";
import FilterConfigurator from "./components/FilterConfigurator";
import PivotTable from "./components/PivotTable";
import { FilterCondition, PivotRequest, PivotResult, ValueWithAggregation } from "./components/types";

function App() {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [rowFields, setRowFields] = useState<string[]>([]);
  const [columnFields, setColumnFields] = useState<string[]>([]);
  const [valueFields, setValueFields] = useState<ValueWithAggregation[]>([]);
  const [filters, setFilters] = useState<FilterCondition[]>([]);
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

  // Generate pivot table
  const generatePivot = async () => {
    if (!filePath) {
      setError("Please select a file first");
      return;
    }

    if (valueFields.length === 0) {
      setError("Please select at least one value field with aggregation");
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
        filters: filters.length > 0 ? filters : undefined
      };

      console.log("Sending request:", request);
      
      const result = await invoke("run_pivot", { request }) as PivotResult;
      setPivotResult(result);
    } catch (err) {
      console.error("Error generating pivot:", err);
      setError(`Error generating pivot: ${err instanceof Error ? err.message : String(err)}`);
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
