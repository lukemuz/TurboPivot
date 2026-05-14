import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { save, open } from "@tauri-apps/plugin-dialog";
import { writeTextFile, readTextFile } from "@tauri-apps/plugin-fs";
import "./App.css";
import SourcePicker from "./components/SourcePicker";
import Shelves, { ShelfState } from "./components/Shelves";
import FilterConfigurator from "./components/FilterConfigurator";
import SortConfigurator from "./components/SortConfigurator";
import DateGroupConfigurator from "./components/DateGroupConfigurator";
import PivotTable from "./components/PivotTable";
import {
  ColumnFormatConfig,
  ColumnInfo,
  DataSource,
  DateGroup,
  FilterCondition,
  PivotRequest,
  PivotResult,
  SortConfig,
} from "./components/types";

function App() {
  const [source, setSource] = useState<DataSource | null>(null);
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [shelfState, setShelfState] = useState<ShelfState>({
    rows: [],
    columns: [],
    values: [],
  });
  const [filters, setFilters] = useState<FilterCondition[]>([]);
  const [sortConfig, setSortConfig] = useState<SortConfig | null>(null);
  const [dateGroups, setDateGroups] = useState<DateGroup[]>([]);
  const [subtotals, setSubtotals] = useState(false);
  const [pivotResult, setPivotResult] = useState<PivotResult | null>(null);
  const [formatOverrides, setFormatOverrides] = useState<Record<string, ColumnFormatConfig>>({});
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  const handleSourceLoaded = (newSource: DataSource, newColumns: ColumnInfo[]) => {
    setSource(newSource);
    setColumns(newColumns);
    setShelfState({ rows: [], columns: [], values: [] });
    setFilters([]);
    setSortConfig(null);
    setDateGroups([]);
    setPivotResult(null);
    setFormatOverrides({});
    setError(null);
    setStatusMessage(null);
  };

  // Virtual columns from date groups are also valid pivot fields.
  const columnsWithVirtual: ColumnInfo[] = [
    ...columns,
    ...dateGroups.map((g) => ({
      name: g.alias,
      // virtual columns are stringified by the date-grouping logic
      dtype: "String" as any,
    })),
  ];

  const buildRequest = (): PivotRequest | null => {
    if (!source) return null;
    return {
      data_path: source.kind === "File" ? source.path : "",
      source,
      rows: shelfState.rows,
      columns: shelfState.columns,
      values: shelfState.values,
      filters: filters.length > 0 ? filters : undefined,
      sort: sortConfig ?? undefined,
      date_groups: dateGroups,
      subtotals,
    };
  };

  const generatePivot = async () => {
    const request = buildRequest();
    if (!request) {
      setError("Please load a data source first");
      return;
    }
    if (shelfState.rows.length === 0 && shelfState.columns.length === 0) {
      setError("Drag at least one field onto Rows or Columns");
      return;
    }
    if (shelfState.values.length === 0) {
      setError("Drag at least one field onto Values");
      return;
    }

    setIsLoading(true);
    setError(null);
    setStatusMessage(null);
    try {
      const result = (await invoke("run_pivot", { request })) as PivotResult;
      setPivotResult(result);
    } catch (err) {
      setError(typeof err === "string" ? err : String(err));
      setPivotResult(null);
    } finally {
      setIsLoading(false);
    }
  };

  const exportAs = async (kind: "csv" | "tsv") => {
    if (!pivotResult) return;
    const cmd = kind === "csv" ? "export_csv" : "export_tsv";
    try {
      const content = (await invoke(cmd, { result: pivotResult })) as string;
      const path = await save({
        filters: [
          {
            name: kind.toUpperCase(),
            extensions: [kind],
          },
        ],
        defaultPath: `pivot.${kind}`,
      });
      if (path) {
        await writeTextFile(path, content);
        setStatusMessage(`Saved ${path}`);
      }
    } catch (err) {
      setError(String(err));
    }
  };

  const copyAsTsv = async () => {
    if (!pivotResult) return;
    try {
      const content = (await invoke("export_tsv", { result: pivotResult })) as string;
      await navigator.clipboard.writeText(content);
      setStatusMessage("Copied to clipboard");
    } catch (err) {
      setError(String(err));
    }
  };

  const saveConfig = async () => {
    const request = buildRequest();
    if (!request) return;
    const config = {
      version: 1,
      request,
      formatOverrides,
    };
    try {
      const path = await save({
        filters: [{ name: "TurboPivot config", extensions: ["json"] }],
        defaultPath: "pivot-config.json",
      });
      if (path) {
        await writeTextFile(path, JSON.stringify(config, null, 2));
        setStatusMessage(`Config saved to ${path}`);
      }
    } catch (err) {
      setError(String(err));
    }
  };

  const loadConfig = async () => {
    try {
      const path = await open({
        multiple: false,
        filters: [{ name: "TurboPivot config", extensions: ["json"] }],
      });
      if (typeof path !== "string") return;
      const content = await readTextFile(path);
      const parsed = JSON.parse(content);
      const req: PivotRequest = parsed.request;

      // Load the source to get column metadata first.
      if (req.source) {
        const cols = (await invoke("get_columns", { source: req.source })) as ColumnInfo[];
        setSource(req.source);
        setColumns(cols);
      }
      setShelfState({
        rows: req.rows ?? [],
        columns: req.columns ?? [],
        values: req.values ?? [],
      });
      setFilters(req.filters ?? []);
      setSortConfig(req.sort ?? null);
      setDateGroups(req.date_groups ?? []);
      setSubtotals(!!req.subtotals);
      setFormatOverrides(parsed.formatOverrides ?? {});
      setStatusMessage(`Loaded ${path}`);
    } catch (err) {
      setError(String(err));
    }
  };

  const onFormatChange = (key: string, next: ColumnFormatConfig) => {
    setFormatOverrides({ ...formatOverrides, [key]: next });
  };

  return (
    <div className="container">
      <h1>TurboPivot</h1>
      <p className="tagline">Lightning fast pivot tables powered by Rust & Polars</p>

      <div className="app-layout">
        <div className="sidebar">
          <SourcePicker isLoading={isLoading} onSourceLoaded={handleSourceLoaded} />

          {columns.length > 0 && (
            <>
              <Shelves
                columns={columnsWithVirtual}
                state={shelfState}
                onChange={setShelfState}
              />

              <DateGroupConfigurator
                columns={columns}
                dateGroups={dateGroups}
                onChange={setDateGroups}
              />

              <FilterConfigurator
                columns={columnsWithVirtual}
                filters={filters}
                onFiltersChange={setFilters}
              />

              <SortConfigurator
                rowFields={shelfState.rows}
                resultColumnHeaders={pivotResult?.column_headers ?? []}
                onSortChange={setSortConfig}
              />

              <label
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.4em",
                  margin: "0.5em 0",
                  fontSize: "0.9em",
                }}
              >
                <input
                  type="checkbox"
                  checked={subtotals}
                  onChange={(e) => setSubtotals(e.target.checked)}
                />
                Show subtotals
              </label>

              <button
                className="generate-button"
                onClick={generatePivot}
                disabled={isLoading || shelfState.values.length === 0}
              >
                Generate Pivot Table
              </button>

              <div
                style={{
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "0.4em",
                  marginTop: "0.5em",
                }}
              >
                <button onClick={() => exportAs("csv")} disabled={!pivotResult}>
                  Export CSV
                </button>
                <button onClick={() => exportAs("tsv")} disabled={!pivotResult}>
                  Export TSV
                </button>
                <button onClick={copyAsTsv} disabled={!pivotResult}>
                  Copy
                </button>
                <button onClick={saveConfig} disabled={!source}>
                  Save config
                </button>
                <button onClick={loadConfig}>Load config</button>
              </div>
            </>
          )}

          {error && <div className="error-message">{error}</div>}
          {statusMessage && (
            <div style={{ color: "#2f855a", fontSize: "0.85em", marginTop: "0.4em" }}>
              {statusMessage}
            </div>
          )}
        </div>

        <div className="main-content">
          <PivotTable
            result={pivotResult}
            isLoading={isLoading}
            formatOverrides={formatOverrides}
            onFormatChange={onFormatChange}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
