import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { ColumnInfo, DataSource } from "./types";

interface SourcePickerProps {
  isLoading: boolean;
  onSourceLoaded: (source: DataSource, columns: ColumnInfo[]) => void;
}

type Mode = "file" | "sqlite" | "postgres";

export default function SourcePicker({ isLoading, onSourceLoaded }: SourcePickerProps) {
  const [mode, setMode] = useState<Mode>("file");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // file
  const [filePath, setFilePath] = useState<string | null>(null);

  // sqlite
  const [sqlitePath, setSqlitePath] = useState("");
  const [sqliteQuery, setSqliteQuery] = useState("");

  // postgres
  const [pgConn, setPgConn] = useState("");
  const [pgQuery, setPgQuery] = useState("");

  const load = async (source: DataSource) => {
    setBusy(true);
    setError(null);
    try {
      const columns = (await invoke("get_columns", { source })) as ColumnInfo[];
      onSourceLoaded(source, columns);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const pickFile = async () => {
    const selected = await open({
      multiple: false,
      filters: [
        { name: "Data files", extensions: ["csv", "parquet", "xlsx", "xls", "ods"] },
      ],
    });
    if (typeof selected === "string") {
      setFilePath(selected);
      await load({ kind: "File", path: selected });
    }
  };

  const pickSqliteFile = async () => {
    const selected = await open({
      multiple: false,
      filters: [{ name: "SQLite", extensions: ["db", "sqlite", "sqlite3"] }],
    });
    if (typeof selected === "string") setSqlitePath(selected);
  };

  return (
    <div className="source-picker">
      <h3>Data Source</h3>
      <div style={{ display: "flex", gap: "0.4em", marginBottom: "0.5em" }}>
        {(["file", "sqlite", "postgres"] as Mode[]).map((m) => (
          <button
            key={m}
            onClick={() => setMode(m)}
            disabled={isLoading || busy}
            style={{
              fontWeight: mode === m ? 700 : 400,
              textTransform: "capitalize",
            }}
          >
            {m === "file" ? "File" : m === "sqlite" ? "SQLite" : "Postgres"}
          </button>
        ))}
      </div>

      {mode === "file" && (
        <div>
          <button onClick={pickFile} disabled={isLoading || busy}>
            {filePath ? "Change file…" : "Select file (CSV / Parquet / XLSX)"}
          </button>
          {filePath && (
            <div style={{ fontSize: "0.85em", marginTop: "0.3em", wordBreak: "break-all" }}>
              {filePath}
            </div>
          )}
        </div>
      )}

      {mode === "sqlite" && (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.4em" }}>
          <div style={{ display: "flex", gap: "0.4em" }}>
            <input
              type="text"
              placeholder="Path to .db / .sqlite"
              value={sqlitePath}
              onChange={(e) => setSqlitePath(e.target.value)}
              style={{ flex: 1 }}
            />
            <button onClick={pickSqliteFile} disabled={busy}>
              Browse…
            </button>
          </div>
          <textarea
            placeholder="SELECT * FROM your_table"
            value={sqliteQuery}
            onChange={(e) => setSqliteQuery(e.target.value)}
            rows={3}
          />
          <button
            onClick={() => load({ kind: "Sqlite", path: sqlitePath, query: sqliteQuery })}
            disabled={busy || !sqlitePath || !sqliteQuery}
          >
            Load
          </button>
        </div>
      )}

      {mode === "postgres" && (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.4em" }}>
          <input
            type="text"
            placeholder="postgres://user:pass@host:5432/db"
            value={pgConn}
            onChange={(e) => setPgConn(e.target.value)}
          />
          <textarea
            placeholder="SELECT * FROM your_table"
            value={pgQuery}
            onChange={(e) => setPgQuery(e.target.value)}
            rows={3}
          />
          <button
            onClick={() =>
              load({ kind: "Postgres", connection_string: pgConn, query: pgQuery })
            }
            disabled={busy || !pgConn || !pgQuery}
          >
            Load
          </button>
          <div style={{ fontSize: "0.75em", color: "#a0aec0" }}>
            Credentials stay local; nothing is transmitted off-machine.
          </div>
        </div>
      )}

      {busy && <div style={{ fontSize: "0.85em" }}>Loading…</div>}
      {error && (
        <div className="error-message" style={{ color: "#c53030", fontSize: "0.85em" }}>
          {error}
        </div>
      )}
    </div>
  );
}
