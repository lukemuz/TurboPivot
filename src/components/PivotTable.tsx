import { useState } from "react";
import { ColumnFormatConfig, ColumnHeader, PivotResult, RowKind } from "./types";
import { formatNumber, inferDefaultFormat } from "./formatting";

interface PivotTableProps {
  result: PivotResult | null;
  isLoading: boolean;
  formatOverrides?: Record<string, ColumnFormatConfig>;
  onFormatChange?: (key: string, next: ColumnFormatConfig) => void;
}

function formatCell(value: any, header: ColumnHeader, override?: ColumnFormatConfig): string {
  const config = override ?? inferDefaultFormat(header);
  return formatNumber(value, config);
}

function getRowKey(row: Record<string, any>, rowHeaders: string[], idx: number): string {
  return `${idx}-${rowHeaders.map((h) => String(row[h])).join("|")}`;
}

export default function PivotTable({
  result,
  isLoading,
  formatOverrides,
  onFormatChange,
}: PivotTableProps) {
  const [menuKey, setMenuKey] = useState<string | null>(null);

  if (isLoading) {
    return <div className="loading">Generating pivot table...</div>;
  }
  if (!result) {
    return <div className="empty-state">Configure and run a pivot to see results here</div>;
  }

  const { data, row_meta, column_headers, row_headers, grand_total } = result;

  const headerGroups: { agg_label: string; span: number; key: string }[] = [];
  let i = 0;
  while (i < column_headers.length) {
    const current = column_headers[i].agg_label;
    let span = 1;
    while (i + span < column_headers.length && column_headers[i + span].agg_label === current) {
      span += 1;
    }
    headerGroups.push({ agg_label: current, span, key: `${current}-${i}` });
    i += span;
  }

  const showAggRow = column_headers.some((h) => h.column_values.length > 0);

  return (
    <div className="pivot-table-container">
      <table className="pivot-table">
        <thead>
          {showAggRow && (
            <tr>
              {row_headers.length > 0 && <th colSpan={row_headers.length}></th>}
              {headerGroups.map((g) => (
                <th key={g.key} colSpan={g.span}>
                  {g.agg_label}
                </th>
              ))}
            </tr>
          )}
          <tr>
            {row_headers.map((h) => (
              <th key={`rh-${h}`}>{h}</th>
            ))}
            {column_headers.map((ch) => {
              const override = formatOverrides?.[ch.key];
              const config = override ?? inferDefaultFormat(ch);
              return (
                <th
                  key={ch.key}
                  title={ch.agg_label}
                  style={{ position: "relative", cursor: onFormatChange ? "pointer" : undefined }}
                  onClick={() => onFormatChange && setMenuKey(menuKey === ch.key ? null : ch.key)}
                >
                  {ch.column_values.length > 0 ? ch.column_values.join(" / ") : ch.agg_label}
                  {onFormatChange && (
                    <span style={{ fontSize: "0.7em", marginLeft: "0.3em", color: "#a0aec0" }}>
                      ▾
                    </span>
                  )}
                  {menuKey === ch.key && onFormatChange && (
                    <FormatMenu
                      config={config}
                      onApply={(next) => {
                        onFormatChange(ch.key, next);
                        setMenuKey(null);
                      }}
                      onClose={() => setMenuKey(null)}
                    />
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIdx) => {
            const meta = row_meta[rowIdx];
            const isSubtotal = meta?.kind === RowKind.Subtotal;
            return (
              <tr
                key={getRowKey(row, row_headers, rowIdx)}
                style={
                  isSubtotal
                    ? { fontWeight: 600, background: "#f7fafc", borderTop: "1px solid #e2e8f0" }
                    : undefined
                }
              >
                {row_headers.map((h, hIdx) => {
                  const value = row[h];
                  const display =
                    value === null || value === undefined
                      ? isSubtotal && hIdx === (meta?.level ?? 0) + 1
                        ? "Subtotal"
                        : ""
                      : String(value);
                  return (
                    <th key={h} style={{ paddingLeft: `${0.5 + hIdx * 0.6}em` }}>
                      {display}
                    </th>
                  );
                })}
                {column_headers.map((ch) => (
                  <td key={ch.key}>
                    {formatCell(row[ch.key], ch, formatOverrides?.[ch.key])}
                  </td>
                ))}
              </tr>
            );
          })}

          {grand_total && (
            <tr
              className="grand-total-row"
              style={{ fontWeight: "bold", borderTop: "2px solid #333", background: "#edf2f7" }}
            >
              {row_headers.map((h, idx) => (
                <th key={h}>{idx === 0 ? grand_total.__total_label__ ?? "Grand Total" : ""}</th>
              ))}
              {column_headers.map((ch) => {
                const v = grand_total[ch.key];
                return (
                  <td key={ch.key} style={{ fontWeight: "bold" }}>
                    {v === undefined || v === null
                      ? "-"
                      : formatCell(v, ch, formatOverrides?.[ch.key])}
                  </td>
                );
              })}
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function FormatMenu({
  config,
  onApply,
  onClose,
}: {
  config: ColumnFormatConfig;
  onApply: (next: ColumnFormatConfig) => void;
  onClose: () => void;
}) {
  const [draft, setDraft] = useState<ColumnFormatConfig>(config);
  return (
    <div
      onClick={(e) => e.stopPropagation()}
      style={{
        position: "absolute",
        top: "100%",
        right: 0,
        zIndex: 10,
        background: "white",
        border: "1px solid #cbd5e0",
        borderRadius: "4px",
        padding: "0.5em",
        boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
        minWidth: "180px",
        textAlign: "left",
        fontWeight: 400,
        cursor: "auto",
      }}
    >
      <div style={{ fontSize: "0.85em", marginBottom: "0.3em" }}>Format</div>
      <select
        value={draft.format}
        onChange={(e) =>
          setDraft({ ...draft, format: e.target.value as ColumnFormatConfig["format"] })
        }
        style={{ width: "100%", marginBottom: "0.4em" }}
      >
        <option value="auto">Auto</option>
        <option value="number">Number</option>
        <option value="integer">Integer</option>
        <option value="currency">Currency</option>
        <option value="percent">Percent</option>
      </select>
      {(draft.format === "number" || draft.format === "currency" || draft.format === "percent") && (
        <label style={{ display: "block", marginBottom: "0.4em", fontSize: "0.85em" }}>
          Decimals
          <input
            type="number"
            min={0}
            max={6}
            value={draft.decimals ?? 2}
            onChange={(e) => setDraft({ ...draft, decimals: parseInt(e.target.value, 10) })}
            style={{ width: "100%" }}
          />
        </label>
      )}
      {draft.format === "currency" && (
        <label style={{ display: "block", marginBottom: "0.4em", fontSize: "0.85em" }}>
          Currency
          <input
            type="text"
            value={draft.currencyCode ?? "USD"}
            onChange={(e) => setDraft({ ...draft, currencyCode: e.target.value.toUpperCase() })}
            placeholder="USD"
            style={{ width: "100%" }}
          />
        </label>
      )}
      <div style={{ display: "flex", gap: "0.4em", justifyContent: "flex-end" }}>
        <button onClick={onClose}>Cancel</button>
        <button onClick={() => onApply(draft)}>Apply</button>
      </div>
    </div>
  );
}
