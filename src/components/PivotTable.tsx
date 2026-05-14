import { ColumnHeader, PivotResult } from "./types";

interface PivotTableProps {
  result: PivotResult | null;
  isLoading: boolean;
}

function formatValue(value: any, header: ColumnHeader | null): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (typeof value !== "number") {
    return String(value);
  }

  const name = (header?.value_field ?? "").toLowerCase();
  const aggSnake = (header?.aggregation ?? "").toString().toLowerCase();

  if (name.includes("percent") || name.includes("rate") || name.includes("ratio")) {
    return `${(value * 100).toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  }

  if (
    name.includes("price") ||
    name.includes("cost") ||
    name.includes("sales") ||
    name.includes("revenue") ||
    name.includes("amount")
  ) {
    return value.toLocaleString(undefined, {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 2,
    });
  }

  if (aggSnake === "count" || name.includes("count") || name.includes("quantity")) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function getRowKey(row: Record<string, any>, rowHeaders: string[]): string {
  return rowHeaders.map((h) => String(row[h])).join("|");
}

export default function PivotTable({ result, isLoading }: PivotTableProps) {
  if (isLoading) {
    return <div className="loading">Generating pivot table...</div>;
  }

  if (!result) {
    return <div className="empty-state">Configure and run a pivot to see results here</div>;
  }

  const { data, column_headers, row_headers, grand_total } = result;

  // Group headers by aggregation+field for the top header row (only useful when
  // there is more than one value field or column field).
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
            {column_headers.map((ch) => (
              <th key={ch.key} title={ch.agg_label}>
                {ch.column_values.length > 0 ? ch.column_values.join(" / ") : ch.agg_label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row) => (
            <tr key={getRowKey(row, row_headers)}>
              {row_headers.map((h) => (
                <th key={h}>{row[h] ?? ""}</th>
              ))}
              {column_headers.map((ch) => (
                <td key={ch.key}>{formatValue(row[ch.key], ch)}</td>
              ))}
            </tr>
          ))}

          {grand_total && (
            <tr className="grand-total-row" style={{ fontWeight: "bold", borderTop: "2px solid #333" }}>
              {row_headers.map((h, idx) => (
                <th key={h}>{idx === 0 ? grand_total.__total_label__ ?? "Grand Total" : ""}</th>
              ))}
              {column_headers.map((ch) => {
                const v = grand_total[ch.key];
                return (
                  <td key={ch.key} style={{ fontWeight: "bold" }}>
                    {v === undefined || v === null ? "-" : formatValue(v, ch)}
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
