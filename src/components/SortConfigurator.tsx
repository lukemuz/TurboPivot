import { useEffect, useState } from "react";
import { ColumnHeader, SortConfig, SortOrder } from "./types";

interface SortConfiguratorProps {
  rowFields: string[];
  resultColumnHeaders: ColumnHeader[];
  onSortChange: (sort: SortConfig | null) => void;
}

export default function SortConfigurator({
  rowFields,
  resultColumnHeaders,
  onSortChange,
}: SortConfiguratorProps) {
  const [sortColumn, setSortColumn] = useState<string>("");
  const [sortOrder, setSortOrder] = useState<SortOrder>(SortOrder.Ascending);

  useEffect(() => {
    if (sortColumn) {
      onSortChange({ column: sortColumn, order: sortOrder });
    } else {
      onSortChange(null);
    }
  }, [sortColumn, sortOrder, onSortChange]);

  // If the previously selected sort column disappears (e.g. after the user
  // changes the pivot config), clear it so we don't send an invalid request.
  useEffect(() => {
    if (!sortColumn) return;
    const valid =
      rowFields.includes(sortColumn) ||
      resultColumnHeaders.some((h) => h.key === sortColumn);
    if (!valid) {
      setSortColumn("");
    }
  }, [rowFields, resultColumnHeaders, sortColumn]);

  return (
    <div className="sort-configurator">
      <div className="sort-header">
        <h3>Sorting</h3>
        {sortColumn && (
          <button onClick={() => setSortColumn("")} className="clear-sort-button" title="Clear Sort">
            Clear
          </button>
        )}
      </div>

      <div className="sort-controls">
        <div className="sort-field">
          <label htmlFor="sort-column">Sort by:</label>
          <select
            id="sort-column"
            value={sortColumn}
            onChange={(e) => setSortColumn(e.target.value)}
          >
            <option value="">No sorting</option>
            {rowFields.length > 0 && (
              <optgroup label="Row Fields">
                {rowFields.map((field) => (
                  <option key={field} value={field}>
                    {field}
                  </option>
                ))}
              </optgroup>
            )}
            {resultColumnHeaders.length > 0 && (
              <optgroup label="Value Columns">
                {resultColumnHeaders.map((h) => (
                  <option key={h.key} value={h.key}>
                    {h.column_values.length > 0
                      ? `${h.agg_label} — ${h.column_values.join(" / ")}`
                      : h.agg_label}
                  </option>
                ))}
              </optgroup>
            )}
          </select>
        </div>

        {sortColumn && (
          <div className="sort-order">
            <label htmlFor="sort-order">Order:</label>
            <select
              id="sort-order"
              value={sortOrder}
              onChange={(e) => setSortOrder(e.target.value as SortOrder)}
            >
              <option value={SortOrder.Ascending}>Ascending (A-Z, 0-9)</option>
              <option value={SortOrder.Descending}>Descending (Z-A, 9-0)</option>
            </select>
          </div>
        )}
      </div>

      {resultColumnHeaders.length === 0 && (
        <div className="sort-hint" style={{ fontSize: "0.85em", color: "#666", marginTop: "0.5em" }}>
          Run the pivot once to enable sorting by value columns.
        </div>
      )}
    </div>
  );
}
