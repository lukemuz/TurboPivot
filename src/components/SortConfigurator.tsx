import { useState, useEffect } from "react";
import { SortConfig, SortOrder } from "./types";

interface SortConfiguratorProps {
  columns: string[];
  rowFields: string[];
  columnFields: string[];
  valueFields: string[];
  onSortChange: (sort: SortConfig | null) => void;
}

export default function SortConfigurator({
  columns,
  rowFields,
  columnFields,
  valueFields,
  onSortChange
}: SortConfiguratorProps) {
  const [sortColumn, setSortColumn] = useState<string>("");
  const [sortOrder, setSortOrder] = useState<SortOrder>(SortOrder.Ascending);

  // Get all available columns for sorting (row fields + value columns)
  const availableSortColumns = [
    ...rowFields,
    ...valueFields.map(v => `${v.split('(')[0].toLowerCase()}_${v.split('(')[1]?.replace(')', '') || v}`)
  ];

  useEffect(() => {
    if (sortColumn) {
      onSortChange({ column: sortColumn, order: sortOrder });
    } else {
      onSortChange(null);
    }
  }, [sortColumn, sortOrder, onSortChange]);

  const handleClearSort = () => {
    setSortColumn("");
    onSortChange(null);
  };

  return (
    <div className="sort-configurator">
      <div className="sort-header">
        <h3>Sorting</h3>
        {sortColumn && (
          <button
            onClick={handleClearSort}
            className="clear-sort-button"
            title="Clear Sort"
          >
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
            <optgroup label="Row Fields">
              {rowFields.map((field) => (
                <option key={field} value={field}>
                  {field}
                </option>
              ))}
            </optgroup>
            {valueFields.length > 0 && (
              <optgroup label="Value Columns">
                {valueFields.map((field) => {
                  // Parse the field to get the actual column name
                  // Format is typically "FieldName (Aggregation)"
                  const match = field.match(/(.+?)\s*\((.+)\)/);
                  if (match) {
                    const [, fieldName, aggType] = match;
                    const columnName = `${aggType.toLowerCase()}_${fieldName}`;
                    return (
                      <option key={columnName} value={columnName}>
                        {field}
                      </option>
                    );
                  }
                  return null;
                })}
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

      {sortColumn && (
        <div className="sort-summary">
          Sorting by <strong>{sortColumn}</strong> ({sortOrder === SortOrder.Ascending ? 'ascending' : 'descending'})
        </div>
      )}
    </div>
  );
}
