import { useMemo } from "react";
import { ColumnDtype, ColumnInfo, FilterCondition, FilterOperator } from "./types";

interface FilterConfiguratorProps {
  columns: ColumnInfo[];
  filters: FilterCondition[];
  onFiltersChange: (filters: FilterCondition[]) => void;
}

const STRING_OPS: { op: FilterOperator; label: string }[] = [
  { op: FilterOperator.Equal, label: "Equals" },
  { op: FilterOperator.NotEqual, label: "Not equal" },
  { op: FilterOperator.Contains, label: "Contains" },
  { op: FilterOperator.StartsWith, label: "Starts with" },
  { op: FilterOperator.EndsWith, label: "Ends with" },
  { op: FilterOperator.In, label: "In list" },
  { op: FilterOperator.NotIn, label: "Not in list" },
  { op: FilterOperator.IsNull, label: "Is empty" },
  { op: FilterOperator.IsNotNull, label: "Is not empty" },
];

const NUMERIC_OPS: { op: FilterOperator; label: string }[] = [
  { op: FilterOperator.Equal, label: "=" },
  { op: FilterOperator.NotEqual, label: "≠" },
  { op: FilterOperator.GreaterThan, label: ">" },
  { op: FilterOperator.LessThan, label: "<" },
  { op: FilterOperator.GreaterThanOrEqual, label: "≥" },
  { op: FilterOperator.LessThanOrEqual, label: "≤" },
  { op: FilterOperator.Between, label: "Between" },
  { op: FilterOperator.In, label: "In list" },
  { op: FilterOperator.IsNull, label: "Is empty" },
  { op: FilterOperator.IsNotNull, label: "Is not empty" },
];

const DATE_OPS: { op: FilterOperator; label: string }[] = [
  { op: FilterOperator.Equal, label: "On" },
  { op: FilterOperator.LessThan, label: "Before" },
  { op: FilterOperator.GreaterThan, label: "After" },
  { op: FilterOperator.Between, label: "Between" },
  { op: FilterOperator.IsNull, label: "Is empty" },
  { op: FilterOperator.IsNotNull, label: "Is not empty" },
];

const BOOL_OPS: { op: FilterOperator; label: string }[] = [
  { op: FilterOperator.Equal, label: "Equals" },
  { op: FilterOperator.NotEqual, label: "Not equal" },
  { op: FilterOperator.IsNull, label: "Is empty" },
  { op: FilterOperator.IsNotNull, label: "Is not empty" },
];

function opsFor(dtype: ColumnDtype | undefined): { op: FilterOperator; label: string }[] {
  switch (dtype) {
    case ColumnDtype.Integer:
    case ColumnDtype.Float:
      return NUMERIC_OPS;
    case ColumnDtype.Date:
    case ColumnDtype.Datetime:
      return DATE_OPS;
    case ColumnDtype.Boolean:
      return BOOL_OPS;
    default:
      return STRING_OPS;
  }
}

function operatorTakesValue(op: FilterOperator): boolean {
  return op !== FilterOperator.IsNull && op !== FilterOperator.IsNotNull;
}

function operatorTakesArray(op: FilterOperator): boolean {
  return op === FilterOperator.In || op === FilterOperator.NotIn || op === FilterOperator.Between;
}

function parseValueForFilter(
  raw: string,
  op: FilterOperator,
  dtype: ColumnDtype | undefined
): any {
  if (!operatorTakesValue(op)) return null;
  if (operatorTakesArray(op)) {
    return raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
      .map((s) => coerce(s, dtype));
  }
  return coerce(raw, dtype);
}

function coerce(s: string, dtype: ColumnDtype | undefined): any {
  if (dtype === ColumnDtype.Integer || dtype === ColumnDtype.Float) {
    const n = Number(s);
    return Number.isNaN(n) ? s : n;
  }
  if (dtype === ColumnDtype.Boolean) {
    if (s.toLowerCase() === "true") return true;
    if (s.toLowerCase() === "false") return false;
  }
  return s;
}

function valueToInputString(value: any, op: FilterOperator): string {
  if (value === null || value === undefined) return "";
  if (operatorTakesArray(op) && Array.isArray(value)) {
    return value.join(", ");
  }
  return String(value);
}

export default function FilterConfigurator({
  columns,
  filters,
  onFiltersChange,
}: FilterConfiguratorProps) {
  const dtypeByName = useMemo(
    () => Object.fromEntries(columns.map((c) => [c.name, c.dtype])),
    [columns]
  );

  const addFilter = () => {
    if (columns.length === 0) return;
    const first = columns[0];
    onFiltersChange([
      ...filters,
      { column: first.name, operator: FilterOperator.Equal, value: "" },
    ]);
  };

  const removeFilter = (index: number) => {
    onFiltersChange(filters.filter((_, i) => i !== index));
  };

  const updateFilter = (index: number, patch: Partial<FilterCondition>) => {
    const next = filters.map((f, i) => (i === index ? { ...f, ...patch } : f));
    onFiltersChange(next);
  };

  return (
    <div className="filter-configurator">
      <div className="filter-header">
        <h3>Filters</h3>
        <button onClick={addFilter} className="add-filter-button" title="Add Filter">
          + Add Filter
        </button>
      </div>

      {filters.length === 0 ? (
        <div className="no-filters">No filters defined</div>
      ) : (
        <div className="filters-list">
          {filters.map((filter, index) => {
            const dtype = dtypeByName[filter.column];
            const ops = opsFor(dtype);
            const takesValue = operatorTakesValue(filter.operator);
            const takesArray = operatorTakesArray(filter.operator);

            return (
              <div key={index} className="filter-item">
                <select
                  value={filter.column}
                  onChange={(e) => {
                    const nextCol = e.target.value;
                    const nextDtype = dtypeByName[nextCol];
                    const allowed = opsFor(nextDtype).map((o) => o.op);
                    const nextOp = allowed.includes(filter.operator)
                      ? filter.operator
                      : allowed[0];
                    updateFilter(index, { column: nextCol, operator: nextOp });
                  }}
                >
                  {columns.map((c) => (
                    <option key={c.name} value={c.name}>
                      {c.name}
                    </option>
                  ))}
                </select>

                <select
                  value={filter.operator}
                  onChange={(e) =>
                    updateFilter(index, { operator: e.target.value as FilterOperator })
                  }
                >
                  {ops.map(({ op, label }) => (
                    <option key={op} value={op}>
                      {label}
                    </option>
                  ))}
                </select>

                {takesValue && (
                  <input
                    type={
                      dtype === ColumnDtype.Date
                        ? "date"
                        : dtype === ColumnDtype.Datetime
                        ? "datetime-local"
                        : "text"
                    }
                    value={valueToInputString(filter.value, filter.operator)}
                    onChange={(e) =>
                      updateFilter(index, {
                        value: parseValueForFilter(e.target.value, filter.operator, dtype),
                      })
                    }
                    placeholder={
                      takesArray
                        ? filter.operator === FilterOperator.Between
                          ? "low, high"
                          : "v1, v2, v3..."
                        : "value"
                    }
                  />
                )}

                <button
                  onClick={() => removeFilter(index)}
                  className="remove-filter-button"
                  title="Remove Filter"
                >
                  ×
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
