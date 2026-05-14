import { ColumnDtype, ColumnInfo, DateGranularity, DateGroup } from "./types";

interface DateGroupConfiguratorProps {
  columns: ColumnInfo[];
  dateGroups: DateGroup[];
  onChange: (groups: DateGroup[]) => void;
}

const GRANULARITIES: { value: DateGranularity; label: string }[] = [
  { value: DateGranularity.Year, label: "Year" },
  { value: DateGranularity.YearQuarter, label: "Year-Quarter" },
  { value: DateGranularity.YearMonth, label: "Year-Month" },
  { value: DateGranularity.Quarter, label: "Quarter (1-4)" },
  { value: DateGranularity.Month, label: "Month (1-12)" },
  { value: DateGranularity.Week, label: "Week (1-53)" },
  { value: DateGranularity.Day, label: "Day (1-31)" },
];

export default function DateGroupConfigurator({
  columns,
  dateGroups,
  onChange,
}: DateGroupConfiguratorProps) {
  const dateColumns = columns.filter(
    (c) => c.dtype === ColumnDtype.Date || c.dtype === ColumnDtype.Datetime
  );

  const addGroup = () => {
    if (dateColumns.length === 0) return;
    const src = dateColumns[0].name;
    onChange([
      ...dateGroups,
      {
        source_field: src,
        granularity: DateGranularity.Year,
        alias: `${src}_Year`,
      },
    ]);
  };

  const removeGroup = (idx: number) => {
    onChange(dateGroups.filter((_, i) => i !== idx));
  };

  const update = (idx: number, patch: Partial<DateGroup>) => {
    onChange(dateGroups.map((g, i) => (i === idx ? { ...g, ...patch } : g)));
  };

  if (dateColumns.length === 0) {
    return null;
  }

  return (
    <div className="date-group-configurator">
      <div className="filter-header">
        <h3>Date Groups</h3>
        <button onClick={addGroup} className="add-filter-button" title="Add Date Group">
          + Add
        </button>
      </div>
      {dateGroups.length === 0 ? (
        <div className="no-filters">No date groups</div>
      ) : (
        <div className="filters-list">
          {dateGroups.map((g, idx) => (
            <div key={idx} className="filter-item">
              <select
                value={g.source_field}
                onChange={(e) => {
                  const src = e.target.value;
                  const granLabel = GRANULARITIES.find((x) => x.value === g.granularity)?.label;
                  update(idx, {
                    source_field: src,
                    alias: `${src}_${granLabel ?? g.granularity}`,
                  });
                }}
              >
                {dateColumns.map((c) => (
                  <option key={c.name} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
              <select
                value={g.granularity}
                onChange={(e) => {
                  const granularity = e.target.value as DateGranularity;
                  const label = GRANULARITIES.find((x) => x.value === granularity)?.label;
                  update(idx, {
                    granularity,
                    alias: `${g.source_field}_${label ?? granularity}`,
                  });
                }}
              >
                {GRANULARITIES.map(({ value, label }) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
              <input
                type="text"
                value={g.alias}
                onChange={(e) => update(idx, { alias: e.target.value })}
                title="Virtual column name"
                placeholder="alias"
              />
              <button
                onClick={() => removeGroup(idx)}
                className="remove-filter-button"
                title="Remove"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
