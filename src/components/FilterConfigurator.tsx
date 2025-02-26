import { useState } from "react";
import { FilterCondition, FilterOperator } from "./types";

interface FilterConfiguratorProps {
  columns: string[];
  onFiltersChange: (filters: FilterCondition[]) => void;
}

export default function FilterConfigurator({ columns, onFiltersChange }: FilterConfiguratorProps) {
  const [filters, setFilters] = useState<FilterCondition[]>([]);

  const addFilter = () => {
    if (columns.length === 0) return;
    
    const newFilter: FilterCondition = {
      column: columns[0],
      operator: FilterOperator.Equal,
      value: ""
    };
    
    const updatedFilters = [...filters, newFilter];
    setFilters(updatedFilters);
    onFiltersChange(updatedFilters);
  };

  const removeFilter = (index: number) => {
    const updatedFilters = filters.filter((_, i) => i !== index);
    setFilters(updatedFilters);
    onFiltersChange(updatedFilters);
  };

  const updateFilter = (index: number, field: keyof FilterCondition, value: any) => {
    const updatedFilters = [...filters];
    
    if (field === "operator") {
      updatedFilters[index].operator = value as FilterOperator;
    } else if (field === "column") {
      updatedFilters[index].column = value as string;
    } else if (field === "value") {
      // Try to parse numbers if possible
      if (!isNaN(Number(value)) && value.trim() !== "") {
        updatedFilters[index].value = Number(value);
      } else if (value.toLowerCase() === "true") {
        updatedFilters[index].value = true;
      } else if (value.toLowerCase() === "false") {
        updatedFilters[index].value = false;
      } else {
        // Handle comma-separated list for 'In' operator
        if (updatedFilters[index].operator === FilterOperator.In) {
          try {
            // Split by comma and trim each item
            const items = value.split(',').map((item: string) => item.trim());
            // Try to convert to numbers where possible
            const parsedItems = items.map((item: string) => {
              if (!isNaN(Number(item)) && item !== "") return Number(item);
              if (item.toLowerCase() === "true") return true;
              if (item.toLowerCase() === "false") return false;
              return item;
            });
            updatedFilters[index].value = parsedItems;
          } catch (e) {
            updatedFilters[index].value = value;
          }
        } else {
          updatedFilters[index].value = value;
        }
      }
    }

    setFilters(updatedFilters);
    onFiltersChange(updatedFilters);
  };

  return (
    <div className="filter-configurator">
      <div className="filter-header">
        <h3>Filters</h3>
        <button 
          onClick={addFilter}
          className="add-filter-button"
          title="Add Filter"
        >
          + Add Filter
        </button>
      </div>

      {filters.length === 0 ? (
        <div className="no-filters">No filters defined</div>
      ) : (
        <div className="filters-list">
          {filters.map((filter, index) => (
            <div key={index} className="filter-item">
              <select
                value={filter.column}
                onChange={(e) => updateFilter(index, "column", e.target.value)}
              >
                {columns.map((col) => (
                  <option key={col} value={col}>
                    {col}
                  </option>
                ))}
              </select>

              <select
                value={filter.operator}
                onChange={(e) => updateFilter(index, "operator", e.target.value)}
              >
                <option value={FilterOperator.Equal}>Equal to</option>
                <option value={FilterOperator.NotEqual}>Not equal to</option>
                <option value={FilterOperator.GreaterThan}>Greater than</option>
                <option value={FilterOperator.LessThan}>Less than</option>
                <option value={FilterOperator.GreaterThanOrEqual}>Greater than or equal</option>
                <option value={FilterOperator.LessThanOrEqual}>Less than or equal</option>
                <option value={FilterOperator.In}>In list</option>
              </select>

              <input
                type="text"
                value={
                  Array.isArray(filter.value) 
                    ? filter.value.join(", ") 
                    : filter.value?.toString() || ""
                }
                onChange={(e) => updateFilter(index, "value", e.target.value)}
                placeholder={
                  filter.operator === FilterOperator.In 
                    ? "Value1, Value2, Value3..." 
                    : "Value"
                }
              />

              <button
                onClick={() => removeFilter(index)}
                className="remove-filter-button"
                title="Remove Filter"
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