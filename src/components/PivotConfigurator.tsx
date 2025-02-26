import { useState } from "react";
import { AggregationType, ValueWithAggregation } from "./types";

interface PivotConfiguratorProps {
  columns: string[];
  onConfigChange: (rows: string[], columns: string[], values: ValueWithAggregation[]) => void;
}

export default function PivotConfigurator({ columns, onConfigChange }: PivotConfiguratorProps) {
  const [rowFields, setRowFields] = useState<string[]>([]);
  const [columnFields, setColumnFields] = useState<string[]>([]);
  const [valueFields, setValueFields] = useState<ValueWithAggregation[]>([]);

  const handleDimensionChange = (field: string, dimension: "rows" | "columns" | "values") => {
    // Remove field from all dimensions first
    const newRowFields = rowFields.filter(f => f !== field);
    const newColumnFields = columnFields.filter(f => f !== field);
    const newValueFields = valueFields.filter(v => v.field !== field);

    // Then add to the selected dimension
    if (dimension === "rows") {
      newRowFields.push(field);
    } else if (dimension === "columns") {
      newColumnFields.push(field);
    } else if (dimension === "values") {
      newValueFields.push({
        field,
        aggregation: AggregationType.Sum // Default aggregation
      });
    }

    // Update state
    setRowFields(newRowFields);
    setColumnFields(newColumnFields);
    setValueFields(newValueFields);

    // Notify parent
    onConfigChange(newRowFields, newColumnFields, newValueFields);
  };

  const handleAggregationChange = (field: string, aggregation: AggregationType) => {
    const updatedValueFields = valueFields.map(v => 
      v.field === field ? { ...v, aggregation } : v
    );
    
    setValueFields(updatedValueFields);
    onConfigChange(rowFields, columnFields, updatedValueFields);
  };

  const getDimensionForField = (field: string): "rows" | "columns" | "values" | "none" => {
    if (rowFields.includes(field)) return "rows";
    if (columnFields.includes(field)) return "columns";
    if (valueFields.some(v => v.field === field)) return "values";
    return "none";
  };

  return (
    <div className="pivot-configurator">
      <h3>Configure Pivot Table</h3>
      
      <div className="column-selection">
        <h4>Available Fields</h4>
        <div className="fields-list">
          {columns.map(column => (
            <div key={column} className="field-item">
              <span>{column}</span>
              <div className="field-actions">
                <select 
                  value={getDimensionForField(column)}
                  onChange={e => handleDimensionChange(column, e.target.value as any)}
                >
                  <option value="none">Not used</option>
                  <option value="rows">Row</option>
                  <option value="columns">Column</option>
                  <option value="values">Value</option>
                </select>
                
                {getDimensionForField(column) === "values" && (
                  <select
                    value={valueFields.find(v => v.field === column)?.aggregation}
                    onChange={e => handleAggregationChange(
                      column, 
                      e.target.value as AggregationType
                    )}
                  >
                    <option value={AggregationType.Sum}>Sum</option>
                    <option value={AggregationType.Mean}>Mean</option>
                    <option value={AggregationType.Count}>Count</option>
                    <option value={AggregationType.Min}>Min</option>
                    <option value={AggregationType.Max}>Max</option>
                    <option value={AggregationType.Median}>Median</option>
                    <option value={AggregationType.Std}>Standard Deviation</option>
                    <option value={AggregationType.Var}>Variance</option>
                  </select>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
      
      <div className="pivot-summary">
        <div className="summary-section">
          <h4>Rows:</h4>
          <div className="selected-fields">
            {rowFields.length > 0 ? rowFields.join(', ') : 'None selected'}
          </div>
        </div>
        
        <div className="summary-section">
          <h4>Columns:</h4>
          <div className="selected-fields">
            {columnFields.length > 0 ? columnFields.join(', ') : 'None selected'}
          </div>
        </div>
        
        <div className="summary-section">
          <h4>Values:</h4>
          <div className="selected-fields">
            {valueFields.length > 0 
              ? valueFields.map(v => `${v.field} (${v.aggregation})`).join(', ') 
              : 'None selected'}
          </div>
        </div>
      </div>
    </div>
  );
} 