// Mirrors the backend enums and structs

export enum AggregationType {
  Sum = "Sum",
  Mean = "Mean",
  Count = "Count",
  Min = "Min",
  Max = "Max",
  First = "First",
  Last = "Last",
  Median = "Median",
  Std = "Std",
  Var = "Var"
}

export interface ValueWithAggregation {
  field: string;
  aggregation: AggregationType;
}

export enum FilterOperator {
  Equal = "Equal",
  NotEqual = "NotEqual",
  GreaterThan = "GreaterThan",
  LessThan = "LessThan",
  GreaterThanOrEqual = "GreaterThanOrEqual",
  LessThanOrEqual = "LessThanOrEqual",
  In = "In"
}

export interface FilterCondition {
  column: string;
  operator: FilterOperator;
  value: any;
}

export enum SortOrder {
  Ascending = "Ascending",
  Descending = "Descending"
}

export interface SortConfig {
  column: string;
  order: SortOrder;
}

export interface PivotRequest {
  data_path: string;
  rows: string[];
  columns: string[];
  values: ValueWithAggregation[];
  filters?: FilterCondition[];
  sort?: SortConfig;
}

export interface ColumnHeader {
  key: string;
  label: string;
  agg_label: string;
  column_values: string[];
  value_field: string;
  aggregation: AggregationType;
}

export interface PivotResult {
  data: Record<string, any>[];
  column_headers: ColumnHeader[];
  row_headers: string[];
  grand_total?: Record<string, any>;
}
