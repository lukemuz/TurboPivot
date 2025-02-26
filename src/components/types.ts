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
  Contains = "Contains",
  In = "In"
}

export interface FilterCondition {
  column: string;
  operator: FilterOperator;
  value: any;
}

export interface PivotRequest {
  data_path: string;
  rows: string[];
  columns: string[];
  values: ValueWithAggregation[];
  filters?: FilterCondition[];
}

export interface PivotResult {
  data: Record<string, any>[];
  column_headers: string[][];
  row_headers: string[];
} 