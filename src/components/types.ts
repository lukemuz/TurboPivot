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
  Var = "Var",
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
  In = "In",
  NotIn = "NotIn",
  Contains = "Contains",
  StartsWith = "StartsWith",
  EndsWith = "EndsWith",
  IsNull = "IsNull",
  IsNotNull = "IsNotNull",
  Between = "Between",
}

export interface FilterCondition {
  column: string;
  operator: FilterOperator;
  value: any;
}

export enum SortOrder {
  Ascending = "Ascending",
  Descending = "Descending",
}

export interface SortConfig {
  column: string;
  order: SortOrder;
}

export enum DateGranularity {
  Year = "Year",
  Quarter = "Quarter",
  Month = "Month",
  Week = "Week",
  Day = "Day",
  YearMonth = "YearMonth",
  YearQuarter = "YearQuarter",
}

export interface DateGroup {
  source_field: string;
  granularity: DateGranularity;
  alias: string;
}

export type DataSource =
  | { kind: "File"; path: string }
  | { kind: "Sqlite"; path: string; query: string }
  | { kind: "Postgres"; connection_string: string; query: string };

export enum ColumnDtype {
  String = "String",
  Integer = "Integer",
  Float = "Float",
  Boolean = "Boolean",
  Date = "Date",
  Datetime = "Datetime",
  Other = "Other",
}

export interface ColumnInfo {
  name: string;
  dtype: ColumnDtype;
}

export interface PivotRequest {
  data_path: string;
  source?: DataSource;
  rows: string[];
  columns: string[];
  values: ValueWithAggregation[];
  filters?: FilterCondition[];
  sort?: SortConfig;
  date_groups: DateGroup[];
  subtotals: boolean;
}

export interface ColumnHeader {
  key: string;
  label: string;
  agg_label: string;
  column_values: string[];
  value_field: string;
  aggregation: AggregationType;
}

export enum RowKind {
  Data = "Data",
  Subtotal = "Subtotal",
  GrandTotal = "GrandTotal",
}

export interface RowMetaEntry {
  kind: RowKind;
  level: number | null;
}

export interface PivotResult {
  data: Record<string, any>[];
  row_meta: RowMetaEntry[];
  column_headers: ColumnHeader[];
  row_headers: string[];
  grand_total?: Record<string, any>;
}

export type NumberFormat = "auto" | "number" | "currency" | "percent" | "integer";

export interface ColumnFormatConfig {
  format: NumberFormat;
  decimals?: number;
  currencyCode?: string;
}
