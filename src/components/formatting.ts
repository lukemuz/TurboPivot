import { AggregationType, ColumnFormatConfig, ColumnHeader, NumberFormat } from "./types";

export function inferDefaultFormat(header: ColumnHeader): ColumnFormatConfig {
  const name = (header.value_field ?? "").toLowerCase();
  if (
    header.aggregation === AggregationType.Count ||
    name.includes("count") ||
    name.includes("quantity")
  ) {
    return { format: "integer" };
  }
  if (name.includes("percent") || name.includes("rate") || name.includes("ratio")) {
    return { format: "percent", decimals: 1 };
  }
  if (
    name.includes("price") ||
    name.includes("cost") ||
    name.includes("sales") ||
    name.includes("revenue") ||
    name.includes("amount")
  ) {
    return { format: "currency", currencyCode: "USD", decimals: 2 };
  }
  return { format: "number", decimals: 2 };
}

export function formatNumber(value: any, config: ColumnFormatConfig): string {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value !== "number") return String(value);

  const fmt: NumberFormat = config.format;
  const decimals = config.decimals;

  switch (fmt) {
    case "integer":
      return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
    case "percent":
      return `${(value * 100).toLocaleString(undefined, {
        maximumFractionDigits: decimals ?? 1,
      })}%`;
    case "currency":
      return value.toLocaleString(undefined, {
        style: "currency",
        currency: config.currencyCode ?? "USD",
        maximumFractionDigits: decimals ?? 2,
      });
    case "number":
      return value.toLocaleString(undefined, {
        maximumFractionDigits: decimals ?? 2,
      });
    case "auto":
    default:
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
}
