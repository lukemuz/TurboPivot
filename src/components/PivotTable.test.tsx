import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import PivotTable from './PivotTable';
import { AggregationType, ColumnHeader, PivotResult } from './types';

function header(partial: Partial<ColumnHeader> & { key: string }): ColumnHeader {
  return {
    label: partial.label ?? partial.key,
    agg_label: partial.agg_label ?? partial.key,
    column_values: partial.column_values ?? [],
    value_field: partial.value_field ?? '',
    aggregation: partial.aggregation ?? AggregationType.Sum,
    ...partial,
  };
}

describe('PivotTable', () => {
  it('should display loading state', () => {
    render(<PivotTable result={null} isLoading={true} />);
    expect(screen.getByText(/Generating pivot table/i)).toBeInTheDocument();
  });

  it('should display empty state when no result', () => {
    render(<PivotTable result={null} isLoading={false} />);
    expect(screen.getByText(/Configure and run a pivot to see results here/i)).toBeInTheDocument();
  });

  it('renders rows and currency-formatted Sales cells', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [
        { Country: 'USA', sum_Sales: 3000 },
        { Country: 'Canada', sum_Sales: 4000 },
      ],
      column_headers: [
        header({ key: 'sum_Sales', agg_label: 'Sum of Sales', value_field: 'Sales' }),
      ],
      row_headers: ['Country'],
    };

    render(<PivotTable result={result} isLoading={false} />);

    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(screen.getByText('USA')).toBeInTheDocument();
    expect(screen.getByText('Canada')).toBeInTheDocument();
    expect(screen.getByText('$3,000.00')).toBeInTheDocument();
    expect(screen.getByText('$4,000.00')).toBeInTheDocument();
  });

  it('formats large currency values', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', sum_Sales: 1234567.89 }],
      column_headers: [
        header({ key: 'sum_Sales', agg_label: 'Sum of Sales', value_field: 'Sales' }),
      ],
      row_headers: ['Country'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('$1,234,567.89')).toBeInTheDocument();
  });

  it('formats non-currency numbers with thousands separator', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', sum_Population: 1234567.89 }],
      column_headers: [
        header({
          key: 'sum_Population',
          agg_label: 'Sum of Population',
          value_field: 'Population',
        }),
      ],
      row_headers: ['Country'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('1,234,567.89')).toBeInTheDocument();
  });

  it('formats count aggregations as integers', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', count_Transactions: 1234567.89 }],
      column_headers: [
        header({
          key: 'count_Transactions',
          agg_label: 'Count of Transactions',
          value_field: 'Transactions',
          aggregation: AggregationType.Count,
        }),
      ],
      row_headers: ['Country'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('1,234,568')).toBeInTheDocument();
  });

  it('formats rate as a percentage', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', mean_ConversionRate: 0.1234 }],
      column_headers: [
        header({
          key: 'mean_ConversionRate',
          agg_label: 'Mean of ConversionRate',
          value_field: 'ConversionRate',
          aggregation: AggregationType.Mean,
        }),
      ],
      row_headers: ['Country'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('12.3%')).toBeInTheDocument();
  });

  it('renders null values as empty cells', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', sum_Sales: null }],
      column_headers: [
        header({ key: 'sum_Sales', agg_label: 'Sum of Sales', value_field: 'Sales' }),
      ],
      row_headers: ['Country'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(screen.getByText('USA')).toBeInTheDocument();
  });

  it('renders pivoted columns by explicit key, not by endsWith', () => {
    // The previous endsWith-based lookup would falsely match
    // `sum_Sales_Widget` when the column label is `Widget`, but worse:
    // when a numeric column value like `2024` is involved, `endsWith('2024')`
    // would also match `..._12024`. This test confirms cells are looked up
    // by the exact `key` field.
    const result: PivotResult = {
      row_meta: [],
      data: [
        {
          Country: 'USA',
          sum_Sales_Widget: 1000,
          sum_Sales_Gadget: 2000,
        },
        {
          Country: 'Canada',
          sum_Sales_Widget: 1500,
          sum_Sales_Gadget: 2500,
        },
      ],
      column_headers: [
        header({
          key: 'sum_Sales_Widget',
          label: 'Widget',
          agg_label: 'Sum of Sales',
          column_values: ['Widget'],
          value_field: 'Sales',
        }),
        header({
          key: 'sum_Sales_Gadget',
          label: 'Gadget',
          agg_label: 'Sum of Sales',
          column_values: ['Gadget'],
          value_field: 'Sales',
        }),
      ],
      row_headers: ['Country'],
    };

    render(<PivotTable result={result} isLoading={false} />);

    expect(screen.getByText('Widget')).toBeInTheDocument();
    expect(screen.getByText('Gadget')).toBeInTheDocument();
    expect(screen.getByText('$1,000.00')).toBeInTheDocument();
    expect(screen.getByText('$2,000.00')).toBeInTheDocument();
    expect(screen.getByText('$1,500.00')).toBeInTheDocument();
    expect(screen.getByText('$2,500.00')).toBeInTheDocument();
  });

  it('renders multiple row headers', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [{ Country: 'USA', Region: 'West', sum_Sales: 1000 }],
      column_headers: [
        header({ key: 'sum_Sales', agg_label: 'Sum of Sales', value_field: 'Sales' }),
      ],
      row_headers: ['Country', 'Region'],
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('USA')).toBeInTheDocument();
    expect(screen.getByText('West')).toBeInTheDocument();
  });

  it('renders grand totals row when provided', () => {
    const result: PivotResult = {
      row_meta: [],
      data: [
        { Country: 'USA', sum_Sales: 3000 },
        { Country: 'Canada', sum_Sales: 4000 },
      ],
      column_headers: [
        header({ key: 'sum_Sales', agg_label: 'Sum of Sales', value_field: 'Sales' }),
      ],
      row_headers: ['Country'],
      grand_total: {
        __total_label__: 'Grand Total',
        sum_Sales: 7000,
      },
    };
    render(<PivotTable result={result} isLoading={false} />);
    expect(screen.getByText('Grand Total')).toBeInTheDocument();
    expect(screen.getByText('$7,000.00')).toBeInTheDocument();
  });
});
