import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import PivotTable from './PivotTable';
import { PivotResult } from './types';

describe('PivotTable', () => {
  it('should display loading state', () => {
    render(<PivotTable result={null} isLoading={true} />);
    expect(screen.getByText(/Generating pivot table/i)).toBeInTheDocument();
  });

  it('should display empty state when no result', () => {
    render(<PivotTable result={null} isLoading={false} />);
    expect(screen.getByText(/Configure and run a pivot to see results here/i)).toBeInTheDocument();
  });

  it('should render pivot table with data', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'sum_Sales': 3000,
        },
        {
          Country: 'Canada',
          'sum_Sales': 4000,
        },
      ],
      column_headers: [['sum_Sales']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Check that table is rendered
    expect(screen.getByRole('table')).toBeInTheDocument();

    // Check row headers
    expect(screen.getByText('USA')).toBeInTheDocument();
    expect(screen.getByText('Canada')).toBeInTheDocument();

    // Check column headers
    expect(screen.getByText('sum_Sales')).toBeInTheDocument();

    // Check data values - formatted as currency since column name contains "Sales"
    expect(screen.getByText('$3,000.00')).toBeInTheDocument();
    expect(screen.getByText('$4,000.00')).toBeInTheDocument();
  });

  it('should format sales data as currency', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'sum_Sales': 1234567.89,
        },
      ],
      column_headers: [['sum_Sales']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Sales columns should be formatted as currency
    expect(screen.getByText('$1,234,567.89')).toBeInTheDocument();
  });

  it('should format non-currency numbers with thousands separator', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'sum_Population': 1234567.89,
        },
      ],
      column_headers: [['sum_Population']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Non-currency columns should be formatted with 2 decimals
    expect(screen.getByText('1,234,567.89')).toBeInTheDocument();
  });

  it('should format count data without decimals', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'count_Transactions': 1234567.89,
        },
      ],
      column_headers: [['count_Transactions']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Count columns should be formatted with no decimals
    expect(screen.getByText('1,234,568')).toBeInTheDocument();
  });

  it('should format percentage data correctly', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'mean_ConversionRate': 0.1234,
        },
      ],
      column_headers: [['mean_ConversionRate']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Percentage columns should be formatted as percentages
    expect(screen.getByText('12.3%')).toBeInTheDocument();
  });

  it('should handle null values', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'sum_Sales': null,
        },
      ],
      column_headers: [['sum_Sales']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    // Table should still render
    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(screen.getByText('USA')).toBeInTheDocument();
  });

  it('should render multiple row headers', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          Region: 'West',
          'sum_Sales': 1000,
        },
      ],
      column_headers: [['sum_Sales']],
      row_headers: ['Country', 'Region'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    expect(screen.getByText('USA')).toBeInTheDocument();
    expect(screen.getByText('West')).toBeInTheDocument();
  });

  it('should render multiple column headers', () => {
    const mockResult: PivotResult = {
      data: [
        {
          Country: 'USA',
          'sum_Sales_Widget': 1000,
          'sum_Sales_Gadget': 2000,
        },
      ],
      column_headers: [['sum_Sales_Widget', 'sum_Sales_Gadget']],
      row_headers: ['Country'],
    };

    render(<PivotTable result={mockResult} isLoading={false} />);

    expect(screen.getByText('sum_Sales_Widget')).toBeInTheDocument();
    expect(screen.getByText('sum_Sales_Gadget')).toBeInTheDocument();
  });
});
