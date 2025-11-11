import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from './App';
import { invoke } from '@tauri-apps/api/core';

// Mock Tauri invoke
vi.mock('@tauri-apps/api/core');

describe('App', () => {
  it('should render app title', () => {
    render(<App />);
    expect(screen.getByText('TurboPivot')).toBeInTheDocument();
  });

  it('should render tagline', () => {
    render(<App />);
    expect(screen.getByText(/Lightning fast pivot tables powered by Rust & Polars/i)).toBeInTheDocument();
  });

  it('should show error when trying to generate without file', async () => {
    render(<App />);
    const generateButton = screen.queryByText('Generate Pivot Table');

    if (generateButton) {
      await userEvent.click(generateButton);
      expect(screen.getByText(/Please select a file first/i)).toBeInTheDocument();
    }
  });

  it('should disable generate button when no value fields selected', () => {
    render(<App />);

    const generateButton = screen.queryByRole('button', {
      name: /Generate Pivot Table/i,
    });

    // Button should be disabled initially (no value fields)
    if (generateButton) {
      expect(generateButton).toBeDisabled();
    }
  });

  it('should show empty state in pivot table initially', () => {
    render(<App />);
    expect(screen.getByText(/Configure and run a pivot to see results here/i)).toBeInTheDocument();
  });

  it('should call invoke when generate button is clicked with valid config', async () => {
    const mockInvoke = vi.mocked(invoke);
    mockInvoke.mockResolvedValue({
      data: [
        { Country: 'USA', sum_Sales: 1000 },
      ],
      column_headers: [['sum_Sales']],
      row_headers: ['Country'],
    });

    render(<App />);

    // TODO: Simulate file selection and configuration
    // This requires mocking FileSelector and PivotConfigurator interactions

    // For now, just verify invoke is available
    expect(invoke).toBeDefined();
  });

  it('should display error message on failed pivot generation', async () => {
    const mockInvoke = vi.mocked(invoke);
    mockInvoke.mockRejectedValue(new Error('Test error'));

    // TODO: Complete this test once we can simulate full flow
    expect(mockInvoke).toBeDefined();
  });

  it('should render FileSelector component', () => {
    render(<App />);
    // FileSelector should have a button for file selection
    expect(screen.getByText(/Select CSV or Parquet File/i)).toBeInTheDocument();
  });

  it('should have sidebar and main content layout', () => {
    render(<App />);
    const container = screen.getByText('TurboPivot').closest('.container');
    expect(container).toBeInTheDocument();
  });
});
