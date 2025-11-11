import { PivotResult } from "./types";

interface PivotTableProps {
  result: PivotResult | null;
  isLoading: boolean;
}

export default function PivotTable({ result, isLoading }: PivotTableProps) {
  if (isLoading) {
    return <div className="loading">Generating pivot table...</div>;
  }

  if (!result) {
    return <div className="empty-state">Configure and run a pivot to see results here</div>;
  }

  // // Get unique values for row headers
  // const rowValues = result.data.map(row => {
  //   const rowKey: Record<string, any> = {};
  //   result.row_headers.forEach(header => {
  //     rowKey[header] = row[header];
  //   });
  //   return rowKey;
  // });

  // Helper to generate a unique key for each row
  const getRowKey = (row: Record<string, any>) => {
    return result.row_headers.map(header => String(row[header])).join('-');
  };

  // Helper to format numbers based on column type
  const formatValue = (value: any, columnName: string): string => {
    if (typeof value !== 'number') {
      return value || '';
    }

    // Detect format based on column name patterns
    const lowerName = columnName.toLowerCase();

    // Percentage format
    if (lowerName.includes('percent') || lowerName.includes('rate') || lowerName.includes('ratio')) {
      return `${(value * 100).toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
    }

    // Currency format
    if (lowerName.includes('price') || lowerName.includes('cost') || lowerName.includes('sales') ||
        lowerName.includes('revenue') || lowerName.includes('amount')) {
      return value.toLocaleString(undefined, {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 2
      });
    }

    // Count format (no decimals)
    if (lowerName.includes('count') || lowerName.includes('quantity')) {
      return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
    }

    // Default number format
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  };

  return (
    <div className="pivot-table-container">
      <table className="pivot-table">
        <thead>
          {result.column_headers.length > 0 && (
            <tr>
              {/* Empty cell for row headers */}
              {result.row_headers.length > 0 && (
                <th colSpan={result.row_headers.length}></th>
              )}
              
              {/* Column headers */}
              {result.column_headers[0].map((header, index) => (
                <th key={index}>{header}</th>
              ))}
            </tr>
          )}
        </thead>
        <tbody>
          {result.data.map((row, _rowIndex) => (
            <tr key={getRowKey(row)}>
              {/* Row headers */}
              {result.row_headers.map(header => (
                <th key={header}>{row[header]}</th>
              ))}
              
              {/* Data cells */}
              {result.column_headers[0].map((colHeader, colIndex) => {
                // Generate the key for this cell based on aggregation
                const valueKeys = Object.keys(row).filter(key => 
                  key.includes('_') && !result.row_headers.includes(key)
                );
                
                // Find matching value for this column
                const matchingKey = valueKeys.find(key => key.endsWith(colHeader));
                const cellValue = matchingKey ? row[matchingKey] : '';
                
                return (
                  <td key={colIndex}>
                    {formatValue(cellValue, colHeader)}
                  </td>
                );
              })}
            </tr>
          ))}

          {/* Grand Total Row */}
          {result.grand_total && (
            <tr className="grand-total-row" style={{ fontWeight: 'bold', borderTop: '2px solid #333' }}>
              {/* Grand total label */}
              {result.row_headers.map((header, index) => (
                <th key={header}>
                  {index === 0 ? (result.grand_total!.__total_label__ || 'Grand Total') : ''}
                </th>
              ))}

              {/* Grand total values */}
              {result.column_headers[0].map((colHeader, colIndex) => {
                const cellValue = result.grand_total![colHeader];
                return (
                  <td key={colIndex} style={{ fontWeight: 'bold' }}>
                    {cellValue !== undefined && cellValue !== null ? formatValue(cellValue, colHeader) : '-'}
                  </td>
                );
              })}
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
} 