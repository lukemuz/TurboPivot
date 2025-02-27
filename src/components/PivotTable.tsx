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
                    {typeof cellValue === 'number' 
                      ? cellValue.toLocaleString(undefined, { maximumFractionDigits: 2 })
                      : cellValue}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
} 