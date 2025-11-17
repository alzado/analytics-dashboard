'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchPivotData, type WidgetConfig } from '@/lib/api'

interface PivotTableWidgetProps {
  widget: WidgetConfig
}

export function PivotTableWidget({ widget }: PivotTableWidgetProps) {
  const { data, isLoading, error } = useQuery({
    queryKey: [
      'widget-pivot',
      widget.id,
      widget.table_id,
      widget.dimensions,
      widget.metrics,
      widget.filters,
      widget.start_date,
      widget.end_date,
    ],
    queryFn: () => {
      // Build query parameters with table_id
      const params = new URLSearchParams()

      // Add table_id for multi-table support
      if (widget.table_id) {
        params.append('table_id', widget.table_id)
      }

      // Add dimensions
      widget.dimensions.forEach(dim => params.append('dimensions', dim))
      widget.table_dimensions.forEach(dim => params.append('dimensions', dim))

      // Add date filters
      if (widget.start_date) params.append('start_date', widget.start_date)
      if (widget.end_date) params.append('end_date', widget.end_date)

      // Add dimension filters
      Object.entries(widget.filters).forEach(([key, values]) => {
        values.forEach(value => params.append(key, value))
      })

      params.append('limit', '20')
      params.append('offset', '0')

      return fetchPivotData(
        [...widget.dimensions, ...widget.table_dimensions],
        {
          start_date: widget.start_date || undefined,
          end_date: widget.end_date || undefined,
          dimension_filters: widget.filters,
        },
        20,
        0,
        undefined, // dimensionValues
        widget.table_id, // tableId - CRITICAL for multi-table support
        true, // skipCount
        widget.metrics // Pass selected metrics to optimize query
      )
    },
    enabled: widget.metrics.length > 0, // Only fetch if metrics are configured
  })

  if (!widget.metrics.length) {
    return (
      <div className="h-full flex items-center justify-center text-gray-400 text-sm">
        <div className="text-center">
          <p>No metrics configured</p>
          <p className="text-xs mt-1">Click the edit button to configure this widget</p>
        </div>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="h-full flex items-center justify-center text-red-500 text-sm">
        <div className="text-center">
          <p>Failed to load data</p>
          <p className="text-xs mt-1">{(error as Error).message}</p>
        </div>
      </div>
    )
  }

  if (!data || data.rows.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-gray-400 text-sm">
        No data available
      </div>
    )
  }

  // Get selected metrics to display
  const displayMetrics = widget.metrics.slice(0, 5) // Limit to 5 metrics for widget

  return (
    <div className="h-full overflow-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50 sticky top-0">
          <tr>
            {widget.dimensions.map((dim) => (
              <th
                key={dim}
                className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >
                {dim}
              </th>
            ))}
            {displayMetrics.map((metric) => (
              <th
                key={metric}
                className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider"
              >
                {metric}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.rows.slice(0, 10).map((row, idx) => (
            <tr key={idx} className="hover:bg-gray-50">
              <td className="px-3 py-2 text-sm text-gray-900 font-medium">
                {row.dimension_value}
              </td>
              {displayMetrics.map((metric) => (
                <td key={metric} className="px-3 py-2 text-sm text-gray-900 text-right">
                  {formatValue(row.metrics[metric], metric)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
        {data.total && (
          <tfoot className="bg-gray-100 sticky bottom-0">
            <tr>
              <td className="px-3 py-2 text-sm font-bold text-gray-900">Total</td>
              {displayMetrics.map((metric) => (
                <td key={metric} className="px-3 py-2 text-sm font-bold text-gray-900 text-right">
                  {formatValue(data.total.metrics[metric], metric)}
                </td>
              ))}
            </tr>
          </tfoot>
        )}
      </table>
    </div>
  )
}

function formatValue(value: number | undefined, metric: string): string {
  if (value === undefined || value === null) return '-'

  // Format based on metric name patterns
  if (metric.includes('rate') || metric.includes('cvr') || metric.includes('ctr') || metric.includes('percentage')) {
    return `${(value * 100).toFixed(2)}%`
  }
  if (metric.includes('revenue') || metric.includes('price') || metric.includes('aov')) {
    return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  }

  // Default number formatting
  if (value % 1 === 0) {
    return value.toLocaleString('en-US')
  }
  return value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
