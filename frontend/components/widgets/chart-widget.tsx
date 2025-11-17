'use client'

import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { ArrowUpDown } from 'lucide-react'
import { fetchPivotData, type WidgetConfig } from '@/lib/api'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'

interface ChartWidgetProps {
  widget: WidgetConfig
}

export function ChartWidget({ widget }: ChartWidgetProps) {
  // Load metric definitions for formatting
  const { getMetricById } = usePivotMetrics(widget.table_id)
  // Check if widget has any dimensions (row or table dimensions)
  const hasDimensions = widget.dimensions.length > 0 || widget.table_dimensions.length > 0

  // Sorting state
  const [sortConfig, setSortConfig] = useState<{ metric: string; direction: 'asc' | 'desc' } | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: [
      'widget-chart',
      widget.id,
      widget.table_id,
      widget.dimensions,
      widget.table_dimensions,
      widget.metrics,
      widget.filters,
      widget.start_date,
      widget.end_date,
    ],
    queryFn: () =>
      fetchPivotData(
        [...widget.dimensions, ...widget.table_dimensions],
        {
          start_date: widget.start_date || undefined,
          end_date: widget.end_date || undefined,
          dimension_filters: widget.filters,
        },
        50,
        0,
        undefined, // dimensionValues
        widget.table_id // tableId - CRITICAL for multi-table support
      ),
    enabled: widget.metrics.length > 0 && hasDimensions,
  })

  if (!widget.metrics.length || !hasDimensions) {
    return (
      <div className="h-full flex items-center justify-center text-gray-400 text-sm">
        <div className="text-center">
          <p>No metrics or dimensions configured</p>
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

  // Format value based on metric type
  const formatValue = (value: number | null | undefined, metricId: string): string => {
    if (value == null || value === undefined || isNaN(value)) {
      return '-'
    }

    const metric = getMetricById(metricId)
    if (!metric) return value.toFixed(2)

    const decimals = metric.decimalPlaces ?? 2

    switch (metric.format) {
      case 'currency':
        return `$${value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`
      case 'percent':
        return `${(value * 100).toFixed(decimals)}%`
      default:
        return value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    }
  }

  // Custom tooltip with formatted values
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
          <p className="font-semibold text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {getMetricById(entry.dataKey)?.label || entry.dataKey}: {formatValue(entry.value, entry.dataKey)}
            </p>
          ))}
        </div>
      )
    }
    return null
  }

  // Transform and sort data for Recharts
  const chartData = useMemo(() => {
    if (!data) return []

    let transformedData = data.rows.slice(0, 20).map((row) => {
      const item: any = {
        name: row.dimension_value,
      }
      widget.metrics.forEach((metric) => {
        item[metric] = row.metrics[metric] || 0
      })
      return item
    })

    // Apply sorting if configured
    if (sortConfig) {
      transformedData = [...transformedData].sort((a, b) => {
        const aValue = a[sortConfig.metric] || 0
        const bValue = b[sortConfig.metric] || 0
        return sortConfig.direction === 'desc' ? bValue - aValue : aValue - bValue
      })
    }

    return transformedData
  }, [data, widget.metrics, sortConfig])

  const chartType = widget.chart_type || 'bar'
  const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899']

  // Get the first metric for Y-axis label (for single metric charts)
  const primaryMetric = widget.metrics[0]
  const primaryMetricDef = getMetricById(primaryMetric)

  // Handle sort toggle
  const handleSort = (metricId: string) => {
    if (sortConfig && sortConfig.metric === metricId) {
      // Toggle direction or clear
      if (sortConfig.direction === 'desc') {
        setSortConfig({ metric: metricId, direction: 'asc' })
      } else {
        setSortConfig(null)
      }
    } else {
      // Set new sort
      setSortConfig({ metric: metricId, direction: 'desc' })
    }
  }

  return (
    <div className="h-full flex flex-col p-2">
      {/* Sort controls */}
      <div className="flex items-center gap-2 mb-2 pb-2 border-b border-gray-200">
        <ArrowUpDown size={14} className="text-gray-400" />
        <span className="text-xs text-gray-500">Sort:</span>
        <div className="flex flex-wrap gap-1">
          {widget.metrics.map((metricId) => {
            const metricDef = getMetricById(metricId)
            const isActive = sortConfig && sortConfig.metric === metricId
            return (
              <button
                key={metricId}
                onClick={() => handleSort(metricId)}
                className={`px-2 py-0.5 rounded text-xs transition-colors ${
                  isActive
                    ? 'bg-purple-600 text-white'
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                }`}
              >
                {metricDef?.label || metricId}
                {isActive && (
                  <span className="ml-1">
                    {sortConfig.direction === 'desc' ? '↓' : '↑'}
                  </span>
                )}
              </button>
            )
          })}
        </div>
      </div>

      {/* Chart */}
      <div className="flex-1">
        <ResponsiveContainer width="100%" height="100%">
        {chartType === 'bar' ? (
          <BarChart data={chartData} margin={{ top: 10, right: 20, left: 20, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="name"
              angle={-45}
              textAnchor="end"
              height={80}
              tick={{ fontSize: 11 }}
            />
            <YAxis
              tick={{ fontSize: 11 }}
              label={primaryMetricDef ? {
                value: primaryMetricDef.label,
                angle: -90,
                position: 'insideLeft',
                style: { fontSize: 11 }
              } : undefined}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            {widget.metrics.map((metric, idx) => {
              const metricDef = getMetricById(metric)
              return (
                <Bar
                  key={metric}
                  dataKey={metric}
                  fill={colors[idx % colors.length]}
                  name={metricDef?.label || metric}
                />
              )
            })}
          </BarChart>
        ) : (
          <LineChart data={chartData} margin={{ top: 10, right: 20, left: 20, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="name"
              angle={-45}
              textAnchor="end"
              height={80}
              tick={{ fontSize: 11 }}
            />
            <YAxis
              tick={{ fontSize: 11 }}
              label={primaryMetricDef ? {
                value: primaryMetricDef.label,
                angle: -90,
                position: 'insideLeft',
                style: { fontSize: 11 }
              } : undefined}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            {widget.metrics.map((metric, idx) => {
              const metricDef = getMetricById(metric)
              return (
                <Line
                  key={metric}
                  type="monotone"
                  dataKey={metric}
                  stroke={colors[idx % colors.length]}
                  strokeWidth={2}
                  name={metricDef?.label || metric}
                  dot={{ r: 3 }}
                  activeDot={{ r: 5 }}
                />
              )
            })}
          </LineChart>
        )}
        </ResponsiveContainer>
      </div>
    </div>
  )
}
