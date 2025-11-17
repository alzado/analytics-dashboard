'use client'

import React, { useState, useMemo } from 'react'
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { BarChart3, LineChart as LineChartIcon } from 'lucide-react'
import type { MetricDefinition } from '@/hooks/use-pivot-metrics'

interface PivotChartVisualizationProps {
  allColumnData: Record<string, any>
  selectedDimensions: string[]
  selectedMetrics: string[]
  tableCombinations: any[]
  getMetricById: (id: string) => MetricDefinition | undefined
  columnOrder: number[]
  tableHeaders: string[]
  sortedDimensionValues: string[]
  chartType: 'bar' | 'line' // Get from config
  setChartType: (type: 'bar' | 'line') => void // Update config
}

// Color palette for chart series
const CHART_COLORS = [
  '#3b82f6', // blue-500
  '#10b981', // green-500
  '#f59e0b', // amber-500
  '#ef4444', // red-500
  '#8b5cf6', // violet-500
  '#ec4899', // pink-500
  '#14b8a6', // teal-500
  '#f97316', // orange-500
]

export function PivotChartVisualization({
  allColumnData,
  selectedDimensions,
  selectedMetrics,
  tableCombinations,
  getMetricById,
  columnOrder,
  tableHeaders,
  sortedDimensionValues,
  chartType,
  setChartType,
}: PivotChartVisualizationProps) {
  const [showChart, setShowChart] = useState<boolean>(true)

  // Transform data for Recharts - one dataset per metric
  const chartDataByMetric = useMemo(() => {
    if (!allColumnData || Object.keys(allColumnData).length === 0 || !selectedMetrics || selectedMetrics.length === 0 || !sortedDimensionValues) {
      return {}
    }

    const dataByMetric: Record<string, any[]> = {}

    // Create a separate chart dataset for each selected metric
    selectedMetrics.forEach(metricId => {
      const transformedData = sortedDimensionValues.map((dimensionValue) => {
        const dataPoint: any = {
          dimension_value: dimensionValue,
        }

        // Add metric values from each column (series)
        columnOrder.forEach((columnIndex, seriesIndex) => {
          const columnData = allColumnData[columnIndex]
          if (columnData && columnData.rows) {
            // Find the row with matching dimension value
            const matchingRow = columnData.rows.find(
              (r: any) => r.dimension_value === dimensionValue
            )

            if (matchingRow && matchingRow.metrics) {
              // Use dynamic metrics dictionary
              const value = matchingRow.metrics[metricId]
              const seriesName = tableHeaders[seriesIndex] || `Series ${seriesIndex + 1}`
              dataPoint[seriesName] = value ?? null
            }
          }
        })

        return dataPoint
      })

      dataByMetric[metricId] = transformedData
    })

    return dataByMetric
  }, [allColumnData, selectedMetrics, columnOrder, tableHeaders, sortedDimensionValues])

  // Get series names (column headers)
  const seriesNames = useMemo(() => {
    return columnOrder.map((_, index) => tableHeaders[index] || `Series ${index + 1}`)
  }, [columnOrder, tableHeaders])

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

  // Custom tooltip factory - creates tooltip component for specific metric
  const createCustomTooltip = (metricId: string) => {
    return ({ active, payload, label }: any) => {
      if (active && payload && payload.length) {
        return (
          <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
            <p className="font-semibold text-gray-900 mb-2">{label}</p>
            {payload.map((entry: any, index: number) => (
              <p key={index} className="text-sm" style={{ color: entry.color }}>
                {entry.name}: {formatValue(entry.value, metricId)}
              </p>
            ))}
          </div>
        )
      }
      return null
    }
  }

  // Don't render if no data
  if (!allColumnData || Object.keys(allColumnData).length === 0) {
    return null
  }

  // Don't render if only one column (need multiple series for comparison)
  if (columnOrder.length < 2) {
    return null
  }

  return (
    <div className="mt-6 space-y-6">
      {/* Header with controls */}
      <div className="bg-white shadow rounded-lg p-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">
            Visualizations ({selectedMetrics.length} {selectedMetrics.length === 1 ? 'metric' : 'metrics'})
          </h3>

          {/* Chart Type Toggle */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowChart(!showChart)}
              className="px-3 py-1 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
            >
              {showChart ? 'Hide All' : 'Show All'}
            </button>
            <div className="flex gap-1 border border-gray-300 rounded-md p-1">
              <button
                onClick={() => setChartType('bar')}
                className={`p-2 rounded ${
                  chartType === 'bar'
                    ? 'bg-blue-500 text-white'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
                title="Bar Chart"
              >
                <BarChart3 size={18} />
              </button>
              <button
                onClick={() => setChartType('line')}
                className={`p-2 rounded ${
                  chartType === 'line'
                    ? 'bg-blue-500 text-white'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
                title="Line Chart"
              >
                <LineChartIcon size={18} />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Render one chart per selected metric */}
      {showChart && selectedMetrics.map((metricId) => {
        const chartData = chartDataByMetric[metricId] || []
        const selectedMetric = getMetricById(metricId)

        if (chartData.length === 0) {
          return null
        }

        return (
          <div key={metricId} className="bg-white shadow rounded-lg p-6">
            {/* Chart Header */}
            <div className="mb-4 border-b pb-3">
              <h4 className="text-md font-semibold text-gray-900">
                {selectedMetric?.label || metricId}
              </h4>
              {selectedMetric?.format && (
                <p className="text-xs text-gray-500 mt-1">
                  Format: {selectedMetric.format === 'currency' ? 'Currency ($)' : selectedMetric.format === 'percent' ? 'Percentage (%)' : 'Number'}
                </p>
              )}
            </div>

            {/* Chart */}
            <div className="w-full" style={{ height: 400 }}>
              <ResponsiveContainer width="100%" height="100%">
                {chartType === 'bar' ? (
                  <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="dimension_value"
                      angle={-45}
                      textAnchor="end"
                      height={100}
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis
                      tick={{ fontSize: 12 }}
                      label={{
                        value: selectedMetric?.label || metricId,
                        angle: -90,
                        position: 'insideLeft',
                      }}
                    />
                    <Tooltip content={createCustomTooltip(metricId)} />
                    <Legend wrapperStyle={{ paddingTop: '20px' }} />
                    {seriesNames.map((seriesName, index) => (
                      <Bar
                        key={seriesName}
                        dataKey={seriesName}
                        fill={CHART_COLORS[index % CHART_COLORS.length]}
                      />
                    ))}
                  </BarChart>
                ) : (
                  <LineChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="dimension_value"
                      angle={-45}
                      textAnchor="end"
                      height={100}
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis
                      tick={{ fontSize: 12 }}
                      label={{
                        value: selectedMetric?.label || metricId,
                        angle: -90,
                        position: 'insideLeft',
                      }}
                    />
                    <Tooltip content={createCustomTooltip(metricId)} />
                    <Legend wrapperStyle={{ paddingTop: '20px' }} />
                    {seriesNames.map((seriesName, index) => (
                      <Line
                        key={seriesName}
                        type="monotone"
                        dataKey={seriesName}
                        stroke={CHART_COLORS[index % CHART_COLORS.length]}
                        strokeWidth={2}
                        dot={{ r: 4 }}
                        activeDot={{ r: 6 }}
                      />
                    ))}
                  </LineChart>
                )}
              </ResponsiveContainer>
            </div>
          </div>
        )
      })}

      {/* No data message */}
      {showChart && Object.keys(chartDataByMetric).length === 0 && (
        <div className="bg-white shadow rounded-lg p-6">
          <div className="flex items-center justify-center py-12 text-gray-500">
            No data available for visualization
          </div>
        </div>
      )}
    </div>
  )
}
