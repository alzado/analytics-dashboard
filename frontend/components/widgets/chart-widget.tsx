'use client'

import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { ArrowUpDown, ArrowUp, ArrowDown, GripVertical, BarChart3, LineChart as LineChartIcon, ChevronDown, ChevronUp } from 'lucide-react'
import { fetchPivotData, fetchDimensionValues, type WidgetConfig } from '@/lib/api'
import type { RelativeDatePreset } from '@/lib/types'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'

interface ChartWidgetProps {
  widget: WidgetConfig
}

export function ChartWidget({ widget }: ChartWidgetProps) {
  // Load metric definitions for formatting
  const { getMetricById } = usePivotMetrics(widget.table_id)

  // Sorting state - supports value, diff, and %diff sorting
  const [sortConfig, setSortConfig] = useState<{
    column: number
    subColumn: 'value' | 'diff' | 'pctDiff'
    metric: string
    direction: 'asc' | 'desc'
  } | null>(widget.row_sort_config as any || null)

  // Table combinations state for multi-table mode
  const [tableCombinations, setTableCombinations] = useState<Array<Record<string, string>>>([])
  const [columnOrder, setColumnOrder] = useState<number[]>(widget.column_order || [])

  // Drag and drop state for column reordering
  const [draggedColumnIndex, setDraggedColumnIndex] = useState<number | null>(null)

  // Selected metric for display (only 1 metric shown in chart)
  const [selectedMetricForDisplay, setSelectedMetricForDisplay] = useState<string>(
    widget.visible_metrics && widget.visible_metrics.length > 0
      ? widget.visible_metrics[0]
      : widget.metrics[0]
  )

  // Chart type selection state (user can override widget.chart_type)
  const [chartType, setChartType] = useState<'bar' | 'line'>(widget.chart_type || 'bar')

  // Configuration panel expand/collapse state
  const [isConfigExpanded, setIsConfigExpanded] = useState<boolean>(true)

  // Check if widget has any dimensions (row or table dimensions)
  const hasDimensions = widget.dimensions.length > 0 || widget.table_dimensions.length > 0

  // Check if we're in multi-table mode
  const isMultiTable = widget.table_dimensions.length > 0

  // Visible metrics (for display) - only show the selected metric
  const visibleMetrics = [selectedMetricForDisplay]

  // Fetch data using two-step approach for multi-table charts
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
      tableCombinations,
      columnOrder,
    ],
    queryFn: async () => {
      if (!isMultiTable || tableCombinations.length === 0) {
        // Single-table mode: fetch normally
        return fetchPivotData(
          widget.dimensions,
          {
            start_date: widget.start_date || undefined,
            end_date: widget.end_date || undefined,
            date_range_type: widget.date_range_type || 'absolute',
            relative_date_preset: (widget.relative_date_preset as RelativeDatePreset | null | undefined) || undefined,
            dimension_filters: widget.filters,
          },
          50,
          0,
          undefined,
          widget.table_id,
          true, // skipCount
          widget.metrics // Pass selected metrics to backend
        )
      }

      // Multi-table mode: use two-step fetch
      const dims = widget.dimensions.length > 0 ? [widget.dimensions[0]] : []

      // Helper to build filters for a combination
      const buildTableFilters = (combination: Record<string, string>) => {
        const tableFilters: any = {
          start_date: widget.start_date || undefined,
          end_date: widget.end_date || undefined,
          date_range_type: widget.date_range_type || 'absolute',
          relative_date_preset: widget.relative_date_preset || undefined,
          dimension_filters: { ...widget.filters },
        }

        // Apply combination values to filters
        Object.entries(combination).forEach(([key, value]) => {
          if (!tableFilters.dimension_filters) {
            tableFilters.dimension_filters = {}
          }
          tableFilters.dimension_filters[key] = [value]
        })

        return tableFilters
      }

      // STEP 1: Fetch primary column
      const primaryColIndex = columnOrder.length > 0 ? columnOrder[0] : 0
      const primaryCombination = tableCombinations[primaryColIndex]
      const primaryFilters = buildTableFilters(primaryCombination)

      const primaryData = await fetchPivotData(
        dims,
        primaryFilters,
        50,
        0,
        undefined,
        widget.table_id,
        true, // skipCount
        widget.metrics // Pass selected metrics to backend
      )

      // STEP 2: Extract dimension values
      const dimensionValues = primaryData.rows.map(row => row.dimension_value)

      // STEP 3: Fetch remaining columns with same dimension values
      const allResults: any[] = new Array(tableCombinations.length)
      allResults[primaryColIndex] = primaryData

      const remainingFetches = tableCombinations
        .map((combination, index) => ({ combination, index }))
        .filter(({ index }) => index !== primaryColIndex)
        .map(async ({ combination, index }) => {
          const tableFilters = buildTableFilters(combination)
          const data = await fetchPivotData(
            dims,
            tableFilters,
            50,
            0,
            dimensionValues, // Use primary column's dimension values
            widget.table_id,
            true, // skipCount
            widget.metrics // Pass selected metrics to backend
          )
          return { index, data }
        })

      const remainingResults = await Promise.all(remainingFetches)
      remainingResults.forEach(({ index, data }) => {
        allResults[index] = data
      })

      // Combine all results into the format expected by chartData transformation
      // Merge rows from all columns, tagging each with its combination label
      const combinedRows: any[] = []
      allResults.forEach((colData, colIndex) => {
        const combination = tableCombinations[colIndex]
        const combinationLabel = Object.values(combination).join(' - ')

        colData.rows.forEach((row: any) => {
          combinedRows.push({
            ...row,
            dimension_value: `${row.dimension_value} - ${combinationLabel}`,
          })
        })
      })

      return { rows: combinedRows, total: allResults[0]?.total || {} }
    },
    enabled: widget.metrics.length > 0 && hasDimensions && (!isMultiTable || tableCombinations.length > 0),
  })

  // Build tableCombinations for multi-table mode
  useEffect(() => {
    if (!isMultiTable || widget.table_dimensions.length === 0) {
      setTableCombinations([])
      setColumnOrder([])
      return
    }

    // Fetch dimension values for each table dimension and build Cartesian product
    const buildCombinations = async () => {
      try {
        const dimensionValuesArrays: Array<Array<{ dimension_id: string; value: string }>> = []

        for (const dimId of widget.table_dimensions) {
          const values = await fetchDimensionValues(dimId, widget.filters, widget.table_id || undefined)
          const valueObjects = values.map(v => ({ dimension_id: dimId, value: v }))
          dimensionValuesArrays.push(valueObjects)
        }

        // Generate Cartesian product
        const combinations: Array<Record<string, string>> = []
        const generate = (current: Record<string, string>, depth: number) => {
          if (depth === dimensionValuesArrays.length) {
            combinations.push({ ...current })
            return
          }

          for (const item of dimensionValuesArrays[depth]) {
            current[item.dimension_id] = item.value
            generate(current, depth + 1)
          }
        }

        generate({}, 0)
        setTableCombinations(combinations)

        // Initialize column order if not already set
        if (!widget.column_order || widget.column_order.length === 0) {
          setColumnOrder(combinations.map((_, idx) => idx))
        }
      } catch (error) {
        console.error('Error building table combinations:', error)
        setTableCombinations([])
      }
    }

    buildCombinations()
  }, [isMultiTable, widget.table_dimensions, widget.filters, widget.table_id, widget.column_order])

  // Helper: Format combination label for display
  const formatCombinationLabel = (combination: Record<string, string>): string => {
    return Object.values(combination).join(' - ')
  }

  // Helper: Match row to combination
  const matchRowToCombination = (row: any, combination: Record<string, string>): boolean => {
    const parts = row.dimension_value.split(' - ')
    const tableParts = parts.slice(widget.dimensions.length)
    const combinationValues = Object.values(combination)

    if (tableParts.length !== combinationValues.length) return false

    for (let i = 0; i < tableParts.length; i++) {
      if (tableParts[i] !== combinationValues[i]) return false
    }

    return true
  }

  // Transform and sort data for Recharts - MUST be before early returns
  const chartData = useMemo(() => {
    if (!data) return []

    if (isMultiTable && tableCombinations.length > 0) {
      // Multi-table mode: Group by dimension, create separate series for each table dimension value
      // Extract unique base dimension values (rows)
      const dimensionValues = new Set<string>()
      data.rows.forEach(row => {
        const parts = row.dimension_value.split(' - ')
        const baseDimValue = parts.slice(0, widget.dimensions.length).join(' - ')
        dimensionValues.add(baseDimValue)
      })

      // Build chart data with one row per dimension value
      let chartRows = Array.from(dimensionValues).map(dimValue => {
        const item: any = { name: dimValue }

        // For each combination in column order, find the matching row
        columnOrder.forEach(colIndex => {
          const combination = tableCombinations[colIndex]
          if (!combination) return

          const combinationLabel = formatCombinationLabel(combination)

          // Find the row that matches this dimension value + combination
          const matchingRow = data.rows.find(row => {
            const parts = row.dimension_value.split(' - ')
            const rowBaseDim = parts.slice(0, widget.dimensions.length).join(' - ')
            return rowBaseDim === dimValue && matchRowToCombination(row, combination)
          })

          if (matchingRow) {
            widget.metrics.forEach(metric => {
              const seriesKey = `${combinationLabel}_${metric}`
              item[seriesKey] = matchingRow.metrics[metric] || 0
            })
          }
        })

        return item
      })

      // Apply sorting if configured
      if (sortConfig) {
        const sortMetric = sortConfig.metric
        const sortColIndex = sortConfig.column
        const sortSubColumn = sortConfig.subColumn

        chartRows = [...chartRows].sort((a, b) => {
          const combination = tableCombinations[sortColIndex]
          if (!combination) return 0

          const combinationLabel = formatCombinationLabel(combination)
          const seriesKey = `${combinationLabel}_${sortMetric}`

          let aValue: number | null = null
          let bValue: number | null = null

          if (sortSubColumn === 'value') {
            aValue = a[seriesKey] ?? null
            bValue = b[seriesKey] ?? null
          } else {
            // For diff and %diff, need first column as baseline
            const firstColIndex = columnOrder[0]
            const firstCombination = tableCombinations[firstColIndex]
            const firstSeriesKey = `${formatCombinationLabel(firstCombination)}_${sortMetric}`

            const aFirst = a[firstSeriesKey] ?? 0
            const bFirst = b[firstSeriesKey] ?? 0
            const aCurrent = a[seriesKey] ?? 0
            const bCurrent = b[seriesKey] ?? 0

            if (sortSubColumn === 'diff') {
              aValue = aCurrent - aFirst
              bValue = bCurrent - bFirst
            } else { // pctDiff
              aValue = aFirst !== 0 ? ((aCurrent / aFirst) - 1) * 100 : null
              bValue = bFirst !== 0 ? ((bCurrent / bFirst) - 1) * 100 : null
            }
          }

          // Handle nulls
          if (aValue === null && bValue === null) return 0
          if (aValue === null) return 1
          if (bValue === null) return -1

          return sortConfig.direction === 'desc' ? bValue - aValue : aValue - bValue
        })
      }

      return chartRows
    } else if (!isMultiTable) {
      // Single-table mode: Original logic
      let transformedData = data.rows.slice(0, 20).map((row) => {
        const item: any = {
          name: row.dimension_value,
        }
        widget.metrics.forEach((metric) => {
          item[metric] = row.metrics[metric] || 0
        })
        return item
      })

      // Apply sorting if configured (simple value-based sorting)
      if (sortConfig && sortConfig.subColumn === 'value') {
        transformedData = [...transformedData].sort((a, b) => {
          const aValue = a[sortConfig.metric] || 0
          const bValue = b[sortConfig.metric] || 0
          return sortConfig.direction === 'desc' ? bValue - aValue : aValue - bValue
        })
      }

      return transformedData
    }

    return []
  }, [data, widget.metrics, widget.dimensions.length, sortConfig, isMultiTable, tableCombinations, columnOrder])

  // Extract ordered table dimension combination labels (for multi-table series)
  const orderedCombinationLabels = useMemo(() => {
    if (!isMultiTable || tableCombinations.length === 0) return []

    return columnOrder.map(colIndex => {
      const combination = tableCombinations[colIndex]
      return combination ? formatCombinationLabel(combination) : ''
    }).filter(label => label !== '')
  }, [isMultiTable, tableCombinations, columnOrder])

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
          {payload.map((entry: any, index: number) => {
            // Extract metric ID from dataKey (handles both single-table and multi-table modes)
            // Multi-table format: "combinationLabel_metricId"
            // Single-table format: "metricId"
            // Note: metricId itself may contain underscores (e.g., "conversion_rate")
            // So we split only on the first underscore to separate combination from metric
            const metricId = entry.dataKey.includes('_')
              ? entry.dataKey.substring(entry.dataKey.indexOf('_') + 1)
              : entry.dataKey

            return (
              <p key={index} className="text-sm" style={{ color: entry.color }}>
                {entry.name}: {formatValue(entry.value, metricId)}
              </p>
            )
          })}
        </div>
      )
    }
    return null
  }

  const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899']

  // Get the selected metric for Y-axis label (updates when user changes display metric)
  const selectedMetricDef = getMetricById(selectedMetricForDisplay)

  // Handle sort toggle for multi-table charts
  const handleSort = (colIndex: number, subColumn: 'value' | 'diff' | 'pctDiff', metricId: string) => {
    if (sortConfig && sortConfig.column === colIndex && sortConfig.subColumn === subColumn && sortConfig.metric === metricId) {
      // Toggle direction or clear
      if (sortConfig.direction === 'desc') {
        setSortConfig({ column: colIndex, subColumn, metric: metricId, direction: 'asc' })
      } else {
        setSortConfig(null)
      }
    } else {
      // Set new sort
      setSortConfig({ column: colIndex, subColumn, metric: metricId, direction: 'desc' })
    }
  }

  // Handle simple sort for single-table charts
  const handleSimpleSort = (metricId: string) => {
    if (sortConfig && sortConfig.metric === metricId) {
      // Toggle direction or clear
      if (sortConfig.direction === 'desc') {
        setSortConfig({ column: 0, subColumn: 'value', metric: metricId, direction: 'asc' })
      } else {
        setSortConfig(null)
      }
    } else {
      // Set new sort
      setSortConfig({ column: 0, subColumn: 'value', metric: metricId, direction: 'desc' })
    }
  }

  // Drag and drop handlers for column reordering
  const handleDragStart = (e: React.DragEvent, columnIndex: number) => {
    setDraggedColumnIndex(columnIndex)
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }

  const handleDrop = (e: React.DragEvent, targetIndex: number) => {
    e.preventDefault()

    if (draggedColumnIndex === null || draggedColumnIndex === targetIndex) {
      setDraggedColumnIndex(null)
      return
    }

    // Reorder the columnOrder array
    const newOrder = [...columnOrder]
    const [removed] = newOrder.splice(draggedColumnIndex, 1)
    newOrder.splice(targetIndex, 0, removed)

    setColumnOrder(newOrder)
    setDraggedColumnIndex(null)
  }

  const handleDragEnd = () => {
    setDraggedColumnIndex(null)
  }

  return (
    <div className="h-full flex flex-col p-2">
      {/* Configuration Panel */}
      <div className="mb-2">
        {/* Collapsible header */}
        <button
          onClick={() => setIsConfigExpanded(!isConfigExpanded)}
          className="flex items-center gap-2 w-full px-2 py-1 hover:bg-gray-50 rounded transition-colors mb-2"
        >
          {isConfigExpanded ? (
            <ChevronUp size={16} className="text-gray-500" />
          ) : (
            <ChevronDown size={16} className="text-gray-500" />
          )}
          <span className="text-xs font-semibold text-gray-700">
            {isConfigExpanded ? 'Hide Configuration' : 'Show Configuration'}
          </span>
        </button>

        {/* Collapsible content */}
        {isConfigExpanded && (
          <div className="space-y-2">
            {/* Chart type and metric selector row */}
            <div className="flex items-center justify-between gap-4 pb-2 border-b border-gray-200">
              {/* Chart type selector */}
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500 font-semibold">Chart Type:</span>
                <div className="flex gap-1">
                  <button
                    onClick={() => setChartType('bar')}
                    className={`px-2 py-1 rounded text-xs transition-colors flex items-center gap-1 ${
                      chartType === 'bar'
                        ? 'bg-green-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                    title="Bar Chart"
                  >
                    <BarChart3 size={14} />
                    <span>Bar</span>
                  </button>
                  <button
                    onClick={() => setChartType('line')}
                    className={`px-2 py-1 rounded text-xs transition-colors flex items-center gap-1 ${
                      chartType === 'line'
                        ? 'bg-green-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                    title="Line Chart"
                  >
                    <LineChartIcon size={14} />
                    <span>Line</span>
                  </button>
                </div>
              </div>

              {/* Metric selector - choose which metric to display */}
              {widget.metrics.length > 1 && (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-500 font-semibold">Display Metric:</span>
                  <div className="flex gap-1">
                    {widget.metrics.map((metricId) => {
                      const metricDef = getMetricById(metricId)
                      const isSelected = selectedMetricForDisplay === metricId
                      return (
                        <button
                          key={metricId}
                          onClick={() => setSelectedMetricForDisplay(metricId)}
                          className={`px-2 py-1 rounded text-xs transition-colors ${
                            isSelected
                              ? 'bg-blue-600 text-white'
                              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                          }`}
                        >
                          {metricDef?.label || metricId}
                        </button>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>

            {/* Sort controls */}
            {isMultiTable && orderedCombinationLabels.length > 0 ? (
              // Multi-table sort controls with value/diff/%diff options
              <div className="flex flex-col gap-1 pb-2 border-b border-gray-200">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <ArrowUpDown size={14} className="text-gray-400" />
                    <span className="text-xs text-gray-500 font-semibold">Sort by Column:</span>
                  </div>
                  <div className="flex items-center gap-1 text-xs text-gray-500">
                    <GripVertical size={12} className="text-gray-400" />
                    <span>Drag to reorder</span>
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {orderedCombinationLabels.map((combinationLabel, colIdx) => {
                    const colIndex = columnOrder[colIdx]
                    const isDragging = draggedColumnIndex === colIdx
                    return (
                      <div
                        key={colIdx}
                        draggable
                        onDragStart={(e) => handleDragStart(e, colIdx)}
                        onDragOver={handleDragOver}
                        onDrop={(e) => handleDrop(e, colIdx)}
                        onDragEnd={handleDragEnd}
                        className={`flex flex-col gap-1 border border-gray-200 rounded p-1 transition-opacity cursor-move ${
                          isDragging ? 'opacity-50 bg-blue-100' : 'bg-gray-50'
                        }`}
                      >
                        <div className="flex items-center gap-1">
                          <GripVertical size={12} className="text-gray-400" />
                          <div className="text-xs font-semibold text-gray-700">{combinationLabel}</div>
                        </div>
                        <div className="flex gap-1">
                          {widget.metrics.map((metricId) => {
                            const metricDef = getMetricById(metricId)
                            const subColumns = colIdx === 0 ? ['value'] : ['value', 'diff', 'pctDiff']
                            return (
                              <div key={metricId} className="flex flex-col gap-0.5">
                                {subColumns.map((subCol) => {
                                  const isActive = sortConfig && sortConfig.column === colIndex && sortConfig.subColumn === subCol && sortConfig.metric === metricId
                                  const label = subCol === 'value' ? metricDef?.label || metricId : (subCol === 'diff' ? 'Δ' : 'Δ%')
                                  return (
                                    <button
                                      key={subCol}
                                      onClick={() => handleSort(colIndex, subCol as any, metricId)}
                                      className={`px-1.5 py-0.5 rounded text-xs transition-colors ${
                                        isActive
                                          ? 'bg-purple-600 text-white'
                                          : 'bg-white text-gray-700 hover:bg-gray-200'
                                      }`}
                                      title={`Sort by ${metricDef?.label || metricId} ${subCol === 'value' ? '' : subCol === 'diff' ? '(difference)' : '(% difference)'}`}
                                    >
                                      {label}
                                      {isActive && (
                                        <span className="ml-1">
                                          {sortConfig.direction === 'desc' ? '↓' : '↑'}
                                        </span>
                                      )}
                                    </button>
                                  )
                                })}
                              </div>
                            )
                          })}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : (
              // Simple sort controls for single-table charts
              <div className="flex items-center gap-2 pb-2 border-b border-gray-200">
                <ArrowUpDown size={14} className="text-gray-400" />
                <span className="text-xs text-gray-500">Sort:</span>
                <div className="flex flex-wrap gap-1">
                  {widget.metrics.map((metricId) => {
                    const metricDef = getMetricById(metricId)
                    const isActive = sortConfig && sortConfig.metric === metricId
                    return (
                      <button
                        key={metricId}
                        onClick={() => handleSimpleSort(metricId)}
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
            )}
          </div>
        )}
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
              tickFormatter={(value) => formatValue(value, selectedMetricForDisplay)}
              label={selectedMetricDef ? {
                value: selectedMetricDef.label,
                angle: -90,
                position: 'insideLeft',
                style: { fontSize: 11 }
              } : undefined}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            {isMultiTable ? (
              // Multi-table mode: Create series for each combination in column order (visible metrics only)
              orderedCombinationLabels.flatMap((combinationLabel, colIdx) =>
                visibleMetrics.map((metric) => {
                  const metricDef = getMetricById(metric)
                  const dataKey = `${combinationLabel}_${metric}`
                  const seriesName = `${combinationLabel} - ${metricDef?.label || metric}`
                  return (
                    <Bar
                      key={dataKey}
                      dataKey={dataKey}
                      fill={colors[colIdx % colors.length]}
                      name={seriesName}
                    />
                  )
                })
              )
            ) : (
              // Single-table mode: Original logic (visible metrics only)
              visibleMetrics.map((metric, idx) => {
                const metricDef = getMetricById(metric)
                return (
                  <Bar
                    key={metric}
                    dataKey={metric}
                    fill={colors[idx % colors.length]}
                    name={metricDef?.label || metric}
                  />
                )
              })
            )}
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
              tickFormatter={(value) => formatValue(value, selectedMetricForDisplay)}
              label={selectedMetricDef ? {
                value: selectedMetricDef.label,
                angle: -90,
                position: 'insideLeft',
                style: { fontSize: 11 }
              } : undefined}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            {isMultiTable ? (
              // Multi-table mode: Create series for each combination in column order (visible metrics only)
              orderedCombinationLabels.flatMap((combinationLabel, colIdx) =>
                visibleMetrics.map((metric) => {
                  const metricDef = getMetricById(metric)
                  const dataKey = `${combinationLabel}_${metric}`
                  const seriesName = `${combinationLabel} - ${metricDef?.label || metric}`
                  return (
                    <Line
                      key={dataKey}
                      type="monotone"
                      dataKey={dataKey}
                      stroke={colors[colIdx % colors.length]}
                      strokeWidth={2}
                      name={seriesName}
                      dot={false}
                      activeDot={{ r: 5 }}
                    />
                  )
                })
              )
            ) : (
              // Single-table mode: Original logic (visible metrics only)
              visibleMetrics.map((metric, idx) => {
                const metricDef = getMetricById(metric)
                return (
                  <Line
                    key={metric}
                    type="monotone"
                    dataKey={metric}
                    stroke={colors[idx % colors.length]}
                    strokeWidth={2}
                    name={metricDef?.label || metric}
                    dot={false}
                    activeDot={{ r: 5 }}
                  />
                )
              })
            )}
          </LineChart>
        )}
        </ResponsiveContainer>
      </div>
    </div>
  )
}
