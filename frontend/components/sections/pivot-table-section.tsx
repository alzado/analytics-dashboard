'use client'

import React, { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchPivotData, fetchPivotChildren, fetchDimensionValues, fetchCustomDimensions } from '@/lib/api'
import type { PivotRow, PivotChildRow, CustomDimension } from '@/lib/types'
import { ChevronRight, ChevronDown, Settings2, ArrowUp, ArrowDown } from 'lucide-react'
import { usePivotConfig } from '@/hooks/use-pivot-config'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'
import { useSchema } from '@/hooks/use-schema'
import { usePivotFilters } from '@/hooks/use-pivot-filters'
import type { MetricDefinition, DimensionDefinition } from '@/hooks/use-pivot-metrics'
import { PivotConfigPanel } from '@/components/pivot/pivot-config-panel'
import { PivotChartVisualization } from '@/components/pivot/pivot-chart-visualization'
import { PivotFilterPanel } from '@/components/pivot/pivot-filter-panel'

// Multi-Pivot Table Card Component
interface MultiPivotTableCardProps {
  headerLabel: string
  filters: any
  rowDimensions: string[]
  metricId: string
  limit: number
  getMetricById: (id: string) => MetricDefinition | undefined
  getDimensionByValue: (value: string) => DimensionDefinition | undefined
}

function MultiPivotTableCard({ headerLabel, filters, rowDimensions, metricId, limit, getMetricById, getDimensionByValue }: MultiPivotTableCardProps) {
  const metric = getMetricById(metricId)
  const [page, setPage] = useState(0)

  // Reset page when filters or dimensions change
  useEffect(() => {
    setPage(0)
  }, [headerLabel, rowDimensions, filters, limit])

  // Fetch data for this specific table
  const { data, isLoading, error } = useQuery({
    queryKey: ['multi-pivot', headerLabel, rowDimensions, filters, limit, page],
    queryFn: () => {
      const offset = page * limit
      // If no row dimensions, just show totals
      if (rowDimensions.length === 0) {
        return fetchPivotData([], filters, 1, 0)
      }
      // Otherwise fetch by first row dimension
      return fetchPivotData([rowDimensions[0]], filters, limit, offset)
    },
  })

  const formatMetricValue = (value: number | null | undefined): string => {
    if (value == null || value === undefined || isNaN(value)) {
      return '-'
    }

    if (!metric) return Math.round(value).toLocaleString()

    const decimals = metric.decimalPlaces ?? 0

    switch (metric.format) {
      case 'currency':
        return `$${value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`
      case 'percent':
        return `${(value * 100).toFixed(decimals)}%`
      default:
        return decimals > 0
          ? value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
          : Math.round(value).toLocaleString()
    }
  }

  return (
    <div className="bg-white shadow rounded-lg p-4 flex flex-col h-full">
      <h4 className="text-xs font-semibold text-gray-900 mb-3 border-b pb-2">{headerLabel}</h4>

      {isLoading ? (
        <div className="flex items-center justify-center py-8 text-gray-500 text-sm">
          Loading...
        </div>
      ) : error ? (
        <div className="text-red-600 text-xs py-4">Error loading data</div>
      ) : !data ? (
        <div className="text-gray-500 text-xs py-4">No data</div>
      ) : (
        <div className="flex-1 overflow-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-50 sticky top-0">
              <tr>
                <th className="px-2 py-1 text-left font-medium text-gray-500">
                  {rowDimensions.length > 0 ? getDimensionByValue(rowDimensions[0])?.label || 'Dimension' : 'Total'}
                </th>
                <th className="px-2 py-1 text-right font-medium text-gray-500">
                  {metric?.label || metricId}
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {data.rows.map((row: any, idx: number) => (
                <tr key={idx} className="hover:bg-gray-50">
                  <td className="px-2 py-1 text-gray-700">{row.dimension_value}</td>
                  <td className="px-2 py-1 text-right text-gray-900 font-medium">
                    {formatMetricValue(row.metrics?.[metricId])}
                  </td>
                </tr>
              ))}
              {data.rows.length === 0 && (
                <tr>
                  <td colSpan={2} className="px-2 py-4 text-center text-gray-500">
                    No data
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Pagination controls */}
      {data && data.total_count && data.total_count > limit && (
        <div className="mt-3 flex items-center justify-between border-t pt-3">
          <button
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
            className="px-3 py-1 text-xs font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Previous
          </button>
          <span className="text-xs text-gray-600">
            Page {page + 1} of {Math.ceil(data.total_count / limit)}
          </span>
          <button
            onClick={() => setPage(p => p + 1)}
            disabled={page >= Math.ceil(data.total_count / limit) - 1}
            className="px-3 py-1 text-xs font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Next
          </button>
        </div>
      )}
    </div>
  )
}

export function PivotTableSection() {
  const {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    setDataSourceDropped,
    setDateRangeDropped,
    addDimension,
    removeDimension,
    addTableDimension,
    removeTableDimension,
    addMetric,
    removeMetric,
    addFilter,
    removeFilter,
    resetToDefaults,
    reorderMetrics,
    setConfigOpen,
    setExpandedRows,
    setSelectedDisplayMetrics,
    toggleDisplayMetric,
    setSortConfig,
  } = usePivotConfig()

  // Load dynamic metrics and dimensions from schema
  const { metrics, dimensions, getMetricById, getDimensionByValue, isLoading: isLoadingSchema } = usePivotMetrics()
  const { schema } = useSchema()

  // Initialize pivot filters hook
  const {
    filters: pivotFilters,
    updateDimensionFilter,
    updateDateRange: updateFilterDateRange,
    clearDimensionFilters,
    toFilterParams,
  } = usePivotFilters({
    start_date: config.startDate,
    end_date: config.endDate,
  })

  // Sync date changes from config to filter state
  useEffect(() => {
    updateFilterDateRange(config.startDate, config.endDate)
  }, [config.startDate, config.endDate, updateFilterDateRange])

  // Get primary sort metric from schema (fallback to first base metric if not set)
  const primarySortMetric = schema?.primary_sort_metric || metrics[0]?.id

  // Get UI state from config (with defaults)
  const isConfigOpen = config.isConfigOpen ?? true
  const expandedRowsArray = config.expandedRows ?? []
  const expandedRows = new Set(expandedRowsArray)
  const selectedDisplayMetrics = config.selectedDisplayMetrics ?? (primarySortMetric ? [primarySortMetric] : (metrics[0]?.id ? [metrics[0].id] : []))
  const sortConfig = config.sortColumn !== undefined ? {
    column: config.sortColumn,
    subColumn: config.sortSubColumn,
    direction: config.sortDirection ?? 'desc',
    metric: config.sortMetric
  } : undefined

  // Fetch custom dimensions
  const { data: customDimensions } = useQuery({
    queryKey: ['custom-dimensions'],
    queryFn: fetchCustomDimensions,
  })

  // Helper function to get dimension label (handles both standard and custom dimensions)
  const getDimensionLabel = (dimensionId: string): string => {
    // First check standard dimensions
    const standardDim = getDimensionByValue(dimensionId)
    if (standardDim) {
      return standardDim.label
    }

    // Check if it's a custom dimension
    if (dimensionId.startsWith('custom_') && customDimensions) {
      const customDimId = dimensionId.replace('custom_', '')
      const customDim = customDimensions.find(d => d.id === customDimId)
      if (customDim) {
        return customDim.name
      }
    }

    // Fallback to the raw dimension ID
    return dimensionId
  }

  // Build complete filters from pivot filter hook
  const filters = useMemo(() => {
    return toFilterParams()
  }, [toFilterParams])

  const selectedDimensions = config.selectedDimensions || []
  const selectedTableDimensions = config.selectedTableDimensions || []
  const selectedMetrics = config.selectedMetrics || []
  const selectedTable = config.selectedTable
  const selectedDateRange = config.selectedDateRange
  const [childrenCache, setChildrenCache] = useState<Record<string, PivotChildRow[]>>({})
  const [currentPage, setCurrentPage] = useState<Record<string, number>>({})
  const [loadingRows, setLoadingRows] = useState<Set<string>>(new Set())
  const [cumulativePercentageCache, setCumulativePercentageCache] = useState<Record<string, number>>({}) // Tracks cumulative % at end of each page
  const [mergeThreshold, setMergeThreshold] = useState(0) // 0 means disabled
  const [isDragOver, setIsDragOver] = useState(false)
  const [isDragOverHeader, setIsDragOverHeader] = useState(false)
  const [isDragOverFirstColumn, setIsDragOverFirstColumn] = useState(false)
  const [dragOverColumn, setDragOverColumn] = useState<string | null>(null)
  const [combinationError, setCombinationError] = useState<string | null>(null)
  const [columnOrder, setColumnOrder] = useState<number[]>([])
  const [draggedColumnIndex, setDraggedColumnIndex] = useState<number | null>(null)
  const [dimensionSortOrder, setDimensionSortOrder] = useState<'asc' | 'desc'>('desc')
  const [childrenSortConfig, setChildrenSortConfig] = useState<{column: string, direction: 'asc' | 'desc'} | null>(null)
  const CHILDREN_PAGE_SIZE = 10

  // Check if we have required configuration
  const isConfigured = !!(config.isDataSourceDropped && config.isDateRangeDropped)

  // Fetch all dimension values in a single query
  const { data: allDimensionValues } = useQuery({
    queryKey: ['all-dimension-values', selectedTableDimensions, filters],
    queryFn: async () => {
      const results: Record<string, string[]> = {}
      for (const dimension of selectedTableDimensions) {
        results[dimension] = await fetchDimensionValues(dimension, filters)
      }
      return results
    },
    enabled: isConfigured && selectedTableDimensions.length > 0,
  })

  // Calculate table combinations (Cartesian product of all table dimension values)
  const tableCombinations = useMemo(() => {
    if (selectedTableDimensions.length === 0) {
      return [{}] // Single table with no additional filters
    }

    // Check if dimension values are loaded
    if (!allDimensionValues) {
      return []
    }

    // Get all dimension values
    const dimensionValueArrays = selectedTableDimensions.map(dim => allDimensionValues[dim] || [])

    // Calculate Cartesian product
    const cartesianProduct = (arrays: string[][]): string[][] => {
      if (arrays.length === 0) return [[]]
      if (arrays.length === 1) return arrays[0].map(v => [v])

      const [first, ...rest] = arrays
      const restProduct = cartesianProduct(rest)
      return first.flatMap(value =>
        restProduct.map(combination => [value, ...combination])
      )
    }

    const combinations = cartesianProduct(dimensionValueArrays)

    // Clear any previous error
    setCombinationError(null)

    // Convert to filter objects
    return combinations.map(combination => {
      const filterObj: Record<string, string> = {}
      selectedTableDimensions.forEach((dim, idx) => {
        filterObj[dim] = combination[idx]
      })
      return filterObj
    })
  }, [selectedTableDimensions, allDimensionValues])

  // Fetch data for all column combinations
  const { data: allColumnData, isLoading: isLoadingColumnData } = useQuery({
    queryKey: ['all-columns', tableCombinations, filters, selectedDimensions, customDimensions],
    queryFn: async () => {
      const results: Record<string, any> = {}
      let firstColumnDimensionValues: string[] | undefined = undefined

      for (let i = 0; i < tableCombinations.length; i++) {
        const combination = tableCombinations[i]

        // Start with base filters
        const tableFilters: Record<string, any> = { ...filters }

        // Convert combination values to appropriate types and handle custom dimensions
        Object.entries(combination).forEach(([key, value]) => {
          // Check if this is a custom dimension
          if (key.startsWith('custom_') && customDimensions) {
            const customDimId = key.replace('custom_', '')
            const customDim = customDimensions.find(d => d.id === customDimId)
            if (customDim) {
              // Find the value with this label
              const dimValue = customDim.values.find(v => v.label === value)
              if (dimValue) {
                // Override the date range with this custom dimension's date range
                tableFilters.start_date = dimValue.start_date
                tableFilters.end_date = dimValue.end_date
              }
            }
          } else {
            // Handle standard dimensions
            if (key === 'n_words_normalized' || key === 'n_attributes') {
              tableFilters[key] = parseInt(value as string, 10)
            } else {
              tableFilters[key] = value
            }
          }
        })

        const dims = selectedDimensions.length > 0 ? [selectedDimensions[0]] : []

        // First column: fetch top 50 normally
        if (i === 0) {
          results[i] = await fetchPivotData(dims, tableFilters, 50)
          // Extract dimension values for subsequent columns
          if (results[i]?.rows) {
            firstColumnDimensionValues = results[i].rows.map((row: any) => row.dimension_value)
          }
        } else {
          // Subsequent columns: fetch only the dimension values from first column
          results[i] = await fetchPivotData(dims, tableFilters, 50, 0, firstColumnDimensionValues)
        }
      }
      return results
    },
    enabled: isConfigured && selectedTableDimensions.length > 0 && tableCombinations.length > 0,
  })

  // Only query the first dimension for hierarchical drill-down
  const firstDimension = selectedDimensions.length > 0 ? [selectedDimensions[0]] : []

  const { data: pivotData, isLoading, error } = useQuery({
    queryKey: ['pivot', firstDimension, filters, selectedTable],
    queryFn: () => {
      // If no dimensions, create a single "All Data" row manually
      if (selectedDimensions.length === 0) {
        return fetchPivotData([], filters, 1).then(data => {
          // Return single row representing all data
          return {
            ...data,
            rows: data.rows.length > 0 ? [{
              ...data.total,
              dimension_value: 'All Data',
              has_children: false, // No drilling down when no dimensions selected
            }] : []
          }
        })
      }
      return fetchPivotData(firstDimension, filters, 50)
    },
    enabled: isConfigured, // Only fetch when data source and date range are configured
  })

  // Apply merge threshold logic - must be before conditional returns
  const processedRows = React.useMemo(() => {
    // In multi-table mode, use rows from the first column of allColumnData
    // In single-table mode, use rows from pivotData
    let rows: any[] = []
    if (selectedTableDimensions.length > 0 && allColumnData && columnOrder.length > 0) {
      // Multi-table mode: use first column data
      const firstColIndex = columnOrder[0]
      rows = allColumnData[firstColIndex]?.rows || []
    } else {
      // Single-table mode: use pivotData
      rows = pivotData?.rows || []
    }

    // Mark rows as having children ONLY if there are more dimensions to drill into (never show search terms)
    // Also convert backend percentage (0-100) to decimal (0-1) for consistent formatting
    const hasMoreDimensions = selectedDimensions.length > 1
    let rowsWithChildren = rows.map(row => ({
      ...row,
      has_children: hasMoreDimensions, // Only allow drilling to next dimension, never to search terms
      percentage_of_total: row.percentage_of_total / 100, // Convert from 0-100 to 0-1
    }))

    // Apply dimension sorting (alphabetically)
    if (selectedDimensions.length > 0) {
      rowsWithChildren.sort((a, b) => {
        const comparison = a.dimension_value.localeCompare(b.dimension_value)
        return dimensionSortOrder === 'asc' ? comparison : -comparison
      })
    }

    // For merge threshold, check which data source we're using
    const dataSource = (selectedTableDimensions.length > 0 && allColumnData && columnOrder.length > 0)
      ? allColumnData[columnOrder[0]]
      : pivotData

    if (!dataSource || mergeThreshold <= 0) {
      return rowsWithChildren
    }

    const mainRows: PivotRow[] = []
    const otherRows: PivotRow[] = []
    let cumulativePercentage = 0

    // Iterate through rows in order, tracking cumulative percentage
    rowsWithChildren.forEach(row => {
      // Check if adding this row would exceed the threshold
      if (cumulativePercentage < mergeThreshold) {
        mainRows.push(row)
        cumulativePercentage += row.percentage_of_total
      } else {
        // This row and all subsequent rows go into "Other"
        otherRows.push(row)
      }
    })

    // If there are rows to merge, create an "Other" row
    if (otherRows.length > 0) {
      // Sum up all metrics from other rows
      const aggregatedMetrics: Record<string, number> = {}
      otherRows.forEach(row => {
        Object.entries(row.metrics || {}).forEach(([key, value]) => {
          aggregatedMetrics[key] = (aggregatedMetrics[key] || 0) + (value || 0)
        })
      })

      const otherRow: PivotRow = {
        dimension_value: 'Other',
        metrics: aggregatedMetrics,
        percentage_of_total: otherRows.reduce((sum, r) => sum + r.percentage_of_total, 0),
        search_term_count: otherRows.reduce((sum, r) => sum + r.search_term_count, 0),
        has_children: false
      }

      return [...mainRows, otherRow]
    }

    return mainRows
  }, [pivotData, mergeThreshold, selectedDimensions, dimensionSortOrder, selectedTableDimensions, allColumnData, columnOrder])

  // Initialize column order when table combinations change
  React.useEffect(() => {
    if (tableCombinations.length > 0) {
      setColumnOrder(tableCombinations.map((_, idx) => idx))
    } else {
      // Reset column order when combinations become empty
      setColumnOrder([])
    }
  }, [tableCombinations])

  // Auto-sort columns by query volume when data loads
  React.useEffect(() => {
    if (allColumnData && Object.keys(allColumnData).length > 0) {
      // Sort column indices by primary sort metric (descending)
      const sortedIndices = Object.keys(allColumnData)
        .map(key => parseInt(key, 10))
        .sort((a, b) => {
          const valueA = allColumnData[a]?.total?.metrics?.[primarySortMetric] || 0
          const valueB = allColumnData[b]?.total?.metrics?.[primarySortMetric] || 0
          return valueB - valueA // Descending order
        })
      setColumnOrder(sortedIndices)
    }
  }, [allColumnData])

  // Clear cache when dimension changes
  React.useEffect(() => {
    setExpandedRows([])
    setChildrenCache({})
    setCurrentPage({})
    setLoadingRows(new Set())
  }, [selectedDimensions])

  // Handle drop events
  const handleDrop = (e: React.DragEvent, zone?: 'header' | 'firstColumn') => {
    e.preventDefault()
    setIsDragOver(false)
    setIsDragOverHeader(false)
    setIsDragOverFirstColumn(false)

    const type = e.dataTransfer.getData('type')

    switch (type) {
      case 'datasource':
        const table = e.dataTransfer.getData('table')
        updateTable(table)
        setDataSourceDropped(true)
        break

      case 'daterange':
        const startDate = e.dataTransfer.getData('startDate')
        const endDate = e.dataTransfer.getData('endDate')
        updateStartDate(startDate)
        updateEndDate(endDate)
        setDateRangeDropped(true)
        break

      case 'dimension':
        const dimensionValue = e.dataTransfer.getData('value')
        // If dropped on header row, add as table dimension
        // If dropped on first column or general area, add as row dimension
        if (zone === 'header') {
          addTableDimension(dimensionValue)
        } else {
          addDimension(dimensionValue)
        }
        break

      case 'metric':
        const metricId = e.dataTransfer.getData('id')
        addMetric(metricId)
        break

      default:
        break
    }
  }

  const handleDragOver = (e: React.DragEvent, zone?: 'header' | 'firstColumn') => {
    e.preventDefault()
    setIsDragOver(true)

    if (zone === 'header') {
      setIsDragOverHeader(true)
      setIsDragOverFirstColumn(false)
    } else if (zone === 'firstColumn') {
      setIsDragOverFirstColumn(true)
      setIsDragOverHeader(false)
    }
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
    setIsDragOverHeader(false)
    setIsDragOverFirstColumn(false)
  }

  // Helper functions to remove items
  const removeDataSource = () => {
    updateTable(null)
    setDataSourceDropped(false)
  }

  const removeDateRange = () => {
    updateStartDate(null)
    updateEndDate(null)
    setDateRangeDropped(false)
  }

  const toggleRow = async (dimensionPath: string, depth: number, grandTotal: number) => {
    const key = `depth${depth}:${dimensionPath}`

    if (expandedRows.has(key)) {
      // Collapse row
      const newExpanded = new Set(expandedRows)
      newExpanded.delete(key)
      setExpandedRows(Array.from(newExpanded))
    } else {
      // Expand row - show immediately and set loading state
      const newExpanded = new Set(expandedRows)
      newExpanded.add(key)
      setExpandedRows(Array.from(newExpanded))

      const newLoading = new Set(loadingRows)
      newLoading.add(key)
      setLoadingRows(newLoading)

      try {
        // Check if we're in multi-table mode (columns are table dimensions)
        const isMultiTableMode = selectedTableDimensions.length > 0

        if (isMultiTableMode) {
          // In multi-table mode, fetch search terms from FIRST column, but get values for ALL columns
          const firstColIndex = columnOrder[0] ?? 0

          // Fetch children for ALL columns
          for (let colIndex = 0; colIndex < tableCombinations.length; colIndex++) {
            const combination = tableCombinations[colIndex]
            const pageKey = `${key}:col_${colIndex}:0`

            if (!childrenCache[pageKey]) {
              // Start with base filters
              const columnFilters: Record<string, any> = { ...filters }

              // Handle combination values and custom dimensions
              Object.entries(combination).forEach(([dimKey, value]) => {
                // Check if this is a custom dimension
                if (dimKey.startsWith('custom_') && customDimensions) {
                  const customDimId = dimKey.replace('custom_', '')
                  const customDim = customDimensions.find(d => d.id === customDimId)
                  if (customDim) {
                    // Find the value with this label
                    const dimValue = customDim.values.find(v => v.label === value)
                    if (dimValue) {
                      // Override the date range with this custom dimension's date range
                      columnFilters.start_date = dimValue.start_date
                      columnFilters.end_date = dimValue.end_date
                    }
                  }
                } else {
                  // Handle standard dimensions
                  if (dimKey === 'n_words_normalized' || dimKey === 'n_attributes') {
                    columnFilters[dimKey] = parseInt(value as string, 10)
                  } else {
                    columnFilters[dimKey] = value
                  }
                }
              })

              // Extract the dimension value from the path
              const pathParts = dimensionPath.split(' - ')
              const dimensionValue = pathParts[pathParts.length - 1]
              const dimension = selectedDimensions[depth]

              // For first column, fetch the page size. For others, fetch a large limit to ensure we get all matching terms
              const fetchLimit = colIndex === firstColIndex ? CHILDREN_PAGE_SIZE : 1000

              // Fetch children with the combined filters
              const children = await fetchPivotChildren(dimension, dimensionValue, columnFilters, fetchLimit, 0)
              setChildrenCache(prev => ({ ...prev, [pageKey]: children }))

              // Store cumulative percentage only for first column
              if (colIndex === firstColIndex) {
                const cumulative = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
                setCumulativePercentageCache(prev => ({ ...prev, [key]: cumulative }))
              }
            }
          }
          setCurrentPage(prev => ({ ...prev, [key]: 0 }))
        } else {
          // Single-table mode (original logic)
          const pageKey = `${key}:0`
          if (!childrenCache[pageKey]) {
            // Special case: No dimensions selected, fetch all search terms
            if (selectedDimensions.length === 0) {
              const children = await fetchPivotChildren('', '', filters, CHILDREN_PAGE_SIZE, 0)
              setChildrenCache(prev => ({ ...prev, [pageKey]: children }))
              // Store cumulative percentage at end of this page
              const cumulative = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
              setCumulativePercentageCache(prev => ({ ...prev, [key]: cumulative }))
            } else {
              // Check if there are more dimensions to drill into
              const nextDepth = depth + 1
              if (nextDepth < selectedDimensions.length) {
                // Fetch next dimension level
                const dimensionsToFetch = selectedDimensions.slice(0, nextDepth + 1)
                const pivotChildren = await fetchPivotData(dimensionsToFetch, filters, 1000)

                // Filter to only children of this parent
                const prefix = `${dimensionPath} - `
                const childRows = pivotChildren.rows.filter(row => row.dimension_value.startsWith(prefix))

                // Calculate percentage relative to grand total (whole dataset)
                const filteredChildren = childRows.map(row => ({
                  search_term: row.dimension_value.replace(prefix, ''),
                  metrics: row.metrics,
                  percentage_of_total: grandTotal > 0 ? ((row.metrics?.[primarySortMetric] || 0) / grandTotal) : 0,
                }))

                setChildrenCache(prev => ({ ...prev, [pageKey]: filteredChildren }))
              } else {
                // No more dimensions - fetch search terms
                // Extract the value at each depth level from the path
                const pathParts = dimensionPath.split(' - ')
                const dimensionValue = pathParts[pathParts.length - 1]
                const dimension = selectedDimensions[depth]

                const children = await fetchPivotChildren(dimension, dimensionValue, filters, CHILDREN_PAGE_SIZE, 0)
                setChildrenCache(prev => ({ ...prev, [pageKey]: children }))
                // Store cumulative percentage at end of this page
                const cumulative = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
                setCumulativePercentageCache(prev => ({ ...prev, [key]: cumulative }))
              }
            }
            setCurrentPage(prev => ({ ...prev, [key]: 0 }))
          } else {
            setCurrentPage(prev => ({ ...prev, [key]: 0 }))
          }
        }
      } catch (error) {
        console.error('Failed to fetch children:', error)
        // Collapse on error
        const errorExpanded = new Set(expandedRows)
        errorExpanded.delete(key)
        setExpandedRows(Array.from(errorExpanded))
      } finally {
        // Remove loading state after fetch completes
        const doneLoading = new Set(loadingRows)
        doneLoading.delete(key)
        setLoadingRows(doneLoading)
      }
    }
  }

  const goToPage = async (dimensionPath: string, depth: number, page: number) => {
    const key = `depth${depth}:${dimensionPath}`

    const newLoading = new Set(loadingRows)
    newLoading.add(key)
    setLoadingRows(newLoading)

    try {
      const offset = page * CHILDREN_PAGE_SIZE
      const isMultiTableMode = selectedTableDimensions.length > 0

      if (isMultiTableMode) {
        // In multi-table mode, fetch for ALL columns but use first column's search terms
        const firstColIndex = columnOrder[0] ?? 0

        for (let colIndex = 0; colIndex < tableCombinations.length; colIndex++) {
          const colPageKey = `${key}:col_${colIndex}:${page}`

          if (!childrenCache[colPageKey]) {
            const combination = tableCombinations[colIndex]

            // Start with base filters
            const columnFilters: Record<string, any> = { ...filters }

            // Handle combination values and custom dimensions
            Object.entries(combination).forEach(([dimKey, value]) => {
              // Check if this is a custom dimension
              if (dimKey.startsWith('custom_') && customDimensions) {
                const customDimId = dimKey.replace('custom_', '')
                const customDim = customDimensions.find(d => d.id === customDimId)
                if (customDim) {
                  // Find the value with this label
                  const dimValue = customDim.values.find(v => v.label === value)
                  if (dimValue) {
                    // Override the date range with this custom dimension's date range
                    columnFilters.start_date = dimValue.start_date
                    columnFilters.end_date = dimValue.end_date
                  }
                }
              } else {
                // Handle standard dimensions
                if (dimKey === 'n_words_normalized' || dimKey === 'n_attributes') {
                  columnFilters[dimKey] = parseInt(value as string, 10)
                } else {
                  columnFilters[dimKey] = value
                }
              }
            })

            // Extract the dimension value from the path
            const pathParts = dimensionPath.split(' - ')
            const dimensionValue = pathParts[pathParts.length - 1]
            const dimension = selectedDimensions[depth]

            // For first column, fetch the page size. For others, fetch from offset 0 with large limit to ensure we get all matching terms
            const fetchLimit = colIndex === firstColIndex ? CHILDREN_PAGE_SIZE : 1000
            const fetchOffset = colIndex === firstColIndex ? offset : 0

            // Fetch children with the combined filters
            const children = await fetchPivotChildren(dimension, dimensionValue, columnFilters, fetchLimit, fetchOffset)
            setChildrenCache(prev => ({ ...prev, [colPageKey]: children }))
          }
        }
      } else {
        // Single-table mode
        const pageKey = `${key}:${page}`

        if (!childrenCache[pageKey]) {
          // Special case: No dimensions selected, fetch all search terms
          if (selectedDimensions.length === 0) {
            const children = await fetchPivotChildren('', '', filters, CHILDREN_PAGE_SIZE, offset)
            setChildrenCache(prev => ({ ...prev, [pageKey]: children }))

            // Calculate cumulative percentage including previous pages
            let previousCumulative = 0
            if (page > 0) {
              // Sum up cumulative percentages from all previous pages
              for (let p = 0; p < page; p++) {
                const prevPageKey = `${key}:${p}`
                const prevChildren = childrenCache[prevPageKey] || []
                previousCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
              }
            }
            const currentPageTotal = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
            const totalCumulative = previousCumulative + currentPageTotal
            setCumulativePercentageCache(prev => ({ ...prev, [key]: totalCumulative }))
          } else {
            // Extract the value at the current depth level from the path
            const pathParts = dimensionPath.split(' - ')
            const dimensionValue = pathParts[pathParts.length - 1]
            const dimension = selectedDimensions[depth]

            const children = await fetchPivotChildren(dimension, dimensionValue, filters, CHILDREN_PAGE_SIZE, offset)
            setChildrenCache(prev => ({ ...prev, [pageKey]: children }))

            // Calculate cumulative percentage including previous pages
            let previousCumulative = 0
            if (page > 0) {
              // Sum up cumulative percentages from all previous pages
              for (let p = 0; p < page; p++) {
                const prevPageKey = `${key}:${p}`
                const prevChildren = childrenCache[prevPageKey] || []
                previousCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
              }
            }
            const currentPageTotal = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
            const totalCumulative = previousCumulative + currentPageTotal
            setCumulativePercentageCache(prev => ({ ...prev, [key]: totalCumulative }))
          }
        }
      }

      setCurrentPage(prev => ({ ...prev, [key]: page }))
    } catch (error) {
      console.error('Failed to fetch page:', error)
    } finally {
      const doneLoading = new Set(loadingRows)
      doneLoading.delete(key)
      setLoadingRows(doneLoading)
    }
  }

  // Helper to format value based on metric format
  const formatMetricValue = (value: number | null | undefined, metricId: string): string => {
    // Handle null/undefined values
    if (value == null || value === undefined || isNaN(value)) {
      return '-'
    }

    const metric = getMetricById(metricId)

    if (!metric) {
      return Math.round(value).toLocaleString()
    }

    const decimals = metric.decimalPlaces ?? 0

    switch (metric.format) {
      case 'currency':
        return `$${value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`
      case 'percent':
        return `${(value * 100).toFixed(decimals)}%`
      default:
        return decimals > 0
          ? value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
          : Math.round(value).toLocaleString()
    }
  }

  // Reset sort when dimensions change
  useEffect(() => {
    setSortConfig(undefined as any, undefined, undefined)
  }, [selectedDimensions, selectedTableDimensions, selectedMetrics, setSortConfig])

  // Handle column sort click
  const handleSort = (column: string | number, subColumn?: 'value' | 'diff' | 'pctDiff', metric?: string) => {
    // If clicking same column/subColumn/metric, toggle direction or clear
    if (sortConfig && sortConfig.column === column && sortConfig.subColumn === subColumn && sortConfig.metric === metric) {
      if (sortConfig.direction === 'desc') {
        setSortConfig(column, subColumn, 'asc', metric)
      } else {
        // Clear sort
        setSortConfig(undefined as any, undefined, undefined, undefined)
      }
    } else {
      // New column or metric, start with descending (highest first for numbers)
      setSortConfig(column, subColumn, 'desc', metric || selectedDisplayMetrics[0] || primarySortMetric)
    }
  }

  // Sort rows based on current sort config
  const sortRows = (rows: PivotRow[]): PivotRow[] => {
    if (!sortConfig) return rows

    const sorted = [...rows].sort((a, b) => {
      let aValue: number | null = null
      let bValue: number | null = null

      if (selectedTableDimensions.length === 0) {
        // Single-table mode - sort by metric
        const metricId = sortConfig.column as string
        aValue = a.metrics?.[metricId] ?? null
        bValue = b.metrics?.[metricId] ?? null
      } else {
        // Multi-table mode - sort by column value/diff/pctDiff
        const columnIndex = sortConfig.column as number
        const columnData = allColumnData?.[columnIndex]

        if (!columnData) return 0

        const aRow = columnData.rows.find((r: any) => r.dimension_value === a.dimension_value)
        const bRow = columnData.rows.find((r: any) => r.dimension_value === b.dimension_value)

        // Use configured sort metric, or fall back to first selected metric
        const sortMetric = sortConfig.metric || selectedDisplayMetrics[0] || primarySortMetric

        if (sortConfig.subColumn === 'value') {
          aValue = aRow?.metrics?.[sortMetric] ?? null
          bValue = bRow?.metrics?.[sortMetric] ?? null
        } else if (sortConfig.subColumn === 'diff' || sortConfig.subColumn === 'pctDiff') {
          // Calculate diff/pctDiff for sorting
          const firstColIndex = columnOrder[0]
          const firstColData = allColumnData?.[firstColIndex]

          const aFirstRow = firstColData?.rows.find((r: any) => r.dimension_value === a.dimension_value)
          const bFirstRow = firstColData?.rows.find((r: any) => r.dimension_value === b.dimension_value)

          const aFirstValue = aFirstRow?.metrics?.[sortMetric] ?? null
          const bFirstValue = bFirstRow?.metrics?.[sortMetric] ?? null
          const aCurrentValue = aRow?.metrics?.[sortMetric] ?? null
          const bCurrentValue = bRow?.metrics?.[sortMetric] ?? null

          if (sortConfig.subColumn === 'diff') {
            aValue = (aCurrentValue ?? 0) - (aFirstValue ?? 0)
            bValue = (bCurrentValue ?? 0) - (bFirstValue ?? 0)
          } else {  // pctDiff
            aValue = (aFirstValue ?? 0) !== 0 ? (((aCurrentValue ?? 0) / (aFirstValue ?? 0)) - 1) * 100 : null
            bValue = (bFirstValue ?? 0) !== 0 ? (((bCurrentValue ?? 0) / (bFirstValue ?? 0)) - 1) * 100 : null
          }
        }
      }

      // Handle null values (push to bottom)
      if (aValue === null && bValue === null) return 0
      if (aValue === null) return 1
      if (bValue === null) return -1

      // Sort based on direction
      const comparison = aValue - bValue
      return sortConfig.direction === 'asc' ? comparison : -comparison
    })

    return sorted
  }

  // Handle children column sort click
  const handleChildrenSort = (column: string) => {
    if (childrenSortConfig && childrenSortConfig.column === column) {
      // Toggle direction or clear
      if (childrenSortConfig.direction === 'desc') {
        setChildrenSortConfig({ column, direction: 'asc' })
      } else {
        setChildrenSortConfig(null)
      }
    } else {
      // New column, start with descending
      setChildrenSortConfig({ column, direction: 'desc' })
    }
  }

  // Sort children rows
  const sortChildren = (children: PivotChildRow[]): PivotChildRow[] => {
    if (!childrenSortConfig) return children

    const sorted = [...children].sort((a, b) => {
      let aValue: any = null
      let bValue: any = null

      if (childrenSortConfig.column === 'dimension') {
        // Sort by dimension value (search_term or next dimension)
        aValue = (a as any).search_term || (a as any).dimension_value || ''
        bValue = (b as any).search_term || (b as any).dimension_value || ''
        const comparison = aValue.toString().localeCompare(bValue.toString())
        return childrenSortConfig.direction === 'asc' ? comparison : -comparison
      } else {
        // Sort by metric
        aValue = (a as any).metrics?.[childrenSortConfig.column] ?? (a as any)[childrenSortConfig.column] ?? null
        bValue = (b as any).metrics?.[childrenSortConfig.column] ?? (b as any)[childrenSortConfig.column] ?? null

        if (aValue === null && bValue === null) return 0
        if (aValue === null) return 1
        if (bValue === null) return -1

        const comparison = aValue - bValue
        return childrenSortConfig.direction === 'asc' ? comparison : -comparison
      }
    })

    return sorted
  }

  // Helper to get value from row by metric ID
  const getRowValue = (row: PivotRow | typeof pivotData.total, metricId: string): number => {
    return (row as any).metrics?.[metricId] ?? 0
  }

  // Recursive helper to render a row and its children
  const renderRow = (
    child: PivotChildRow,
    parentPath: string,
    depth: number,
    indentLevel: number,
    siblings: PivotChildRow[],
    indexInSiblings: number,
    grandTotal: number,
    allSiblingsUpToThis: PivotChildRow[]
  ): React.ReactNode => {
    const dimensionPath = parentPath ? `${parentPath} - ${child.search_term}` : child.search_term
    const rowKey = `depth${depth}:${dimensionPath}`
    const isExpanded = expandedRows.has(rowKey)
    const isLoading = loadingRows.has(rowKey)
    const currentPageNum = currentPage[rowKey] || 0
    const pageKey = `${rowKey}:${currentPageNum}`
    const children = childrenCache[pageKey] || []

    // Check if this row can have children (more dimensions to drill into)
    // depth 0 = first dimension, depth 1 = second dimension, etc.
    // Can have children if there's another dimension level below this one
    const canHaveChildren = depth < selectedDimensions.length - 1
    const bgColor = indentLevel % 2 === 1 ? 'bg-blue-50' : 'bg-blue-100'

    // Calculate cumulative percentage relative to grand total (all siblings up to and including this row)
    // Get cumulative from previous pages using the parent's row key
    const parentRowKey = `depth${depth}:${parentPath}`
    const parentPageNum = currentPage[parentRowKey] || 0
    let previousPagesCumulative = 0
    if (parentPageNum > 0) {
      // Sum up all items from previous pages
      for (let p = 0; p < parentPageNum; p++) {
        const prevPageKey = `${parentRowKey}:${p}`
        const prevChildren = childrenCache[prevPageKey] || []
        previousPagesCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
      }
    }
    const currentPageCumulative = allSiblingsUpToThis
      .reduce((sum, sib) => sum + ((sib as any).percentage_of_total || 0), 0)
    const cumulativePercentage = previousPagesCumulative + currentPageCumulative

    return (
      <React.Fragment key={dimensionPath}>
        <tr
          className={`${bgColor} cursor-pointer hover:opacity-80`}
          onClick={() => canHaveChildren && toggleRow(dimensionPath, depth, grandTotal)}
        >
          <td className={`px-6 py-4 whitespace-nowrap text-sm text-gray-700`} style={{paddingLeft: `${indentLevel * 2 + 1.5}rem`}}>
            <div className="flex items-center gap-2">
              {canHaveChildren && (
                isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
              )}
              <span>{child.search_term}</span>
            </div>
          </td>
          {selectedMetrics.map((metricId) => {
            let value = (child as any).metrics?.[metricId]

            // Handle cumulative percentage specially
            if (metricId === 'cumulative_percentage') {
              value = cumulativePercentage
            }

            return (
              <td
                key={metricId}
                className={`px-6 py-4 whitespace-nowrap text-sm text-gray-700 text-right`}
              >
                {value != null && value !== undefined ? formatMetricValue(value, metricId) : '-'}
              </td>
            )
          })}
        </tr>
        {isExpanded && (
          <>
            {isLoading && children.length === 0 ? (
              <tr className={bgColor}>
                <td colSpan={selectedMetrics.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                  Loading...
                </td>
              </tr>
            ) : (
              <>
                {children.map((grandchild, idx) => {
                  // Get all children from previous pages
                  let allPreviousPagesChildren: PivotChildRow[] = []
                  if (currentPageNum > 0) {
                    for (let p = 0; p < currentPageNum; p++) {
                      const prevPageKey = `${rowKey}:${p}`
                      const prevChildren = childrenCache[prevPageKey] || []
                      allPreviousPagesChildren = [...allPreviousPagesChildren, ...prevChildren]
                    }
                  }
                  // Add children from current page up to this index
                  const allChildrenUpToThis = [...allPreviousPagesChildren, ...children.slice(0, idx + 1)]
                  return renderRow(grandchild, dimensionPath, depth + 1, indentLevel + 1, children, idx, grandTotal, allChildrenUpToThis)
                })}
                {children.length > 0 && children.length === CHILDREN_PAGE_SIZE && !canHaveChildren && (
                  <tr className={bgColor}>
                    <td colSpan={selectedMetrics.length + 1} className="px-6 py-4">
                      {isLoading ? (
                        <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                          Loading...
                        </div>
                      ) : (
                        <div className="flex items-center justify-center gap-4">
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              const currentPageNum = currentPage[rowKey] || 0
                              if (currentPageNum > 0) {
                                goToPage(dimensionPath, depth, currentPageNum - 1)
                              }
                            }}
                            disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                          >
                            Previous
                          </button>
                          <span className="text-sm text-gray-600">
                            Page {(currentPage[rowKey] || 0) + 1}
                          </span>
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              const currentPageNum = currentPage[rowKey] || 0
                              goToPage(dimensionPath, depth, currentPageNum + 1)
                            }}
                            disabled={children.length < CHILDREN_PAGE_SIZE}
                            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                          >
                            Next
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </>
            )}
          </>
        )}
      </React.Fragment>
    )
  }

  // Show empty state if not configured
  if (!isConfigured) {
    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Pivot Table</h2>
            <p className="text-sm text-gray-600 mt-1">Configure your data source to get started</p>
          </div>
          <button
            onClick={() => setConfigOpen(!isConfigOpen)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isConfigOpen
                ? 'bg-gray-200 text-gray-700'
                : 'bg-blue-600 text-white hover:bg-blue-700'
            }`}
          >
            <Settings2 className="h-4 w-4" />
            {isConfigOpen ? 'Hide' : 'Configure'}
          </button>
        </div>

        <div className="flex gap-4">
          <PivotConfigPanel
            isOpen={isConfigOpen}
            onClose={() => setConfigOpen(false)}
            config={config}
            updateTable={updateTable}
            updateDateRange={updateDateRange}
            updateStartDate={updateStartDate}
            updateEndDate={updateEndDate}
            setDataSourceDropped={setDataSourceDropped}
            setDateRangeDropped={setDateRangeDropped}
            addDimension={addDimension}
            removeDimension={removeDimension}
            addMetric={addMetric}
            removeMetric={removeMetric}
            addFilter={addFilter}
            removeFilter={removeFilter}
            resetToDefaults={resetToDefaults}
            dimensionFilters={pivotFilters.dimension_filters}
            onDimensionFilterChange={updateDimensionFilter}
            onClearDimensionFilters={clearDimensionFilters}
            currentFilters={filters}
          />

          <div
            className={`flex-1 min-w-0 bg-white shadow overflow-hidden rounded-lg transition-all ${
              isDragOver ? 'ring-4 ring-blue-400 bg-blue-50' : ''
            }`}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
          >
            <div className="flex flex-col items-center justify-center h-96 text-center p-8">
              <div className={`mb-4 transition-colors ${isDragOver ? 'text-blue-600' : 'text-gray-400'}`}>
                <Settings2 className="h-16 w-16 mx-auto" />
              </div>
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                {isDragOver ? 'Drop here to configure' : 'Drop to Configure Pivot Table'}
              </h3>
              <p className="text-sm text-gray-600 max-w-md mb-4">
                {isDragOver
                  ? 'Release to add this configuration'
                  : 'Drag and drop data source, date range, dimensions, and metrics from the sidebar'}
              </p>
              <div className="text-xs text-gray-500 space-y-2">
                <div className="flex items-center gap-2 justify-center">
                  <span className={config.isDataSourceDropped ? 'text-green-600' : 'text-gray-400'}>
                    {config.isDataSourceDropped ? '✓' : '○'}
                  </span>
                  <span>Data Source</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={config.isDateRangeDropped ? 'text-green-600' : 'text-gray-400'}>
                    {config.isDateRangeDropped ? '✓' : '○'}
                  </span>
                  <span>Date Range</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={selectedDimensions.length > 0 ? 'text-green-600' : 'text-gray-400'}>
                    {selectedDimensions.length > 0 ? '✓' : '○'}
                  </span>
                  <span>Dimensions ({selectedDimensions.length})</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={selectedMetrics.length > 0 ? 'text-green-600' : 'text-gray-400'}>
                    {selectedMetrics.length > 0 ? '✓' : '○'}
                  </span>
                  <span>Metrics ({selectedMetrics.length})</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading pivot table
      </div>
    )
  }

  // Show loading state while schema is being fetched
  if (isLoadingSchema) {
    return (
      <div className="flex items-center justify-center p-12">
        <div className="text-gray-500">Loading schema...</div>
      </div>
    )
  }

  if (!pivotData) {
    return null
  }

  return (
    <div className="space-y-4 h-full">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Pivot Table</h2>
          <p className="text-sm text-gray-600 mt-1">
            {selectedDimensions.length > 0 ? (
              <>
                Grouped by: <span className="font-medium">
                  {selectedDimensions.map(dim => getDimensionLabel(dim)).join(' > ')}
                </span>
              </>
            ) : (
              <span className="font-medium">Showing aggregated totals</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Multi-Metric Selector for Multi-Table Mode */}
          {selectedTableDimensions.length > 0 && (
            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600">Display Metrics:</label>
              <div className="relative">
                <select
                  multiple
                  value={selectedDisplayMetrics}
                  onChange={(e) => {
                    const selected = Array.from(e.target.selectedOptions, option => option.value)
                    if (selected.length > 0) {
                      setSelectedDisplayMetrics(selected)
                    }
                  }}
                  className="px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm min-w-[200px] max-h-[120px]"
                  size={Math.min(5, metrics.length)}
                >
                  {metrics.map((metric) => (
                    <option key={metric.id} value={metric.id}>
                      {metric.label}
                    </option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">
                  Hold Ctrl/Cmd to select multiple ({selectedDisplayMetrics.length} selected)
                </p>
              </div>
            </div>
          )}
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Show top:</label>
            <input
              type="number"
              min="0"
              max="100"
              step="1"
              value={mergeThreshold}
              onChange={(e) => setMergeThreshold(parseFloat(e.target.value) || 0)}
              className="w-20 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="100"
            />
            <span className="text-sm text-gray-600">%</span>
            {mergeThreshold > 0 && mergeThreshold < 100 && (
              <button
                onClick={() => setMergeThreshold(0)}
                className="text-sm text-blue-600 hover:text-blue-800"
              >
                Clear
              </button>
            )}
          </div>
          <button
            onClick={() => setConfigOpen(!isConfigOpen)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isConfigOpen
                ? 'bg-gray-200 text-gray-700'
                : 'bg-blue-600 text-white hover:bg-blue-700'
            }`}
          >
            <Settings2 className="h-4 w-4" />
            {isConfigOpen ? 'Hide' : 'Configure'}
          </button>
        </div>
      </div>

      {/* Combination Error Message */}
      {combinationError && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {combinationError}
        </div>
      )}

      <div className="flex gap-4">
        <PivotConfigPanel
          isOpen={isConfigOpen}
          onClose={() => setConfigOpen(false)}
          config={config}
          updateTable={updateTable}
          updateDateRange={updateDateRange}
          updateStartDate={updateStartDate}
          updateEndDate={updateEndDate}
          setDataSourceDropped={setDataSourceDropped}
          setDateRangeDropped={setDateRangeDropped}
          addDimension={addDimension}
          removeDimension={removeDimension}
          addMetric={addMetric}
          removeMetric={removeMetric}
          addFilter={addFilter}
          removeFilter={removeFilter}
          resetToDefaults={resetToDefaults}
          dimensionFilters={pivotFilters.dimension_filters}
          onDimensionFilterChange={updateDimensionFilter}
          onClearDimensionFilters={clearDimensionFilters}
          currentFilters={filters}
        />

        <div className="flex-1 min-w-0 space-y-4">
          {/* Configuration Bar */}
          {isConfigured && (
            <div className="bg-white shadow rounded-lg p-4">
              <div className="flex flex-wrap gap-2">
                {/* Data Source Chip */}
                {config.isDataSourceDropped && (
                  <div className="flex items-center gap-1 px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
                    <span>{config.selectedTable}</span>
                    <button
                      onClick={removeDataSource}
                      className="ml-1 hover:bg-blue-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                )}

                {/* Date Range Chip */}
                {config.isDateRangeDropped && config.startDate && config.endDate && (
                  <div className="flex items-center gap-1 px-3 py-1 bg-purple-100 text-purple-800 rounded-full text-sm">
                    <span>{config.startDate} → {config.endDate}</span>
                    <button
                      onClick={removeDateRange}
                      className="ml-1 hover:bg-purple-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                )}

                {/* Row Dimension Chips */}
                {selectedDimensions.map((dim) => (
                  <div key={dim} className="flex items-center gap-1 px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm">
                    <span>Row: {getDimensionLabel(dim)}</span>
                    <button
                      onClick={() => removeDimension(dim)}
                      className="ml-1 hover:bg-green-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ))}

                {/* Table Dimension Chips */}
                {selectedTableDimensions.map((dim) => (
                  <div key={dim} className="flex items-center gap-1 px-3 py-1 bg-yellow-100 text-yellow-800 rounded-full text-sm">
                    <span>Table: {getDimensionLabel(dim)}</span>
                    <button
                      onClick={() => removeTableDimension(dim)}
                      className="ml-1 hover:bg-yellow-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ))}

                {/* Metric Chips */}
                {selectedMetrics.map((metricId) => {
                  const metric = getMetricById(metricId)
                  return (
                    <div key={metricId} className="flex items-center gap-1 px-3 py-1 bg-orange-100 text-orange-800 rounded-full text-sm">
                      <span>{metric?.label || metricId}</span>
                      <button
                        onClick={() => removeMetric(metricId)}
                        className="ml-1 hover:bg-orange-200 rounded-full p-0.5"
                      >
                        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  )
                })}

                {/* Dimension Filter Chips */}
                {Object.entries(pivotFilters.dimension_filters).map(([dimensionId, values]) => {
                  if (!values || values.length === 0) return null
                  const dimension = schema?.dimensions?.find(d => d.id === dimensionId)
                  const dimensionLabel = dimension?.display_name || dimensionId

                  return values.map((value, idx) => (
                    <div key={`${dimensionId}-${value}-${idx}`} className="flex items-center gap-1 px-3 py-1 bg-pink-100 text-pink-800 rounded-full text-sm">
                      <span>{dimensionLabel}: {value}</span>
                      <button
                        onClick={() => {
                          // Remove this specific value from the dimension filter
                          const newValues = values.filter(v => v !== value)
                          updateDimensionFilter(dimensionId, newValues)
                        }}
                        className="ml-1 hover:bg-pink-200 rounded-full p-0.5"
                      >
                        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  ))
                })}
              </div>
            </div>
          )}

          {/* Column Pivot Mode - dimension combinations as columns */}
          {selectedTableDimensions.length > 0 && tableCombinations.length > 0 ? (
            <div
              className={`bg-white shadow rounded-lg transition-all ${
                isDragOver ? 'ring-4 ring-blue-400 bg-blue-50' : ''
              }`}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
            >
              <div className="overflow-x-auto overflow-y-auto h-[calc(100vh-12rem)]">
                <table className="min-w-full divide-y divide-gray-200 w-max">
                  <thead className="bg-gray-50 sticky top-0 z-10">
                    {/* First header row - dimension column labels */}
                    <tr
                      onDragOver={(e) => handleDragOver(e, 'header')}
                      onDragLeave={handleDragLeave}
                      onDrop={(e) => handleDrop(e, 'header')}
                      className={`transition-all ${isDragOverHeader ? 'bg-yellow-100 ring-2 ring-yellow-400' : ''}`}
                    >
                      <th
                        rowSpan={2}
                        className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider transition-all border-r-2 border-gray-300 cursor-pointer hover:bg-gray-100 ${
                          isDragOverFirstColumn ? 'bg-green-100 ring-2 ring-green-400' : ''
                        }`}
                        onClick={() => {
                          setDimensionSortOrder(prev => prev === 'asc' ? 'desc' : 'asc')
                          setSortConfig(undefined as any, undefined, undefined)
                        }}
                        onDragOver={(e) => {
                          e.stopPropagation()
                          handleDragOver(e, 'firstColumn')
                        }}
                        onDragLeave={handleDragLeave}
                        onDrop={(e) => {
                          e.stopPropagation()
                          handleDrop(e, 'firstColumn')
                        }}
                      >
                        <div className="flex items-center gap-1">
                          <span>
                            {selectedDimensions.length === 0
                              ? 'Summary'
                              : getDimensionLabel(selectedDimensions[0])}
                          </span>
                          {selectedDimensions.length > 0 && (
                            <span className="text-gray-400">
                              {dimensionSortOrder === 'asc' ? '↑' : '↓'}
                            </span>
                          )}
                        </div>
                      </th>
                      {columnOrder.map((originalColIndex, orderIndex) => {
                        const combination = tableCombinations[originalColIndex]

                        // Safety check: skip if combination doesn't exist
                        if (!combination) return null

                        const headerParts = selectedTableDimensions.map((dim) => {
                          const dimLabel = getDimensionLabel(dim)
                          return `${dimLabel}: ${combination[dim]}`
                        })
                        const headerLabel = headerParts.join(' | ')

                        return (
                          <th
                            key={`col-${originalColIndex}`}
                            colSpan={orderIndex === 0 ? 1 : 3}
                            draggable
                            onDragStart={(e) => {
                              setDraggedColumnIndex(orderIndex)
                              e.dataTransfer.effectAllowed = 'move'
                            }}
                            onDragOver={(e) => {
                              e.preventDefault()
                              e.dataTransfer.dropEffect = 'move'
                            }}
                            onDrop={(e) => {
                              e.preventDefault()
                              if (draggedColumnIndex !== null && draggedColumnIndex !== orderIndex) {
                                // Reorder columns
                                const newOrder = [...columnOrder]
                                const [removed] = newOrder.splice(draggedColumnIndex, 1)
                                newOrder.splice(orderIndex, 0, removed)
                                setColumnOrder(newOrder)
                                // Clear children cache to refetch for new first column
                                setChildrenCache({})
                                setExpandedRows([])
                              }
                              setDraggedColumnIndex(null)
                            }}
                            onDragEnd={() => setDraggedColumnIndex(null)}
                            className={`px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-move transition-colors ${
                              orderIndex === 0 ? 'border-r-2 border-gray-300' : 'border-r border-gray-200'
                            } ${draggedColumnIndex === orderIndex ? 'bg-blue-200' : ''} ${orderIndex === 0 ? 'bg-green-50' : ''}`}
                          >
                            <div className="flex items-center justify-center gap-2">
                              {orderIndex === 0 && <span className="text-green-600 text-sm">★</span>}
                              <div>{headerLabel}</div>
                            </div>
                          </th>
                        )
                      })}
                    </tr>
                    {/* Second header row - metric sub-columns */}
                    <tr className="bg-gray-100">
                      {columnOrder.map((originalColIndex, orderIndex) => {
                        if (orderIndex === 0) {
                          // First column - show metric badges for sorting selection
                          return (
                            <th
                              key={`metric-${originalColIndex}`}
                              className="px-6 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-r-2 border-gray-300"
                            >
                              <div className="flex flex-col items-end gap-1">
                                <span className="text-gray-500">Sort by:</span>
                                <div className="flex flex-wrap gap-1 justify-end">
                                  {selectedDisplayMetrics.map((metricId) => {
                                    const metric = getMetricById(metricId)
                                    const isActiveSortMetric = sortConfig && sortConfig.column === originalColIndex && sortConfig.subColumn === 'value' && sortConfig.metric === metricId
                                    return (
                                      <button
                                        key={metricId}
                                        onClick={() => handleSort(originalColIndex, 'value', metricId)}
                                        className={`px-2 py-0.5 rounded text-xs transition-colors ${
                                          isActiveSortMetric
                                            ? 'bg-blue-600 text-white'
                                            : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                                        }`}
                                        title={`Sort by ${metric?.label}`}
                                      >
                                        {metric?.label}
                                        {isActiveSortMetric && (
                                          <span className="ml-1">{sortConfig.direction === 'desc' ? '↓' : '↑'}</span>
                                        )}
                                      </button>
                                    )
                                  })}
                                </div>
                              </div>
                            </th>
                          )
                        } else {
                          // Columns 2+ - Value, Diff, % Diff with metric badges
                          return (
                            <React.Fragment key={`metrics-${originalColIndex}`}>
                              {/* Value column with metric badges */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-l border-gray-200">
                                <div className="flex flex-col items-end gap-1">
                                  <span className="text-gray-500">Value</span>
                                  <div className="flex flex-wrap gap-1 justify-end">
                                    {selectedDisplayMetrics.map((metricId) => {
                                      const metric = getMetricById(metricId)
                                      const isActiveSortMetric = sortConfig && sortConfig.column === originalColIndex && sortConfig.subColumn === 'value' && sortConfig.metric === metricId
                                      return (
                                        <button
                                          key={metricId}
                                          onClick={() => handleSort(originalColIndex, 'value', metricId)}
                                          className={`px-1.5 py-0.5 rounded text-xs transition-colors ${
                                            isActiveSortMetric
                                              ? 'bg-blue-600 text-white'
                                              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                                          }`}
                                          title={`Sort by ${metric?.label}`}
                                        >
                                          {metric?.label?.substring(0, 3)}
                                          {isActiveSortMetric && (
                                            <span className="ml-0.5">{sortConfig.direction === 'desc' ? '↓' : '↑'}</span>
                                          )}
                                        </button>
                                      )
                                    })}
                                  </div>
                                </div>
                              </th>
                              {/* Diff column with metric badges */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider">
                                <div className="flex flex-col items-end gap-1">
                                  <span className="text-gray-500">Diff</span>
                                  <div className="flex flex-wrap gap-1 justify-end">
                                    {selectedDisplayMetrics.map((metricId) => {
                                      const metric = getMetricById(metricId)
                                      const isActiveSortMetric = sortConfig && sortConfig.column === originalColIndex && sortConfig.subColumn === 'diff' && sortConfig.metric === metricId
                                      return (
                                        <button
                                          key={metricId}
                                          onClick={() => handleSort(originalColIndex, 'diff', metricId)}
                                          className={`px-1.5 py-0.5 rounded text-xs transition-colors ${
                                            isActiveSortMetric
                                              ? 'bg-blue-600 text-white'
                                              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                                          }`}
                                          title={`Sort by ${metric?.label}`}
                                        >
                                          {metric?.label?.substring(0, 3)}
                                          {isActiveSortMetric && (
                                            <span className="ml-0.5">{sortConfig.direction === 'desc' ? '↓' : '↑'}</span>
                                          )}
                                        </button>
                                      )
                                    })}
                                  </div>
                                </div>
                              </th>
                              {/* % Diff column with metric badges */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-r border-gray-200">
                                <div className="flex flex-col items-end gap-1">
                                  <span className="text-gray-500">% Diff</span>
                                  <div className="flex flex-wrap gap-1 justify-end">
                                    {selectedDisplayMetrics.map((metricId) => {
                                      const metric = getMetricById(metricId)
                                      const isActiveSortMetric = sortConfig && sortConfig.column === originalColIndex && sortConfig.subColumn === 'pctDiff' && sortConfig.metric === metricId
                                      return (
                                        <button
                                          key={metricId}
                                          onClick={() => handleSort(originalColIndex, 'pctDiff', metricId)}
                                          className={`px-1.5 py-0.5 rounded text-xs transition-colors ${
                                            isActiveSortMetric
                                              ? 'bg-blue-600 text-white'
                                              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                                          }`}
                                          title={`Sort by ${metric?.label}`}
                                        >
                                          {metric?.label?.substring(0, 3)}
                                          {isActiveSortMetric && (
                                            <span className="ml-0.5">{sortConfig.direction === 'desc' ? '↓' : '↑'}</span>
                                          )}
                                        </button>
                                      )
                                    })}
                                  </div>
                                </div>
                              </th>
                            </React.Fragment>
                          )
                        }
                      })}
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {/* Render rows based on row dimensions or show single summary row */}
                    {selectedDimensions.length === 0 ? (
                      <>
                        {/* Make Total row expandable */}
                        {(() => {
                          const rowKey = `depth0:Total`
                          const isExpanded = expandedRows.has(rowKey)
                          const isLoading = loadingRows.has(rowKey)
                          const currentPageNum = currentPage[rowKey] || 0
                          const firstColIndex = columnOrder[0] ?? 0
                          const colPageKey = `${rowKey}:col_${firstColIndex}:${currentPageNum}`
                          const firstColChildren = childrenCache[colPageKey] || []

                          return (
                            <>
                              <tr className="hover:bg-gray-50">
                                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                  <span>Total</span>
                                </td>
                                {columnOrder.map((originalColIndex, orderIndex) => {
                                  const columnData = allColumnData?.[originalColIndex]

                                  if (orderIndex === 0) {
                                    // First column - show all selected metrics stacked
                                    return (
                                      <td
                                        key={`col-${originalColIndex}`}
                                        className="px-6 py-4 text-sm text-gray-900 text-right border-r-2 border-gray-300"
                                      >
                                        <div className="space-y-1">
                                          {selectedDisplayMetrics.map((metricId) => {
                                            const value = columnData?.total?.metrics?.[metricId] ?? null
                                            const metric = getMetricById(metricId)
                                            return (
                                              <div key={metricId} className="whitespace-nowrap">
                                                <span className="text-xs text-gray-500">{metric?.label}: </span>
                                                <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                              </div>
                                            )
                                          })}
                                        </div>
                                      </td>
                                    )
                                  } else {
                                    // Columns 2+ - show all selected metrics with diff and % diff
                                    const firstColIndex = columnOrder[0]
                                    const firstColData = allColumnData?.[firstColIndex]

                                    return (
                                      <React.Fragment key={`col-${originalColIndex}`}>
                                        <td className="px-4 py-4 text-sm text-gray-900 text-right border-l border-gray-200">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = columnData?.total?.metrics?.[metricId] ?? null
                                              const metric = getMetricById(metricId)
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  <span className="text-xs text-gray-500">{metric?.label}: </span>
                                                  <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                        <td className="px-4 py-4 text-sm text-gray-700 text-right">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = columnData?.total?.metrics?.[metricId] ?? null
                                              const firstValue = firstColData?.total?.metrics?.[metricId] ?? null
                                              const diff = (value ?? 0) - (firstValue ?? 0)
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  {diff != null ? formatMetricValue(diff, metricId) : '-'}
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                        <td className="px-4 py-4 text-sm text-gray-700 text-right border-r border-gray-200">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = columnData?.total?.metrics?.[metricId] ?? null
                                              const firstValue = firstColData?.total?.metrics?.[metricId] ?? null
                                              const pctDiff = (firstValue ?? 0) !== 0
                                                ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                                : null
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  {pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                      </React.Fragment>
                                    )
                                  }
                                })}
                              </tr>
                              {/* No drill-down in column-pivot mode - dimensions already shown as columns */}
                              {false && isExpanded && (
                                <>
                                  {isLoading && firstColChildren.length === 0 ? (
                                    <tr className="bg-blue-50">
                                      <td colSpan={columnOrder.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                                        Loading...
                                      </td>
                                    </tr>
                                  ) : (
                                    (() => {
                                      // Build a map of search term -> column data for all columns
                                      const childrenByColumn: Record<number, Record<string, PivotChildRow>> = {}
                                      columnOrder.forEach((originalColIndex) => {
                                        const colPageKey = `${rowKey}:col_${originalColIndex}:${currentPageNum}`
                                        const colChildren = childrenCache[colPageKey] || []
                                        childrenByColumn[originalColIndex] = {}
                                        colChildren.forEach(child => {
                                          childrenByColumn[originalColIndex][child.search_term] = child
                                        })
                                      })

                                      return (
                                        <>
                                          {/* Drill-down header row */}
                                          <tr className="bg-blue-100 border-t-2 border-blue-300">
                                            <th
                                              className="px-6 py-2 text-left text-xs font-medium text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-blue-200"
                                              onClick={() => handleChildrenSort('dimension')}
                                            >
                                              <div className="flex items-center gap-1 pl-8">
                                                <span>Search Term</span>
                                                {childrenSortConfig?.column === 'dimension' && (
                                                  <span className="text-gray-500">
                                                    {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                                  </span>
                                                )}
                                              </div>
                                            </th>
                                            {columnOrder.map((originalColIndex, orderIndex) => {
                                              const metric = getMetricById(selectedDisplayMetrics[0] || primarySortMetric)
                                              if (orderIndex === 0) {
                                                const isSorted = childrenSortConfig?.column === selectedDisplayMetric
                                                return (
                                                  <th
                                                    key={`child-header-${originalColIndex}`}
                                                    className="px-6 py-2 text-right text-xs font-medium text-gray-700 uppercase tracking-wider border-r-2 border-blue-300 cursor-pointer hover:bg-blue-200"
                                                    onClick={() => handleChildrenSort(selectedDisplayMetrics[0] || primarySortMetric)}
                                                  >
                                                    <div className="flex items-center justify-end gap-1">
                                                      {metric?.label}
                                                      {isSorted && (
                                                        <span className="text-gray-500">
                                                          {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                                        </span>
                                                      )}
                                                    </div>
                                                  </th>
                                                )
                                              } else {
                                                return (
                                                  <th
                                                    key={`child-header-${originalColIndex}`}
                                                    colSpan={3}
                                                    className="px-4 py-2 text-center text-xs font-medium text-gray-700 uppercase tracking-wider border-r border-blue-200"
                                                  >
                                                    {metric?.label}
                                                  </th>
                                                )
                                              }
                                            })}
                                          </tr>
                                          {sortChildren(firstColChildren).map((child, idx) => (
                                            <tr key={`Total-child-${idx}`} className="bg-blue-50 hover:bg-blue-100">
                                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                                                <div className="pl-8">{child.search_term}</div>
                                              </td>
                                              {columnOrder.map((originalColIndex, orderIndex) => {
                                                // Look up the child data for this search term in this column
                                                const childData = childrenByColumn[originalColIndex][child.search_term]
                                                const value = childData ? (childData as any)[selectedDisplayMetrics[0] || primarySortMetric] : null

                                                if (orderIndex === 0) {
                                                  // First column - just show the value
                                                  return (
                                                    <td
                                                      key={`col-${originalColIndex}`}
                                                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-r-2 border-gray-300"
                                                    >
                                                      {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                    </td>
                                                  )
                                                } else {
                                                  // Columns 2+ - show value, diff, % diff
                                                  const firstColIndex = columnOrder[0]
                                                  const firstChildData = childrenByColumn[firstColIndex][child.search_term]
                                                  const firstValue = firstChildData ? (firstChildData as any)[selectedDisplayMetrics[0] || primarySortMetric] : null

                                                  const diff = (value ?? 0) - (firstValue ?? 0)
                                                  const pctDiff = (firstValue ?? 0) !== 0
                                                    ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                                    : null

                                                  return (
                                                    <React.Fragment key={`col-${originalColIndex}`}>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-l border-gray-200">
                                                        {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                      </td>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-600 text-right">
                                                        {diff != null ? formatMetricValue(diff, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                      </td>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-600 text-right border-r border-gray-200">
                                                        {pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                                                      </td>
                                                    </React.Fragment>
                                                  )
                                                }
                                              })}
                                            </tr>
                                          ))}
                                        </>
                                      )
                                    })()
                                  )}
                                  {firstColChildren.length >= CHILDREN_PAGE_SIZE && (
                                    <tr className="bg-blue-50">
                                      <td colSpan={columnOrder.length + 1} className="px-6 py-4">
                                        {isLoading ? (
                                          <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                                            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                                            Loading...
                                          </div>
                                        ) : (
                                          <div className="flex items-center justify-center gap-4">
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation()
                                                const currentPageNum = currentPage[rowKey] || 0
                                                if (currentPageNum > 0) {
                                                  goToPage('Total', 0, currentPageNum - 1)
                                                }
                                              }}
                                              disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                                              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                            >
                                              Previous
                                            </button>
                                            <span className="text-sm text-gray-600">
                                              Page {(currentPage[rowKey] || 0) + 1}
                                            </span>
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation()
                                                const currentPageNum = currentPage[rowKey] || 0
                                                goToPage('Total', 0, currentPageNum + 1)
                                              }}
                                              disabled={firstColChildren.length < CHILDREN_PAGE_SIZE}
                                              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                            >
                                              Next
                                            </button>
                                          </div>
                                        )}
                                      </td>
                                    </tr>
                                  )}
                                </>
                              )}
                            </>
                          )
                        })()}
                      </>
                    ) : (
                      <>
                        {sortRows(processedRows).map((row, rowIndex) => {
                          const dimensionPath = row.dimension_value
                          const rowKey = `depth0:${dimensionPath}`
                          const isExpanded = expandedRows.has(rowKey)
                          const isLoading = loadingRows.has(rowKey)
                          const currentPageNum = currentPage[rowKey] || 0
                          const pageKey = `${rowKey}:${currentPageNum}`
                          const children = childrenCache[pageKey] || []
                          const grandTotal = pivotData?.total.metrics?.[primarySortMetric] || 0

                          return (
                            <React.Fragment key={row.dimension_value}>
                              <tr className="hover:bg-gray-50">
                                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                  <span>{row.dimension_value}</span>
                                </td>
                                {columnOrder.map((originalColIndex, orderIndex) => {
                                  const columnData = allColumnData?.[originalColIndex]
                                  const rowData = columnData?.rows.find((r: any) => r.dimension_value === row.dimension_value)

                                  if (orderIndex === 0) {
                                    // First column - show all selected metrics stacked
                                    return (
                                      <td
                                        key={`col-${originalColIndex}`}
                                        className="px-6 py-4 text-sm text-gray-900 text-right border-r-2 border-gray-300"
                                      >
                                        <div className="space-y-1">
                                          {selectedDisplayMetrics.map((metricId) => {
                                            const value = rowData?.metrics?.[metricId] ?? null
                                            const metric = getMetricById(metricId)
                                            return (
                                              <div key={metricId} className="whitespace-nowrap">
                                                <span className="text-xs text-gray-500">{metric?.label}: </span>
                                                <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                              </div>
                                            )
                                          })}
                                        </div>
                                      </td>
                                    )
                                  } else {
                                    // Columns 2+ - show all selected metrics with diff and % diff
                                    const firstColIndex = columnOrder[0]
                                    const firstColData = allColumnData?.[firstColIndex]
                                    const firstRowData = firstColData?.rows.find((r: any) => r.dimension_value === row.dimension_value)

                                    return (
                                      <React.Fragment key={`col-${originalColIndex}`}>
                                        <td className="px-4 py-4 text-sm text-gray-900 text-right border-l border-gray-200">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = rowData?.metrics?.[metricId] ?? null
                                              const metric = getMetricById(metricId)
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  <span className="text-xs text-gray-500">{metric?.label}: </span>
                                                  <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                        <td className="px-4 py-4 text-sm text-gray-700 text-right">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = rowData?.metrics?.[metricId] ?? null
                                              const firstValue = firstRowData?.metrics?.[metricId] ?? null
                                              const diff = (value ?? 0) - (firstValue ?? 0)
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  {diff != null ? formatMetricValue(diff, metricId) : '-'}
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                        <td className="px-4 py-4 text-sm text-gray-700 text-right border-r border-gray-200">
                                          <div className="space-y-1">
                                            {selectedDisplayMetrics.map((metricId) => {
                                              const value = rowData?.metrics?.[metricId] ?? null
                                              const firstValue = firstRowData?.metrics?.[metricId] ?? null
                                              const pctDiff = (firstValue ?? 0) !== 0
                                                ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                                : null
                                              return (
                                                <div key={metricId} className="whitespace-nowrap">
                                                  {pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </td>
                                      </React.Fragment>
                                    )
                                  }
                                })}
                              </tr>
                              {/* Expanded children rows */}
                              {isExpanded && (
                                <>
                                  {isLoading ? (
                                    <tr className="bg-blue-50">
                                      <td colSpan={tableCombinations.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                                        Loading...
                                      </td>
                                    </tr>
                                  ) : (
                                    (() => {
                                      // Get search terms from FIRST column, but show values from ALL columns
                                      const firstColIndex = columnOrder[0] ?? 0
                                      const firstColPageKey = `${rowKey}:col_${firstColIndex}:${currentPageNum}`
                                      const firstColChildren = childrenCache[firstColPageKey] || []

                                      // Build a map of search term -> column data for all columns
                                      const childrenByColumn: Record<number, Record<string, PivotChildRow>> = {}
                                      columnOrder.forEach((originalColIndex) => {
                                        const colPageKey = `${rowKey}:col_${originalColIndex}:${currentPageNum}`
                                        const colChildren = childrenCache[colPageKey] || []
                                        childrenByColumn[originalColIndex] = {}
                                        colChildren.forEach(child => {
                                          childrenByColumn[originalColIndex][child.search_term] = child
                                        })
                                      })

                                      return (
                                        <>
                                          {/* Drill-down header row */}
                                          <tr className="bg-blue-100 border-t-2 border-blue-300">
                                            <th
                                              className="px-6 py-2 text-left text-xs font-medium text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-blue-200"
                                              onClick={() => handleChildrenSort('dimension')}
                                            >
                                              <div className="flex items-center gap-1 pl-8">
                                                <span>Search Term</span>
                                                {childrenSortConfig?.column === 'dimension' && (
                                                  <span className="text-gray-500">
                                                    {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                                  </span>
                                                )}
                                              </div>
                                            </th>
                                            {columnOrder.map((originalColIndex, orderIndex) => {
                                              const metric = getMetricById(selectedDisplayMetrics[0] || primarySortMetric)
                                              if (orderIndex === 0) {
                                                const isSorted = childrenSortConfig?.column === selectedDisplayMetric
                                                return (
                                                  <th
                                                    key={`child-header-${originalColIndex}`}
                                                    className="px-6 py-2 text-right text-xs font-medium text-gray-700 uppercase tracking-wider border-r-2 border-blue-300 cursor-pointer hover:bg-blue-200"
                                                    onClick={() => handleChildrenSort(selectedDisplayMetrics[0] || primarySortMetric)}
                                                  >
                                                    <div className="flex items-center justify-end gap-1">
                                                      {metric?.label}
                                                      {isSorted && (
                                                        <span className="text-gray-500">
                                                          {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                                        </span>
                                                      )}
                                                    </div>
                                                  </th>
                                                )
                                              } else {
                                                return (
                                                  <th
                                                    key={`child-header-${originalColIndex}`}
                                                    colSpan={3}
                                                    className="px-4 py-2 text-center text-xs font-medium text-gray-700 uppercase tracking-wider border-r border-blue-200"
                                                  >
                                                    {metric?.label}
                                                  </th>
                                                )
                                              }
                                            })}
                                          </tr>
                                          {sortChildren(firstColChildren).map((child, idx) => (
                                            <tr key={`${dimensionPath}-child-${idx}`} className="bg-blue-50 hover:bg-blue-100">
                                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                                                <div className="pl-8">{child.search_term}</div>
                                              </td>
                                              {columnOrder.map((originalColIndex, orderIndex) => {
                                                // Look up the child data for this search term in this column
                                                const childData = childrenByColumn[originalColIndex][child.search_term]
                                                const value = childData ? (childData as any)[selectedDisplayMetrics[0] || primarySortMetric] : null

                                                if (orderIndex === 0) {
                                                  // First column - just show the value
                                                  return (
                                                    <td
                                                      key={`col-${originalColIndex}`}
                                                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-r-2 border-gray-300"
                                                    >
                                                      {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                    </td>
                                                  )
                                                } else {
                                                  // Columns 2+ - show value, diff, % diff
                                                  const firstColIndex = columnOrder[0]
                                                  const firstChildData = childrenByColumn[firstColIndex][child.search_term]
                                                  const firstValue = firstChildData ? (firstChildData as any)[selectedDisplayMetrics[0] || primarySortMetric] : null

                                                  const diff = (value ?? 0) - (firstValue ?? 0)
                                                  const pctDiff = (firstValue ?? 0) !== 0
                                                    ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                                    : null

                                                  return (
                                                    <React.Fragment key={`col-${originalColIndex}`}>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-l border-gray-200">
                                                        {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                      </td>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-600 text-right">
                                                        {diff != null ? formatMetricValue(diff, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                                      </td>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-600 text-right border-r border-gray-200">
                                                        {pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                                                      </td>
                                                    </React.Fragment>
                                                  )
                                                }
                                              })}
                                            </tr>
                                          ))}
                                          {firstColChildren.length >= CHILDREN_PAGE_SIZE && (
                                            <tr className="bg-blue-50">
                                              <td colSpan={columnOrder.length + 1} className="px-6 py-4">
                                                {isLoading ? (
                                                  <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                                                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                                                    Loading...
                                                  </div>
                                                ) : (
                                                  <div className="flex items-center justify-center gap-4">
                                                    <button
                                                      onClick={(e) => {
                                                        e.stopPropagation()
                                                        const currentPageNum = currentPage[rowKey] || 0
                                                        if (currentPageNum > 0) {
                                                          goToPage(dimensionPath, 0, currentPageNum - 1)
                                                        }
                                                      }}
                                                      disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                                                      className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                                    >
                                                      Previous
                                                    </button>
                                                    <span className="text-sm text-gray-600">
                                                      Page {(currentPage[rowKey] || 0) + 1}
                                                    </span>
                                                    <button
                                                      onClick={(e) => {
                                                        e.stopPropagation()
                                                        const currentPageNum = currentPage[rowKey] || 0
                                                        goToPage(dimensionPath, 0, currentPageNum + 1)
                                                      }}
                                                      disabled={firstColChildren.length < CHILDREN_PAGE_SIZE}
                                                      className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                                    >
                                                      Next
                                                    </button>
                                                  </div>
                                                )}
                                              </td>
                                            </tr>
                                          )}
                                        </>
                                      )
                                    })()
                                  )}
                                </>
                              )}
                            </React.Fragment>
                          )
                        })}
                      </>
                    )}
                  </tbody>
                  <tfoot className="bg-gray-100 font-semibold sticky bottom-0 z-10">
                    {selectedDimensions.length > 0 && (
                      <tr>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          Total
                        </td>
                        {columnOrder.map((originalColIndex, orderIndex) => {
                          const columnData = allColumnData?.[originalColIndex]
                          const value = columnData?.total?.metrics?.[selectedDisplayMetrics[0] || primarySortMetric] ?? null

                          if (orderIndex === 0) {
                            // First column - just show the value
                            return (
                              <td
                                key={`col-${originalColIndex}`}
                                className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right border-r-2 border-gray-300"
                              >
                                {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                              </td>
                            )
                          } else {
                            // Columns 2+ - show value, diff, % diff
                            const firstColIndex = columnOrder[0]
                            const firstColData = allColumnData?.[firstColIndex]
                            const firstValue = firstColData?.total?.metrics?.[selectedDisplayMetrics[0] || primarySortMetric] ?? null

                            const diff = (value ?? 0) - (firstValue ?? 0)
                            const pctDiff = (firstValue ?? 0) !== 0
                              ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                              : null

                            return (
                              <React.Fragment key={`col-${originalColIndex}`}>
                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900 text-right border-l border-gray-200">
                                  {value != null ? formatMetricValue(value, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                </td>
                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-700 text-right">
                                  {diff != null ? formatMetricValue(diff, selectedDisplayMetrics[0] || primarySortMetric) : '-'}
                                </td>
                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-r border-gray-200">
                                  {pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                                </td>
                              </React.Fragment>
                            )
                          }
                        })}
                      </tr>
                    )}
                  </tfoot>
                </table>
              </div>
            </div>
          ) : selectedTableDimensions.length > 0 && tableCombinations.length === 0 ? (
            <div className="bg-yellow-50 border border-yellow-200 text-yellow-700 px-4 py-3 rounded">
              Loading dimension combinations...
            </div>
          ) : (
            /* Single Table Mode */
            <div
              className={`bg-white shadow rounded-lg transition-all ${
                isDragOver ? 'ring-4 ring-blue-400 bg-blue-50' : ''
              }`}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
            >
            <div className="overflow-x-auto overflow-y-auto h-[calc(100vh-12rem)]">
            <table className="min-w-full divide-y divide-gray-200 w-max">
            <thead className="bg-gray-50 sticky top-0 z-10">
              <tr
                onDragOver={(e) => handleDragOver(e, 'header')}
                onDragLeave={handleDragLeave}
                onDrop={(e) => handleDrop(e, 'header')}
                className={`transition-all ${isDragOverHeader ? 'bg-yellow-100 ring-2 ring-yellow-400' : ''}`}
              >
                <th
                  className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider transition-all cursor-pointer hover:bg-gray-100 ${
                    isDragOverFirstColumn ? 'bg-green-100 ring-2 ring-green-400' : ''
                  }`}
                  onClick={() => {
                    setDimensionSortOrder(prev => prev === 'asc' ? 'desc' : 'asc')
                    setSortConfig(undefined as any, undefined, undefined)
                  }}
                  onDragOver={(e) => {
                    e.stopPropagation()
                    handleDragOver(e, 'firstColumn')
                  }}
                  onDragLeave={handleDragLeave}
                  onDrop={(e) => {
                    e.stopPropagation()
                    handleDrop(e, 'firstColumn')
                  }}
                >
                  <div className="flex items-center gap-1">
                    <span>
                      {selectedDimensions.length === 0
                        ? 'Summary'
                        : getDimensionLabel(selectedDimensions[0])}
                    </span>
                    {selectedDimensions.length > 0 && (
                      <span className="text-gray-400">
                        {dimensionSortOrder === 'asc' ? '↑' : '↓'}
                      </span>
                    )}
                  </div>
                </th>
                {selectedMetrics.map((metricId, index) => {
                  const metric = getMetricById(metricId)
                  const isSorted = sortConfig && sortConfig.column === metricId
                  return (
                    <th
                      key={metricId}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.effectAllowed = 'move'
                        e.dataTransfer.setData('text/plain', metricId)
                        e.dataTransfer.setData('columnIndex', String(index))
                      }}
                      onDragOver={(e) => {
                        e.preventDefault()
                        e.dataTransfer.dropEffect = 'move'
                        setDragOverColumn(metricId)
                      }}
                      onDragLeave={() => {
                        setDragOverColumn(null)
                      }}
                      onDrop={(e) => {
                        e.preventDefault()
                        const draggedIndex = parseInt(e.dataTransfer.getData('columnIndex'))
                        const targetIndex = index
                        if (draggedIndex !== targetIndex) {
                          reorderMetrics(draggedIndex, targetIndex)
                        }
                        setDragOverColumn(null)
                      }}
                      onClick={(e) => {
                        // Only handle sort if not currently dragging
                        if (!e.currentTarget.classList.contains('dragging')) {
                          handleSort(metricId)
                        }
                      }}
                      className={`px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-move hover:bg-gray-200 transition-colors ${
                        dragOverColumn === metricId ? 'bg-blue-100' : isSorted ? 'bg-gray-100' : ''
                      }`}
                      title={metric?.description}
                    >
                      <div className="flex items-center justify-end gap-1">
                        {metric?.label || metricId}
                        {isSorted && (
                          sortConfig.direction === 'desc' ? <ArrowDown size={14} /> : <ArrowUp size={14} />
                        )}
                      </div>
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {(() => {
                const sortedRows = sortRows(processedRows)
                return sortedRows.map((row, index) => {
                  const rowKey = `depth0:${row.dimension_value}`
                  const isExpanded = expandedRows.has(rowKey)
                  const isLoading = loadingRows.has(rowKey)
                  const currentPageNum = currentPage[rowKey] || 0
                  const pageKey = `${rowKey}:${currentPageNum}`
                  const children = childrenCache[pageKey] || []

                  // Get grand total from the whole dataset using primary sort metric
                  const grandTotal = pivotData.total.metrics?.[primarySortMetric] || 0

                  // Calculate cumulative percentage based on sorted order (already in decimal 0-1 format)
                  const cumulativePercentage = sortedRows
                    .slice(0, index + 1)
                    .reduce((sum, r) => sum + r.percentage_of_total, 0)

                  // Calculate cumulative search term count based on sorted order
                  const cumulativeTerms = sortedRows
                    .slice(0, index + 1)
                    .reduce((sum, r) => sum + r.search_term_count, 0)

                return (
                  <React.Fragment key={row.dimension_value}>
                    <tr
                      className="hover:bg-gray-50 cursor-pointer"
                      onClick={() => row.has_children && toggleRow(row.dimension_value, 0, grandTotal)}
                    >
                      <td
                        className={`px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 transition-all ${
                          isDragOverFirstColumn ? 'bg-green-100 ring-2 ring-green-400' : ''
                        }`}
                        onDragOver={(e) => {
                          e.stopPropagation()
                          handleDragOver(e, 'firstColumn')
                        }}
                        onDragLeave={handleDragLeave}
                        onDrop={(e) => {
                          e.stopPropagation()
                          handleDrop(e, 'firstColumn')
                        }}
                      >
                        <div className="flex items-center gap-2">
                          {row.has_children && (
                            isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
                          )}
                          <span>{row.dimension_value}</span>
                        </div>
                      </td>
                      {selectedMetrics.map((metricId) => {
                        let value: number

                        // Handle special computed metrics
                        if (metricId === 'cumulative_percentage') {
                          value = cumulativePercentage // Already in decimal format (0-1)
                        } else if (metricId === 'cumulative_terms') {
                          value = cumulativeTerms
                        } else {
                          value = getRowValue(row, metricId)
                        }

                        return (
                          <td
                            key={metricId}
                            className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right"
                          >
                            {formatMetricValue(value, metricId)}
                          </td>
                        )
                      })}
                    </tr>
                    {isExpanded && (
                      <>
                        {isLoading && children.length === 0 ? (
                          <tr className="bg-blue-50">
                            <td colSpan={selectedMetrics.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                              Loading...
                            </td>
                          </tr>
                        ) : (
                          <>
                            {/* Drill-down header row for single-table mode */}
                            <tr className="bg-blue-100 border-t-2 border-blue-300">
                              <th
                                className="px-6 py-2 text-left text-xs font-medium text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-blue-200"
                                onClick={() => handleChildrenSort('dimension')}
                              >
                                <div className="flex items-center gap-1 pl-8">
                                  <span>{selectedDimensions.length > 1 ? getDimensionLabel(selectedDimensions[1]) : 'Search Term'}</span>
                                  {childrenSortConfig?.column === 'dimension' && (
                                    <span className="text-gray-500">
                                      {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                    </span>
                                  )}
                                </div>
                              </th>
                              {selectedMetrics.map((metricId) => {
                                const metric = getMetricById(metricId)
                                const isSorted = childrenSortConfig?.column === metricId
                                return (
                                  <th
                                    key={`child-header-${metricId}`}
                                    className="px-6 py-2 text-right text-xs font-medium text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-blue-200"
                                    onClick={() => handleChildrenSort(metricId)}
                                  >
                                    <div className="flex items-center justify-end gap-1">
                                      {metric?.label || metricId}
                                      {isSorted && (
                                        <span className="text-gray-500">
                                          {childrenSortConfig.direction === 'asc' ? '↑' : '↓'}
                                        </span>
                                      )}
                                    </div>
                                  </th>
                                )
                              })}
                            </tr>
                            {sortChildren(children).map((child, idx) => {
                              // Get all children from previous pages
                              let allPreviousPagesChildren: PivotChildRow[] = []
                              if (currentPageNum > 0) {
                                for (let p = 0; p < currentPageNum; p++) {
                                  const prevPageKey = `${rowKey}:${p}`
                                  const prevChildren = childrenCache[prevPageKey] || []
                                  allPreviousPagesChildren = [...allPreviousPagesChildren, ...prevChildren]
                                }
                              }
                              // Add children from current page up to this index
                              const allChildrenUpToThis = [...allPreviousPagesChildren, ...children.slice(0, idx + 1)]
                              return renderRow(child, row.dimension_value, 1, 1, children, idx, grandTotal, allChildrenUpToThis)
                            })}
                            {children.length > 0 && children.length === CHILDREN_PAGE_SIZE && (
                              <tr className="bg-blue-50">
                                <td colSpan={selectedMetrics.length + 1} className="px-6 py-4">
                                  {isLoading ? (
                                    <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                                      Loading...
                                    </div>
                                  ) : (
                                    <div className="flex items-center justify-center gap-4">
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          const currentPageNum = currentPage[rowKey] || 0
                                          if (currentPageNum > 0) {
                                            goToPage(row.dimension_value, 0, currentPageNum - 1)
                                          }
                                        }}
                                        disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                                        className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                      >
                                        Previous
                                      </button>
                                      <span className="text-sm text-gray-600">
                                        Page {(currentPage[rowKey] || 0) + 1}
                                      </span>
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          const currentPageNum = currentPage[rowKey] || 0
                                          goToPage(row.dimension_value, 0, currentPageNum + 1)
                                        }}
                                        disabled={children.length < CHILDREN_PAGE_SIZE}
                                        className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                      >
                                        Next
                                      </button>
                                    </div>
                                  )}
                                </td>
                              </tr>
                            )}
                          </>
                        )}
                      </>
                    )}
                  </React.Fragment>
                )
                })
              })()}
            </tbody>
            <tfoot className="bg-gray-100 font-semibold sticky bottom-0 z-10">
              <tr>
                <td
                  className={`px-6 py-4 whitespace-nowrap text-sm text-gray-900 transition-all ${
                    isDragOverFirstColumn ? 'bg-green-100 ring-2 ring-green-400' : ''
                  }`}
                  onDragOver={(e) => {
                    e.stopPropagation()
                    handleDragOver(e, 'firstColumn')
                  }}
                  onDragLeave={handleDragLeave}
                  onDrop={(e) => {
                    e.stopPropagation()
                    handleDrop(e, 'firstColumn')
                  }}
                >
                  Total
                </td>
                {selectedMetrics.map((metricId) => {
                  let value: number

                  // Handle special computed metrics for total row
                  if (metricId === 'percentage_of_total' || metricId === 'cumulative_percentage') {
                    value = 1.0 // 100%
                  } else if (metricId === 'cumulative_terms') {
                    value = pivotData.total.search_term_count
                  } else {
                    value = getRowValue(pivotData.total, metricId)
                  }

                  return (
                    <td
                      key={metricId}
                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right"
                    >
                      {formatMetricValue(value, metricId)}
                    </td>
                  )
                })}
              </tr>
            </tfoot>
          </table>
        </div>
          </div>
          )}

          {/* Chart Visualization (only in multi-table mode) */}
          {selectedTableDimensions.length > 0 && tableCombinations.length > 0 && allColumnData && (
            <PivotChartVisualization
              allColumnData={allColumnData}
              selectedDimensions={selectedDimensions}
              selectedMetrics={metrics.map(m => m.id)}
              tableCombinations={tableCombinations}
              getMetricById={getMetricById}
              columnOrder={columnOrder}
              tableHeaders={columnOrder.map((originalColIndex) => {
                const combination = tableCombinations[originalColIndex]
                if (!combination) return `Column ${originalColIndex + 1}`
                const headerParts = selectedTableDimensions.map((dim) => {
                  const dimLabel = getDimensionLabel(dim)
                  return `${dimLabel}: ${combination[dim]}`
                })
                return headerParts.join(' | ')
              })}
              sortedDimensionValues={sortRows(processedRows).map(row => row.dimension_value)}
              selectedDisplayMetrics={selectedDisplayMetrics}
            />
          )}
        </div>
      </div>
    </div>
  )
}
