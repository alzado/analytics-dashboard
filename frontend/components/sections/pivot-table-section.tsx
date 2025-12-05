'use client'

import React, { useState, useMemo, useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchPivotData, fetchPivotChildren, fetchDimensionValues, fetchCustomDimensions, fetchTables, updateWidget } from '@/lib/api'
import type { PivotRow, PivotChildRow, PivotResponse, CustomDimension, DateRangeType, RelativeDatePreset } from '@/lib/types'
import { ChevronRight, ChevronDown, Settings2, ArrowUp, ArrowDown, Database, Save, GripVertical, Download } from 'lucide-react'
import { usePivotConfig } from '@/hooks/use-pivot-config'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'
import { useSchema } from '@/hooks/use-schema'
import { usePivotFilters } from '@/hooks/use-pivot-filters'
import { useDashboard } from '@/lib/contexts/dashboard-context'
import type { MetricDefinition, DimensionDefinition } from '@/hooks/use-pivot-metrics'
import { PivotConfigPanel } from '@/components/pivot/pivot-config-panel'
import { PivotChartVisualization } from '@/components/pivot/pivot-chart-visualization'
import { PivotFilterPanel } from '@/components/pivot/pivot-filter-panel'
import { SignificanceIndicator, SignificanceButton } from '@/components/pivot/significance-indicator'
import { useSignificance } from '@/hooks/use-significance'
import DashboardSelectorModal from '@/components/modals/dashboard-selector-modal'
import { WidgetSelectorModal, type WidgetSelection } from '@/components/modals/widget-selector-modal'
import ExportModal, { type ExportFormat, type ExportData, type ExportOptions } from '@/components/modals/export-modal'
import type { WidgetCreateRequest } from '@/lib/api'
import html2canvas from 'html2canvas'

// Sort Dropdown Component for cleaner headers
interface SortDropdownProps {
  label: string
  metrics: string[]
  getMetricById: (id: string) => MetricDefinition | undefined
  activeMetric?: string | null
  activeDirection?: 'asc' | 'desc'
  onSort: (metricId: string) => void
  align?: 'left' | 'right'
  color?: 'blue' | 'purple'
  showFullLabel?: boolean
}

function SortDropdown({
  label,
  metrics,
  getMetricById,
  activeMetric,
  activeDirection,
  onSort,
  align = 'right',
  color = 'blue',
  showFullLabel = false
}: SortDropdownProps) {
  const [isOpen, setIsOpen] = useState(false)
  const dropdownRef = React.useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const activeMetricDef = activeMetric ? getMetricById(activeMetric) : null
  const hasActiveSort = !!activeMetric
  const bgColor = color === 'purple' ? 'bg-purple-600' : 'bg-blue-600'
  const hoverBgColor = color === 'purple' ? 'hover:bg-purple-700' : 'hover:bg-blue-700'

  return (
    <div className={`relative inline-block ${align === 'left' ? 'text-left' : 'text-right'}`} ref={dropdownRef}>
      <button
        onClick={(e) => {
          e.stopPropagation()
          setIsOpen(!isOpen)
        }}
        className={`inline-flex items-center gap-1 px-2 py-1 rounded text-xs transition-colors ${
          hasActiveSort
            ? `${bgColor} text-white ${hoverBgColor}`
            : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
        }`}
      >
        <span>{label}</span>
        {hasActiveSort && activeMetricDef && (
          <>
            <span className="font-medium">
              {showFullLabel ? activeMetricDef.label : activeMetricDef.label?.substring(0, 3)}
            </span>
            <span>{activeDirection === 'desc' ? '↓' : '↑'}</span>
          </>
        )}
        <ChevronDown className={`w-3 h-3 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {isOpen && (
        <div
          className={`absolute z-50 mt-1 bg-white border border-gray-200 rounded-md shadow-lg min-w-[120px] ${
            align === 'left' ? 'left-0' : 'right-0'
          }`}
        >
          <div className="py-1">
            {metrics.map((metricId) => {
              const metric = getMetricById(metricId)
              const isActive = activeMetric === metricId
              return (
                <button
                  key={metricId}
                  onClick={(e) => {
                    e.stopPropagation()
                    onSort(metricId)
                    setIsOpen(false)
                  }}
                  className={`w-full text-left px-3 py-1.5 text-xs transition-colors ${
                    isActive
                      ? `${bgColor} text-white`
                      : 'text-gray-700 hover:bg-gray-100'
                  }`}
                >
                  <span className="flex items-center justify-between">
                    <span>{metric?.label}</span>
                    {isActive && (
                      <span className="ml-2">{activeDirection === 'desc' ? '↓' : '↑'}</span>
                    )}
                  </span>
                </button>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

// Multi-Pivot Table Card Component
interface MultiPivotTableCardProps {
  headerLabel: string
  filters: any
  rowDimensions: string[]
  metricId: string
  limit: number
  getMetricById: (id: string) => MetricDefinition | undefined
  getDimensionByValue: (value: string) => DimensionDefinition | undefined
  tableId?: string
}

function MultiPivotTableCard({ headerLabel, filters, rowDimensions, metricId, limit, getMetricById, getDimensionByValue, tableId }: MultiPivotTableCardProps) {
  const metric = getMetricById(metricId)
  const [page, setPage] = useState(0)

  // Reset page when filters or dimensions change
  useEffect(() => {
    setPage(0)
  }, [headerLabel, rowDimensions, filters, limit])

  // Fetch data for this specific table
  const { data, isLoading, error } = useQuery({
    queryKey: ['multi-pivot', headerLabel, rowDimensions, filters, limit, page, tableId],
    queryFn: () => {
      const offset = page * limit
      // If no row dimensions, just show totals
      if (rowDimensions.length === 0) {
        return fetchPivotData([], filters, 1, 0, undefined, tableId)
      }
      // Otherwise fetch by first row dimension
      return fetchPivotData([rowDimensions[0]], filters, limit, offset, undefined, tableId)
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

interface PivotTableSectionProps {
  widgetMode?: boolean
  widgetConfig?: any
  onTabChange?: (tab: string) => void
}

export function PivotTableSection(props: PivotTableSectionProps = {}) {
  const { widgetMode = false, widgetConfig, onTabChange } = props
  const {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    updateFullDateRange,
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
    setChartType,
    triggerFetch,
    isStale,
    fetchRequested,
  } = usePivotConfig()


  // Dashboard context for widget editing
  const { editingWidget, setEditingWidget, currentDashboardId: editingDashboardId, setCurrentDashboardId } = useDashboard()
  const isEditingWidget = editingWidget !== null
  const stopEditingWidget = () => setEditingWidget(null)

  // Load dynamic metrics and dimensions from schema
  const { metrics, dimensions, getMetricById, getDimensionByValue, isLoading: isLoadingSchema } = usePivotMetrics(config.selectedTable || undefined)
  const { schema } = useSchema(config.selectedTable || undefined)

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
    date_range_type: config.dateRangeType,
    relative_date_preset: config.relativeDatePreset,
  })

  // Query client for cache invalidation
  const queryClient = useQueryClient()

  // Fix invalid filter values
  useEffect(() => {
    // Fix corrupted filter values
    if (pivotFilters.date_range_type !== 'absolute' && pivotFilters.date_range_type !== 'relative') {
      // Reset to absolute with the dates that are in config
      updateFilterDateRange('absolute', null, config.startDate, config.endDate)
      updateFullDateRange('absolute', null, config.startDate, config.endDate)
    }
  }, [config.startDate, config.endDate, config.dateRangeType, config.relativeDatePreset, pivotFilters, updateFilterDateRange, updateFullDateRange])

  // Wrapper function to update both config and filters when date range changes
  const handleDateRangeChange = useCallback((
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => {
    // Update filters state
    updateFilterDateRange(type, preset, startDate, endDate)

    // Update config state (which persists to localStorage)
    updateFullDateRange(type, preset, startDate, endDate)
  }, [updateFilterDateRange, updateFullDateRange])

  // Update widget mutation
  const updateWidgetMutation = useMutation({
    mutationFn: ({ dashboardId, widgetId, updates }: { dashboardId: string; widgetId: string; updates: any }) =>
      updateWidget(dashboardId, widgetId, updates),
    onSuccess: async (updatedDashboard) => {
      // Invalidate and refetch dashboard data
      await queryClient.invalidateQueries({ queryKey: ['dashboards'] })
      await queryClient.invalidateQueries({ queryKey: ['dashboard', editingDashboardId] })

      // Wait for refetch to complete
      await queryClient.refetchQueries({ queryKey: ['dashboard', editingDashboardId] })

      setSuccessMessage(`Widget updated successfully! Redirecting to dashboard...`)

      // Navigate to the dashboard after ensuring data is fresh
      setTimeout(() => {
        setSuccessMessage(null)
        stopEditingWidget()

        if (editingDashboardId && onTabChange) {
          setCurrentDashboardId(editingDashboardId)
          onTabChange('dashboards')
        }
      }, 500)
    },
  })

  // Initial sync of date range from config to filters on mount only
  useEffect(() => {
    updateFilterDateRange(
      config.dateRangeType || 'absolute',
      config.relativeDatePreset || null,
      config.startDate,
      config.endDate
    )
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // Only run on mount

  // Load widget configuration when editing a widget
  useEffect(() => {
    if (editingWidget) {
      // Load table
      updateTable(editingWidget.table_id)
      setDataSourceDropped(true)

      // Load dates
      if (editingWidget.start_date) updateStartDate(editingWidget.start_date)
      if (editingWidget.end_date) updateEndDate(editingWidget.end_date)
      if (editingWidget.start_date && editingWidget.end_date) {
        setDateRangeDropped(true)
      }

      // Load expanded rows state
      if (editingWidget.expanded_rows) {
        setExpandedRows(editingWidget.expanded_rows)
      }

      // Load column order and sort state
      if (editingWidget.column_order) {
        setColumnOrder(editingWidget.column_order)
      }
      if (editingWidget.column_sort) {
        setColumnSortConfig(editingWidget.column_sort)
      }

      // Load row sort config
      if (editingWidget.row_sort_config) {
        setSortConfig(editingWidget.row_sort_config as any)
      }

      // Clear existing dimensions and metrics
      // Then add from widget
      setTimeout(() => {
        // Load dimensions
        editingWidget.dimensions.forEach(dim => addDimension(dim))
        editingWidget.table_dimensions.forEach(dim => addTableDimension(dim))

        // Load metrics
        editingWidget.metrics.forEach(metric => addMetric(metric))

        // Load filters
        Object.entries(editingWidget.filters).forEach(([dimensionId, values]) => {
          // Ensure values is an array
          const valuesArray = Array.isArray(values) ? values : [values]
          updateDimensionFilter(dimensionId, valuesArray as string[])
        })
      }, 100)
    }
  }, [editingWidget])

  // Load widget configuration when in widget mode
  useEffect(() => {
    if (widgetMode && widgetConfig) {
      // Set table
      if (widgetConfig.table_id) {
        updateTable(widgetConfig.table_id)
        setDataSourceDropped(true)
      }

      // Set date range
      if (widgetConfig.start_date || widgetConfig.end_date) {
        updateStartDate(widgetConfig.start_date || '')
        updateEndDate(widgetConfig.end_date || '')
        setDateRangeDropped(true)
      }

      // Load expanded rows state
      if (widgetConfig.expanded_rows) {
        setExpandedRows(widgetConfig.expanded_rows)
      }

      // Load column order and sort state
      if (widgetConfig.column_order && widgetConfig.column_sort) {
        setColumnOrder(widgetConfig.column_order)
        setColumnSortConfig(widgetConfig.column_sort as { metric: string; direction: 'asc' | 'desc' })
      }

      // Load row sort config
      if (widgetConfig.row_sort_config) {
        setSortConfig(widgetConfig.row_sort_config as any)
      }

      // Load configuration
      setTimeout(() => {
        // Load dimensions
        widgetConfig.dimensions?.forEach((dim: string) => addDimension(dim))
        widgetConfig.table_dimensions?.forEach((dim: string) => addTableDimension(dim))

        // Load metrics
        widgetConfig.metrics?.forEach((metric: string) => addMetric(metric))

        // Load filters
        if (widgetConfig.filters) {
          Object.entries(widgetConfig.filters).forEach(([dimensionId, values]: [string, any]) => {
            // Ensure values is an array
            const valuesArray = Array.isArray(values) ? values : [values]
            updateDimensionFilter(dimensionId, valuesArray as string[])
          })
        }
      }, 100)
    }
  }, [widgetMode, widgetConfig])

  // Get primary sort metric from schema (fallback to first base metric if not set)
  const primarySortMetric = schema?.primary_sort_metric || metrics[0]?.id

  // Get UI state from config (with defaults)
  const isConfigOpen = config.isConfigOpen ?? true
  const expandedRowsArray = config.expandedRows ?? []
  const expandedRows = new Set(expandedRowsArray)
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

  // Use fetched config snapshot for query keys (prevents auto-refetch on config changes)
  // These values only change when user clicks "Fetch Data"
  const fetchedConfig = config.fetchedConfig
  const fetchedDimensions = fetchedConfig?.selectedDimensions || []
  const fetchedTableDimensions = fetchedConfig?.selectedTableDimensions || []
  const fetchedMetrics = fetchedConfig?.selectedMetrics || []
  const fetchedTable = fetchedConfig?.selectedTable || null

  // Build filters from fetched config (for stable query keys)
  const fetchedFilters = useMemo(() => {
    if (!fetchedConfig) return null
    return {
      start_date: fetchedConfig.startDate,
      end_date: fetchedConfig.endDate,
      date_range_type: fetchedConfig.dateRangeType,
      relative_date_preset: fetchedConfig.relativeDatePreset,
      dimension_filters: pivotFilters.dimension_filters, // Use current dimension filters
    }
  }, [fetchedConfig, pivotFilters.dimension_filters])
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
  const [columnSortConfig, setColumnSortConfig] = useState<{ metric: string; direction: 'asc' | 'desc' } | null>(null)
  const [childrenSortConfig, setChildrenSortConfig] = useState<{column: string, direction: 'asc' | 'desc'} | null>(null)
  const [isDashboardModalOpen, setIsDashboardModalOpen] = useState(false)
  const [isExportModalOpen, setIsExportModalOpen] = useState(false)
  const [isWidgetSelectorOpen, setIsWidgetSelectorOpen] = useState(false)
  const [selectedWidgetType, setSelectedWidgetType] = useState<WidgetSelection | null>(null)
  const [isUpdatingWidget, setIsUpdatingWidget] = useState(false) // Track if we're updating vs creating new
  const [successMessage, setSuccessMessage] = useState<string | null>(null)
  const CHILDREN_PAGE_SIZE = 10

  // Check if we have required configuration
  // Require data source, date range, and at least one metric to be configured
  const isConfigured = !!(config.isDataSourceDropped && config.isDateRangeDropped && selectedMetrics.length > 0)

  // Fetch all dimension values in a single query
  // Uses fetchedConfig values for stable query keys (only refetches when user clicks "Fetch Data")
  const { data: allDimensionValues } = useQuery({
    queryKey: ['all-dimension-values', fetchedTableDimensions, fetchedFilters, fetchedTable],
    queryFn: async () => {
      if (!fetchedFilters) return {}
      const results: Record<string, string[]> = {}
      for (const dimension of fetchedTableDimensions) {
        results[dimension] = await fetchDimensionValues(dimension, fetchedFilters, fetchedTable || undefined)
      }
      return results
    },
    enabled: fetchRequested && isConfigured && fetchedTableDimensions.length > 0,
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

  // Fetch data for all column combinations using two-step approach
  // Uses fetchedConfig values for stable query keys (only refetches when user clicks "Fetch Data")
  const { data: allColumnData, isLoading: isLoadingColumnData } = useQuery({
    queryKey: ['all-columns', tableCombinations, fetchedFilters, fetchedDimensions, customDimensions, fetchedMetrics, columnOrder],
    queryFn: async (): Promise<Record<number, PivotResponse>> => {
      const results: Record<number, PivotResponse> = {}

      if (tableCombinations.length === 0 || !fetchedFilters) {
        return results
      }

      // Pass ALL selected dimensions to create combined rows (e.g., "Channel A - Country A")
      const dims = fetchedDimensions

      // Helper function to build table filters for a combination
      const buildTableFilters = (combination: any) => {
        // Start with base filters (use fetched filters for stability)
        const tableFilters: Record<string, any> = { ...fetchedFilters }

        // Get the dimension keys that are being used as table dimensions
        const tableDimensionKeys = Object.keys(combination)

        // Remove table dimensions from dimension_filters to avoid conflicts
        if (tableFilters.dimension_filters) {
          const cleanedDimensionFilters = { ...tableFilters.dimension_filters }
          tableDimensionKeys.forEach(key => {
            if (!key.startsWith('custom_')) {
              delete cleanedDimensionFilters[key]
            }
          })
          tableFilters.dimension_filters = cleanedDimensionFilters
        }

        // Apply the specific values for this combination
        Object.entries(combination).forEach(([key, value]) => {
          if (key.startsWith('custom_') && customDimensions) {
            const customDimId = key.replace('custom_', '')
            const customDim = customDimensions.find(d => d.id === customDimId)
            if (customDim && customDim.values) {
              const dimValue = customDim.values.find(v => v.label === value)
              if (dimValue) {
                // Set date fields - handle both relative and absolute dates
                tableFilters.start_date = dimValue.start_date || null
                tableFilters.end_date = dimValue.end_date || null
                tableFilters.date_range_type = dimValue.date_range_type || 'absolute'
                tableFilters.relative_date_preset = dimValue.relative_date_preset || null
              }
            }
          } else {
            // For regular dimensions, add to dimension_filters
            if (!tableFilters.dimension_filters) {
              tableFilters.dimension_filters = {}
            }
            tableFilters.dimension_filters[key] = [value as string]
          }
        })

        return tableFilters
      }

      // TWO-STEP FETCH: First get primary (starred) column, then fetch others with same dimension values

      // STEP 1: Fetch primary column first
      const primaryColIndex = columnOrder.length > 0 ? columnOrder[0] : 0
      const primaryCombination = tableCombinations[primaryColIndex]
      const primaryFilters = buildTableFilters(primaryCombination)

      const primaryData = await fetchPivotData(
        dims,
        primaryFilters,
        100, // limit
        0,
        undefined, // no dimension_values filter for primary column
        fetchedTable || undefined,
        true,
        fetchedMetrics
      )

      results[primaryColIndex] = primaryData

      // STEP 2: Extract dimension values from primary column results
      const dimensionValues = primaryData.rows.map(row => row.dimension_value)

      // STEP 3: Fetch remaining columns using the same dimension values
      const remainingFetches = tableCombinations
        .map((combination, index) => ({ combination, index }))
        .filter(({ index }) => index !== primaryColIndex)
        .map(async ({ combination, index }) => {
          const tableFilters = buildTableFilters(combination)
          return {
            index,
            data: await fetchPivotData(
              dims,
              tableFilters,
              100, // limit ignored when dimension_values provided
              0,
              dimensionValues, // Filter to primary column's dimension values
              fetchedTable || undefined,
              true,
              fetchedMetrics
            )
          }
        })

      // Wait for all remaining fetches to complete
      const remainingResults = await Promise.all(remainingFetches)

      // Store results
      remainingResults.forEach(({ index, data }) => {
        results[index] = data
      })

      return results
    },
    enabled: fetchRequested && isConfigured && fetchedTableDimensions.length > 0 && tableCombinations.length > 0,
  })

  // Pass ALL selected dimensions to create combined rows (e.g., "Channel A - Country A")
  // Use fetched dimensions for stable query keys
  const allFetchedDimensions = fetchedDimensions

  // Main pivot data query - uses fetchedConfig values for stable query keys
  const { data: pivotData, isLoading, error } = useQuery({
    queryKey: ['pivot', allFetchedDimensions, fetchedFilters, fetchedTable, fetchedMetrics],
    queryFn: (): Promise<PivotResponse> => {
      if (!fetchedFilters) return Promise.resolve({ rows: [], total: {} as PivotRow, available_dimensions: [] })
      // If no dimensions, create a single "All Data" row manually
      if (fetchedDimensions.length === 0) {
        return fetchPivotData([], fetchedFilters, 1, 0, undefined, fetchedTable || undefined, true, fetchedMetrics).then(data => {
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
      // Pass all dimensions for combined row display
      return fetchPivotData(allFetchedDimensions, fetchedFilters, 100, 0, undefined, fetchedTable || undefined, true, fetchedMetrics)
    },
    enabled: fetchRequested && isConfigured, // Only fetch when data source and date range are configured AND fetch is requested
  })

  // Get rows for significance testing (before hook call)
  // These are the raw rows from the data source, before merge threshold processing
  const significanceRows = useMemo(() => {
    if (selectedTableDimensions.length > 0 && allColumnData && columnOrder.length > 0) {
      // Multi-table mode: use first column data
      const firstColIndex = columnOrder[0]
      return allColumnData[firstColIndex]?.rows || []
    } else {
      // Single-table mode: use pivotData
      return pivotData?.rows || []
    }
  }, [selectedTableDimensions, allColumnData, columnOrder, pivotData])

  // Significance testing hook
  const {
    hasResults: hasSignificanceResults,
    isLoading: isSignificanceLoading,
    error: significanceError,
    runSignificanceTest,
    clearResults: clearSignificanceResults,
    getSignificanceForCell,
    controlColumnIndex,
  } = useSignificance({
    allColumnData: allColumnData ?? null,
    tableCombinations,
    columnOrder,
    selectedMetrics,
    selectedTableDimensions,
    selectedRowDimensions: selectedDimensions,
    pivotRows: significanceRows,
    filters,
    tableId: selectedTable || undefined,
    enabled: isConfigured && selectedTableDimensions.length > 0 && columnOrder.length >= 2,
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

    // Combined dimensions don't support drill-down - all dimensions are already in a single row
    // Also convert backend percentage (0-100) to decimal (0-1) for consistent formatting
    let rowsWithChildren = rows.map(row => ({
      ...row,
      has_children: false, // No drill-down for combined dimensions
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

  // Clear significance results when table configuration changes
  React.useEffect(() => {
    clearSignificanceResults()
  }, [selectedMetrics, selectedDimensions, selectedTableDimensions, pivotFilters, columnOrder, clearSignificanceResults])

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
        const tableId = e.dataTransfer.getData('tableId')
        updateTable(tableId)
        setDataSourceDropped(true)
        break

      case 'daterange':
        const dateRangeType = e.dataTransfer.getData('dateRangeType') as DateRangeType || 'absolute'
        const relativeDatePreset = e.dataTransfer.getData('relativeDatePreset') as RelativeDatePreset || null
        const startDate = e.dataTransfer.getData('startDate') || null
        const endDate = e.dataTransfer.getData('endDate') || null

        // Use the new full update function
        updateFullDateRange(dateRangeType, relativeDatePreset, startDate, endDate)
        updateFilterDateRange(dateRangeType, relativeDatePreset, startDate, endDate)
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
                  if (customDim && customDim.values) {
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
                const pivotChildren = await fetchPivotData(dimensionsToFetch, filters, 1000, 0, undefined, selectedTable || undefined, true, selectedMetrics)

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
                if (customDim && customDim.values) {
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
        // _pct metrics are already in percentage form (e.g., 86.5 for 86.5%)
        // Other percent metrics (like conversion rates) are in decimal form (e.g., 0.865)
        if (metricId.endsWith('_pct')) {
          return `${value.toFixed(decimals)}%`
        }
        return `${(value * 100).toFixed(decimals)}%`
      default:
        return decimals > 0
          ? value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
          : Math.round(value).toLocaleString()
    }
  }

  // Helper to get format type for significance indicator
  // Checks metric definition first, then falls back to ID-based detection
  const getFormatTypeForMetric = (metricId: string): 'number' | 'currency' | 'percent' => {
    const metric = getMetricById(metricId)

    // If metric is found and has a format, use it
    if (metric?.format === 'percent') return 'percent'
    if (metric?.format === 'currency') return 'currency'

    // Fallback: detect from metric ID patterns
    // _pct suffix indicates percentage
    if (metricId.endsWith('_pct')) return 'percent'
    // Common percentage metric patterns
    if (metricId.includes('rate') || metricId.includes('ratio') || metricId.includes('conversion')) return 'percent'
    // Common currency patterns
    if (metricId.includes('revenue') || metricId.includes('price') || metricId.includes('cost') || metricId.includes('aov')) return 'currency'

    return 'number'
  }

  // Reset sort when dimensions change
  useEffect(() => {
    setSortConfig(undefined as any, undefined, undefined)
  }, [selectedDimensions, selectedTableDimensions, selectedMetrics, setSortConfig])

  // Handle column sort click (sorts rows within columns)
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
      setSortConfig(column, subColumn, 'desc', metric || selectedMetrics[0] || primarySortMetric)
    }
  }

  // Handle column order sort (reorders columns by metric values)
  const handleColumnSort = (metricId: string) => {
    // If clicking same metric, toggle direction or clear
    if (columnSortConfig && columnSortConfig.metric === metricId) {
      if (columnSortConfig.direction === 'desc') {
        setColumnSortConfig({ metric: metricId, direction: 'asc' })
      } else {
        // Clear column sort
        setColumnSortConfig(null)
        // Reset to original order
        setColumnOrder(tableCombinations.map((_, i) => i))
      }
    } else {
      // New metric, start with descending
      setColumnSortConfig({ metric: metricId, direction: 'desc' })
    }
  }

  // Effect to reorder columns when column sort config changes
  useEffect(() => {
    // Skip auto-sorting entirely in widget mode - widgets always use saved columnOrder
    if (widgetMode) {
      return
    }

    if (!columnSortConfig || !allColumnData || Object.keys(allColumnData).length === 0) return

    // Get all column indices from allColumnData
    const availableIndices = Object.keys(allColumnData).map(k => parseInt(k))
    if (availableIndices.length === 0) return

    // Get total metric values for each column
    const columnValues = availableIndices.map(colIndex => {
      const colData = allColumnData[colIndex]
      if (!colData || !colData.total) return { colIndex, value: 0 }
      const value = colData.total.metrics?.[columnSortConfig.metric] ?? 0
      return { colIndex, value }
    })

    // Sort by metric value
    const sorted = [...columnValues].sort((a, b) => {
      if (columnSortConfig.direction === 'desc') {
        return b.value - a.value
      } else {
        return a.value - b.value
      }
    })

    // Update column order
    const newOrder = sorted.map(item => item.colIndex)
    setColumnOrder(newOrder)

    // Clear children cache to refetch for new first column
    setChildrenCache({})
    setExpandedRows([])
  }, [columnSortConfig, allColumnData, widgetMode])

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
        const sortMetric = sortConfig.metric || selectedMetrics[0] || primarySortMetric

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
  const getRowValue = (row: PivotRow | PivotRow, metricId: string): number => {
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
            dateRangeType={pivotFilters.date_range_type}
            relativeDatePreset={pivotFilters.relative_date_preset}
            onDateRangeChange={handleDateRangeChange}
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

  // Show "ready to fetch" state when configured but not yet fetched
  if (!fetchRequested) {
    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Pivot Table</h2>
            <p className="text-sm text-gray-600 mt-1">Configure your options and click "Fetch Data" when ready</p>
          </div>
          <div className="flex items-center gap-3">
            {/* Fetch BigQuery Data button */}
            <button
              onClick={triggerFetch}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors"
              title="Fetch data from BigQuery"
            >
              <Database className="h-4 w-4" />
              Fetch Data
            </button>
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
            dateRangeType={pivotFilters.date_range_type}
            relativeDatePreset={pivotFilters.relative_date_preset}
            onDateRangeChange={handleDateRangeChange}
          />

          <div className="flex-1 min-w-0 bg-white shadow overflow-hidden rounded-lg">
            <div className="flex flex-col items-center justify-center h-96 text-center p-8">
              <div className="mb-4 text-gray-400">
                <Database className="h-16 w-16 mx-auto" />
              </div>
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                Ready to Fetch Data
              </h3>
              <p className="text-sm text-gray-600 max-w-md mb-4">
                Your pivot table is configured. Click "Fetch Data" to query BigQuery.
                You can modify dimensions, metrics, and filters before fetching.
              </p>
              <button
                onClick={triggerFetch}
                className="flex items-center gap-2 px-6 py-3 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors text-lg"
              >
                <Database className="h-5 w-5" />
                Fetch BigQuery Data
              </button>
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

  // Handler for "Save to Dashboard" button (creating new widget)
  const handleSaveToDashboard = () => {
    // Only allow saving if we have a configured pivot with metrics
    if (!isConfigured || selectedMetrics.length === 0) {
      return
    }

    setIsUpdatingWidget(false) // We're creating a new widget

    // In multi-table mode, show widget selector first
    if (selectedTableDimensions.length > 0) {
      setIsWidgetSelectorOpen(true)
    } else {
      // In pivot mode, go directly to dashboard selector (only one option: pivot table)
      setSelectedWidgetType({ type: 'table' })
      setIsDashboardModalOpen(true)
    }
  }

  // Handler for "Update Widget" button (updating existing widget)
  const handleUpdateWidgetClick = () => {
    if (!isEditingWidget || !editingWidget || !editingDashboardId) return
    if (!isConfigured || selectedMetrics.length === 0) return

    setIsUpdatingWidget(true) // We're updating existing widget

    // In multi-table mode, show widget selector to choose what to update
    if (selectedTableDimensions.length > 0) {
      setIsWidgetSelectorOpen(true)
    } else {
      // In pivot mode, update directly (only one option: pivot table)
      setSelectedWidgetType({ type: 'table' })
      performWidgetUpdate({ type: 'table' })
    }
  }

  // Handler for widget selection
  const handleWidgetSelection = (selection: WidgetSelection) => {
    setSelectedWidgetType(selection)
    setIsWidgetSelectorOpen(false)

    if (isUpdatingWidget) {
      // Update existing widget
      performWidgetUpdate(selection)
    } else {
      // Create new widget
      setIsDashboardModalOpen(true)
    }
  }

  // Perform the actual widget update
  const performWidgetUpdate = (selection: WidgetSelection) => {
    if (!isEditingWidget || !editingWidget || !editingDashboardId) return

    // Build updates based on selection
    let widgetType: 'table' | 'chart'
    let displayMode: 'pivot-table' | 'multi-table' | 'single-metric-chart'
    let widgetMetrics: string[]
    let widgetDimensions: string[]
    let widgetTableDimensions: string[]
    let widgetTitle: string

    if (selection.type === 'chart' && 'metricId' in selection) {
      widgetType = 'chart'
      displayMode = 'single-metric-chart'
      widgetMetrics = selectedMetrics // Save ALL selected metrics (not just the clicked one)
      widgetDimensions = selectedDimensions
      widgetTableDimensions = selectedTableDimensions // Keep table dimensions for charts
      const metricLabel = getMetricById(selection.metricId)?.label || selection.metricId
      widgetTitle = `${selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')} - ${metricLabel}`
    } else if (selection.type === 'multi-table') {
      widgetType = 'table'
      displayMode = 'multi-table'
      widgetMetrics = selectedMetrics
      widgetDimensions = selectedDimensions
      widgetTableDimensions = selectedTableDimensions
      const dimensionLabels = selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')
      const tableDimensionLabels = selectedTableDimensions.map(d => getDimensionLabel(d)).join(' × ')
      widgetTitle = `${dimensionLabels} - ${tableDimensionLabels} - ${selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')}`
    } else {
      widgetType = 'table'
      displayMode = 'pivot-table'
      widgetMetrics = selectedMetrics
      widgetDimensions = selectedDimensions
      widgetTableDimensions = []
      widgetTitle = `${selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')} - ${selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')}`
    }

    const widgetFilters = Object.fromEntries(
      Object.entries(pivotFilters.dimension_filters)
        .filter(([_, values]) => values && values.length > 0)
    )

    console.log('Saving widget with filters:', widgetFilters)
    console.log('pivotFilters.dimension_filters:', pivotFilters.dimension_filters)

    const updates = {
      type: widgetType,
      display_mode: displayMode,
      title: widgetTitle,
      dimensions: widgetDimensions,
      table_dimensions: widgetTableDimensions,
      metrics: widgetMetrics,
      filters: widgetFilters,
      start_date: config.startDate || null,
      end_date: config.endDate || null,
      chart_type: widgetType === 'chart' ? (config.chartType || 'bar') : null,
      // UI state - complete persistence
      expanded_rows: config.expandedRows || [],
      column_order: columnOrder.length > 0 ? columnOrder : undefined,
      column_sort: columnSortConfig || undefined,
      // Additional editor state for complete persistence
      date_range_type: config.dateRangeType || null,
      relative_date_preset: config.relativeDatePreset || null,
      visible_metrics: widgetType === 'chart' && selection.type === 'chart' && 'metricId' in selection
        ? [selection.metricId]  // For charts, set the clicked metric as the default visible metric
        : config.selectedDisplayMetrics || null,
      merge_threshold: mergeThreshold || null,
      dimension_sort_order: dimensionSortOrder || null,
      children_sort_config: childrenSortConfig || null,
      row_sort_config: sortConfig || null,
    }

    updateWidgetMutation.mutate({
      dashboardId: editingDashboardId,
      widgetId: editingWidget.id,
      updates,
    })
  }

  const handleDashboardSuccess = async (dashboardId: string, dashboardName: string) => {
    // Invalidate and refetch dashboard data
    await queryClient.invalidateQueries({ queryKey: ['dashboards'] })
    await queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })

    // Wait for refetch to complete
    await queryClient.refetchQueries({ queryKey: ['dashboard', dashboardId] })

    setSuccessMessage(`Widget added to "${dashboardName}" successfully! Redirecting to dashboard...`)

    // Navigate to the dashboard after ensuring data is fresh
    setTimeout(() => {
      setSuccessMessage(null)
      // Reset widget selection state
      setSelectedWidgetType(null)

      if (onTabChange) {
        setCurrentDashboardId(dashboardId)
        onTabChange('dashboards')
      }
    }, 500)
  }

  const handleDashboardModalClose = () => {
    setIsDashboardModalOpen(false)
    // Reset widget selection state when closing
    setSelectedWidgetType(null)
  }

  // Build export data for modal
  // rowLimit: optional limit on number of dimension value rows (for PNG export)
  const getExportData = (rowLimit?: number): ExportData | null => {
    // Build metadata
    const dateRange = pivotFilters.date_range_type === 'relative' && pivotFilters.relative_date_preset
      ? `${pivotFilters.relative_date_preset} (${config.startDate} to ${config.endDate})`
      : `${config.startDate} to ${config.endDate}`

    const dimensionLabels = selectedDimensions.length > 0
      ? selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')
      : ''

    const tableDimensionLabels = selectedTableDimensions.length > 0
      ? selectedTableDimensions.map(d => getDimensionLabel(d)).join(' x ')
      : undefined

    const metricLabels = selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')

    const activeFilters = Object.entries(pivotFilters.dimension_filters || {})
      .filter(([_, values]) => values && values.length > 0)
      .map(([dimId, values]) => ({
        label: getDimensionLabel(dimId),
        values: (values as string[]).join(', ')
      }))

    // Check if we're in multi-table mode
    const isMultiTableMode = selectedTableDimensions.length > 0 && allColumnData && columnOrder.length > 0

    if (isMultiTableMode) {
      // Multi-table mode export
      const firstColIndex = columnOrder[0]
      const firstColData = allColumnData?.[firstColIndex]

      if (!firstColData?.rows) return null

      // Get column labels from tableCombinations
      const getColumnLabel = (colIndex: number) => {
        const combination = tableCombinations[colIndex]
        if (!combination) return `Column ${colIndex}`
        return Object.entries(combination)
          .map(([dimId, value]) => `${getDimensionLabel(dimId)}: ${value}`)
          .join(', ')
      }

      // Build headers: Dimension | Metric | Col1 Value | Col2 Value | Col2 Diff | Col2 % Diff | ...
      const dimensionLabel = selectedDimensions.length > 0
        ? selectedDimensions.map(d => getDimensionLabel(d)).join(' - ')
        : 'Total'

      const headers: string[] = [dimensionLabel, 'Metric']
      columnOrder.forEach((colIndex, orderIndex) => {
        const colLabel = getColumnLabel(colIndex)
        if (orderIndex === 0) {
          headers.push(colLabel)
        } else {
          headers.push(`${colLabel}`)
          headers.push('Diff')
          headers.push('% Diff')
        }
      })

      // Build rows - one row per dimension value per metric
      const rows: (string | number)[][] = []

      // Get dimension values from first column
      const allDimensionValues = selectedDimensions.length === 0
        ? ['Total']
        : firstColData.rows.map((r: any) => r.dimension_value)

      // Apply row limit if specified (limit dimension values, not metric rows)
      const dimensionRowCount = allDimensionValues.length
      const dimensionValues = rowLimit && rowLimit < allDimensionValues.length
        ? allDimensionValues.slice(0, rowLimit)
        : allDimensionValues

      dimensionValues.forEach((dimValue: string) => {
        selectedMetrics.forEach((metricId) => {
          const metricLabel = getMetricById(metricId)?.label || metricId
          const row: (string | number)[] = [dimValue, metricLabel]

          // Get first column value for diff calculations
          let firstValue: number | null = null
          if (selectedDimensions.length === 0) {
            firstValue = allColumnData?.[columnOrder[0]]?.total?.metrics?.[metricId] ?? null
          } else {
            const firstRowData = allColumnData?.[columnOrder[0]]?.rows?.find((r: any) => r.dimension_value === dimValue)
            firstValue = firstRowData?.metrics?.[metricId] ?? null
          }

          columnOrder.forEach((colIndex, orderIndex) => {
            let value: number | null = null
            if (selectedDimensions.length === 0) {
              value = allColumnData?.[colIndex]?.total?.metrics?.[metricId] ?? null
            } else {
              const rowData = allColumnData?.[colIndex]?.rows?.find((r: any) => r.dimension_value === dimValue)
              value = rowData?.metrics?.[metricId] ?? null
            }

            if (orderIndex === 0) {
              row.push(value != null ? formatMetricValue(value, metricId) : '-')
            } else {
              row.push(value != null ? formatMetricValue(value, metricId) : '-')
              // Diff
              const diff = (value ?? 0) - (firstValue ?? 0)
              row.push(formatMetricValue(diff, metricId))
              // % Diff
              const pctDiff = (firstValue ?? 0) !== 0
                ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                : null
              row.push(pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-')
            }
          })

          rows.push(row)
        })
      })

      return {
        metadata: {
          exportDate: new Date().toISOString(),
          dataSource: selectedTable || undefined,
          dateRange,
          dimensions: dimensionLabels,
          tableDimensions: tableDimensionLabels,
          metrics: metricLabels,
          filters: activeFilters,
        },
        headers,
        rows,
        dimensionRowCount,  // Total dimension values before any limit
      }
    } else {
      // Single table mode (original behavior)
      if (!pivotData?.rows) return null

      const dimensionLabel = selectedDimensions.length > 0
        ? selectedDimensions.map(d => getDimensionLabel(d)).join(' - ')
        : 'Dimension'
      const metricHeaders = selectedMetrics.map(m => getMetricById(m)?.label || m)
      const headers = [dimensionLabel, ...metricHeaders]

      // Apply row limit if specified
      const dimensionRowCount = pivotData.rows.length
      const rowsToExport = rowLimit && rowLimit < pivotData.rows.length
        ? pivotData.rows.slice(0, rowLimit)
        : pivotData.rows

      const rows = rowsToExport.map(row => {
        const metricValues = selectedMetrics.map(metricId => {
          const value = row.metrics[metricId]
          return formatMetricValue(value, metricId)
        })
        return [row.dimension_value, ...metricValues]
      })

      return {
        metadata: {
          exportDate: new Date().toISOString(),
          dataSource: selectedTable || undefined,
          dateRange,
          dimensions: dimensionLabels,
          tableDimensions: tableDimensionLabels,
          metrics: metricLabels,
          filters: activeFilters,
        },
        headers,
        rows,
        dimensionRowCount,  // Total rows before any limit
      }
    }
  }

  // Handle export based on format
  const handleExport = async (options: ExportOptions) => {
    const data = getExportData(options.rowLimit)
    if (!data) return

    if (options.format === 'csv') {
      exportAsCSV(data)
    } else if (options.format === 'html') {
      exportAsHTML(data)
    } else if (options.format === 'png') {
      await exportAsPNG(data)
    }
  }

  // Export as CSV
  const exportAsCSV = (data: ExportData) => {
    // Helper to escape CSV cells
    const escapeCell = (cell: string | number | null | undefined): string => {
      const str = String(cell ?? '')
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`
      }
      return str
    }

    // Build metadata rows
    const metadataRows: string[][] = [
      ['Export Date', data.metadata.exportDate],
    ]
    if (data.metadata.dataSource) {
      metadataRows.push(['Data Source', data.metadata.dataSource])
    }
    metadataRows.push(['Date Range', data.metadata.dateRange])
    if (data.metadata.dimensions) {
      metadataRows.push(['Dimensions', data.metadata.dimensions])
    }
    if (data.metadata.tableDimensions) {
      metadataRows.push(['Table Dimensions', data.metadata.tableDimensions])
    }
    metadataRows.push(['Metrics', data.metadata.metrics])
    data.metadata.filters.forEach(f => {
      metadataRows.push(['Filter: ' + f.label, f.values])
    })

    // Combine all rows
    const allRows: (string | number)[][] = [
      ...metadataRows,
      [],
      data.headers,
      ...data.rows
    ]

    const csvContent = allRows
      .map(row => row.map(escapeCell).join(','))
      .join('\n')

    downloadFile(csvContent, 'text/csv;charset=utf-8;', `pivot-table-${new Date().toISOString().split('T')[0]}.csv`)
  }

  // Export as HTML with table and chart
  const exportAsHTML = (data: ExportData) => {
    // Get raw numeric values for chart (before formatting)
    const chartData = pivotData?.rows.map(row => ({
      label: row.dimension_value,
      values: selectedMetrics.map(metricId => row.metrics[metricId] ?? 0)
    })) || []

    const metricLabels = selectedMetrics.map(m => getMetricById(m)?.label || m)

    // Generate colors for each metric
    const colors = [
      'rgba(59, 130, 246, 0.8)',   // blue
      'rgba(16, 185, 129, 0.8)',   // green
      'rgba(245, 158, 11, 0.8)',   // amber
      'rgba(239, 68, 68, 0.8)',    // red
      'rgba(139, 92, 246, 0.8)',   // purple
      'rgba(236, 72, 153, 0.8)',   // pink
      'rgba(20, 184, 166, 0.8)',   // teal
      'rgba(249, 115, 22, 0.8)',   // orange
    ]

    const htmlContent = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Pivot Table Export - ${new Date().toISOString().split('T')[0]}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f3f4f6; padding: 24px; color: #1f2937; }
    .container { max-width: 1200px; margin: 0 auto; }
    h1 { font-size: 24px; font-weight: 600; margin-bottom: 8px; }
    .subtitle { color: #6b7280; margin-bottom: 24px; }
    .card { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 24px; overflow: hidden; }
    .card-header { padding: 16px 20px; border-bottom: 1px solid #e5e7eb; font-weight: 600; }
    .card-body { padding: 20px; }
    .metadata { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; }
    .metadata-item { display: flex; flex-direction: column; }
    .metadata-label { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; }
    .metadata-value { font-size: 14px; font-weight: 500; margin-top: 2px; }
    .chart-container { height: 400px; position: relative; }
    .chart-controls { margin-bottom: 16px; display: flex; gap: 8px; flex-wrap: wrap; }
    .chart-controls button { padding: 8px 16px; border: 1px solid #d1d5db; border-radius: 6px; background: white; cursor: pointer; font-size: 14px; transition: all 0.2s; }
    .chart-controls button:hover { border-color: #3b82f6; color: #3b82f6; }
    .chart-controls button.active { background: #3b82f6; color: white; border-color: #3b82f6; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th { background: #f9fafb; text-align: left; padding: 12px 16px; font-weight: 600; border-bottom: 2px solid #e5e7eb; }
    th:not(:first-child) { text-align: right; }
    td { padding: 12px 16px; border-bottom: 1px solid #e5e7eb; }
    td:not(:first-child) { text-align: right; font-variant-numeric: tabular-nums; }
    tr:hover { background: #f9fafb; }
    .filters { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .filter-tag { background: #fef3c7; color: #92400e; padding: 4px 12px; border-radius: 9999px; font-size: 13px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Pivot Table Export</h1>
    <p class="subtitle">Generated on ${new Date().toLocaleString()}</p>

    <!-- Metadata Card -->
    <div class="card">
      <div class="card-header">Export Configuration</div>
      <div class="card-body">
        <div class="metadata">
          ${data.metadata.dataSource ? `<div class="metadata-item"><span class="metadata-label">Data Source</span><span class="metadata-value">${data.metadata.dataSource}</span></div>` : ''}
          <div class="metadata-item"><span class="metadata-label">Date Range</span><span class="metadata-value">${data.metadata.dateRange}</span></div>
          ${data.metadata.dimensions ? `<div class="metadata-item"><span class="metadata-label">Dimensions</span><span class="metadata-value">${data.metadata.dimensions}</span></div>` : ''}
          ${data.metadata.tableDimensions ? `<div class="metadata-item"><span class="metadata-label">Table Dimensions</span><span class="metadata-value">${data.metadata.tableDimensions}</span></div>` : ''}
          <div class="metadata-item"><span class="metadata-label">Metrics</span><span class="metadata-value">${data.metadata.metrics}</span></div>
        </div>
        ${data.metadata.filters.length > 0 ? `
        <div class="filters">
          ${data.metadata.filters.map(f => `<span class="filter-tag">${f.label}: ${f.values}</span>`).join('')}
        </div>` : ''}
      </div>
    </div>

    <!-- Chart Card -->
    <div class="card">
      <div class="card-header">Chart Visualization</div>
      <div class="card-body">
        <div class="chart-controls">
          <button class="active" onclick="setChartType('bar')">Bar Chart</button>
          <button onclick="setChartType('line')">Line Chart</button>
          <button onclick="setChartType('pie')">Pie Chart</button>
        </div>
        <div class="chart-controls">
          ${metricLabels.map((label, i) => `<button class="${i === 0 ? 'active' : ''}" onclick="setMetric(${i})">${label}</button>`).join('')}
        </div>
        <div class="chart-container">
          <canvas id="chart"></canvas>
        </div>
      </div>
    </div>

    <!-- Table Card -->
    <div class="card">
      <div class="card-header">Data Table (${data.rows.length} rows)</div>
      <div class="card-body" style="padding: 0; overflow-x: auto;">
        <table>
          <thead>
            <tr>
              ${data.headers.map(h => `<th>${h}</th>`).join('')}
            </tr>
          </thead>
          <tbody>
            ${data.rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const chartData = ${JSON.stringify(chartData)};
    const metricLabels = ${JSON.stringify(metricLabels)};
    const colors = ${JSON.stringify(colors)};
    let currentMetricIndex = 0;
    let currentChartType = 'bar';
    let chart;

    function createChart() {
      const ctx = document.getElementById('chart').getContext('2d');
      const labels = chartData.map(d => d.label);
      const values = chartData.map(d => d.values[currentMetricIndex]);

      if (chart) chart.destroy();

      const config = {
        type: currentChartType === 'pie' ? 'pie' : currentChartType,
        data: {
          labels: labels,
          datasets: [{
            label: metricLabels[currentMetricIndex],
            data: values,
            backgroundColor: currentChartType === 'pie' ? colors.slice(0, labels.length) : colors[currentMetricIndex % colors.length],
            borderColor: currentChartType === 'line' ? colors[currentMetricIndex % colors.length] : undefined,
            borderWidth: currentChartType === 'line' ? 2 : 0,
            fill: currentChartType === 'line' ? false : undefined,
            tension: 0.3,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: currentChartType === 'pie' }
          },
          scales: currentChartType === 'pie' ? {} : {
            y: { beginAtZero: true }
          }
        }
      };

      chart = new Chart(ctx, config);
    }

    function setChartType(type) {
      currentChartType = type;
      document.querySelectorAll('.chart-controls:first-of-type button').forEach(b => b.classList.remove('active'));
      event.target.classList.add('active');
      createChart();
    }

    function setMetric(index) {
      currentMetricIndex = index;
      document.querySelectorAll('.chart-controls:nth-of-type(2) button').forEach(b => b.classList.remove('active'));
      event.target.classList.add('active');
      createChart();
    }

    createChart();
  </script>
</body>
</html>`

    downloadFile(htmlContent, 'text/html;charset=utf-8;', `pivot-table-${new Date().toISOString().split('T')[0]}.html`)
  }

  // Helper to download file
  const downloadFile = (content: string, mimeType: string, filename: string) => {
    const blob = new Blob([content], { type: mimeType })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    link.click()
    URL.revokeObjectURL(url)
  }

  // Build table HTML for PNG export
  const buildExportTableHTML = (data: ExportData): string => {
    // Check if we're in multi-table mode
    const isMultiTableMode = selectedTableDimensions.length > 0 && allColumnData && columnOrder.length > 0

    // Helper to format significance indicator for export
    const formatSignificanceForExport = (sigResult: any) => {
      if (!sigResult) return { indicator: '', displayProb: '' }
      const probBeat = sigResult.prob_beat_control * 100
      // For "worse" direction, show probability of being worse (1 - prob_beat_control)
      // For "better" direction, show probability of being better (prob_beat_control)
      const displayProb = sigResult.direction === 'worse'
        ? (100 - probBeat).toFixed(1)
        : probBeat.toFixed(1)

      if (sigResult.direction === 'better') {
        return {
          indicator: `<span style="color: #16a34a; font-weight: 500;">↑</span>`,
          displayProb: `<span style="color: #16a34a;">${displayProb}%</span>`
        }
      } else if (sigResult.direction === 'worse') {
        return {
          indicator: `<span style="color: #dc2626; font-weight: 500;">↓</span>`,
          displayProb: `<span style="color: #dc2626;">${displayProb}%</span>`
        }
      }
      return {
        indicator: `<span style="color: #6b7280;">~</span>`,
        displayProb: `<span style="color: #6b7280;">${displayProb}%</span>`
      }
    }

    // Build metadata section
    const metadataHTML = `
      <div style="margin-bottom: 24px; padding-bottom: 16px; border-bottom: 2px solid #e5e7eb;">
        <h1 style="font-size: 20px; font-weight: 600; margin-bottom: 8px; color: #111827; margin-top: 0;">Pivot Table Export</h1>
        <div style="display: flex; flex-wrap: wrap; gap: 16px; font-size: 14px; color: #6b7280;">
          ${data.metadata.dataSource ? `<span><strong>Data Source:</strong> ${data.metadata.dataSource}</span>` : ''}
          <span><strong>Date Range:</strong> ${data.metadata.dateRange}</span>
          ${data.metadata.dimensions ? `<span><strong>Dimensions:</strong> ${data.metadata.dimensions}</span>` : ''}
          <span><strong>Metrics:</strong> ${data.metadata.metrics}</span>
        </div>
        ${data.metadata.filters.length > 0 ? `
          <div style="margin-top: 8px; display: flex; flex-wrap: wrap; gap: 8px;">
            ${data.metadata.filters.map(f => `
              <span style="background: #fef3c7; color: #92400e; padding: 2px 10px; border-radius: 9999px; font-size: 12px;">
                ${f.label}: ${f.values}
              </span>
            `).join('')}
          </div>
        ` : ''}
        <div style="margin-top: 8px; font-size: 12px; color: #9ca3af;">
          Generated: ${new Date().toLocaleString()}
        </div>
      </div>
    `

    if (isMultiTableMode) {
      // Multi-table mode - render with proper structure matching the UI
      const getColumnLabel = (colIndex: number) => {
        const combination = tableCombinations[colIndex]
        if (!combination) return `Column ${colIndex}`
        return selectedTableDimensions.map((dim) => {
          const dimLabel = getDimensionLabel(dim)
          return `${dimLabel}: ${combination[dim]}`
        }).join(' | ')
      }

      // Extract unique dimension values from data.rows (respects row limit)
      // In multi-table mode, data.rows format is: [dimValue, metricLabel, ...values]
      // Each dimension value appears multiple times (once per metric), so we need unique values
      const dimensionValues: string[] = []
      const seenDimValues = new Set<string>()
      data.rows.forEach(row => {
        const dimValue = String(row[0])
        if (!seenDimValues.has(dimValue)) {
          seenDimValues.add(dimValue)
          dimensionValues.push(dimValue)
        }
      })

      // Build header rows
      const headerRow1 = `
        <tr style="background: #f9fafb;">
          <th style="padding: 12px 16px; text-align: left; font-weight: 600; border-bottom: 1px solid #e5e7eb; border-right: 1px solid #e5e7eb; white-space: nowrap; color: #111827;">
            ${selectedDimensions.length > 0 ? selectedDimensions.map(d => getDimensionLabel(d)).join(' - ') : 'Summary'}
          </th>
          <th style="padding: 12px 16px; text-align: left; font-weight: 600; border-bottom: 1px solid #e5e7eb; border-right: 2px solid #d1d5db; white-space: nowrap; color: #111827;">
            Metric
          </th>
          ${columnOrder.map((colIndex, orderIndex) => {
            // First column = 1, others = 4 (value, diff, %diff, sig) if significance results exist, else 3
            const colSpan = orderIndex === 0 ? 1 : (hasSignificanceResults ? 4 : 3)
            const bgColor = orderIndex === 0 ? 'background: #ecfdf5;' : ''
            return `
              <th colspan="${colSpan}" style="padding: 12px 16px; text-align: center; font-weight: 600; border-bottom: 1px solid #e5e7eb; ${orderIndex === 0 ? 'border-right: 2px solid #d1d5db;' : 'border-right: 1px solid #e5e7eb;'} white-space: nowrap; color: #111827; ${bgColor}">
                ${orderIndex === 0 ? '★ ' : ''}${getColumnLabel(colIndex)}
              </th>
            `
          }).join('')}
        </tr>
      `

      // Sub-header row with Value, Diff, % Diff, Sig
      const headerRow2 = `
        <tr style="background: #f9fafb;">
          <th style="padding: 8px 16px; border-bottom: 2px solid #e5e7eb; border-right: 1px solid #e5e7eb;"></th>
          <th style="padding: 8px 16px; border-bottom: 2px solid #e5e7eb; border-right: 2px solid #d1d5db;"></th>
          ${columnOrder.map((_, orderIndex) => {
            if (orderIndex === 0) {
              return `<th style="padding: 8px 16px; text-align: right; font-weight: 500; font-size: 12px; border-bottom: 2px solid #e5e7eb; border-right: 2px solid #d1d5db; color: #6b7280; background: #ecfdf5;">Value</th>`
            } else {
              const sigHeader = hasSignificanceResults
                ? `<th style="padding: 8px 16px; text-align: right; font-weight: 500; font-size: 12px; border-bottom: 2px solid #e5e7eb; border-right: 1px solid #e5e7eb; color: #6b7280;">Sig</th>`
                : ''
              return `
                <th style="padding: 8px 16px; text-align: right; font-weight: 500; font-size: 12px; border-bottom: 2px solid #e5e7eb; color: #6b7280;">Value</th>
                <th style="padding: 8px 16px; text-align: right; font-weight: 500; font-size: 12px; border-bottom: 2px solid #e5e7eb; color: #6b7280;">Diff</th>
                <th style="padding: 8px 16px; text-align: right; font-weight: 500; font-size: 12px; border-bottom: 2px solid #e5e7eb; ${hasSignificanceResults ? '' : 'border-right: 1px solid #e5e7eb;'} color: #6b7280;">% Diff</th>
                ${sigHeader}
              `
            }
          }).join('')}
        </tr>
      `

      // Build data rows - render dimension value on each row but only show on first metric
      let rowsHTML = ''
      dimensionValues.forEach((dimValue: string, dimIndex: number) => {
        const numMetrics = selectedMetrics.length

        selectedMetrics.forEach((metricId, metricIndex) => {
          const metricLabel = getMetricById(metricId)?.label || metricId
          const isFirstMetric = metricIndex === 0
          const isLastMetric = metricIndex === numMetrics - 1
          const rowBg = dimIndex % 2 === 0 ? '#ffffff' : '#f9fafb'

          // Get first column value for diff calculations
          let firstValue: number | null = null
          if (selectedDimensions.length === 0) {
            firstValue = allColumnData?.[columnOrder[0]]?.total?.metrics?.[metricId] ?? null
          } else {
            const firstRowData = allColumnData?.[columnOrder[0]]?.rows?.find((r: any) => r.dimension_value === dimValue)
            firstValue = firstRowData?.metrics?.[metricId] ?? null
          }

          rowsHTML += `<tr style="background: ${rowBg};">`

          // Dimension value cell - show value only on first metric row, empty on others
          // Use border-bottom only on last metric row of each group
          const dimCellBorderBottom = isLastMetric ? 'border-bottom: 2px solid #d1d5db;' : 'border-bottom: 1px solid #e5e7eb;'
          rowsHTML += `
            <td style="padding: ${isFirstMetric ? '12px' : '4px'} 16px; text-align: left; font-weight: 500; border-right: 1px solid #e5e7eb; ${dimCellBorderBottom} white-space: nowrap; color: #111827; vertical-align: top;">
              ${isFirstMetric ? dimValue : ''}
            </td>
          `

          // Metric name cell
          rowsHTML += `
            <td style="padding: 8px 16px; text-align: left; font-size: 13px; border-right: 2px solid #d1d5db; ${isLastMetric ? 'border-bottom: 2px solid #d1d5db;' : 'border-bottom: 1px solid #e5e7eb;'} white-space: nowrap; color: #6b7280;">
              ${metricLabel}
            </td>
          `

          // Data columns
          columnOrder.forEach((colIndex, orderIndex) => {
            let value: number | null = null
            if (selectedDimensions.length === 0) {
              value = allColumnData?.[colIndex]?.total?.metrics?.[metricId] ?? null
            } else {
              const rowData = allColumnData?.[colIndex]?.rows?.find((r: any) => r.dimension_value === dimValue)
              value = rowData?.metrics?.[metricId] ?? null
            }

            const formattedValue = value != null ? formatMetricValue(value, metricId) : '-'
            const borderBottom = isLastMetric ? 'border-bottom: 2px solid #d1d5db;' : 'border-bottom: 1px solid #e5e7eb;'

            if (orderIndex === 0) {
              rowsHTML += `
                <td style="padding: 8px 16px; text-align: right; font-weight: 500; border-right: 2px solid #d1d5db; ${borderBottom} white-space: nowrap; color: #111827; background: #ecfdf5;">
                  ${formattedValue}
                </td>
              `
            } else {
              const diff = (value ?? 0) - (firstValue ?? 0)
              const pctDiff = (firstValue ?? 0) !== 0
                ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                : null

              // Get significance result for this cell
              const rowId = selectedDimensions.length === 0 ? undefined : dimValue
              const sigResult = hasSignificanceResults ? getSignificanceForCell(colIndex, metricId, rowId) : null
              const sigData = formatSignificanceForExport(sigResult)

              // Significance column (only if results exist)
              const sigCell = hasSignificanceResults
                ? `<td style="padding: 8px 16px; text-align: right; border-right: 1px solid #e5e7eb; ${borderBottom} white-space: nowrap;">
                    ${sigData.indicator} ${sigData.displayProb}
                  </td>`
                : ''

              rowsHTML += `
                <td style="padding: 8px 16px; text-align: right; font-weight: 500; ${borderBottom} white-space: nowrap; color: #111827;">
                  ${formattedValue}
                </td>
                <td style="padding: 8px 16px; text-align: right; ${borderBottom} white-space: nowrap; color: #374151;">
                  ${formatMetricValue(diff, metricId)}
                </td>
                <td style="padding: 8px 16px; text-align: right; ${hasSignificanceResults ? '' : 'border-right: 1px solid #e5e7eb;'} ${borderBottom} white-space: nowrap; color: #374151;">
                  ${pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}
                </td>
                ${sigCell}
              `
            }
          })

          rowsHTML += '</tr>'
        })
      })

      const tableHTML = `
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
          <thead>
            ${headerRow1}
            ${headerRow2}
          </thead>
          <tbody>
            ${rowsHTML}
          </tbody>
        </table>
      `

      return metadataHTML + tableHTML
    } else {
      // Single table mode - simple table
      const tableHTML = `
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
          <thead>
            <tr style="background: #f9fafb;">
              ${data.headers.map((h, i) => `
                <th style="padding: 12px 16px; text-align: ${i === 0 ? 'left' : 'right'}; font-weight: 600; border-bottom: 2px solid #e5e7eb; white-space: nowrap; color: #111827;">
                  ${h}
                </th>
              `).join('')}
            </tr>
          </thead>
          <tbody>
            ${data.rows.map((row, rowIdx) => `
              <tr style="background: ${rowIdx % 2 === 0 ? '#ffffff' : '#f9fafb'};">
                ${row.map((cell, i) => `
                  <td style="padding: 12px 16px; text-align: ${i === 0 ? 'left' : 'right'}; border-bottom: 1px solid #e5e7eb; white-space: nowrap; color: #374151;">
                    ${cell}
                  </td>
                `).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      `

      return metadataHTML + tableHTML
    }
  }

  // Export as PNG image
  const exportAsPNG = async (data: ExportData) => {
    // Warn for very large tables
    if (data.rows.length > 500) {
      const confirmed = window.confirm(
        `This table has ${data.rows.length} rows. Large tables may take longer to export. Continue?`
      )
      if (!confirmed) return
    }

    // Show loading overlay
    const loadingOverlay = document.createElement('div')
    loadingOverlay.className = 'fixed inset-0 bg-black/50 flex items-center justify-center z-[100]'
    loadingOverlay.innerHTML = `
      <div style="background: white; border-radius: 8px; padding: 24px; display: flex; flex-direction: column; align-items: center; gap: 12px;">
        <div style="width: 32px; height: 32px; border: 3px solid #e5e7eb; border-top-color: #3b82f6; border-radius: 50%; animation: spin 1s linear infinite;"></div>
        <p style="color: #374151; margin: 0;">Generating image...</p>
      </div>
      <style>
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      </style>
    `
    document.body.appendChild(loadingOverlay)

    try {
      // Create off-screen container for the complete table
      const container = document.createElement('div')
      container.style.cssText = `
        position: absolute;
        left: -9999px;
        top: 0;
        background: white;
        padding: 24px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      `
      document.body.appendChild(container)

      // Build the complete table HTML
      container.innerHTML = buildExportTableHTML(data)

      // Wait for styles to apply
      await new Promise(resolve => setTimeout(resolve, 100))

      // Capture with html2canvas
      const canvas = await html2canvas(container, {
        scale: 2, // 2x for high DPI/retina quality
        useCORS: true,
        logging: false,
        backgroundColor: '#ffffff',
        windowWidth: container.scrollWidth,
        windowHeight: container.scrollHeight,
      })

      // Clean up the container
      document.body.removeChild(container)

      // Convert to PNG and download
      canvas.toBlob((blob) => {
        if (blob) {
          const url = URL.createObjectURL(blob)
          const link = document.createElement('a')
          link.href = url
          link.download = `pivot-table-${new Date().toISOString().split('T')[0]}.png`
          link.click()
          URL.revokeObjectURL(url)
        }
      }, 'image/png')
    } catch (error) {
      console.error('PNG export failed:', error)
      alert('Failed to generate image. The table may be too large. Try exporting fewer rows or using CSV/HTML format.')
    } finally {
      // Remove loading overlay
      document.body.removeChild(loadingOverlay)
    }
  }

  // Build widget config from current pivot state and selected widget type
  const currentWidgetConfig: Omit<WidgetCreateRequest, 'position'> = (() => {
    // Determine widget type and metrics based on selection
    let widgetType: 'table' | 'chart'
    let widgetMetrics: string[]
    let widgetTitle: string
    let widgetDimensions: string[]
    let widgetTableDimensions: string[]

    if (selectedWidgetType?.type === 'chart' && 'metricId' in selectedWidgetType) {
      // Single metric chart
      widgetType = 'chart'
      widgetMetrics = selectedMetrics // Save ALL selected metrics (not just the clicked one)
      widgetDimensions = selectedDimensions
      widgetTableDimensions = selectedTableDimensions // Keep table dimensions for charts
      const metricLabel = getMetricById(selectedWidgetType.metricId)?.label || selectedWidgetType.metricId
      widgetTitle = `${selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')} - ${metricLabel}`
    } else if (selectedWidgetType?.type === 'multi-table') {
      // Multi-table view with all metrics and table dimensions
      widgetType = 'table'
      widgetMetrics = selectedMetrics
      widgetDimensions = selectedDimensions
      widgetTableDimensions = selectedTableDimensions
      const dimensionLabels = selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')
      const tableDimensionLabels = selectedTableDimensions.map(d => getDimensionLabel(d)).join(' × ')
      widgetTitle = `${dimensionLabels} - ${tableDimensionLabels} - ${selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')}`
    } else if (selectedWidgetType?.type === 'table') {
      // Standard pivot table with all metrics (no table dimensions)
      widgetType = 'table'
      widgetMetrics = selectedMetrics
      widgetDimensions = selectedDimensions
      widgetTableDimensions = [] // Clear table dimensions for standard pivot
      widgetTitle = `${selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')} - ${selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')}`
    } else {
      // Default (shouldn't happen, but fallback)
      widgetType = selectedTableDimensions.length > 0 ? 'chart' : 'table'
      widgetMetrics = selectedMetrics
      widgetDimensions = selectedDimensions
      widgetTableDimensions = selectedTableDimensions
      widgetTitle = `${selectedDimensions.map(d => getDimensionLabel(d)).join(' > ')} - ${selectedMetrics.map(m => getMetricById(m)?.label || m).join(', ')}`
    }

    // Determine display mode based on selection
    let displayMode: 'pivot-table' | 'multi-table' | 'single-metric-chart'
    if (selectedWidgetType?.type === 'chart') {
      displayMode = 'single-metric-chart'
    } else if (selectedWidgetType?.type === 'multi-table') {
      displayMode = 'multi-table'
    } else {
      displayMode = 'pivot-table'
    }

    // Truncate title to 100 characters (backend validation limit)
    const truncatedTitle = widgetTitle.length > 100
      ? widgetTitle.substring(0, 97) + '...'
      : widgetTitle

    const widgetConfig = {
      type: widgetType,
      display_mode: displayMode,
      table_id: selectedTable || '',
      title: truncatedTitle,
      dimensions: widgetDimensions,
      table_dimensions: widgetTableDimensions,
      metrics: widgetMetrics,
      filters: Object.fromEntries(
        Object.entries(pivotFilters.dimension_filters)
          .filter(([_, values]) => values && values.length > 0)
      ),
      start_date: config.startDate || null,
      end_date: config.endDate || null,
      chart_type: widgetType === 'chart' ? (config.chartType || 'bar') : null,
      // UI state - complete persistence
      expanded_rows: config.expandedRows || [],
      column_order: columnOrder.length > 0 ? columnOrder : undefined,
      column_sort: columnSortConfig || undefined,
      // Additional editor state for complete persistence
      date_range_type: config.dateRangeType || null,
      relative_date_preset: config.relativeDatePreset || null,
      visible_metrics: widgetType === 'chart' && selectedWidgetType?.type === 'chart' && 'metricId' in selectedWidgetType
        ? [selectedWidgetType.metricId]  // For charts, set the clicked metric as the default visible metric
        : config.selectedDisplayMetrics || null,
      merge_threshold: mergeThreshold || null,
      dimension_sort_order: dimensionSortOrder || null,
      children_sort_config: childrenSortConfig || null,
      row_sort_config: sortConfig || null,
    }

    return widgetConfig
  })()

  // Check if configuration is valid for saving
  const canSaveToDashboard = isConfigured && selectedMetrics.length > 0 && selectedTable

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
        {!widgetMode && (
          <div className="flex items-center gap-3">
            {/* Fetch BigQuery Data button */}
            <button
              onClick={triggerFetch}
              disabled={(isLoading || isLoadingColumnData) || !isConfigured}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                isConfigured
                  ? 'bg-blue-600 text-white hover:bg-blue-700 disabled:bg-blue-400'
                  : 'bg-gray-300 text-gray-500 cursor-not-allowed'
              }`}
              title={!isConfigured ? 'Configure data source, date range, and select at least one metric' : 'Fetch data from BigQuery'}
            >
              <Database className="h-4 w-4" />
              {isLoading || isLoadingColumnData ? 'Fetching...' : 'Fetch Data'}
            </button>
            {/* Stale indicator - show when config changed after last fetch */}
            {isStale && !(isLoading || isLoadingColumnData) && (
              <span className="px-3 py-1 text-xs font-medium bg-yellow-100 text-yellow-800 border border-yellow-300 rounded-full">
                Config changed - click Fetch to update
              </span>
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
            {isEditingWidget && (
              // Update Widget button (when editing an existing widget)
              <button
                onClick={handleUpdateWidgetClick}
                disabled={!canSaveToDashboard}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                  canSaveToDashboard
                    ? 'bg-purple-600 text-white hover:bg-purple-700'
                    : 'bg-gray-300 text-gray-500 cursor-not-allowed'
                }`}
                title={!canSaveToDashboard ? 'Configure data source, date range, and select at least one metric' : 'Update widget with current configuration'}
              >
                <Save className="h-4 w-4" />
                Update Widget
              </button>
            )}
            {/* Add to Dashboard button - always show (for creating new or copying) */}
            <button
              onClick={handleSaveToDashboard}
              disabled={!canSaveToDashboard}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                canSaveToDashboard
                  ? 'bg-green-600 text-white hover:bg-green-700'
                  : 'bg-gray-300 text-gray-500 cursor-not-allowed'
              }`}
              title={!canSaveToDashboard ? 'Configure data source, date range, and select at least one metric' : isEditingWidget ? 'Copy this configuration to a new widget' : 'Save this visualization to a dashboard'}
            >
              <Save className="h-4 w-4" />
              {isEditingWidget ? 'Copy to Dashboard' : 'Add to Dashboard'}
            </button>
            <button
              onClick={() => setIsExportModalOpen(true)}
              disabled={!pivotData?.rows?.length}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                pivotData?.rows?.length
                  ? 'bg-blue-600 text-white hover:bg-blue-700'
                  : 'bg-gray-300 text-gray-500 cursor-not-allowed'
              }`}
              title="Export data as CSV or HTML"
            >
              <Download className="h-4 w-4" />
              Export
            </button>
            {/* Significance Test Button - only show when using table dimensions (multi-column mode) */}
            {selectedTableDimensions.length > 0 && (
              <SignificanceButton
                onClick={runSignificanceTest}
                isLoading={isSignificanceLoading}
                hasResults={hasSignificanceResults}
                disabled={!isConfigured}
                columnCount={columnOrder.length}
                totalRows={processedRows.length}
              />
            )}
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
        )}
      </div>

      {/* Success Message */}
      {!widgetMode && successMessage && (
        <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded flex items-center justify-between">
          <span>{successMessage}</span>
          <button
            onClick={() => setSuccessMessage(null)}
            className="text-green-700 hover:text-green-900"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      )}

      {/* Combination Error Message */}
      {combinationError && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {combinationError}
        </div>
      )}

      <div className="flex gap-4">
        {!widgetMode && (
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
            dateRangeType={pivotFilters.date_range_type}
            relativeDatePreset={pivotFilters.relative_date_preset}
            onDateRangeChange={handleDateRangeChange}
          />
        )}

        <div className="flex-1 min-w-0 space-y-4">
          {/* Configuration Bar */}
          {!widgetMode && isConfigured && (
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

                {/* Metric Chips - Drag to reorder */}
                {selectedMetrics.map((metricId, index) => {
                  const metric = getMetricById(metricId)
                  return (
                    <div
                      key={metricId}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.effectAllowed = 'move'
                        e.dataTransfer.setData('metricChipIndex', String(index))
                        e.dataTransfer.setData('metricChipId', metricId)
                      }}
                      onDragOver={(e) => {
                        e.preventDefault()
                        e.dataTransfer.dropEffect = 'move'
                      }}
                      onDrop={(e) => {
                        e.preventDefault()
                        const fromIndex = parseInt(e.dataTransfer.getData('metricChipIndex'))
                        if (!isNaN(fromIndex) && fromIndex !== index) {
                          reorderMetrics(fromIndex, index)
                        }
                      }}
                      className="flex items-center gap-1 px-3 py-1 bg-orange-100 text-orange-800 rounded-full text-sm cursor-move hover:bg-orange-200 transition-colors"
                    >
                      <GripVertical className="w-3 h-3 text-orange-400" />
                      <span>{metric?.label || metricId}</span>
                      <button
                        onClick={() => removeMetric(metricId)}
                        className="ml-1 hover:bg-orange-300 rounded-full p-0.5"
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
                  if (!values || !Array.isArray(values) || values.length === 0) return null
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
                        className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider transition-all border-r border-gray-200 cursor-pointer hover:bg-gray-100 ${
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
                              : selectedDimensions.map(d => getDimensionLabel(d)).join(' - ')}
                          </span>
                          {selectedDimensions.length > 0 && (
                            <span className="text-gray-400">
                              {dimensionSortOrder === 'asc' ? '↑' : '↓'}
                            </span>
                          )}
                        </div>
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider border-r-2 border-gray-300">
                        Metric
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
                            className={`px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-move transition-all duration-200 ${
                              orderIndex === 0 ? 'border-r-2 border-gray-300' : 'border-r border-gray-200'
                            } ${draggedColumnIndex === orderIndex ? 'bg-blue-300 opacity-50 scale-95' : 'hover:bg-gray-200'} ${orderIndex === 0 ? 'bg-green-50' : ''}`}
                            title="Drag to reorder columns"
                          >
                            <div className="flex items-center justify-center gap-2">
                              <GripVertical className="h-4 w-4 text-gray-400" />
                              {orderIndex === 0 && <span className="text-green-600 text-sm">★</span>}
                              <div>{headerLabel}</div>
                            </div>
                          </th>
                        )
                      })}
                    </tr>
                    {/* Second header row - metric sub-columns */}
                    <tr className="bg-gray-100">
                      {/* Row dimension column - Sort columns by metric */}
                      <th className="px-6 py-2 text-left text-xs font-medium text-gray-600 uppercase tracking-wider border-r border-gray-200">
                        <SortDropdown
                          label="Sort cols"
                          metrics={selectedMetrics}
                          getMetricById={getMetricById}
                          activeMetric={columnSortConfig?.metric}
                          activeDirection={columnSortConfig?.direction}
                          onSort={handleColumnSort}
                          align="left"
                          color="purple"
                          showFullLabel={true}
                        />
                      </th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-600 uppercase tracking-wider border-r-2 border-gray-300">
                      </th>
                      {columnOrder.map((originalColIndex, orderIndex) => {
                        if (orderIndex === 0) {
                          // First column - show sort dropdown
                          const activeMetricForCol = sortConfig?.column === originalColIndex && sortConfig?.subColumn === 'value' ? sortConfig.metric : null
                          return (
                            <th
                              key={`metric-${originalColIndex}`}
                              className="px-6 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-r-2 border-gray-300"
                            >
                              <SortDropdown
                                label="Sort"
                                metrics={selectedMetrics}
                                getMetricById={getMetricById}
                                activeMetric={activeMetricForCol}
                                activeDirection={sortConfig?.direction}
                                onSort={(metricId) => handleSort(originalColIndex, 'value', metricId)}
                                align="right"
                                color="blue"
                                showFullLabel={true}
                              />
                            </th>
                          )
                        } else {
                          // Columns 2+ - Value, Diff, % Diff with sort dropdowns
                          const activeValueMetric = sortConfig?.column === originalColIndex && sortConfig?.subColumn === 'value' ? sortConfig.metric : null
                          const activeDiffMetric = sortConfig?.column === originalColIndex && sortConfig?.subColumn === 'diff' ? sortConfig.metric : null
                          const activePctDiffMetric = sortConfig?.column === originalColIndex && sortConfig?.subColumn === 'pctDiff' ? sortConfig.metric : null
                          return (
                            <React.Fragment key={`metrics-${originalColIndex}`}>
                              {/* Value column */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-l border-gray-200">
                                <SortDropdown
                                  label="Value"
                                  metrics={selectedMetrics}
                                  getMetricById={getMetricById}
                                  activeMetric={activeValueMetric}
                                  activeDirection={sortConfig?.direction}
                                  onSort={(metricId) => handleSort(originalColIndex, 'value', metricId)}
                                  align="right"
                                  color="blue"
                                />
                              </th>
                              {/* Diff column */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider">
                                <SortDropdown
                                  label="Diff"
                                  metrics={selectedMetrics}
                                  getMetricById={getMetricById}
                                  activeMetric={activeDiffMetric}
                                  activeDirection={sortConfig?.direction}
                                  onSort={(metricId) => handleSort(originalColIndex, 'diff', metricId)}
                                  align="right"
                                  color="blue"
                                />
                              </th>
                              {/* % Diff column */}
                              <th className="px-4 py-2 text-right text-xs font-medium text-gray-600 uppercase tracking-wider border-r border-gray-200">
                                <SortDropdown
                                  label="% Diff"
                                  metrics={selectedMetrics}
                                  getMetricById={getMetricById}
                                  activeMetric={activePctDiffMetric}
                                  activeDirection={sortConfig?.direction}
                                  onSort={(metricId) => handleSort(originalColIndex, 'pctDiff', metricId)}
                                  align="right"
                                  color="blue"
                                />
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
                        {/* No dimensions - show Total with sub-rows per metric */}
                        {(() => {
                          const firstColIndex = columnOrder[0]
                          const firstColData = allColumnData?.[firstColIndex]

                          return (
                            <>
                              {selectedMetrics.map((metricId, metricIndex) => {
                                const metric = getMetricById(metricId)
                                const isFirstMetric = metricIndex === 0
                                const isLastMetric = metricIndex === selectedMetrics.length - 1
                                const firstValue = firstColData?.total?.metrics?.[metricId] ?? null

                                return (
                                  <tr key={`total-${metricId}`} className="hover:bg-gray-50">
                                    {/* Total label - only render on first metric row with rowSpan */}
                                    {isFirstMetric && (
                                      <td
                                        rowSpan={selectedMetrics.length}
                                        className="px-6 py-2 whitespace-nowrap text-sm font-medium text-gray-900 border-r border-gray-200 align-middle"
                                      >
                                        Total
                                      </td>
                                    )}
                                    {/* Metric name */}
                                    <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-xs text-gray-500 border-r-2 border-gray-300`}>
                                      {metric?.label}
                                    </td>
                                    {/* Data columns */}
                                    {columnOrder.map((originalColIndex, orderIndex) => {
                                      const columnData = allColumnData?.[originalColIndex]
                                      const value = columnData?.total?.metrics?.[metricId] ?? null

                                      if (orderIndex === 0) {
                                        return (
                                          <td
                                            key={`col-${originalColIndex}`}
                                            className={`px-6 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm text-gray-900 text-right border-r-2 border-gray-300`}
                                          >
                                            <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                          </td>
                                        )
                                      } else {
                                        const diff = (value ?? 0) - (firstValue ?? 0)
                                        const pctDiff = (firstValue ?? 0) !== 0
                                          ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                          : null

                                        // Get significance result for indicator (but don't color diff values)
                                        const sigResult = getSignificanceForCell(originalColIndex, metricId)
                                        // Don't color diff/pctDiff - only the significance indicator shows color
                                        const diffColorClass = 'text-gray-700'

                                        return (
                                          <React.Fragment key={`col-${originalColIndex}`}>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm text-gray-900 text-right border-l border-gray-200`}>
                                              <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                            </td>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm ${diffColorClass} text-right`}>
                                              {diff != null ? formatMetricValue(diff, metricId) : '-'}
                                            </td>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm ${diffColorClass} text-right border-r border-gray-200`}>
                                              <div className="flex items-center justify-end gap-1">
                                                <span>{pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}</span>
                                                <span className="inline-block min-w-[60px] text-right">
                                                  <SignificanceIndicator
                                                    result={sigResult}
                                                    compact={true}
                                                    formatType={getFormatTypeForMetric(metricId)}
                                                  />
                                                </span>
                                              </div>
                                            </td>
                                          </React.Fragment>
                                        )
                                      }
                                    })}
                                  </tr>
                                )
                              })}
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

                          // Get data for all columns for this row
                          const firstColIndex = columnOrder[0]
                          const firstColData = allColumnData?.[firstColIndex]
                          const firstRowData = firstColData?.rows.find((r: any) => r.dimension_value === row.dimension_value)

                          return (
                            <React.Fragment key={row.dimension_value}>
                              {/* Render one sub-row per metric */}
                              {selectedMetrics.map((metricId, metricIndex) => {
                                const metric = getMetricById(metricId)
                                const isFirstMetric = metricIndex === 0
                                const isLastMetric = metricIndex === selectedMetrics.length - 1

                                return (
                                  <tr
                                    key={`${row.dimension_value}-${metricId}`}
                                    className={`hover:bg-gray-50 ${isLastMetric ? 'border-b-2 border-gray-300' : ''}`}
                                  >
                                    {/* Dimension value cell - only render on first metric row with rowSpan */}
                                    {isFirstMetric && (
                                      <td
                                        rowSpan={selectedMetrics.length}
                                        className="px-6 py-2 whitespace-nowrap text-sm font-medium text-gray-900 border-r border-gray-200 align-middle"
                                      >
                                        {row.dimension_value}
                                      </td>
                                    )}
                                    {/* Metric name column */}
                                    <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-xs text-gray-500 border-r-2 border-gray-300`}>
                                      {metric?.label}
                                    </td>

                                    {/* Data columns */}
                                    {columnOrder.map((originalColIndex, orderIndex) => {
                                      const columnData = allColumnData?.[originalColIndex]
                                      const rowData = columnData?.rows.find((r: any) => r.dimension_value === row.dimension_value)
                                      const value = rowData?.metrics?.[metricId] ?? null

                                      if (orderIndex === 0) {
                                        // First/control column - just show value
                                        return (
                                          <td
                                            key={`col-${originalColIndex}`}
                                            className={`px-6 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm text-gray-900 text-right border-r-2 border-gray-300`}
                                          >
                                            <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                          </td>
                                        )
                                      } else {
                                        // Comparison columns - show value, diff, % diff
                                        const firstValue = firstRowData?.metrics?.[metricId] ?? null
                                        const diff = (value ?? 0) - (firstValue ?? 0)
                                        const pctDiff = (firstValue ?? 0) !== 0
                                          ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                          : null

                                        // Get significance result for indicator (but don't color diff values)
                                        const sigResult = getSignificanceForCell(originalColIndex, metricId, row.dimension_value)
                                        // Don't color diff/pctDiff - only the significance indicator shows color
                                        const diffColorClass = 'text-gray-700'

                                        return (
                                          <React.Fragment key={`col-${originalColIndex}`}>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm text-gray-900 text-right border-l border-gray-200`}>
                                              <span className="font-medium">{value != null ? formatMetricValue(value, metricId) : '-'}</span>
                                            </td>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm ${diffColorClass} text-right`}>
                                              {diff != null ? formatMetricValue(diff, metricId) : '-'}
                                            </td>
                                            <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} text-sm ${diffColorClass} text-right border-r border-gray-200`}>
                                              <div className="flex items-center justify-end gap-1">
                                                <span>{pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}</span>
                                                <span className="inline-block min-w-[60px] text-right">
                                                  <SignificanceIndicator
                                                    result={sigResult}
                                                    compact={true}
                                                    formatType={getFormatTypeForMetric(metricId)}
                                                  />
                                                </span>
                                              </div>
                                            </td>
                                          </React.Fragment>
                                        )
                                      }
                                    })}
                                  </tr>
                                )
                              })}
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
                                              const metric = getMetricById(selectedMetrics[0] || primarySortMetric)
                                              if (orderIndex === 0) {
                                                const isSorted = childrenSortConfig?.column === (selectedMetrics[0] || primarySortMetric)
                                                return (
                                                  <th
                                                    key={`child-header-${originalColIndex}`}
                                                    className="px-6 py-2 text-right text-xs font-medium text-gray-700 uppercase tracking-wider border-r-2 border-blue-300 cursor-pointer hover:bg-blue-200"
                                                    onClick={() => handleChildrenSort(selectedMetrics[0] || primarySortMetric)}
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
                                                const value = childData ? (childData as any)[selectedMetrics[0] || primarySortMetric] : null

                                                if (orderIndex === 0) {
                                                  // First column - just show the value
                                                  return (
                                                    <td
                                                      key={`col-${originalColIndex}`}
                                                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-r-2 border-gray-300"
                                                    >
                                                      {value != null ? formatMetricValue(value, selectedMetrics[0] || primarySortMetric) : '-'}
                                                    </td>
                                                  )
                                                } else {
                                                  // Columns 2+ - show value, diff, % diff
                                                  const firstColIndex = columnOrder[0]
                                                  const firstChildData = childrenByColumn[firstColIndex][child.search_term]
                                                  const firstValue = firstChildData ? (firstChildData as any)[selectedMetrics[0] || primarySortMetric] : null

                                                  const diff = (value ?? 0) - (firstValue ?? 0)
                                                  const pctDiff = (firstValue ?? 0) !== 0
                                                    ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                                    : null

                                                  return (
                                                    <React.Fragment key={`col-${originalColIndex}`}>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-700 text-right border-l border-gray-200">
                                                        {value != null ? formatMetricValue(value, selectedMetrics[0] || primarySortMetric) : '-'}
                                                      </td>
                                                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-600 text-right">
                                                        {diff != null ? formatMetricValue(diff, selectedMetrics[0] || primarySortMetric) : '-'}
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
                      <>
                        {selectedMetrics.map((metricId, metricIndex) => {
                          const metric = getMetricById(metricId)
                          const isFirstMetric = metricIndex === 0
                          const isLastMetric = metricIndex === selectedMetrics.length - 1
                          const firstColIndex = columnOrder[0]
                          const firstColData = allColumnData?.[firstColIndex]
                          const firstValue = firstColData?.total?.metrics?.[metricId] ?? null

                          return (
                            <tr key={`total-${metricId}`} className={isFirstMetric ? 'border-t-2 border-gray-500' : ''}>
                              {/* Total label - only render on first metric row with rowSpan */}
                              {isFirstMetric && (
                                <td
                                  rowSpan={selectedMetrics.length}
                                  className="px-6 py-2 whitespace-nowrap text-sm font-semibold text-gray-900 border-r border-gray-200 align-middle"
                                >
                                  Total
                                </td>
                              )}
                              {/* Metric name */}
                              <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-xs text-gray-500 font-normal border-r-2 border-gray-300`}>
                                {metric?.label}
                              </td>
                              {columnOrder.map((originalColIndex, orderIndex) => {
                                const columnData = allColumnData?.[originalColIndex]
                                const value = columnData?.total?.metrics?.[metricId] ?? null

                                if (orderIndex === 0) {
                                  return (
                                    <td
                                      key={`col-${originalColIndex}`}
                                      className={`px-6 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-sm text-gray-900 text-right border-r-2 border-gray-300`}
                                    >
                                      {value != null ? formatMetricValue(value, metricId) : '-'}
                                    </td>
                                  )
                                } else {
                                  const diff = (value ?? 0) - (firstValue ?? 0)
                                  const pctDiff = (firstValue ?? 0) !== 0
                                    ? (((value ?? 0) / (firstValue ?? 0)) - 1) * 100
                                    : null

                                  // Get significance result for indicator (but don't color diff values)
                                  const sigResult = getSignificanceForCell(originalColIndex, metricId)
                                  // Don't color diff/pctDiff - only the significance indicator shows color
                                  const diffColorClass = 'text-gray-700'

                                  return (
                                    <React.Fragment key={`col-${originalColIndex}`}>
                                      <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-sm text-gray-900 text-right border-l border-gray-200`}>
                                        {value != null ? formatMetricValue(value, metricId) : '-'}
                                      </td>
                                      <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-sm ${diffColorClass} text-right`}>
                                        {diff != null ? formatMetricValue(diff, metricId) : '-'}
                                      </td>
                                      <td className={`px-4 ${isFirstMetric ? 'pt-3' : 'pt-1'} ${isLastMetric ? 'pb-3' : 'pb-1'} whitespace-nowrap text-sm ${diffColorClass} text-right border-r border-gray-200`}>
                                        <div className="flex items-center justify-end gap-1">
                                          <span>{pctDiff != null ? `${pctDiff.toFixed(2)}%` : '-'}</span>
                                          <SignificanceIndicator
                                            result={sigResult}
                                            compact={true}
                                            formatType={getFormatTypeForMetric(metricId)}
                                          />
                                        </div>
                                      </td>
                                    </React.Fragment>
                                  )
                                }
                              })}
                            </tr>
                          )
                        })}
                      </>
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
                        : selectedDimensions.map(d => getDimensionLabel(d)).join(' - ')}
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

          {/* Chart Visualization (only in multi-table mode AND not in widget mode) */}
          {!widgetMode && selectedTableDimensions.length > 0 && tableCombinations.length > 0 && allColumnData && (
            <PivotChartVisualization
              allColumnData={allColumnData}
              selectedDimensions={selectedDimensions}
              selectedMetrics={selectedMetrics}
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
              chartType={config.chartType || 'bar'}
              setChartType={setChartType}
            />
          )}
        </div>
      </div>

      {/* Widget Selector Modal */}
      {!widgetMode && isWidgetSelectorOpen && (
        <WidgetSelectorModal
          metrics={selectedMetrics.map(id => getMetricById(id)!).filter(Boolean)}
          hasTableDimensions={selectedTableDimensions.length > 0}
          onSelect={handleWidgetSelection}
          onCancel={() => setIsWidgetSelectorOpen(false)}
        />
      )}

      {/* Dashboard Selector Modal */}
      {!widgetMode && (
        <DashboardSelectorModal
          isOpen={isDashboardModalOpen}
          onClose={handleDashboardModalClose}
          widgetConfig={currentWidgetConfig}
          onSuccess={handleDashboardSuccess}
        />
      )}

      {/* Export Modal */}
      {!widgetMode && (
        <ExportModal
          isOpen={isExportModalOpen}
          onClose={() => setIsExportModalOpen(false)}
          onExport={handleExport}
          data={getExportData()}
        />
      )}
    </div>
  )
}
