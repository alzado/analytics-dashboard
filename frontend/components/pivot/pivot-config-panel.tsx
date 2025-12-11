'use client'

import { useState, useEffect, useMemo, useRef } from 'react'
import { X, GripVertical, Settings, Database, Calendar, ChevronDown, ChevronRight, Plus, Edit, Copy, Trash2 } from 'lucide-react'
import { SearchInput } from '@/components/ui/search-input'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchBigQueryInfo,
  fetchTableDateRange,
  fetchCustomDimensions,
  createCustomDimension,
  updateCustomDimension,
  deleteCustomDimension,
  duplicateCustomDimension,
  fetchDimensionValues,
  fetchTables
} from '@/lib/api'
import type { PivotConfig } from '@/hooks/use-pivot-config'
import type { CustomDimension, CustomDimensionCreate, CustomDimensionUpdate, DateRangeType, RelativeDatePreset } from '@/lib/types'
import { usePivotMetrics, type MetricDefinition } from '@/hooks/use-pivot-metrics'
import { useSchema } from '@/hooks/use-schema'
import CustomDimensionModal from '@/components/modals/custom-dimension-modal'
import { DateRangeSelector } from '@/components/ui/date-range-selector'

interface PivotConfigPanelProps {
  isOpen: boolean
  onClose: () => void
  config: PivotConfig
  updateTable: (tableName: string | null) => void
  updateDateRange: (dateRange: any) => void
  updateStartDate: (date: string | null) => void
  updateEndDate: (date: string | null) => void
  setDataSourceDropped: (dropped: boolean) => void
  setDateRangeDropped: (dropped: boolean) => void
  addDimension: (dimension: string) => void
  removeDimension: (dimension: string) => void
  addMetric: (metricId: string) => void
  removeMetric: (metricId: string) => void
  addFilter: (filter: any) => void
  removeFilter: (index: number) => void
  resetToDefaults: () => void
  // New props for dimension filters
  dimensionFilters?: Record<string, string[]>
  onDimensionFilterChange?: (dimensionId: string, values: string[]) => void
  onClearDimensionFilters?: () => void
  currentFilters?: any
  // Date range props
  dateRangeType?: DateRangeType
  relativeDatePreset?: RelativeDatePreset | null
  onDateRangeChange?: (
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => void
}

interface AvailableMetricProps {
  metric: MetricDefinition
  onAdd: (id: string) => void
  isSelected: boolean
}

function AvailableMetric({ metric, onAdd, isSelected }: AvailableMetricProps) {
  return (
    <div
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData('type', 'metric')
        e.dataTransfer.setData('id', metric.id)
        e.dataTransfer.setData('label', metric.label)
      }}
      className="p-2 bg-orange-50 border-2 border-orange-300 rounded cursor-move hover:bg-orange-100 transition-colors"
    >
      <div className="flex items-center gap-2">
        <GripVertical className="h-3 w-3 text-orange-600" />
        <div className="flex-1">
          <div className="text-xs font-medium text-gray-900">
            {metric.label}
          </div>
          <div className="text-xs text-orange-600">Drag to table</div>
        </div>
      </div>
    </div>
  )
}

interface DimensionFilterInlineProps {
  dimension: any
  selectedValues: string[]
  onFilterChange: (values: string[]) => void
  currentFilters?: any
  tableId?: string
}

function DimensionFilterInline({
  dimension,
  selectedValues,
  onFilterChange,
  currentFilters,
  tableId,
}: DimensionFilterInlineProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [availableValues, setAvailableValues] = useState<string[]>([])
  const [isLoadingValues, setIsLoadingValues] = useState(false)

  // Load available values when dropdown opens
  useEffect(() => {
    if (isOpen && availableValues.length === 0) {
      setIsLoadingValues(true)
      fetchDimensionValues(dimension.id, currentFilters || {}, tableId)
        .then(values => {
          setAvailableValues(values)
        })
        .catch(err => {
          console.error(`Failed to load values for ${dimension.id}:`, err)
        })
        .finally(() => {
          setIsLoadingValues(false)
        })
    }
  }, [isOpen, dimension.id, availableValues.length, currentFilters])

  const toggleValue = (value: string) => {
    if (selectedValues.includes(value)) {
      onFilterChange(selectedValues.filter(v => v !== value))
    } else {
      onFilterChange([...selectedValues, value])
    }
  }

  const selectAll = () => {
    onFilterChange(availableValues)
  }

  const clearSelection = () => {
    onFilterChange([])
  }

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={`w-full flex items-center justify-between px-2 py-1.5 text-xs border rounded transition-colors ${
          selectedValues.length > 0
            ? 'border-pink-400 bg-pink-50 text-pink-900'
            : 'border-gray-300 bg-white text-gray-700 hover:border-gray-400'
        }`}
      >
        <span className="truncate">
          {dimension.display_name}
          {selectedValues.length > 0 && (
            <span className="ml-1 text-xs font-medium">
              ({selectedValues.length})
            </span>
          )}
        </span>
        <ChevronDown
          className={`h-3 w-3 transition-transform ${isOpen ? 'transform rotate-180' : ''}`}
        />
      </button>

      {isOpen && (
        <>
          {/* Overlay to close dropdown when clicking outside */}
          <div
            className="fixed inset-0 z-10"
            onClick={() => setIsOpen(false)}
          />

          {/* Dropdown content */}
          <div className="absolute z-20 mt-1 w-full bg-white border border-gray-300 rounded shadow-lg max-h-48 overflow-auto">
            {isLoadingValues ? (
              <div className="p-3 text-center text-xs text-gray-500">
                Loading...
              </div>
            ) : (
              <>
                {/* Select all / Clear all buttons */}
                <div className="sticky top-0 bg-gray-50 border-b border-gray-200 px-2 py-1 flex gap-2">
                  <button
                    onClick={selectAll}
                    className="text-xs text-blue-600 hover:text-blue-800"
                  >
                    Select all
                  </button>
                  <span className="text-gray-300">|</span>
                  <button
                    onClick={clearSelection}
                    className="text-xs text-gray-600 hover:text-gray-800"
                  >
                    Clear
                  </button>
                </div>

                {/* Value checkboxes */}
                <div className="py-1">
                  {availableValues.map(value => (
                    <label
                      key={value}
                      className="flex items-center gap-2 px-2 py-1 hover:bg-gray-50 cursor-pointer"
                    >
                      <input
                        type="checkbox"
                        checked={selectedValues.includes(value)}
                        onChange={() => toggleValue(value)}
                        className="h-3 w-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                      />
                      <span className="text-xs text-gray-700 truncate flex-1">
                        {value}
                      </span>
                    </label>
                  ))}

                  {availableValues.length === 0 && (
                    <div className="p-3 text-center text-xs text-gray-500">
                      No values available
                    </div>
                  )}
                </div>
              </>
            )}
          </div>
        </>
      )}
    </div>
  )
}

export function PivotConfigPanel({
  isOpen,
  onClose,
  config,
  updateTable,
  updateDateRange,
  updateStartDate,
  updateEndDate,
  setDataSourceDropped,
  setDateRangeDropped,
  addDimension,
  removeDimension,
  addMetric,
  removeMetric,
  addFilter,
  removeFilter,
  resetToDefaults,
  dimensionFilters = {},
  onDimensionFilterChange,
  onClearDimensionFilters,
  currentFilters,
  dateRangeType = 'absolute',
  relativeDatePreset = null,
  onDateRangeChange,
}: PivotConfigPanelProps) {
  // Load dynamic metrics and dimensions from schema (pass selectedTable to load table-specific schema)
  const { schema } = useSchema(config.selectedTable || undefined)
  const { metrics: AVAILABLE_METRICS, dimensions: AVAILABLE_DIMENSIONS } = usePivotMetrics(config.selectedTable || undefined)

  // Get filterable dimensions
  const filterableDimensions = schema?.dimensions?.filter(d => d.is_filterable) || []

  const [isDataSourceExpanded, setIsDataSourceExpanded] = useState(false)
  const [isDateRangeExpanded, setIsDateRangeExpanded] = useState(false)
  const [isDimensionsExpanded, setIsDimensionsExpanded] = useState(false)
  const [isMetricsExpanded, setIsMetricsExpanded] = useState(false)
  const [isFiltersExpanded, setIsFiltersExpanded] = useState(false)

  // Search state
  const [dimensionSearch, setDimensionSearch] = useState('')
  const [metricSearch, setMetricSearch] = useState('')
  const [filterSearch, setFilterSearch] = useState('')

  // Custom Dimension Modal State
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [modalMode, setModalMode] = useState<'create' | 'edit'>('create')
  const [editingDimension, setEditingDimension] = useState<CustomDimension | null>(null)

  // Pending filter changes (not yet applied)
  const [pendingFilters, setPendingFilters] = useState<Record<string, string[]>>(dimensionFilters || {})

  // Local state for current date range (to avoid stale data during drag)
  const [currentStartDate, setCurrentStartDate] = useState<string | null>(config.startDate || null)
  const [currentEndDate, setCurrentEndDate] = useState<string | null>(config.endDate || null)

  // Sync pending filters when actual filters change externally
  useEffect(() => {
    setPendingFilters(dimensionFilters || {})
  }, [dimensionFilters])

  // Sync local date state when config changes
  useEffect(() => {
    setCurrentStartDate(config.startDate || null)
    setCurrentEndDate(config.endDate || null)
  }, [config.startDate, config.endDate])

  const queryClient = useQueryClient()

  // Wrapper for onDateRangeChange that updates local state immediately
  const handleDateRangeChange = (
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => {
    // Update local state immediately
    setCurrentStartDate(startDate)
    setCurrentEndDate(endDate)
    // Call parent callback
    if (onDateRangeChange) {
      onDateRangeChange(type, preset, startDate, endDate)
    }
  }

  // Fetch custom dimensions
  const { data: customDimensions = [], isLoading: customDimensionsLoading } = useQuery({
    queryKey: ['custom-dimensions'],
    queryFn: fetchCustomDimensions,
  })

  // Create custom dimension mutation
  const createMutation = useMutation({
    mutationFn: (data: CustomDimensionCreate) => createCustomDimension(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['custom-dimensions'] })
      setIsModalOpen(false)
    },
  })

  // Update custom dimension mutation
  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: CustomDimensionUpdate }) =>
      updateCustomDimension(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['custom-dimensions'] })
      setIsModalOpen(false)
      setEditingDimension(null)
    },
  })

  // Delete custom dimension mutation
  const deleteMutation = useMutation({
    mutationFn: (id: string) => deleteCustomDimension(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['custom-dimensions'] })
    },
  })

  // Duplicate custom dimension mutation
  const duplicateMutation = useMutation({
    mutationFn: (id: string) => duplicateCustomDimension(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['custom-dimensions'] })
    },
  })

  // Modal handlers
  const handleCreateNewDimension = () => {
    setModalMode('create')
    setEditingDimension(null)
    setIsModalOpen(true)
  }

  const handleEditDimension = (dimension: CustomDimension) => {
    setModalMode('edit')
    setEditingDimension(dimension)
    setIsModalOpen(true)
  }

  const handleModalSave = (data: CustomDimensionCreate | { id: string; data: CustomDimensionUpdate }) => {
    if (modalMode === 'create') {
      createMutation.mutate(data as CustomDimensionCreate)
    } else {
      updateMutation.mutate(data as { id: string; data: CustomDimensionUpdate })
    }
  }

  const handleDeleteDimension = (id: string) => {
    if (confirm('Are you sure you want to delete this custom dimension?')) {
      deleteMutation.mutate(id)
    }
  }

  const handleDuplicateDimension = (id: string) => {
    duplicateMutation.mutate(id)
  }

  // Filter handlers
  const handlePendingFilterChange = (dimensionId: string, values: string[]) => {
    setPendingFilters(prev => {
      const updated = { ...prev }
      if (values.length === 0) {
        delete updated[dimensionId]
      } else {
        updated[dimensionId] = values
      }
      return updated
    })
  }

  const applyPendingFilters = () => {
    if (onDimensionFilterChange) {
      // Apply all pending filter changes
      Object.entries(pendingFilters).forEach(([dimensionId, values]) => {
        onDimensionFilterChange(dimensionId, values)
      })
      // Clear any filters that were removed
      if (dimensionFilters) {
        Object.keys(dimensionFilters).forEach(dimensionId => {
          if (!pendingFilters[dimensionId]) {
            onDimensionFilterChange(dimensionId, [])
          }
        })
      }
    }
  }

  const clearPendingFilters = () => {
    setPendingFilters({})
  }

  const hasPendingChanges = JSON.stringify(pendingFilters) !== JSON.stringify(dimensionFilters || {})

  // Fetch available tables
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: () => fetchTables(),
  })

  const tables = tablesData?.tables || []

  // Fetch BigQuery info for the dropped table
  const { data: bqInfo, isLoading: bqInfoLoading } = useQuery({
    queryKey: ['bigquery-info', config.selectedTable],
    queryFn: () => fetchBigQueryInfo(config.selectedTable || undefined),
    enabled: !!config.selectedTable || tables.length === 0,
    retry: false,
  })

  // Fetch date range for selected table
  const { data: dateRangeInfo, isLoading: dateRangeLoading } = useQuery({
    queryKey: ['table-dates', config.selectedTable],
    queryFn: () => fetchTableDateRange(config.selectedTable!),
    enabled: !!config.selectedTable,
    retry: false,
  })

  // Track the previous table to detect actual table changes (not just mount)
  const prevTableRef = useRef<string | null | undefined>(undefined) // undefined = not yet initialized

  // Auto-set start and end dates only when table actually changes (user selects different table)
  useEffect(() => {
    if (dateRangeInfo?.has_date_column && dateRangeInfo.min_date && dateRangeInfo.max_date) {
      const isFirstMount = prevTableRef.current === undefined
      const tableActuallyChanged = !isFirstMount && prevTableRef.current !== config.selectedTable
      const noDatesSet = !config.startDate && !config.endDate

      // Only auto-set dates if:
      // 1. The table actually changed (user selected a different table), OR
      // 2. No dates are currently set at all
      // Do NOT auto-set on first mount if dates already exist
      if (tableActuallyChanged || noDatesSet) {
        updateStartDate(dateRangeInfo.min_date)
        updateEndDate(dateRangeInfo.max_date)
      }

      // Always update the ref to track current table
      prevTableRef.current = config.selectedTable
    }
  }, [dateRangeInfo?.min_date, dateRangeInfo?.max_date, config.selectedTable, config.startDate, config.endDate])

  // Filter out metrics that are already selected
  const selectedMetrics = config.selectedMetrics || []
  const availableMetricsToAdd = AVAILABLE_METRICS.filter(
    (m) => !selectedMetrics.includes(m.id)
  )

  // Filter metrics by search term and sort alphabetically
  const filteredMetrics = availableMetricsToAdd
    .filter((m) => {
      if (!metricSearch) return true
      const term = metricSearch.toLowerCase()
      return (
        m.label.toLowerCase().includes(term) ||
        m.id.toLowerCase().includes(term) ||
        m.category.toLowerCase().includes(term)
      )
    })
    .sort((a, b) => a.label.localeCompare(b.label))

  // Filter out dimensions that are already selected (either as row or table dimensions)
  const selectedDimensions = config.selectedDimensions || []
  const selectedTableDimensions = config.selectedTableDimensions || []
  const availableDimensionsToAdd = AVAILABLE_DIMENSIONS.filter(
    (d) => !selectedDimensions.includes(d.value) && !selectedTableDimensions.includes(d.value)
  )

  // Filter dimensions by search term and sort alphabetically
  const filteredDimensions = availableDimensionsToAdd
    .filter((d) => {
      if (!dimensionSearch) return true
      const term = dimensionSearch.toLowerCase()
      return (
        d.label.toLowerCase().includes(term) ||
        d.value.toLowerCase().includes(term)
      )
    })
    .sort((a, b) => a.label.localeCompare(b.label))

  // Filter custom dimensions by search term and sort alphabetically
  const filteredCustomDimensions = customDimensions
    .filter((d) => {
      if (!dimensionSearch) return true
      const term = dimensionSearch.toLowerCase()
      return d.name.toLowerCase().includes(term)
    })
    .sort((a, b) => a.name.localeCompare(b.name))

  // Filter filterable dimensions by search term and sort alphabetically
  const filteredFilterableDimensions = filterableDimensions
    .filter((d) => {
      if (!filterSearch) return true
      const term = filterSearch.toLowerCase()
      return (
        d.display_name.toLowerCase().includes(term) ||
        d.id.toLowerCase().includes(term)
      )
    })
    .sort((a, b) => a.display_name.localeCompare(b.display_name))

  // Group available metrics by category
  const metricsByCategory = filteredMetrics.reduce((acc, metric) => {
    if (!acc[metric.category]) {
      acc[metric.category] = []
    }
    acc[metric.category].push(metric)
    return acc
  }, {} as Record<string, MetricDefinition[]>)

  if (!isOpen) return null

  return (
    <div className="w-80 flex-shrink-0 bg-gray-50 border-r border-gray-200 flex flex-col h-[calc(100vh-4rem)] overflow-hidden sticky top-16">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-4 flex-shrink-0">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <Settings className="h-4 w-4 text-blue-600" />
            <h3 className="text-sm font-bold text-gray-900">Configuration</h3>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <p className="text-xs text-gray-600">
          Drag metrics to reorder
        </p>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Data Source Info */}
        <div>
          <button
            onClick={() => setIsDataSourceExpanded(!isDataSourceExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase flex items-center gap-1">
              <Database className="h-3 w-3" />
              Data Source
            </h4>
            {isDataSourceExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isDataSourceExpanded && (
            <>
              {tables.length === 0 ? (
                <div className="p-3 text-center bg-yellow-50 border border-yellow-200 rounded">
                  <p className="text-xs text-yellow-700">
                    No tables configured
                  </p>
                  <p className="text-xs text-yellow-600 mt-1">
                    Create a table in the Tables tab first
                  </p>
                </div>
              ) : (
                <div className="space-y-2">
                  {/* List of all tables as draggable cards */}
                  {tables.map((table) => {
                    const isSelected = config.selectedTable === table.table_id
                    return (
                      <div
                        key={table.table_id}
                        draggable
                        onDragStart={(e) => {
                          e.dataTransfer.setData('type', 'datasource')
                          e.dataTransfer.setData('table', table.table)
                          e.dataTransfer.setData('tableId', table.table_id)
                          e.dataTransfer.setData('tablePath', `${table.project_id}.${table.dataset}.${table.table}`)
                        }}
                        className={`p-3 border-2 rounded cursor-move hover:bg-blue-50 transition-colors ${
                          isSelected
                            ? 'bg-blue-50 border-blue-400'
                            : 'bg-white border-gray-200 hover:border-blue-300'
                        }`}
                      >
                        <div className="flex items-center gap-2 mb-1">
                          <GripVertical className="h-3 w-3 text-gray-400" />
                          {isSelected && <span className="text-green-600">✓</span>}
                          <div className="text-xs font-medium text-gray-900">{table.name}</div>
                        </div>
                        <div className="text-xs text-gray-500 ml-5">
                          {table.project_id}.{table.dataset}.{table.table}
                        </div>
                        <div className="text-xs text-blue-600 mt-1 ml-5">
                          {isSelected ? 'Selected • Drag to table' : 'Drag to table'}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </>
          )}
        </div>

        {/* Date Range Selection - Only show when data source is dropped */}
        {config.selectedTable && (
          <div>
            <button
              onClick={() => setIsDateRangeExpanded(!isDateRangeExpanded)}
              className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
            >
              <h4 className="text-xs font-semibold text-gray-700 uppercase flex items-center gap-1">
                <Calendar className="h-3 w-3" />
                Date Range
              </h4>
              {isDateRangeExpanded ? (
                <ChevronDown className="h-3 w-3 text-gray-500" />
              ) : (
                <ChevronRight className="h-3 w-3 text-gray-500" />
              )}
            </button>
            {isDateRangeExpanded && (
              <>
                {dateRangeLoading ? (
                  <div className="p-3 text-center bg-white border border-gray-200 rounded">
                    <p className="text-xs text-gray-500">Loading dates...</p>
                  </div>
                ) : dateRangeInfo?.has_date_column ? (
                  <div className="space-y-2">
                    <div className="p-3 bg-white border border-gray-200 rounded">
                      <div className="text-xs text-gray-600 mb-2">Available Range:</div>
                      <div className="text-xs text-gray-700">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-gray-500">From:</span>
                          <span className="font-medium">{dateRangeInfo.min_date}</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-gray-500">To:</span>
                          <span className="font-medium">{dateRangeInfo.max_date}</span>
                        </div>
                      </div>
                    </div>

                    {/* Date Range Selector */}
                    <div className="p-3 bg-white border border-gray-200 rounded">
                      {onDateRangeChange ? (
                        <DateRangeSelector
                          dateRangeType={dateRangeType || 'absolute'}
                          relativeDatePreset={relativeDatePreset || null}
                          startDate={currentStartDate}
                          endDate={currentEndDate}
                          onDateRangeChange={handleDateRangeChange}
                        />
                      ) : (
                        <div className="space-y-2">
                          <div>
                            <label className="text-xs text-gray-600 block mb-1">Start Date</label>
                            <input
                              type="date"
                              value={config.startDate || ''}
                              onChange={(e) => updateStartDate(e.target.value)}
                              min={dateRangeInfo.min_date || undefined}
                              max={config.endDate || dateRangeInfo.max_date || undefined}
                              className="w-full text-xs border border-gray-300 rounded px-2 py-1"
                            />
                          </div>
                          <div>
                            <label className="text-xs text-gray-600 block mb-1">End Date</label>
                            <input
                              type="date"
                              value={config.endDate || ''}
                              onChange={(e) => updateEndDate(e.target.value)}
                              min={config.startDate || dateRangeInfo.min_date || undefined}
                              max={dateRangeInfo.max_date || undefined}
                              className="w-full text-xs border border-gray-300 rounded px-2 py-1"
                            />
                          </div>
                        </div>
                      )}
                    </div>

                    {/* Draggable Date Range Chip */}
                    {((dateRangeType === 'relative' && relativeDatePreset) || (config.startDate && config.endDate)) && (
                      <div
                        draggable
                        onDragStart={(e) => {
                          e.dataTransfer.setData('type', 'daterange')
                          if (dateRangeType === 'relative' && relativeDatePreset) {
                            e.dataTransfer.setData('dateRangeType', 'relative')
                            e.dataTransfer.setData('relativeDatePreset', relativeDatePreset)
                          } else {
                            e.dataTransfer.setData('dateRangeType', 'absolute')
                            e.dataTransfer.setData('startDate', currentStartDate || '')
                            e.dataTransfer.setData('endDate', currentEndDate || '')
                          }
                        }}
                        className="p-2 bg-purple-50 border-2 border-purple-300 rounded cursor-move hover:bg-purple-100 transition-colors"
                      >
                        <div className="flex items-center gap-2">
                          <GripVertical className="h-3 w-3 text-purple-600" />
                          <div className="flex-1">
                            <div className="text-xs font-medium text-purple-900">
                              {dateRangeType === 'relative' && relativeDatePreset ? (
                                <>
                                  {relativeDatePreset.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                  <span className="ml-1 text-purple-600">(relative)</span>
                                </>
                              ) : (
                                `${config.startDate} → ${config.endDate}`
                              )}
                            </div>
                            <div className="text-xs text-purple-600">Drag to table</div>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="p-3 text-center bg-yellow-50 border border-yellow-200 rounded">
                    <p className="text-xs text-yellow-700">This table has no date column</p>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {/* Dimension Selection - Only show when data source is dropped */}
        {config.selectedTable && (
        <div>
          <button
            onClick={() => setIsDimensionsExpanded(!isDimensionsExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Dimensions ({availableDimensionsToAdd.length + customDimensions.length} available)
            </h4>
            {isDimensionsExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isDimensionsExpanded && (
            <div className="space-y-3">
              {/* Search Input */}
              <SearchInput
                placeholder="Search dimensions..."
                value={dimensionSearch}
                onChange={setDimensionSearch}
              />

              {/* Create Custom Dimension Button */}
              <button
                onClick={handleCreateNewDimension}
                className="w-full p-2 bg-blue-50 border-2 border-dashed border-blue-300 rounded hover:bg-blue-100 transition-colors flex items-center justify-center gap-2 text-blue-600"
              >
                <Plus className="h-3 w-3" />
                <span className="text-xs font-medium">Create Custom Dimension</span>
              </button>

              {/* Custom Dimensions */}
              {filteredCustomDimensions.length > 0 && (
                <div className="space-y-2">
                  <h6 className="text-xs font-medium text-gray-600">Custom Dimensions</h6>
                  {filteredCustomDimensions.map((dimension) => {
                    const dimensionValue = `custom_${dimension.id}`
                    const isSelected = selectedDimensions.includes(dimensionValue) ||
                                      selectedTableDimensions.includes(dimensionValue)

                    return (
                      <div
                        key={dimension.id}
                        className={`p-2 bg-blue-50 border-2 rounded group ${
                          isSelected
                            ? 'border-gray-400 opacity-50 cursor-not-allowed'
                            : 'border-blue-300 cursor-move hover:bg-blue-100'
                        } transition-colors`}
                        draggable={!isSelected}
                        onDragStart={(e) => {
                          if (!isSelected) {
                            e.dataTransfer.setData('type', 'dimension')
                            e.dataTransfer.setData('value', dimensionValue)
                            e.dataTransfer.setData('label', dimension.name)
                          }
                        }}
                      >
                        <div className="flex items-center gap-2">
                          <GripVertical className="h-3 w-3 text-blue-600" />
                          <div className="flex-1">
                            <div className="text-xs font-medium text-gray-900">
                              {dimension.name}
                            </div>
                            <div className="text-xs text-blue-600">
                              {dimension.type === 'date_range'
                                ? `${dimension.values?.length || 0} date ranges`
                                : `${dimension.metric_values?.length || 0} conditions`} • {isSelected ? 'In use' : 'Drag to table'}
                            </div>
                          </div>
                          {!isSelected && (
                            <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                              <button
                                onClick={() => handleEditDimension(dimension)}
                                className="p-1 hover:bg-blue-200 rounded transition-colors"
                                title="Edit"
                              >
                                <Edit className="h-3 w-3 text-blue-700" />
                              </button>
                              <button
                                onClick={() => handleDuplicateDimension(dimension.id)}
                                className="p-1 hover:bg-blue-200 rounded transition-colors"
                                title="Duplicate"
                              >
                                <Copy className="h-3 w-3 text-blue-700" />
                              </button>
                              <button
                                onClick={() => handleDeleteDimension(dimension.id)}
                                className="p-1 hover:bg-red-200 rounded transition-colors"
                                title="Delete"
                              >
                                <Trash2 className="h-3 w-3 text-red-600" />
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}

              {/* Built-in Dimensions */}
              {filteredDimensions.length > 0 && (
                <div className="space-y-2">
                  <h6 className="text-xs font-medium text-gray-600">Built-in Dimensions</h6>
                  {filteredDimensions.map((dimension) => (
                    <div
                      key={dimension.value}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.setData('type', 'dimension')
                        e.dataTransfer.setData('value', dimension.value)
                        e.dataTransfer.setData('label', dimension.label)
                      }}
                      className="p-2 bg-green-50 border-2 border-green-300 rounded cursor-move hover:bg-green-100 transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        <GripVertical className="h-3 w-3 text-green-600" />
                        <div className="flex-1">
                          <div className="text-xs font-medium text-gray-900">
                            {dimension.label}
                          </div>
                          <div className="text-xs text-green-600">Drag to table</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {filteredDimensions.length === 0 && filteredCustomDimensions.length === 0 && (
                <div className="p-4 text-center bg-white border border-gray-200 rounded">
                  <p className="text-xs text-gray-500">{dimensionSearch ? 'No dimensions match your search' : 'All dimensions added'}</p>
                  <p className="text-xs text-gray-400 mt-1">{dimensionSearch ? 'Try a different search term' : 'Remove dimensions from table or create a custom one'}</p>
                </div>
              )}
            </div>
          )}
        </div>
        )}

        {/* Filters - Only show when data source is dropped */}
        {config.selectedTable && (
        <div>
          <button
            onClick={() => setIsFiltersExpanded(!isFiltersExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Filters
            </h4>
            {isFiltersExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isFiltersExpanded && (
            <div className="space-y-2">
              {filterableDimensions.length > 0 ? (
                <>
                  {/* Search Input */}
                  <SearchInput
                    placeholder="Search filters..."
                    value={filterSearch}
                    onChange={setFilterSearch}
                  />

                  {/* Active filter count */}
                  {Object.keys(dimensionFilters || {}).length > 0 && (
                    <div className="px-2 py-1 bg-blue-50 rounded">
                      <span className="text-xs text-blue-700 font-medium">
                        {Object.keys(dimensionFilters || {}).length} dimension filter(s) active
                      </span>
                    </div>
                  )}

                  {/* Dimension filter dropdowns */}
                  {filteredFilterableDimensions.map(dimension => (
                    <DimensionFilterInline
                      key={dimension.id}
                      dimension={dimension}
                      selectedValues={pendingFilters[dimension.id] || []}
                      onFilterChange={(values) => handlePendingFilterChange(dimension.id, values)}
                      currentFilters={currentFilters}
                      tableId={config.selectedTable || undefined}
                    />
                  ))}

                  {/* No results message */}
                  {filteredFilterableDimensions.length === 0 && filterSearch && (
                    <div className="p-3 text-center bg-gray-50 border border-gray-200 rounded">
                      <p className="text-xs text-gray-500">No filters match your search</p>
                    </div>
                  )}

                  {/* Apply/Clear buttons */}
                  <div className="flex gap-2 pt-2">
                    <button
                      onClick={applyPendingFilters}
                      disabled={!hasPendingChanges}
                      className={`flex-1 px-3 py-2 text-xs font-medium rounded transition-colors ${
                        hasPendingChanges
                          ? 'bg-blue-600 text-white hover:bg-blue-700'
                          : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                      }`}
                    >
                      Apply Filters
                    </button>
                    <button
                      onClick={clearPendingFilters}
                      disabled={Object.keys(pendingFilters).length === 0}
                      className={`flex-1 px-3 py-2 text-xs font-medium rounded transition-colors ${
                        Object.keys(pendingFilters).length > 0
                          ? 'bg-gray-600 text-white hover:bg-gray-700'
                          : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                      }`}
                    >
                      Clear Pending
                    </button>
                  </div>
                </>
              ) : (
                <div className="p-3 text-center bg-pink-50 border border-pink-200 rounded">
                  <p className="text-xs text-pink-700">No filterable dimensions available</p>
                  <p className="text-xs text-gray-500 mt-1">
                    Dimensions must be marked as filterable in the schema
                  </p>
                </div>
              )}
            </div>
          )}
        </div>
        )}

        {/* Metrics - Available to Add - Only show when data source is dropped */}
        {config.selectedTable && (
        <div>
          <button
            onClick={() => setIsMetricsExpanded(!isMetricsExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Metrics ({availableMetricsToAdd.length} available)
            </h4>
            {isMetricsExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isMetricsExpanded && (
            <div className="space-y-3">
              {/* Search Input */}
              <SearchInput
                placeholder="Search metrics..."
                value={metricSearch}
                onChange={setMetricSearch}
              />

              {availableMetricsToAdd.length === 0 ? (
                <div className="p-4 text-center bg-white border border-gray-200 rounded">
                  <p className="text-xs text-gray-500">All metrics added</p>
                  <p className="text-xs text-gray-400 mt-1">Remove metrics from table to add more</p>
                </div>
              ) : filteredMetrics.length === 0 ? (
                <div className="p-4 text-center bg-white border border-gray-200 rounded">
                  <p className="text-xs text-gray-500">No metrics match your search</p>
                  <p className="text-xs text-gray-400 mt-1">Try a different search term</p>
                </div>
              ) : (
                <>
                  {Object.entries(metricsByCategory).map(([category, metrics]) => (
                    <div key={category}>
                      <h6 className="text-xs font-medium text-gray-600 mb-1">
                        {category}
                      </h6>
                      <div className="space-y-1">
                        {metrics.map((metric) => (
                          <AvailableMetric
                            key={metric.id}
                            metric={metric}
                            onAdd={addMetric}
                            isSelected={false}
                          />
                        ))}
                      </div>
                    </div>
                  ))}
                </>
              )}
            </div>
          )}
        </div>
        )}
      </div>

      {/* Footer */}
      <div className="bg-white border-t border-gray-200 p-4">
        <button
          onClick={resetToDefaults}
          className="w-full text-xs text-gray-600 hover:text-gray-900 transition-colors py-1"
        >
          Reset to Defaults
        </button>
      </div>

      {/* Custom Dimension Modal */}
      <CustomDimensionModal
        isOpen={isModalOpen}
        onClose={() => {
          setIsModalOpen(false)
          setEditingDimension(null)
        }}
        onSave={handleModalSave}
        editingDimension={editingDimension}
        mode={modalMode}
        tableId={config.selectedTable || undefined}
      />
    </div>
  )
}
