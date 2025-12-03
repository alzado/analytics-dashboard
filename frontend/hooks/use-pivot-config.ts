import { useState, useEffect, useCallback, useMemo } from 'react'
import type { DateRange, DateRangeType, RelativeDatePreset } from '@/lib/types'

const STORAGE_KEY = 'pivot-table-config'

export interface DimensionFilter {
  dimension: string
  value: string
  label: string
}

export interface PivotConfig {
  selectedTable: string | null
  selectedDateRange: DateRange | null
  startDate: string | null
  endDate: string | null
  dateRangeType?: DateRangeType
  relativeDatePreset?: RelativeDatePreset | null
  isDataSourceDropped: boolean
  isDateRangeDropped: boolean
  selectedDimensions: string[]
  selectedTableDimensions: string[]
  selectedMetrics: string[]
  selectedFilters: DimensionFilter[]
  // UI state (persisted)
  isConfigOpen?: boolean
  expandedRows?: string[]
  selectedDisplayMetrics?: string[] // Changed from single metric to array
  sortColumn?: string | number
  sortSubColumn?: 'value' | 'diff' | 'pctDiff'
  sortDirection?: 'asc' | 'desc'
  sortMetric?: string // Which metric to sort by
  chartType?: 'bar' | 'line' // Chart type for visualizations
  // Manual fetch state (not persisted)
  fetchRequested?: boolean
  lastFetchedConfigHash?: string | null
  // Snapshot of config used for the last fetch (used for stable query keys)
  fetchedConfig?: {
    selectedTable: string | null
    selectedDimensions: string[]
    selectedTableDimensions: string[]
    selectedMetrics: string[]
    selectedFilters: DimensionFilter[]
    startDate: string | null
    endDate: string | null
    dateRangeType?: DateRangeType
    relativeDatePreset?: RelativeDatePreset | null
  } | null
}

interface UsePivotConfigReturn {
  config: PivotConfig
  updateTable: (tableName: string | null) => void
  updateDateRange: (dateRange: DateRange | null) => void
  updateStartDate: (date: string | null) => void
  updateEndDate: (date: string | null) => void
  updateFullDateRange: (type: DateRangeType, preset: RelativeDatePreset | null, startDate: string | null, endDate: string | null) => void
  setDataSourceDropped: (dropped: boolean) => void
  setDateRangeDropped: (dropped: boolean) => void
  addDimension: (dimension: string) => void
  removeDimension: (dimension: string) => void
  reorderDimensions: (fromIndex: number, toIndex: number) => void
  addTableDimension: (dimension: string) => void
  removeTableDimension: (dimension: string) => void
  clearTableDimensions: () => void
  updateMetrics: (metrics: string[]) => void
  addMetric: (metricId: string) => void
  removeMetric: (metricId: string) => void
  reorderMetrics: (fromIndex: number, toIndex: number) => void
  addFilter: (filter: DimensionFilter) => void
  removeFilter: (index: number) => void
  resetToDefaults: () => void
  // UI state methods
  setConfigOpen: (open: boolean) => void
  setExpandedRows: (rows: string[]) => void
  setSelectedDisplayMetrics: (metrics: string[]) => void
  toggleDisplayMetric: (metric: string) => void
  setSortConfig: (column: string | number, subColumn?: 'value' | 'diff' | 'pctDiff', direction?: 'asc' | 'desc', metric?: string) => void
  setChartType: (type: 'bar' | 'line') => void
  // Manual fetch control
  triggerFetch: () => void
  isStale: boolean
  fetchRequested: boolean
}

const DEFAULT_CONFIG: PivotConfig = {
  selectedTable: null,
  selectedDateRange: null,
  startDate: null,
  endDate: null,
  dateRangeType: 'absolute',
  relativeDatePreset: null,
  isDataSourceDropped: false,
  isDateRangeDropped: false,
  selectedDimensions: [],
  selectedTableDimensions: [],
  selectedMetrics: [], // Will be populated by components using usePivotMetrics
  selectedFilters: [],
  // UI state defaults
  isConfigOpen: true,
  expandedRows: [],
  selectedDisplayMetrics: [], // Will be populated by component based on available metrics
  sortColumn: undefined,
  sortSubColumn: undefined,
  sortDirection: undefined,
  chartType: 'bar', // Default chart type
  // Manual fetch state defaults
  fetchRequested: false,
  lastFetchedConfigHash: null,
  fetchedConfig: null,
}

export function usePivotConfig(): UsePivotConfigReturn {
  const [config, setConfig] = useState<PivotConfig>(DEFAULT_CONFIG)

  // Load config from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored) as Partial<PivotConfig> & { selectedDisplayMetric?: string }
        // Only restore configuration data, not UI state
        // Handle migration from old selectedDisplayMetric (string) to new selectedDisplayMetrics (array)
        let displayMetrics = DEFAULT_CONFIG.selectedDisplayMetrics
        if (Array.isArray((parsed as any).selectedDisplayMetrics)) {
          displayMetrics = (parsed as any).selectedDisplayMetrics
        } else if (typeof (parsed as any).selectedDisplayMetric === 'string') {
          // Migrate from old single metric to array
          displayMetrics = [(parsed as any).selectedDisplayMetric]
        }

        // Validate and sanitize date range type
        let dateRangeType: DateRangeType =
          (parsed.dateRangeType === 'absolute' || parsed.dateRangeType === 'relative')
            ? parsed.dateRangeType
            : 'absolute'

        // Fix corrupted data where dates were stored in wrong fields
        let startDate = parsed.startDate ?? null
        let endDate = parsed.endDate ?? null
        let relativeDatePreset = parsed.relativeDatePreset ?? null

        // If dateRangeType looks like a date (YYYY-MM-DD), it's corrupted - recover silently
        if (parsed.dateRangeType && /^\d{4}-\d{2}-\d{2}$/.test(parsed.dateRangeType as string)) {
          startDate = parsed.dateRangeType as string
          dateRangeType = 'absolute'
        }

        // If relativeDatePreset looks like a date (YYYY-MM-DD), it's corrupted - recover silently
        if (parsed.relativeDatePreset && /^\d{4}-\d{2}-\d{2}$/.test(parsed.relativeDatePreset as string)) {
          endDate = parsed.relativeDatePreset as string
          relativeDatePreset = null
        }

        const migratedConfig: PivotConfig = {
          ...DEFAULT_CONFIG,
          // Configuration data (persist these)
          selectedTable: parsed.selectedTable ?? null,
          selectedDateRange: parsed.selectedDateRange ?? null,
          startDate,
          endDate,
          dateRangeType,
          relativeDatePreset,
          isDataSourceDropped: parsed.isDataSourceDropped ?? false,
          isDateRangeDropped: parsed.isDateRangeDropped ?? false,
          selectedDimensions: Array.isArray(parsed.selectedDimensions) ? parsed.selectedDimensions : [],
          selectedTableDimensions: Array.isArray(parsed.selectedTableDimensions) ? parsed.selectedTableDimensions : [],
          selectedMetrics: Array.isArray(parsed.selectedMetrics) ? parsed.selectedMetrics : [],
          selectedFilters: Array.isArray(parsed.selectedFilters) ? parsed.selectedFilters : [],
          // UI state (always use defaults, don't persist)
          isConfigOpen: DEFAULT_CONFIG.isConfigOpen,
          expandedRows: DEFAULT_CONFIG.expandedRows,
          selectedDisplayMetrics: displayMetrics,
          sortColumn: DEFAULT_CONFIG.sortColumn,
          sortSubColumn: DEFAULT_CONFIG.sortSubColumn,
          sortDirection: DEFAULT_CONFIG.sortDirection,
        }
        setConfig(migratedConfig)
      }
    } catch (error) {
      console.error('Failed to load pivot config from localStorage:', error)
    }
  }, [])

  // Save only configuration data to localStorage, not UI state
  useEffect(() => {
    try {
      const configToSave = {
        selectedTable: config.selectedTable,
        selectedDateRange: config.selectedDateRange,
        startDate: config.startDate,
        endDate: config.endDate,
        dateRangeType: config.dateRangeType,
        relativeDatePreset: config.relativeDatePreset,
        isDataSourceDropped: config.isDataSourceDropped,
        isDateRangeDropped: config.isDateRangeDropped,
        selectedDimensions: config.selectedDimensions,
        selectedTableDimensions: config.selectedTableDimensions,
        selectedMetrics: config.selectedMetrics,
        selectedFilters: config.selectedFilters,
        // Explicitly exclude UI state from persistence
      }
      localStorage.setItem(STORAGE_KEY, JSON.stringify(configToSave))
    } catch (error) {
      console.error('Failed to save pivot config to localStorage:', error)
    }
  }, [config])

  const updateTable = useCallback((tableName: string | null) => {
    setConfig((prev) => ({ ...prev, selectedTable: tableName }))
  }, [])

  const updateDateRange = useCallback((dateRange: DateRange | null) => {
    setConfig((prev) => ({ ...prev, selectedDateRange: dateRange }))
  }, [])

  const updateStartDate = useCallback((date: string | null) => {
    setConfig((prev) => ({ ...prev, startDate: date }))
  }, [])

  const updateEndDate = useCallback((date: string | null) => {
    setConfig((prev) => ({ ...prev, endDate: date }))
  }, [])

  const updateFullDateRange = useCallback((
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => {
    // Validate that type is either 'absolute' or 'relative'
    const validType: DateRangeType = (type === 'absolute' || type === 'relative') ? type : 'absolute'

    setConfig((prev) => ({
      ...prev,
      dateRangeType: validType,
      relativeDatePreset: preset,
      startDate,
      endDate,
    }))
  }, [])

  const setDataSourceDropped = useCallback((dropped: boolean) => {
    setConfig((prev) => ({ ...prev, isDataSourceDropped: dropped }))
  }, [])

  const setDateRangeDropped = useCallback((dropped: boolean) => {
    setConfig((prev) => ({ ...prev, isDateRangeDropped: dropped }))
  }, [])

  const addDimension = useCallback((dimension: string) => {
    setConfig((prev) => {
      // Don't add if already in row dimensions or table dimensions
      if (prev.selectedDimensions.includes(dimension) || prev.selectedTableDimensions.includes(dimension)) {
        return prev
      }
      return {
        ...prev,
        selectedDimensions: [...prev.selectedDimensions, dimension],
      }
    })
  }, [])

  const removeDimension = useCallback((dimension: string) => {
    setConfig((prev) => ({
      ...prev,
      selectedDimensions: prev.selectedDimensions.filter((dim) => dim !== dimension),
    }))
  }, [])

  const reorderDimensions = useCallback((fromIndex: number, toIndex: number) => {
    setConfig((prev) => {
      const dimensions = [...prev.selectedDimensions]
      const [removed] = dimensions.splice(fromIndex, 1)
      dimensions.splice(toIndex, 0, removed)
      return {
        ...prev,
        selectedDimensions: dimensions,
      }
    })
  }, [])

  const addTableDimension = useCallback((dimension: string) => {
    setConfig((prev) => {
      // Don't add if already in table dimensions or row dimensions
      if (prev.selectedTableDimensions.includes(dimension) || prev.selectedDimensions.includes(dimension)) {
        return prev
      }
      return {
        ...prev,
        selectedTableDimensions: [...prev.selectedTableDimensions, dimension],
      }
    })
  }, [])

  const removeTableDimension = useCallback((dimension: string) => {
    setConfig((prev) => ({
      ...prev,
      selectedTableDimensions: prev.selectedTableDimensions.filter((dim) => dim !== dimension),
    }))
  }, [])

  const clearTableDimensions = useCallback(() => {
    setConfig((prev) => ({
      ...prev,
      selectedTableDimensions: [],
    }))
  }, [])

  const updateMetrics = useCallback((metrics: string[]) => {
    setConfig((prev) => ({ ...prev, selectedMetrics: metrics }))
  }, [])

  const addMetric = useCallback((metricId: string) => {
    setConfig((prev) => {
      if (prev.selectedMetrics.includes(metricId)) {
        return prev
      }
      return {
        ...prev,
        selectedMetrics: [...prev.selectedMetrics, metricId],
      }
    })
  }, [])

  const removeMetric = useCallback((metricId: string) => {
    setConfig((prev) => ({
      ...prev,
      selectedMetrics: prev.selectedMetrics.filter((id) => id !== metricId),
    }))
  }, [])

  const reorderMetrics = useCallback((fromIndex: number, toIndex: number) => {
    setConfig((prev) => {
      const metrics = [...prev.selectedMetrics]
      const [removed] = metrics.splice(fromIndex, 1)
      metrics.splice(toIndex, 0, removed)
      return {
        ...prev,
        selectedMetrics: metrics,
      }
    })
  }, [])

  const addFilter = useCallback((filter: DimensionFilter) => {
    setConfig((prev) => ({
      ...prev,
      selectedFilters: [...prev.selectedFilters, filter],
    }))
  }, [])

  const removeFilter = useCallback((index: number) => {
    setConfig((prev) => ({
      ...prev,
      selectedFilters: prev.selectedFilters.filter((_, i) => i !== index),
    }))
  }, [])

  const resetToDefaults = useCallback(() => {
    setConfig(DEFAULT_CONFIG)
  }, [])

  // UI state methods
  const setConfigOpen = useCallback((open: boolean) => {
    setConfig((prev) => ({ ...prev, isConfigOpen: open }))
  }, [])

  const setExpandedRows = useCallback((rows: string[]) => {
    setConfig((prev) => ({ ...prev, expandedRows: rows }))
  }, [])

  const setSelectedDisplayMetrics = useCallback((metrics: string[]) => {
    setConfig((prev) => ({ ...prev, selectedDisplayMetrics: metrics }))
  }, [])

  const toggleDisplayMetric = useCallback((metric: string) => {
    setConfig((prev) => {
      const currentMetrics = prev.selectedDisplayMetrics || []
      if (currentMetrics.includes(metric)) {
        // Remove if already selected (but keep at least one metric)
        const newMetrics = currentMetrics.filter(m => m !== metric)
        return { ...prev, selectedDisplayMetrics: newMetrics.length > 0 ? newMetrics : currentMetrics }
      } else {
        // Add to selection
        return { ...prev, selectedDisplayMetrics: [...currentMetrics, metric] }
      }
    })
  }, [])

  const setSortConfig = useCallback((column: string | number, subColumn?: 'value' | 'diff' | 'pctDiff', direction?: 'asc' | 'desc', metric?: string) => {
    setConfig((prev) => ({
      ...prev,
      sortColumn: column,
      sortSubColumn: subColumn,
      sortDirection: direction,
      sortMetric: metric,
    }))
  }, [])

  const setChartType = useCallback((type: 'bar' | 'line') => {
    setConfig((prev) => ({
      ...prev,
      chartType: type,
    }))
  }, [])

  // Compute config hash for staleness detection
  // This includes all fields that affect the BigQuery query
  const configHash = useMemo(() => JSON.stringify({
    selectedTable: config.selectedTable,
    selectedDimensions: config.selectedDimensions,
    selectedTableDimensions: config.selectedTableDimensions,
    selectedMetrics: config.selectedMetrics,
    selectedFilters: config.selectedFilters,
    startDate: config.startDate,
    endDate: config.endDate,
    dateRangeType: config.dateRangeType,
    relativeDatePreset: config.relativeDatePreset,
  }), [
    config.selectedTable,
    config.selectedDimensions,
    config.selectedTableDimensions,
    config.selectedMetrics,
    config.selectedFilters,
    config.startDate,
    config.endDate,
    config.dateRangeType,
    config.relativeDatePreset,
  ])

  // Check if config has changed since last fetch
  const isStale = !!(
    config.fetchRequested &&
    config.lastFetchedConfigHash !== null &&
    config.lastFetchedConfigHash !== configHash
  )

  // Trigger a fetch and update the snapshot
  const triggerFetch = useCallback(() => {
    setConfig((prev) => ({
      ...prev,
      fetchRequested: true,
      lastFetchedConfigHash: configHash,
      // Store snapshot of current config for stable query keys
      fetchedConfig: {
        selectedTable: prev.selectedTable,
        selectedDimensions: prev.selectedDimensions,
        selectedTableDimensions: prev.selectedTableDimensions,
        selectedMetrics: prev.selectedMetrics,
        selectedFilters: prev.selectedFilters,
        startDate: prev.startDate,
        endDate: prev.endDate,
        dateRangeType: prev.dateRangeType,
        relativeDatePreset: prev.relativeDatePreset,
      },
    }))
  }, [configHash])

  return {
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
    reorderDimensions,
    addTableDimension,
    removeTableDimension,
    clearTableDimensions,
    updateMetrics,
    addMetric,
    removeMetric,
    reorderMetrics,
    addFilter,
    removeFilter,
    resetToDefaults,
    // UI state methods
    setConfigOpen,
    setExpandedRows,
    setSelectedDisplayMetrics,
    toggleDisplayMetric,
    setSortConfig,
    setChartType,
    // Manual fetch control
    triggerFetch,
    isStale,
    fetchRequested: config.fetchRequested ?? false,
  }
}
