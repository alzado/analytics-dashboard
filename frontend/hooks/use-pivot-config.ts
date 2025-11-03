import { useState, useEffect, useCallback } from 'react'
import { DEFAULT_METRICS } from '@/lib/pivot-metrics'
import type { DateRange } from '@/lib/types'

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
  isDataSourceDropped: boolean
  isDateRangeDropped: boolean
  selectedDimensions: string[]
  selectedMetrics: string[]
  selectedFilters: DimensionFilter[]
}

interface UsePivotConfigReturn {
  config: PivotConfig
  updateTable: (tableName: string | null) => void
  updateDateRange: (dateRange: DateRange | null) => void
  updateStartDate: (date: string | null) => void
  updateEndDate: (date: string | null) => void
  setDataSourceDropped: (dropped: boolean) => void
  setDateRangeDropped: (dropped: boolean) => void
  addDimension: (dimension: string) => void
  removeDimension: (dimension: string) => void
  reorderDimensions: (fromIndex: number, toIndex: number) => void
  updateMetrics: (metrics: string[]) => void
  addMetric: (metricId: string) => void
  removeMetric: (metricId: string) => void
  reorderMetrics: (fromIndex: number, toIndex: number) => void
  addFilter: (filter: DimensionFilter) => void
  removeFilter: (index: number) => void
  resetToDefaults: () => void
}

const DEFAULT_CONFIG: PivotConfig = {
  selectedTable: null,
  selectedDateRange: null,
  startDate: null,
  endDate: null,
  isDataSourceDropped: false,
  isDateRangeDropped: false,
  selectedDimensions: [],
  selectedMetrics: DEFAULT_METRICS,
  selectedFilters: [],
}

export function usePivotConfig(): UsePivotConfigReturn {
  const [config, setConfig] = useState<PivotConfig>(DEFAULT_CONFIG)

  // Load config from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored) as Partial<PivotConfig>
        // Merge with defaults to ensure all fields exist (handles migration from old versions)
        const migratedConfig: PivotConfig = {
          ...DEFAULT_CONFIG,
          ...parsed,
          // Ensure new fields have defaults if missing
          isDataSourceDropped: parsed.isDataSourceDropped ?? false,
          isDateRangeDropped: parsed.isDateRangeDropped ?? false,
          startDate: parsed.startDate ?? null,
          endDate: parsed.endDate ?? null,
          selectedDimensions: Array.isArray(parsed.selectedDimensions) ? parsed.selectedDimensions : [],
          selectedFilters: Array.isArray(parsed.selectedFilters) ? parsed.selectedFilters : [],
        }
        setConfig(migratedConfig)
      }
    } catch (error) {
      console.error('Failed to load pivot config from localStorage:', error)
    }
  }, [])

  // Save config to localStorage whenever it changes
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(config))
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

  const setDataSourceDropped = useCallback((dropped: boolean) => {
    setConfig((prev) => ({ ...prev, isDataSourceDropped: dropped }))
  }, [])

  const setDateRangeDropped = useCallback((dropped: boolean) => {
    setConfig((prev) => ({ ...prev, isDateRangeDropped: dropped }))
  }, [])

  const addDimension = useCallback((dimension: string) => {
    setConfig((prev) => {
      if (prev.selectedDimensions.includes(dimension)) {
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

  return {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    setDataSourceDropped,
    setDateRangeDropped,
    addDimension,
    removeDimension,
    reorderDimensions,
    updateMetrics,
    addMetric,
    removeMetric,
    reorderMetrics,
    addFilter,
    removeFilter,
    resetToDefaults,
  }
}
