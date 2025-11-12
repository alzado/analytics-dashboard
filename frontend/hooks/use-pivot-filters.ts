import { useState, useCallback } from 'react'
import type { FilterParams } from '@/lib/types'

export interface PivotFiltersState {
  start_date: string | null
  end_date: string | null
  dimension_filters: Record<string, string[]>
}

export function usePivotFilters(initialFilters?: Partial<FilterParams>) {
  const [filters, setFilters] = useState<PivotFiltersState>({
    start_date: initialFilters?.start_date || null,
    end_date: initialFilters?.end_date || null,
    dimension_filters: initialFilters?.dimension_filters || {},
  })

  // Add or update filter values for a dimension
  const updateDimensionFilter = useCallback((dimensionId: string, values: string[]) => {
    setFilters(prev => ({
      ...prev,
      dimension_filters: {
        ...prev.dimension_filters,
        [dimensionId]: values,
      },
    }))
  }, [])

  // Add a single value to a dimension filter (for multi-select)
  const addDimensionFilterValue = useCallback((dimensionId: string, value: string) => {
    setFilters(prev => {
      const currentValues = prev.dimension_filters[dimensionId] || []
      if (currentValues.includes(value)) {
        return prev // Already exists
      }
      return {
        ...prev,
        dimension_filters: {
          ...prev.dimension_filters,
          [dimensionId]: [...currentValues, value],
        },
      }
    })
  }, [])

  // Remove a single value from a dimension filter
  const removeDimensionFilterValue = useCallback((dimensionId: string, value: string) => {
    setFilters(prev => {
      const currentValues = prev.dimension_filters[dimensionId] || []
      const newValues = currentValues.filter(v => v !== value)

      if (newValues.length === 0) {
        // Remove dimension entirely if no values left
        const { [dimensionId]: _, ...remainingFilters } = prev.dimension_filters
        return {
          ...prev,
          dimension_filters: remainingFilters,
        }
      }

      return {
        ...prev,
        dimension_filters: {
          ...prev.dimension_filters,
          [dimensionId]: newValues,
        },
      }
    })
  }, [])

  // Remove all filters for a dimension
  const removeDimensionFilter = useCallback((dimensionId: string) => {
    setFilters(prev => {
      const { [dimensionId]: _, ...remainingFilters } = prev.dimension_filters
      return {
        ...prev,
        dimension_filters: remainingFilters,
      }
    })
  }, [])

  // Update date range
  const updateDateRange = useCallback((startDate: string | null, endDate: string | null) => {
    setFilters(prev => ({
      ...prev,
      start_date: startDate,
      end_date: endDate,
    }))
  }, [])

  // Clear all filters
  const clearAllFilters = useCallback(() => {
    setFilters({
      start_date: null,
      end_date: null,
      dimension_filters: {},
    })
  }, [])

  // Clear only dimension filters (keep date range)
  const clearDimensionFilters = useCallback(() => {
    setFilters(prev => ({
      ...prev,
      dimension_filters: {},
    }))
  }, [])

  // Get active filter count
  const getActiveFilterCount = useCallback(() => {
    let count = 0
    if (filters.start_date || filters.end_date) count += 1
    count += Object.keys(filters.dimension_filters).length
    return count
  }, [filters])

  // Check if a dimension has active filters
  const hasDimensionFilter = useCallback((dimensionId: string) => {
    return dimensionId in filters.dimension_filters && filters.dimension_filters[dimensionId].length > 0
  }, [filters])

  // Get filter values for a specific dimension
  const getDimensionFilterValues = useCallback((dimensionId: string): string[] => {
    return filters.dimension_filters[dimensionId] || []
  }, [filters])

  // Convert to API FilterParams format
  const toFilterParams = useCallback((): FilterParams => {
    return {
      start_date: filters.start_date,
      end_date: filters.end_date,
      dimension_filters: filters.dimension_filters,
    }
  }, [filters])

  return {
    filters,
    updateDimensionFilter,
    addDimensionFilterValue,
    removeDimensionFilterValue,
    removeDimensionFilter,
    updateDateRange,
    clearAllFilters,
    clearDimensionFilters,
    getActiveFilterCount,
    hasDimensionFilter,
    getDimensionFilterValues,
    toFilterParams,
  }
}
