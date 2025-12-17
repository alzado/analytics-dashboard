'use client'

import { useState, useEffect } from 'react'
import { X, Filter, ChevronDown, Check } from 'lucide-react'
import { useSchema } from '@/hooks/use-schema'
import { fetchDimensionValues } from '@/lib/api'
import { DateRangeSelector } from '@/components/ui/date-range-selector'
import type { DimensionDef, FilterParams, DateRangeType, RelativeDatePreset } from '@/lib/types'

interface PivotFilterPanelProps {
  filters: Record<string, string[]>
  onFilterChange: (dimensionId: string, values: string[]) => void
  onClearFilters: () => void
  currentFilters?: FilterParams
  tableId?: string
  // Date range props
  dateRangeType?: DateRangeType
  relativeDatePreset?: RelativeDatePreset | null
  startDate?: string | null
  endDate?: string | null
  onDateRangeChange?: (
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => void
}

export function PivotFilterPanel({
  filters,
  onFilterChange,
  onClearFilters,
  currentFilters,
  tableId,
  dateRangeType = 'absolute',
  relativeDatePreset = null,
  startDate = null,
  endDate = null,
  onDateRangeChange,
}: PivotFilterPanelProps) {
  const { schema, isLoadingSchema: schemaLoading } = useSchema(tableId)

  // Get only filterable dimensions from schema
  const filterableDimensions = schema?.dimensions?.filter(d => d.is_filterable) || []

  const activeFilterCount = Object.keys(filters).length

  if (schemaLoading) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-4 mb-4">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-32 mb-4"></div>
          <div className="space-y-3">
            <div className="h-10 bg-gray-200 rounded"></div>
            <div className="h-10 bg-gray-200 rounded"></div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white border-2 border-blue-500 rounded-lg p-4 shadow-lg">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Filter className="h-5 w-5 text-blue-600" />
          <h3 className="font-semibold text-gray-900">
            Dimension Filters ({filterableDimensions.length} available)
          </h3>
          {activeFilterCount > 0 && (
            <span className="bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded-full">
              {activeFilterCount} active
            </span>
          )}
        </div>
        {activeFilterCount > 0 && (
          <button
            onClick={onClearFilters}
            className="text-sm text-gray-600 hover:text-gray-900 flex items-center gap-1"
          >
            <X className="h-4 w-4" />
            Clear all
          </button>
        )}
      </div>

      {/* Date Range Selector */}
      {onDateRangeChange && (
        <div className="mb-4">
          <h4 className="text-sm font-medium text-gray-700 mb-2">Date Range</h4>
          <DateRangeSelector
            dateRangeType={dateRangeType}
            relativeDatePreset={relativeDatePreset}
            startDate={startDate}
            endDate={endDate}
            onDateRangeChange={onDateRangeChange}
          />
        </div>
      )}

      <div className="space-y-3">
        {filterableDimensions.length > 0 ? (
          filterableDimensions.map(dimension => (
            <DimensionFilter
              key={dimension.id}
              dimension={dimension}
              selectedValues={filters[dimension.id] || []}
              onFilterChange={values => onFilterChange(dimension.id, values)}
              currentFilters={currentFilters}
              tableId={tableId}
            />
          ))
        ) : (
          <div className="text-center py-8">
            <p className="text-sm text-gray-500">No filterable dimensions available</p>
            <p className="text-xs text-gray-400 mt-1">
              Dimensions must be marked as filterable in the schema
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

interface DimensionFilterProps {
  dimension: DimensionDef
  selectedValues: string[]
  onFilterChange: (values: string[]) => void
  currentFilters?: FilterParams
  tableId?: string
}

function DimensionFilter({
  dimension,
  selectedValues,
  onFilterChange,
  currentFilters,
  tableId,
}: DimensionFilterProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [availableValues, setAvailableValues] = useState<string[]>([])
  const [isLoadingValues, setIsLoadingValues] = useState(false)

  // Load available values when dropdown opens
  // Don't pass date filters - show ALL possible values regardless of current date range
  // This makes filters more user-friendly since users can see all available options
  useEffect(() => {
    if (isOpen && availableValues.length === 0) {
      setIsLoadingValues(true)
      // Pass only dimension_filters (not date filters) to show all possible values
      const filtersWithoutDates = {
        dimension_filters: currentFilters?.dimension_filters || {}
      }
      fetchDimensionValues(dimension.id, filtersWithoutDates, tableId)
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
  }, [isOpen, dimension.id, availableValues.length, currentFilters?.dimension_filters, tableId])

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
        className={`w-full flex items-center justify-between px-3 py-2 text-sm border rounded-md transition-colors ${
          selectedValues.length > 0
            ? 'border-blue-500 bg-blue-50 text-blue-900'
            : 'border-gray-300 bg-white text-gray-700 hover:border-gray-400'
        }`}
      >
        <span className="truncate">
          {dimension.display_name}
          {selectedValues.length > 0 && (
            <span className="ml-2 text-xs">
              ({selectedValues.length} selected)
            </span>
          )}
        </span>
        <ChevronDown
          className={`h-4 w-4 transition-transform ${isOpen ? 'transform rotate-180' : ''}`}
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
          <div className="absolute z-20 mt-1 w-full bg-white border border-gray-300 rounded-md shadow-lg max-h-64 overflow-auto">
            {isLoadingValues ? (
              <div className="p-4 text-center text-sm text-gray-500">
                Loading values...
              </div>
            ) : (
              <>
                {/* Select all / Clear all buttons */}
                <div className="sticky top-0 bg-gray-50 border-b border-gray-200 px-3 py-2 flex gap-2">
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
                      className="flex items-center gap-2 px-3 py-2 hover:bg-gray-50 cursor-pointer"
                    >
                      <div className="relative flex items-center">
                        <input
                          type="checkbox"
                          checked={selectedValues.includes(value)}
                          onChange={() => toggleValue(value)}
                          className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                        />
                        {selectedValues.includes(value) && (
                          <Check className="absolute h-3 w-3 text-blue-600 pointer-events-none" style={{ left: '2px' }} />
                        )}
                      </div>
                      <span className="text-sm text-gray-700 truncate flex-1">
                        {value}
                      </span>
                    </label>
                  ))}

                  {availableValues.length === 0 && (
                    <div className="p-4 text-center text-sm text-gray-500">
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
