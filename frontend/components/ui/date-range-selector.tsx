'use client'

import { useState, useEffect } from 'react'
import { Calendar, ChevronDown } from 'lucide-react'
import type { DateRangeType, RelativeDatePreset } from '@/lib/types'

interface DateRangeSelectorProps {
  dateRangeType: DateRangeType
  relativeDatePreset: RelativeDatePreset | null
  startDate: string | null
  endDate: string | null
  onDateRangeChange: (
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => void
}

interface PresetOption {
  value: RelativeDatePreset
  label: string
}

const PRESET_GROUPS: { category: string; options: PresetOption[] }[] = [
  {
    category: 'Quick Select',
    options: [
      { value: 'today', label: 'Today' },
      { value: 'yesterday', label: 'Yesterday' },
    ],
  },
  {
    category: 'Last X Days',
    options: [
      { value: 'last_7_days', label: 'Last 7 days' },
      { value: 'last_30_days', label: 'Last 30 days' },
      { value: 'last_90_days', label: 'Last 90 days' },
    ],
  },
  {
    category: 'Week',
    options: [
      { value: 'this_week', label: 'This week' },
      { value: 'last_week', label: 'Last week' },
    ],
  },
  {
    category: 'Month',
    options: [
      { value: 'this_month', label: 'This month' },
      { value: 'last_month', label: 'Last month' },
    ],
  },
  {
    category: 'Quarter',
    options: [
      { value: 'this_quarter', label: 'This quarter' },
      { value: 'last_quarter', label: 'Last quarter' },
    ],
  },
  {
    category: 'Year',
    options: [
      { value: 'this_year', label: 'This year' },
      { value: 'last_year', label: 'Last year' },
    ],
  },
]

export function DateRangeSelector({
  dateRangeType,
  relativeDatePreset,
  startDate,
  endDate,
  onDateRangeChange,
}: DateRangeSelectorProps) {
  const [isOpen, setIsOpen] = useState(false)
  // Track UI mode locally to avoid triggering updates when just switching tabs
  const [localMode, setLocalMode] = useState<DateRangeType>(dateRangeType)

  // Update localMode when prop changes (e.g., when editing a dimension)
  useEffect(() => {
    setLocalMode(dateRangeType)
  }, [dateRangeType])

  const handleTypeChange = (type: DateRangeType) => {
    // Just update local UI state, don't trigger filter change yet
    setLocalMode(type)
  }

  const handlePresetSelect = (preset: RelativeDatePreset) => {
    onDateRangeChange('relative', preset, null, null)
    setIsOpen(false)
  }

  const handleAbsoluteDateChange = (field: 'start' | 'end', value: string) => {
    if (field === 'start') {
      onDateRangeChange('absolute', null, value, endDate)
    } else {
      onDateRangeChange('absolute', null, startDate, value)
    }
  }

  const getDisplayLabel = (): string => {
    if (dateRangeType === 'relative' && relativeDatePreset) {
      const allOptions = PRESET_GROUPS.flatMap(g => g.options)
      const option = allOptions.find(o => o.value === relativeDatePreset)
      return option?.label || relativeDatePreset
    } else if (startDate && endDate) {
      return `${startDate} to ${endDate}`
    } else if (startDate) {
      return `From ${startDate}`
    } else if (endDate) {
      return `Until ${endDate}`
    }
    return 'Select date range'
  }

  return (
    <div className="space-y-3">
      {/* Type Toggle */}
      <div className="flex gap-2 border-b border-gray-200 pb-2">
        <button
          onClick={() => handleTypeChange('relative')}
          className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
            localMode === 'relative'
              ? 'bg-blue-100 text-blue-700 border border-blue-300'
              : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
          }`}
        >
          Relative
        </button>
        <button
          onClick={() => handleTypeChange('absolute')}
          className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
            localMode === 'absolute'
              ? 'bg-blue-100 text-blue-700 border border-blue-300'
              : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
          }`}
        >
          Absolute
        </button>
      </div>

      {/* Relative Date Selector */}
      {localMode === 'relative' && (
        <div className="relative">
          <button
            onClick={() => setIsOpen(!isOpen)}
            className="w-full flex items-center justify-between px-3 py-2 text-sm border border-blue-500 bg-blue-50 text-blue-900 rounded-md transition-colors"
          >
            <div className="flex items-center gap-2">
              <Calendar className="h-4 w-4" />
              <span>{getDisplayLabel()}</span>
            </div>
            <ChevronDown
              className={`h-4 w-4 transition-transform ${isOpen ? 'transform rotate-180' : ''}`}
            />
          </button>

          {isOpen && (
            <>
              {/* Overlay to close dropdown */}
              <div
                className="fixed inset-0 z-10"
                onClick={() => setIsOpen(false)}
              />

              {/* Dropdown content */}
              <div className="absolute z-20 mt-1 w-full bg-white border border-gray-300 rounded-md shadow-lg max-h-96 overflow-auto">
                {PRESET_GROUPS.map((group, idx) => (
                  <div key={group.category}>
                    {idx > 0 && <div className="border-t border-gray-200" />}
                    <div className="px-3 py-2 bg-gray-50">
                      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                        {group.category}
                      </p>
                    </div>
                    <div className="py-1">
                      {group.options.map(option => (
                        <button
                          key={option.value}
                          onClick={() => handlePresetSelect(option.value)}
                          className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-50 transition-colors ${
                            relativeDatePreset === option.value
                              ? 'bg-blue-50 text-blue-700 font-medium'
                              : 'text-gray-700'
                          }`}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}

      {/* Absolute Date Inputs */}
      {localMode === 'absolute' && (
        <div className="space-y-2">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              Start Date
            </label>
            <input
              type="date"
              value={startDate || ''}
              onChange={e => handleAbsoluteDateChange('start', e.target.value)}
              className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              End Date
            </label>
            <input
              type="date"
              value={endDate || ''}
              onChange={e => handleAbsoluteDateChange('end', e.target.value)}
              className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>
      )}

      {/* Date Range Preview */}
      <div className="text-xs text-gray-500 px-1">
        <span className="font-medium">Current: </span>
        {getDisplayLabel()}
      </div>
    </div>
  )
}
