'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchFilterOptions } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { X, Filter } from 'lucide-react'

interface FilterSidebarProps {
  isOpen: boolean
  onClose: () => void
}

export function FilterSidebar({ isOpen, onClose }: FilterSidebarProps) {
  const { filters, updateFilters, resetFilters } = useFilters()
  const { data: options } = useQuery({
    queryKey: ['filter-options'],
    queryFn: fetchFilterOptions,
  })

  if (!isOpen) return null

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black bg-opacity-50 z-40 lg:hidden"
        onClick={onClose}
      />

      {/* Sidebar */}
      <div className={`
        fixed top-0 right-0 h-full w-80 bg-white shadow-lg z-50 overflow-y-auto
        transform transition-transform duration-300 ease-in-out
        ${isOpen ? 'translate-x-0' : 'translate-x-full'}
      `}>
        <div className="p-6">
          {/* Header */}
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-2">
              <Filter size={20} />
              <h2 className="text-lg font-semibold">Filters</h2>
            </div>
            <button
              onClick={onClose}
              className="p-2 hover:bg-gray-100 rounded-md"
            >
              <X size={20} />
            </button>
          </div>

          {/* Date Range */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Date Range
            </label>
            <div className="space-y-2">
              <input
                type="date"
                value={filters.start_date || ''}
                onChange={(e) => updateFilters({ start_date: e.target.value || null })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
                max={options?.date_range.max}
              />
              <input
                type="date"
                value={filters.end_date || ''}
                onChange={(e) => updateFilters({ end_date: e.target.value || null })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
                min={filters.start_date || undefined}
                max={options?.date_range.max}
              />
            </div>
          </div>

          {/* Country */}
          {options?.countries && options.countries.length > 0 && (
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Country
              </label>
              <select
                value={filters.country || ''}
                onChange={(e) => updateFilters({ country: e.target.value || null })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              >
                <option value="">All Countries</option>
                {options.countries.map((country) => (
                  <option key={country} value={country}>
                    {country}
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Channel */}
          {options?.channels && options.channels.length > 0 && (
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Channel
              </label>
              <select
                value={filters.channel || ''}
                onChange={(e) => updateFilters({ channel: e.target.value || null })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              >
                <option value="">All Channels</option>
                {options.channels.map((channel) => (
                  <option key={channel} value={channel}>
                    {channel}
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Number of Attributes */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Number of Attributes
            </label>
            <div className="space-y-2">
              <input
                type="number"
                placeholder="Min"
                value={filters.n_attributes_min ?? ''}
                onChange={(e) => updateFilters({
                  n_attributes_min: e.target.value ? parseInt(e.target.value) : null
                })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
                min="0"
              />
              <input
                type="number"
                placeholder="Max"
                value={filters.n_attributes_max ?? ''}
                onChange={(e) => updateFilters({
                  n_attributes_max: e.target.value ? parseInt(e.target.value) : null
                })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
                min={filters.n_attributes_min || 0}
              />
            </div>
          </div>

          {/* Attributes */}
          {options?.attributes && (
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Attributes
              </label>
              <div className="space-y-2">
                {options.attributes.map((attr) => (
                  <label key={attr} className="flex items-center">
                    <input
                      type="checkbox"
                      checked={(filters as any)[`attr_${attr}`] || false}
                      onChange={(e) => updateFilters({
                        [`attr_${attr}`]: e.target.checked ? true : null
                      } as any)}
                      className="mr-2 h-4 w-4 text-blue-600 border-gray-300 rounded"
                    />
                    <span className="text-sm text-gray-700 capitalize">{attr}</span>
                  </label>
                ))}
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-2">
            <button
              onClick={resetFilters}
              className="flex-1 px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
            >
              Reset
            </button>
            <button
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
            >
              Apply
            </button>
          </div>
        </div>
      </div>
    </>
  )
}
