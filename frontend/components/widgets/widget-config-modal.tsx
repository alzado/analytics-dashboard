'use client'

import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { X, Calendar } from 'lucide-react'
import { updateWidget, fetchSchema, fetchGroupableDimensions, type WidgetConfig } from '@/lib/api'

interface WidgetConfigModalProps {
  dashboardId: string
  widget: WidgetConfig
  onClose: () => void
}

export function WidgetConfigModal({ dashboardId, widget, onClose }: WidgetConfigModalProps) {
  const queryClient = useQueryClient()
  const [title, setTitle] = useState(widget.title)
  const [selectedDimensions, setSelectedDimensions] = useState<string[]>(widget.dimensions)
  const [selectedTableDimensions, setSelectedTableDimensions] = useState<string[]>(widget.table_dimensions)
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(widget.metrics)
  const [startDate, setStartDate] = useState(widget.start_date || '')
  const [endDate, setEndDate] = useState(widget.end_date || '')
  const [chartType, setChartType] = useState<'bar' | 'line'>(widget.chart_type || 'bar')

  // Additional editor state for complete persistence
  const [displayMode, setDisplayMode] = useState<'pivot-table' | 'multi-table' | 'single-metric-chart' | undefined>(widget.display_mode)
  const [dateRangeType, setDateRangeType] = useState<'absolute' | 'relative'>(widget.date_range_type || 'absolute')
  const [relativeDatePreset, setRelativeDatePreset] = useState<string>(widget.relative_date_preset || 'last_7_days')
  const [visibleMetrics, setVisibleMetrics] = useState<string[]>(widget.visible_metrics || [])
  const [mergeThreshold, setMergeThreshold] = useState<number>(widget.merge_threshold || 0)
  const [dimensionSortOrder, setDimensionSortOrder] = useState<'asc' | 'desc'>(widget.dimension_sort_order || 'desc')
  const [showAdvanced, setShowAdvanced] = useState(false)

  // Fetch schema for the widget's table
  const { data: schema } = useQuery({
    queryKey: ['schema', widget.table_id],
    queryFn: () => fetchSchema(widget.table_id),
  })

  // Fetch dimensions
  const { data: dimensions } = useQuery({
    queryKey: ['dimensions', widget.table_id],
    queryFn: () => fetchGroupableDimensions(widget.table_id),
  })

  const updateMutation = useMutation({
    mutationFn: (updates: any) => {
      console.log('Updating widget with:', updates)
      return updateWidget(dashboardId, widget.id, updates)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })
      onClose()
    },
    onError: (error: any) => {
      console.error('Widget update error:', error)
      console.error('Error response:', error.response?.data)
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    updateMutation.mutate({
      title,
      dimensions: selectedDimensions,
      table_dimensions: selectedTableDimensions,
      metrics: selectedMetrics,
      start_date: startDate || null,
      end_date: endDate || null,
      chart_type: widget.type === 'chart' ? chartType : null,
      // UI state - complete persistence
      display_mode: displayMode || null,
      expanded_rows: widget.expanded_rows || null,
      column_order: widget.column_order || null,
      column_sort: widget.column_sort || null,
      // Additional editor state
      date_range_type: dateRangeType || null,
      relative_date_preset: dateRangeType === 'relative' ? relativeDatePreset : null,
      visible_metrics: visibleMetrics.length > 0 ? visibleMetrics : null,
      merge_threshold: mergeThreshold !== null && mergeThreshold !== undefined ? mergeThreshold : null,
      dimension_sort_order: dimensionSortOrder || null,
      children_sort_config: widget.children_sort_config || null,
    })
  }

  const allMetrics = [
    ...(schema?.base_metrics || []).map((m) => ({ id: m.id, name: m.display_name })),
    ...(schema?.calculated_metrics || []).map((m) => ({ id: m.id, name: m.display_name })),
  ]

  const toggleDimension = (dimId: string) => {
    setSelectedDimensions((prev) =>
      prev.includes(dimId) ? prev.filter((d) => d !== dimId) : [...prev, dimId]
    )
  }

  const toggleTableDimension = (dimId: string) => {
    setSelectedTableDimensions((prev) =>
      prev.includes(dimId) ? prev.filter((d) => d !== dimId) : [...prev, dimId]
    )
  }

  const toggleMetric = (metricId: string) => {
    setSelectedMetrics((prev) =>
      prev.includes(metricId) ? prev.filter((m) => m !== metricId) : [...prev, metricId]
    )
  }

  const toggleVisibleMetric = (metricId: string) => {
    setVisibleMetrics((prev) =>
      prev.includes(metricId) ? prev.filter((m) => m !== metricId) : [...prev, metricId]
    )
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-hidden shadow-xl">
        <div className="flex items-center justify-between p-6 border-b border-gray-200">
          <h2 className="text-xl font-bold text-gray-900">Configure Widget</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X className="h-6 w-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex flex-col h-full">
          <div className="flex-1 overflow-y-auto p-6">
            {/* Title */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Widget Title *
              </label>
              <input
                type="text"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            {/* Date Range */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                <Calendar className="inline h-4 w-4 mr-1" />
                Date Range (optional)
              </label>

              {/* Date Range Type Toggle */}
              <div className="mb-3 flex gap-2">
                <button
                  type="button"
                  onClick={() => setDateRangeType('absolute')}
                  className={`px-3 py-1 text-sm rounded ${
                    dateRangeType === 'absolute'
                      ? 'bg-blue-100 text-blue-700 font-medium'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  Absolute Dates
                </button>
                <button
                  type="button"
                  onClick={() => setDateRangeType('relative')}
                  className={`px-3 py-1 text-sm rounded ${
                    dateRangeType === 'relative'
                      ? 'bg-blue-100 text-blue-700 font-medium'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  Relative Dates
                </button>
              </div>

              {dateRangeType === 'absolute' ? (
                <div className="grid grid-cols-2 gap-4">
                  <input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  />
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              ) : (
                <select
                  value={relativeDatePreset}
                  onChange={(e) => setRelativeDatePreset(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                >
                  <option value="last_7_days">Last 7 days</option>
                  <option value="last_30_days">Last 30 days</option>
                  <option value="last_90_days">Last 90 days</option>
                  <option value="this_month">This month</option>
                  <option value="last_month">Last month</option>
                  <option value="this_year">This year</option>
                </select>
              )}
            </div>

            {/* Dimensions */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Dimensions (select 1-2)
              </label>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-48 overflow-y-auto border border-gray-200 rounded-lg p-3">
                {dimensions?.map((dim) => (
                  <label
                    key={dim.id}
                    className="flex items-center space-x-2 p-2 hover:bg-gray-50 rounded cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={selectedDimensions.includes(dim.id)}
                      onChange={() => toggleDimension(dim.id)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm">{dim.display_name}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* Metrics */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Metrics (select at least 1)
              </label>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-64 overflow-y-auto border border-gray-200 rounded-lg p-3">
                {allMetrics.map((metric) => (
                  <label
                    key={metric.id}
                    className="flex items-center space-x-2 p-2 hover:bg-gray-50 rounded cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={selectedMetrics.includes(metric.id)}
                      onChange={() => toggleMetric(metric.id)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm">{metric.name}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* Table Dimensions */}
            {widget.type === 'table' && (
              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Table Dimensions (optional - for multi-table view)
                </label>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-48 overflow-y-auto border border-gray-200 rounded-lg p-3">
                  {dimensions?.map((dim) => (
                    <label
                      key={dim.id}
                      className="flex items-center space-x-2 p-2 hover:bg-gray-50 rounded cursor-pointer"
                    >
                      <input
                        type="checkbox"
                        checked={selectedTableDimensions.includes(dim.id)}
                        onChange={() => toggleTableDimension(dim.id)}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-sm">{dim.display_name}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}

            {/* Advanced Settings */}
            <div className="mb-6 border-t pt-4">
              <button
                type="button"
                onClick={() => setShowAdvanced(!showAdvanced)}
                className="flex items-center gap-2 text-sm font-medium text-gray-700 hover:text-gray-900"
              >
                <span>{showAdvanced ? '▼' : '▶'}</span>
                Advanced Settings
              </button>

              {showAdvanced && (
                <div className="mt-4 space-y-4 bg-gray-50 p-4 rounded-lg">
                  {/* Display Mode */}
                  {widget.type === 'table' && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Display Mode
                      </label>
                      <select
                        value={displayMode || 'pivot-table'}
                        onChange={(e) => setDisplayMode(e.target.value as any)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="pivot-table">Pivot Table</option>
                        <option value="multi-table">Multi-Table</option>
                      </select>
                    </div>
                  )}

                  {/* Visible Metrics (for multi-table mode) */}
                  {displayMode === 'multi-table' && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Visible Metrics in Multi-Table View
                      </label>
                      <div className="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto border border-gray-200 rounded-lg p-3">
                        {allMetrics.map((metric) => (
                          <label
                            key={metric.id}
                            className="flex items-center space-x-2 p-2 hover:bg-gray-50 rounded cursor-pointer"
                          >
                            <input
                              type="checkbox"
                              checked={visibleMetrics.includes(metric.id)}
                              onChange={() => toggleVisibleMetric(metric.id)}
                              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                            />
                            <span className="text-sm">{metric.name}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Merge Threshold */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Merge Threshold (0 = disabled)
                    </label>
                    <input
                      type="number"
                      min="0"
                      max="100"
                      value={mergeThreshold}
                      onChange={(e) => setMergeThreshold(parseInt(e.target.value) || 0)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Group rows with fewer items than this threshold
                    </p>
                  </div>

                  {/* Dimension Sort Order */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Dimension Sort Order
                    </label>
                    <div className="flex gap-3">
                      <button
                        type="button"
                        onClick={() => setDimensionSortOrder('desc')}
                        className={`flex-1 px-3 py-2 border-2 rounded-lg ${
                          dimensionSortOrder === 'desc'
                            ? 'border-blue-500 bg-blue-50'
                            : 'border-gray-300 hover:border-gray-400'
                        }`}
                      >
                        Descending
                      </button>
                      <button
                        type="button"
                        onClick={() => setDimensionSortOrder('asc')}
                        className={`flex-1 px-3 py-2 border-2 rounded-lg ${
                          dimensionSortOrder === 'asc'
                            ? 'border-blue-500 bg-blue-50'
                            : 'border-gray-300 hover:border-gray-400'
                        }`}
                      >
                        Ascending
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Chart Type (only for chart widgets) */}
            {widget.type === 'chart' && (
              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Chart Type
                </label>
                <div className="grid grid-cols-2 gap-3">
                  <button
                    type="button"
                    onClick={() => setChartType('bar')}
                    className={`p-3 border-2 rounded-lg ${
                      chartType === 'bar'
                        ? 'border-blue-500 bg-blue-50'
                        : 'border-gray-300 hover:border-gray-400'
                    }`}
                  >
                    Bar Chart
                  </button>
                  <button
                    type="button"
                    onClick={() => setChartType('line')}
                    className={`p-3 border-2 rounded-lg ${
                      chartType === 'line'
                        ? 'border-blue-500 bg-blue-50'
                        : 'border-gray-300 hover:border-gray-400'
                    }`}
                  >
                    Line Chart
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 p-6 border-t border-gray-200 bg-gray-50">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={!title.trim() || selectedMetrics.length === 0 || updateMutation.isPending}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              {updateMutation.isPending ? 'Saving...' : 'Save Changes'}
            </button>
          </div>

          {updateMutation.isError && (
            <div className="mx-6 mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm">
              Failed to update widget. Please try again.
            </div>
          )}
        </form>
      </div>
    </div>
  )
}
