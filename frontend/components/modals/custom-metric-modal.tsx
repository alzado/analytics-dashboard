'use client'

import { useState, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, Plus, Trash2 } from 'lucide-react'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'
import { useSchema } from '@/hooks/use-schema'
import type {
  CustomMetric,
  CustomMetricCreate,
  CustomMetricUpdate,
} from '@/lib/types'

interface CustomMetricModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: CustomMetricCreate | { id: string; data: CustomMetricUpdate }) => void
  editingMetric?: CustomMetric | null
  mode: 'create' | 'edit'
  tableId?: string
}

const AGGREGATION_TYPES = [
  { value: 'sum', label: 'Sum', description: 'Add all values together' },
  { value: 'avg', label: 'Average', description: 'Calculate the mean' },
  { value: 'avg_per_day', label: 'Avg Per Day', description: 'Total divided by days in date range' },
  { value: 'max', label: 'Maximum', description: 'Find the highest value' },
  { value: 'min', label: 'Minimum', description: 'Find the lowest value' },
  { value: 'count', label: 'Count', description: 'Count the number of rows' },
]

export default function CustomMetricModal({
  isOpen,
  onClose,
  onSave,
  editingMetric,
  mode,
  tableId,
}: CustomMetricModalProps) {
  // Load available metrics and dimensions from schema
  const { metrics } = usePivotMetrics(tableId)
  const { dimensions } = useSchema(tableId)

  // Form state
  const [name, setName] = useState('')
  const [metricId, setMetricId] = useState('')
  const [sourceMetric, setSourceMetric] = useState('')
  const [aggregationType, setAggregationType] = useState<'sum' | 'avg' | 'avg_per_day' | 'max' | 'min' | 'count'>('sum')
  const [excludeDimensions, setExcludeDimensions] = useState<string[]>([])
  const [description, setDescription] = useState('')
  const [errors, setErrors] = useState<{ [key: string]: string }>({})

  // Available metrics dropdown
  const availableMetrics = metrics.map(m => ({
    value: m.id,
    label: m.label
  }))

  // Available dimensions for exclusion
  const availableDimensions = dimensions?.map(d => ({
    value: d.id,
    label: d.display_name
  })) || []

  // Populate form when editing
  useEffect(() => {
    if (mode === 'edit' && editingMetric) {
      setName(editingMetric.name)
      setMetricId(editingMetric.metric_id)
      setSourceMetric(editingMetric.source_metric)
      setAggregationType(editingMetric.aggregation_type)
      setExcludeDimensions(editingMetric.exclude_dimensions || [])
      setDescription(editingMetric.description || '')
    } else if (mode === 'create') {
      // Reset form
      setName('')
      setMetricId('')
      setSourceMetric('')
      setAggregationType('sum')
      setExcludeDimensions([])
      setDescription('')
    }
    setErrors({})
  }, [mode, editingMetric, isOpen])

  // Auto-generate metric ID from name
  const handleNameChange = (value: string) => {
    setName(value)
    if (mode === 'create') {
      const generatedId = value
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, '')
        .replace(/\s+/g, '_')
        .substring(0, 50)
      setMetricId(generatedId)
    }
  }

  const toggleDimension = (dimId: string) => {
    setExcludeDimensions(prev =>
      prev.includes(dimId)
        ? prev.filter(d => d !== dimId)
        : [...prev, dimId]
    )
  }

  const validate = (): boolean => {
    const newErrors: { [key: string]: string } = {}

    if (!name.trim()) {
      newErrors.name = 'Name is required'
    }

    if (!metricId.trim()) {
      newErrors.metricId = 'Metric ID is required'
    } else if (!/^[a-z][a-z0-9_]*$/.test(metricId)) {
      newErrors.metricId = 'ID must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (!sourceMetric) {
      newErrors.sourceMetric = 'Source metric is required'
    }

    // avg_per_day doesn't need excluded dimensions - it just divides by date range
    if (aggregationType !== 'avg_per_day' && excludeDimensions.length === 0) {
      newErrors.excludeDimensions = 'Select at least one dimension to exclude (e.g., "date" to aggregate across all days)'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = () => {
    if (!validate()) {
      return
    }

    if (mode === 'create') {
      const createData: CustomMetricCreate = {
        name: name.trim(),
        metric_id: metricId.trim(),
        source_metric: sourceMetric,
        aggregation_type: aggregationType,
        exclude_dimensions: excludeDimensions,
        table_id: tableId,
        description: description.trim() || undefined,
      }
      onSave(createData)
    } else if (mode === 'edit' && editingMetric) {
      const updateData: CustomMetricUpdate = {
        name: name.trim(),
        source_metric: sourceMetric,
        aggregation_type: aggregationType,
        exclude_dimensions: excludeDimensions,
        description: description.trim() || undefined,
      }
      onSave({ id: editingMetric.id, data: updateData })
    }
  }

  const handleClose = () => {
    setErrors({})
    onClose()
  }

  if (!isOpen) return null

  const modalContent = (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b">
          <h2 className="text-2xl font-semibold">
            {mode === 'create' ? 'Create Custom Metric' : 'Edit Custom Metric'}
          </h2>
          <button
            onClick={handleClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X size={24} />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Description */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <p className="text-sm text-blue-800">
              Custom metrics allow you to re-aggregate data across specific dimensions.
              For example, you can sum queries across all dates to get a total count
              that ignores the date breakdown.
            </p>
          </div>

          {/* Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Metric Name *
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => handleNameChange(e.target.value)}
              placeholder="e.g., Total Queries (All Time)"
              className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.name ? 'border-red-500' : 'border-gray-300'
              }`}
            />
            {errors.name && (
              <p className="text-red-500 text-sm mt-1">{errors.name}</p>
            )}
          </div>

          {/* Metric ID */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Metric ID *
            </label>
            <input
              type="text"
              value={metricId}
              onChange={(e) => setMetricId(e.target.value)}
              placeholder="e.g., total_queries_all_time"
              disabled={mode === 'edit'}
              className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.metricId ? 'border-red-500' : 'border-gray-300'
              } ${mode === 'edit' ? 'bg-gray-100 cursor-not-allowed' : ''}`}
            />
            {errors.metricId && (
              <p className="text-red-500 text-sm mt-1">{errors.metricId}</p>
            )}
            <p className="text-xs text-gray-500 mt-1">
              Unique identifier used internally. Cannot be changed after creation.
            </p>
          </div>

          {/* Source Metric */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Source Metric *
            </label>
            <select
              value={sourceMetric}
              onChange={(e) => setSourceMetric(e.target.value)}
              className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.sourceMetric ? 'border-red-500' : 'border-gray-300'
              }`}
            >
              <option value="">Select a metric...</option>
              {availableMetrics.map(m => (
                <option key={m.value} value={m.value}>{m.label}</option>
              ))}
            </select>
            {errors.sourceMetric && (
              <p className="text-red-500 text-sm mt-1">{errors.sourceMetric}</p>
            )}
            <p className="text-xs text-gray-500 mt-1">
              The metric to re-aggregate
            </p>
          </div>

          {/* Aggregation Type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Aggregation Type *
            </label>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
              {AGGREGATION_TYPES.map(agg => (
                <button
                  key={agg.value}
                  type="button"
                  onClick={() => setAggregationType(agg.value as any)}
                  className={`px-3 py-2 border rounded-lg text-sm transition-colors ${
                    aggregationType === agg.value
                      ? 'border-blue-500 bg-blue-50 text-blue-700'
                      : 'border-gray-300 hover:border-gray-400'
                  }`}
                >
                  <div className="font-medium">{agg.label}</div>
                  <div className="text-xs text-gray-500">{agg.description}</div>
                </button>
              ))}
            </div>
          </div>

          {/* Exclude Dimensions */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Dimensions to Exclude *
            </label>
            <p className="text-xs text-gray-500 mb-3">
              Select dimensions to aggregate across. For example, exclude "date" to get totals across all days.
            </p>
            <div className="flex flex-wrap gap-2">
              {availableDimensions.map(dim => (
                <button
                  key={dim.value}
                  type="button"
                  onClick={() => toggleDimension(dim.value)}
                  className={`px-3 py-1.5 text-sm rounded-full border transition-colors ${
                    excludeDimensions.includes(dim.value)
                      ? 'bg-blue-100 border-blue-300 text-blue-800'
                      : 'bg-gray-100 border-gray-300 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  {dim.label}
                </button>
              ))}
            </div>
            {errors.excludeDimensions && (
              <p className="text-red-500 text-sm mt-2">{errors.excludeDimensions}</p>
            )}
            {excludeDimensions.length > 0 && (
              <p className="text-xs text-green-600 mt-2">
                Will aggregate "{sourceMetric || 'metric'}" across: {excludeDimensions.join(', ')}
              </p>
            )}
          </div>

          {/* Description */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Description (Optional)
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Describe what this custom metric calculates..."
              rows={2}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-6 border-t bg-gray-50">
          <button
            onClick={handleClose}
            className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
          >
            {mode === 'create' ? 'Create Metric' : 'Save Changes'}
          </button>
        </div>
      </div>
    </div>
  )

  return typeof window !== 'undefined' ? createPortal(modalContent, document.body) : null
}
