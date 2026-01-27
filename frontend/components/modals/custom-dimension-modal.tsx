'use client'

import { useState, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, Plus, Trash2, Calendar, TrendingUp } from 'lucide-react'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'
import { DateRangeSelector } from '@/components/ui/date-range-selector'
import type {
  CustomDimension,
  CustomDimensionValue,
  CustomDimensionCreate,
  CustomDimensionUpdate,
  MetricDimensionValue,
  MetricCondition,
  MetricConditionOperator,
  DateRangeType,
  RelativeDatePreset
} from '@/lib/types'

interface CustomDimensionModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: CustomDimensionCreate | { id: string; data: CustomDimensionUpdate }) => void
  editingDimension?: CustomDimension | null
  mode: 'create' | 'edit'
  tableId?: string
}

const OPERATORS: { value: MetricConditionOperator; label: string; requiresValue: boolean; requiresMaxValue: boolean }[] = [
  { value: '>', label: 'Greater than (>)', requiresValue: true, requiresMaxValue: false },
  { value: '<', label: 'Less than (<)', requiresValue: true, requiresMaxValue: false },
  { value: '>=', label: 'Greater or equal (≥)', requiresValue: true, requiresMaxValue: false },
  { value: '<=', label: 'Less or equal (≤)', requiresValue: true, requiresMaxValue: false },
  { value: '=', label: 'Equals (=)', requiresValue: true, requiresMaxValue: false },
  { value: 'between', label: 'Between', requiresValue: true, requiresMaxValue: true },
  { value: 'is_null', label: 'Is Null', requiresValue: false, requiresMaxValue: false },
  { value: 'is_not_null', label: 'Is Not Null', requiresValue: false, requiresMaxValue: false }
]

export default function CustomDimensionModal({
  isOpen,
  onClose,
  onSave,
  editingDimension,
  mode,
  tableId,
}: CustomDimensionModalProps) {
  // Load dynamic metrics from schema
  const { metrics } = usePivotMetrics(tableId)

  // Transform metrics to dropdown format
  const AVAILABLE_METRICS = metrics.map(m => ({
    value: m.id,
    label: m.label
  }))

  const [dimensionType, setDimensionType] = useState<'date_range' | 'metric_condition'>('date_range')
  const [dimensionName, setDimensionName] = useState('')

  // Date range state
  const [values, setValues] = useState<CustomDimensionValue[]>([
    { label: '', start_date: '', end_date: '', date_range_type: 'absolute', relative_date_preset: null }
  ])

  // Metric condition state
  const [metric, setMetric] = useState('')
  const [metricValues, setMetricValues] = useState<MetricDimensionValue[]>([
    { label: '', conditions: [{ operator: '>', value: null, value_max: null }] }
  ])

  const [errors, setErrors] = useState<{ [key: string]: string }>({})

  // Populate form when editing
  useEffect(() => {
    if (mode === 'edit' && editingDimension) {
      setDimensionName(editingDimension.name)
      setDimensionType(editingDimension.type)

      if (editingDimension.type === 'date_range' && editingDimension.values) {
        setValues(editingDimension.values)
      } else if (editingDimension.type === 'metric_condition') {
        setMetric(editingDimension.metric || '')
        setMetricValues(editingDimension.metric_values || [{ label: '', conditions: [{ operator: '>', value: null, value_max: null }] }])
      }
    } else if (mode === 'create') {
      // Reset form for create mode
      setDimensionType('date_range')
      setDimensionName('')
      setValues([
        { label: '', start_date: '', end_date: '', date_range_type: 'absolute', relative_date_preset: null }
      ])
      setMetric('')
      setMetricValues([{ label: '', conditions: [{ operator: '>', value: null, value_max: null }] }])
    }
    setErrors({})
  }, [mode, editingDimension, isOpen])

  // Date range methods
  const addValue = () => {
    setValues([...values, { label: '', start_date: '', end_date: '', date_range_type: 'absolute', relative_date_preset: null }])
  }

  const removeValue = (index: number) => {
    if (values.length > 1) {
      setValues(values.filter((_, i) => i !== index))
    }
  }

  const updateValue = (index: number, field: keyof CustomDimensionValue, value: string) => {
    const newValues = [...values]
    newValues[index] = { ...newValues[index], [field]: value }
    setValues(newValues)
  }

  const updateValueDateRange = (
    index: number,
    type: DateRangeType,
    preset: RelativeDatePreset | null,
    startDate: string | null,
    endDate: string | null
  ) => {
    const newValues = [...values]
    newValues[index] = {
      ...newValues[index],
      date_range_type: type,
      relative_date_preset: preset,
      start_date: startDate || '',
      end_date: endDate || ''
    }
    setValues(newValues)
  }

  // Metric condition methods
  const addMetricValue = () => {
    setMetricValues([...metricValues, { label: '', conditions: [{ operator: '>', value: null, value_max: null }] }])
  }

  const removeMetricValue = (index: number) => {
    if (metricValues.length > 1) {
      setMetricValues(metricValues.filter((_, i) => i !== index))
    }
  }

  const updateMetricValueLabel = (index: number, label: string) => {
    const newValues = [...metricValues]
    newValues[index] = { ...newValues[index], label }
    setMetricValues(newValues)
  }

  const addCondition = (valueIndex: number) => {
    const newValues = [...metricValues]
    newValues[valueIndex].conditions.push({ operator: '>', value: null, value_max: null })
    setMetricValues(newValues)
  }

  const removeCondition = (valueIndex: number, conditionIndex: number) => {
    const newValues = [...metricValues]
    if (newValues[valueIndex].conditions.length > 1) {
      newValues[valueIndex].conditions.splice(conditionIndex, 1)
      setMetricValues(newValues)
    }
  }

  const updateCondition = (valueIndex: number, conditionIndex: number, field: keyof MetricCondition, value: any) => {
    const newValues = [...metricValues]
    newValues[valueIndex].conditions[conditionIndex] = {
      ...newValues[valueIndex].conditions[conditionIndex],
      [field]: value
    }
    setMetricValues(newValues)
  }

  const validate = (): boolean => {
    const newErrors: { [key: string]: string } = {}

    // Validate dimension name
    if (!dimensionName.trim()) {
      newErrors.dimensionName = 'Dimension name is required'
    } else if (dimensionName.length > 100) {
      newErrors.dimensionName = 'Dimension name must be 100 characters or less'
    }

    if (dimensionType === 'date_range') {
      // Validate date range values
      values.forEach((value, index) => {
        if (!value.label.trim()) {
          newErrors[`value_${index}_label`] = 'Label is required'
        }

        // For relative dates, check if preset is selected
        if (value.date_range_type === 'relative') {
          if (!value.relative_date_preset) {
            newErrors[`value_${index}_start`] = 'Please select a relative date preset'
          }
        } else {
          // For absolute dates, check if dates are filled
          if (!value.start_date) {
            newErrors[`value_${index}_start`] = 'Start date is required'
          }
          if (!value.end_date) {
            newErrors[`value_${index}_end`] = 'End date is required'
          }
          if (value.start_date && value.end_date && value.start_date > value.end_date) {
            newErrors[`value_${index}_date`] = 'End date must be after start date'
          }
        }
      })

      // Check for duplicate labels
      const labels = values.map(v => v.label.trim().toLowerCase()).filter(l => l)
      const duplicateLabels = labels.filter((label, index) => labels.indexOf(label) !== index)
      if (duplicateLabels.length > 0) {
        newErrors.duplicateLabels = 'Duplicate labels are not allowed'
      }
    } else if (dimensionType === 'metric_condition') {
      // Validate metric
      if (!metric) {
        newErrors.metric = 'Metric selection is required'
      }

      // Validate metric values
      metricValues.forEach((value, valueIndex) => {
        if (!value.label.trim()) {
          newErrors[`metricValue_${valueIndex}_label`] = 'Label is required'
        }

        value.conditions.forEach((condition, condIndex) => {
          const operatorInfo = OPERATORS.find(op => op.value === condition.operator)

          if (operatorInfo?.requiresValue && (condition.value === null || condition.value === undefined)) {
            newErrors[`metricValue_${valueIndex}_cond_${condIndex}_value`] = 'Value is required'
          }

          if (operatorInfo?.requiresMaxValue && (condition.value_max === null || condition.value_max === undefined)) {
            newErrors[`metricValue_${valueIndex}_cond_${condIndex}_valueMax`] = 'Max value is required'
          }

          if (operatorInfo?.requiresMaxValue && condition.value !== null && condition.value !== undefined && condition.value_max !== null && condition.value_max !== undefined) {
            if (condition.value_max <= condition.value) {
              newErrors[`metricValue_${valueIndex}_cond_${condIndex}_range`] = 'Max must be greater than min'
            }
          }
        })
      })

      // Check for duplicate labels
      const metricLabels = metricValues.map(v => v.label.trim().toLowerCase()).filter(l => l)
      const duplicateMetricLabels = metricLabels.filter((label, index) => metricLabels.indexOf(label) !== index)
      if (duplicateMetricLabels.length > 0) {
        newErrors.duplicateMetricLabels = 'Duplicate labels are not allowed'
      }
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = () => {
    if (!validate()) {
      return
    }

    if (mode === 'create') {
      const createData: CustomDimensionCreate = {
        name: dimensionName.trim(),
        type: dimensionType,
        table_id: tableId,
        ...(dimensionType === 'date_range' ? {
          values: values.map(v => ({
            label: v.label.trim(),
            start_date: v.start_date,
            end_date: v.end_date,
            date_range_type: v.date_range_type,
            relative_date_preset: v.relative_date_preset
          }))
        } : {
          metric,
          metric_values: metricValues.map(v => ({
            label: v.label.trim(),
            conditions: v.conditions
          }))
        })
      }
      onSave(createData)
    } else if (mode === 'edit' && editingDimension) {
      const updateData: CustomDimensionUpdate = {
        name: dimensionName.trim(),
        ...(dimensionType === 'date_range' ? {
          values: values.map(v => ({
            label: v.label.trim(),
            start_date: v.start_date,
            end_date: v.end_date,
            date_range_type: v.date_range_type,
            relative_date_preset: v.relative_date_preset
          }))
        } : {
          metric,
          metric_values: metricValues.map(v => ({
            label: v.label.trim(),
            conditions: v.conditions
          }))
        })
      }
      onSave({ id: editingDimension.id, data: updateData })
    }
  }

  const handleClose = () => {
    setErrors({})
    onClose()
  }

  if (!isOpen) return null

  const modalContent = (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b">
          <h2 className="text-2xl font-semibold">
            {mode === 'create' ? 'Create Custom Dimension' : 'Edit Custom Dimension'}
          </h2>
          <button
            onClick={handleClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X size={24} />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Dimension Name */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Dimension Name *
            </label>
            <input
              type="text"
              value={dimensionName}
              onChange={(e) => setDimensionName(e.target.value)}
              placeholder="e.g., Seasonal Periods, Performance Tiers"
              className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.dimensionName ? 'border-red-500' : 'border-gray-300'
              }`}
            />
            {errors.dimensionName && (
              <p className="text-red-500 text-sm mt-1">{errors.dimensionName}</p>
            )}
          </div>

          {/* Dimension Type Selector */}
          {mode === 'create' && (
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-3">
                Dimension Type *
              </label>
              <div className="grid grid-cols-2 gap-4">
                <button
                  type="button"
                  onClick={() => setDimensionType('date_range')}
                  className={`flex items-center gap-3 p-4 border-2 rounded-lg transition-all ${
                    dimensionType === 'date_range'
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <Calendar size={24} className={dimensionType === 'date_range' ? 'text-blue-600' : 'text-gray-400'} />
                  <div className="text-left">
                    <div className="font-medium text-gray-900">Date Range</div>
                    <div className="text-xs text-gray-500">Group by time periods</div>
                  </div>
                </button>
                <button
                  type="button"
                  onClick={() => setDimensionType('metric_condition')}
                  className={`flex items-center gap-3 p-4 border-2 rounded-lg transition-all ${
                    dimensionType === 'metric_condition'
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <TrendingUp size={24} className={dimensionType === 'metric_condition' ? 'text-blue-600' : 'text-gray-400'} />
                  <div className="text-left">
                    <div className="font-medium text-gray-900">Metric Condition</div>
                    <div className="text-xs text-gray-500">Group by metric thresholds</div>
                  </div>
                </button>
              </div>
            </div>
          )}

          {/* Date Range Section */}
          {dimensionType === 'date_range' && (
            <div className="mb-4">
              <div className="flex items-center justify-between mb-3">
                <label className="block text-sm font-medium text-gray-700">
                  Date Range Values * (minimum 1)
                </label>
                <button
                  onClick={addValue}
                  className="flex items-center gap-2 px-3 py-1.5 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-colors text-sm"
                >
                  <Plus size={16} />
                  Add Value
                </button>
              </div>

              {errors.duplicateLabels && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-3 mb-4">
                  <p className="text-red-600 text-sm">{errors.duplicateLabels}</p>
                </div>
              )}

              <div className="space-y-4">
                {values.map((value, index) => (
                  <div key={index} className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                    <div className="flex items-start justify-between mb-3">
                      <h4 className="text-sm font-medium text-gray-700">Value {index + 1}</h4>
                      {values.length > 1 && (
                        <button
                          onClick={() => removeValue(index)}
                          className="text-red-500 hover:text-red-700 transition-colors"
                        >
                          <Trash2 size={16} />
                        </button>
                      )}
                    </div>

                    <div className="mb-3">
                      <label className="block text-xs font-medium text-gray-600 mb-1">
                        Label *
                      </label>
                      <input
                        type="text"
                        value={value.label}
                        onChange={(e) => updateValue(index, 'label', e.target.value)}
                        placeholder="e.g., Holiday Season, Q1 2024"
                        className={`w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                          errors[`value_${index}_label`] ? 'border-red-500' : 'border-gray-300'
                        }`}
                      />
                      {errors[`value_${index}_label`] && (
                        <p className="text-red-500 text-xs mt-1">{errors[`value_${index}_label`]}</p>
                      )}
                    </div>

                    <div>
                      <DateRangeSelector
                        dateRangeType={(value.date_range_type as DateRangeType) || 'absolute'}
                        relativeDatePreset={(value.relative_date_preset as RelativeDatePreset) || null}
                        startDate={value.start_date}
                        endDate={value.end_date}
                        onDateRangeChange={(type, preset, startDate, endDate) =>
                          updateValueDateRange(index, type, preset, startDate, endDate)
                        }
                      />
                      {(errors[`value_${index}_start`] || errors[`value_${index}_end`] || errors[`value_${index}_date`]) && (
                        <p className="text-red-500 text-xs mt-1">
                          {errors[`value_${index}_start`] || errors[`value_${index}_end`] || errors[`value_${index}_date`]}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Metric Condition Section */}
          {dimensionType === 'metric_condition' && (
            <div className="space-y-6">
              {/* Metric Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Metric *
                </label>
                <select
                  value={metric}
                  onChange={(e) => setMetric(e.target.value)}
                  className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                    errors.metric ? 'border-red-500' : 'border-gray-300'
                  }`}
                >
                  <option value="">Select a metric...</option>
                  {AVAILABLE_METRICS.map(m => (
                    <option key={m.value} value={m.value}>{m.label}</option>
                  ))}
                </select>
                {errors.metric && (
                  <p className="text-red-500 text-sm mt-1">{errors.metric}</p>
                )}
              </div>

              {/* Metric Values */}
              <div>
                <div className="flex items-center justify-between mb-3">
                  <label className="block text-sm font-medium text-gray-700">
                    Dimension Values * (minimum 1)
                  </label>
                  <button
                    onClick={addMetricValue}
                    className="flex items-center gap-2 px-3 py-1.5 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-colors text-sm"
                  >
                    <Plus size={16} />
                    Add Value
                  </button>
                </div>

                {errors.duplicateMetricLabels && (
                  <div className="bg-red-50 border border-red-200 rounded-lg p-3 mb-4">
                    <p className="text-red-600 text-sm">{errors.duplicateMetricLabels}</p>
                  </div>
                )}

                <div className="space-y-4">
                  {metricValues.map((value, valueIndex) => (
                    <div key={valueIndex} className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                      <div className="flex items-start justify-between mb-3">
                        <h4 className="text-sm font-medium text-gray-700">Value {valueIndex + 1}</h4>
                        {metricValues.length > 1 && (
                          <button
                            onClick={() => removeMetricValue(valueIndex)}
                            className="text-red-500 hover:text-red-700 transition-colors"
                          >
                            <Trash2 size={16} />
                          </button>
                        )}
                      </div>

                      {/* Label */}
                      <div className="mb-3">
                        <label className="block text-xs font-medium text-gray-600 mb-1">
                          Label *
                        </label>
                        <input
                          type="text"
                          value={value.label}
                          onChange={(e) => updateMetricValueLabel(valueIndex, e.target.value)}
                          placeholder="e.g., High Performance, Low CVR"
                          className={`w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                            errors[`metricValue_${valueIndex}_label`] ? 'border-red-500' : 'border-gray-300'
                          }`}
                        />
                        {errors[`metricValue_${valueIndex}_label`] && (
                          <p className="text-red-500 text-xs mt-1">{errors[`metricValue_${valueIndex}_label`]}</p>
                        )}
                      </div>

                      {/* Conditions */}
                      <div>
                        <div className="flex items-center justify-between mb-2">
                          <label className="block text-xs font-medium text-gray-600">
                            Conditions (all must match) *
                          </label>
                          <button
                            onClick={() => addCondition(valueIndex)}
                            className="flex items-center gap-1 px-2 py-1 bg-green-50 text-green-600 rounded text-xs hover:bg-green-100 transition-colors"
                          >
                            <Plus size={12} />
                            Add Condition
                          </button>
                        </div>

                        <div className="space-y-2">
                          {value.conditions.map((condition, condIndex) => {
                            const operatorInfo = OPERATORS.find(op => op.value === condition.operator)
                            return (
                              <div key={condIndex} className="bg-white rounded border border-gray-200 p-3">
                                <div className="flex items-start gap-2">
                                  <div className="flex-1 space-y-2">
                                    <select
                                      value={condition.operator}
                                      onChange={(e) => updateCondition(valueIndex, condIndex, 'operator', e.target.value)}
                                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                    >
                                      {OPERATORS.map(op => (
                                        <option key={op.value} value={op.value}>{op.label}</option>
                                      ))}
                                    </select>

                                    {operatorInfo?.requiresValue && (
                                      <div className="flex gap-2">
                                        <div className="flex-1">
                                          <input
                                            type="number"
                                            step="any"
                                            value={condition.value ?? ''}
                                            onChange={(e) => updateCondition(valueIndex, condIndex, 'value', e.target.value ? parseFloat(e.target.value) : null)}
                                            placeholder={operatorInfo.requiresMaxValue ? "Min value" : "Value"}
                                            className={`w-full px-2 py-1.5 border rounded text-xs focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                                              errors[`metricValue_${valueIndex}_cond_${condIndex}_value`] || errors[`metricValue_${valueIndex}_cond_${condIndex}_range`]
                                                ? 'border-red-500'
                                                : 'border-gray-300'
                                            }`}
                                          />
                                        </div>
                                        {operatorInfo.requiresMaxValue && (
                                          <div className="flex-1">
                                            <input
                                              type="number"
                                              step="any"
                                              value={condition.value_max ?? ''}
                                              onChange={(e) => updateCondition(valueIndex, condIndex, 'value_max', e.target.value ? parseFloat(e.target.value) : null)}
                                              placeholder="Max value"
                                              className={`w-full px-2 py-1.5 border rounded text-xs focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                                                errors[`metricValue_${valueIndex}_cond_${condIndex}_valueMax`] || errors[`metricValue_${valueIndex}_cond_${condIndex}_range`]
                                                  ? 'border-red-500'
                                                  : 'border-gray-300'
                                              }`}
                                            />
                                          </div>
                                        )}
                                      </div>
                                    )}

                                    {errors[`metricValue_${valueIndex}_cond_${condIndex}_value`] && (
                                      <p className="text-red-500 text-xs">{errors[`metricValue_${valueIndex}_cond_${condIndex}_value`]}</p>
                                    )}
                                    {errors[`metricValue_${valueIndex}_cond_${condIndex}_valueMax`] && (
                                      <p className="text-red-500 text-xs">{errors[`metricValue_${valueIndex}_cond_${condIndex}_valueMax`]}</p>
                                    )}
                                    {errors[`metricValue_${valueIndex}_cond_${condIndex}_range`] && (
                                      <p className="text-red-500 text-xs">{errors[`metricValue_${valueIndex}_cond_${condIndex}_range`]}</p>
                                    )}
                                  </div>

                                  {value.conditions.length > 1 && (
                                    <button
                                      onClick={() => removeCondition(valueIndex, condIndex)}
                                      className="text-red-500 hover:text-red-700 transition-colors mt-1"
                                    >
                                      <Trash2 size={14} />
                                    </button>
                                  )}
                                </div>
                              </div>
                            )
                          })}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
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
            {mode === 'create' ? 'Create Dimension' : 'Save Changes'}
          </button>
        </div>
      </div>
    </div>
  )

  // Use portal to render modal at document body level, bypassing any overflow-hidden containers
  return typeof window !== 'undefined' ? createPortal(modalContent, document.body) : null
}
