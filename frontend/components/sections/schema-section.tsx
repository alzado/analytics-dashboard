'use client'

import { useState, useEffect } from 'react'
import { Pencil, Trash2, Copy, FileText, Calendar } from 'lucide-react'
import { useSchema } from '@/hooks/use-schema'
import { usePivotMetrics } from '@/hooks/use-pivot-metrics'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchTables, copySchema, applySchemaTemplate, createDailyAverageMetric } from '@/lib/api'
import { FormulaBuilderModal } from '@/components/modals/formula-builder-modal'
import type {
  BaseMetric,
  CalculatedMetric,
  DimensionDef,
  MetricCreate,
  CalculatedMetricCreate,
  DimensionCreate,
  DimensionUpdate,
  MetricUpdate,
  CalculatedMetricUpdate,
} from '@/lib/types'

interface SchemaSectionProps {
  tableId?: string
}

export function SchemaSection({ tableId }: SchemaSectionProps) {
  const [schemaTab, setSchemaTab] = useState<'base' | 'calculated' | 'dimensions'>('base')
  const [isDetecting, setIsDetecting] = useState(false)
  const [detectionResult, setDetectionResult] = useState<string | null>(null)

  // Edit modal state
  const [editingDimension, setEditingDimension] = useState<DimensionDef | null>(null)
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [editingBaseMetric, setEditingBaseMetric] = useState<BaseMetric | null>(null)
  const [isBaseMetricModalOpen, setIsBaseMetricModalOpen] = useState(false)
  const [editingCalculatedMetric, setEditingCalculatedMetric] = useState<CalculatedMetric | null>(null)
  const [isCalculatedMetricModalOpen, setIsCalculatedMetricModalOpen] = useState(false)

  // Create modal state
  const [isCreateBaseMetricModalOpen, setIsCreateBaseMetricModalOpen] = useState(false)
  const [isCreateCalculatedMetricModalOpen, setIsCreateCalculatedMetricModalOpen] = useState(false)
  const [isCreateDimensionModalOpen, setIsCreateDimensionModalOpen] = useState(false)

  // Pivot config state
  const [primarySortMetric, setPrimarySortMetric] = useState<string>('')
  const [avgPerDayMetric, setAvgPerDayMetric] = useState<string>('')
  const [paginationThreshold, setPaginationThreshold] = useState<number>(100)
  const [configResult, setConfigResult] = useState<string | null>(null)

  // Schema copy/template state
  const [showCopyMenu, setShowCopyMenu] = useState(false)
  const [showTemplateMenu, setShowTemplateMenu] = useState(false)
  const queryClient = useQueryClient()

  // Fetch available tables for schema copy
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: fetchTables,
  })

  // Copy schema mutation
  const copySchemaMutation = useMutation({
    mutationFn: copySchema,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      setDetectionResult('Schema copied successfully to this table!')
      setShowCopyMenu(false)
    },
    onError: (error) => {
      setDetectionResult(`Error copying schema: ${error instanceof Error ? error.message : 'Unknown error'}`)
    },
  })

  // Apply template mutation
  const applyTemplateMutation = useMutation({
    mutationFn: applySchemaTemplate,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      setDetectionResult('Template applied successfully to this table!')
      setShowTemplateMenu(false)
    },
    onError: (error) => {
      setDetectionResult(`Error applying template: ${error instanceof Error ? error.message : 'Unknown error'}`)
    },
  })

  const handleCopySchema = (sourceTableId: string) => {
    if (!tableId) {
      setDetectionResult('Error: No table selected')
      return
    }
    if (confirm('This will overwrite your current schema for this table. Continue?')) {
      copySchemaMutation.mutate({ source_table_id: sourceTableId, target_table_id: tableId })
    }
  }

  const handleApplyTemplate = (templateName: 'ecommerce' | 'saas' | 'marketing') => {
    if (confirm(`Apply ${templateName} template? This will overwrite this table's schema.`)) {
      applyTemplateMutation.mutate({ template_name: templateName })
    }
  }

  const {
    baseMetrics,
    calculatedMetrics,
    dimensions,
    isLoadingBaseMetrics,
    isLoadingCalculatedMetrics,
    isLoadingDimensions,
    detectSchema,
    resetSchema,
    createBaseMetric,
    updateBaseMetric,
    deleteBaseMetric,
    createCalculatedMetric,
    updateCalculatedMetric,
    deleteCalculatedMetric,
    createDimension,
    updateDimension,
    deleteDimension,
    schema,
    updatePivotConfig,
    isUpdatingPivotConfig,
  } = useSchema(tableId)

  const { metrics } = usePivotMetrics(tableId)

  // Load current pivot config values when schema is loaded
  useEffect(() => {
    if (schema) {
      setPrimarySortMetric(schema.primary_sort_metric || '')
      setAvgPerDayMetric(schema.avg_per_day_metric || '')
      setPaginationThreshold(schema.pagination_threshold || 100)
    }
  }, [schema])

  const handleSavePivotConfig = async () => {
    setConfigResult(null)
    try {
      await updatePivotConfig({
        primary_sort_metric: primarySortMetric || undefined,
        avg_per_day_metric: avgPerDayMetric || undefined,
        pagination_threshold: paginationThreshold,
      })
      setConfigResult('Pivot table configuration saved successfully!')
    } catch (error) {
      setConfigResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleDetectSchema = async () => {
    if (!tableId) {
      setDetectionResult('Error: No table selected')
      return
    }
    setIsDetecting(true)
    setDetectionResult(null)
    try {
      const result = await detectSchema(tableId)
      const metricsCount = result?.detected_base_metrics?.length || 0
      const dimensionsCount = result?.detected_dimensions?.length || 0
      setDetectionResult(
        `Schema detected! Found ${metricsCount} metrics and ${dimensionsCount} dimensions.`
      )
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    } finally {
      setIsDetecting(false)
    }
  }

  const handleResetSchema = async () => {
    if (!confirm('Are you sure you want to reset the schema to default values?')) {
      return
    }
    try {
      await resetSchema()
      setDetectionResult('Schema reset to defaults successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleEditDimension = (dimension: DimensionDef) => {
    setEditingDimension(dimension)
    setIsEditModalOpen(true)
  }

  const handleDeleteDimension = async (id: string) => {
    if (!confirm('Are you sure you want to delete this dimension?')) {
      return
    }
    try {
      await deleteDimension(id)
      setDetectionResult('Dimension deleted successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleSaveDimension = async (data: DimensionUpdate) => {
    if (!editingDimension) return

    try {
      await updateDimension({ id: editingDimension.id, data })
      setIsEditModalOpen(false)
      setEditingDimension(null)
      setDetectionResult('Dimension updated successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleEditBaseMetric = (metric: BaseMetric) => {
    setEditingBaseMetric(metric)
    setIsBaseMetricModalOpen(true)
  }

  const handleDeleteBaseMetric = async (id: string) => {
    if (!confirm('Are you sure you want to delete this base metric?')) {
      return
    }
    try {
      await deleteBaseMetric(id)
      setDetectionResult('Base metric deleted successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleCreateDailyAverage = async (metricId: string) => {
    try {
      await createDailyAverageMetric(metricId, tableId || undefined)
      // Invalidate queries to refresh the metrics list
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['calculatedMetrics'] })
      setDetectionResult(`Daily average metric "${metricId}_per_day" created successfully!`)
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleSaveBaseMetric = async (data: MetricUpdate) => {
    if (!editingBaseMetric) return

    try {
      const result = await updateBaseMetric({ id: editingBaseMetric.id, data })
      setIsBaseMetricModalOpen(false)
      setEditingBaseMetric(null)

      // Show success message with cascade info
      if (result.cascade_updated_count > 0) {
        setDetectionResult(
          `Base metric updated successfully! Also updated ${result.cascade_updated_count} dependent calculated metric(s): ${result.cascade_updated_metrics.join(', ')}`
        )
      } else {
        setDetectionResult('Base metric updated successfully!')
      }
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleEditCalculatedMetric = (metric: CalculatedMetric) => {
    setEditingCalculatedMetric(metric)
    setIsCalculatedMetricModalOpen(true)
  }

  const handleDeleteCalculatedMetric = async (id: string) => {
    if (!confirm('Are you sure you want to delete this calculated metric?')) {
      return
    }
    try {
      await deleteCalculatedMetric(id)
      setDetectionResult('Calculated metric deleted successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleSaveCalculatedMetric = async (data: CalculatedMetricUpdate) => {
    if (!editingCalculatedMetric) return

    try {
      const result = await updateCalculatedMetric({ id: editingCalculatedMetric.id, data })
      setIsCalculatedMetricModalOpen(false)
      setEditingCalculatedMetric(null)

      // Show success message with cascade info
      if (result.cascade_updated_count > 0) {
        setDetectionResult(
          `Calculated metric updated successfully! Also updated ${result.cascade_updated_count} dependent calculated metric(s): ${result.cascade_updated_metrics.join(', ')}`
        )
      } else {
        setDetectionResult('Calculated metric updated successfully!')
      }
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleCreateBaseMetric = async (data: MetricCreate) => {
    try {
      await createBaseMetric(data)
      setIsCreateBaseMetricModalOpen(false)
      setDetectionResult('Base metric created successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleCreateCalculatedMetric = async (data: CalculatedMetricCreate) => {
    try {
      await createCalculatedMetric(data)
      setIsCreateCalculatedMetricModalOpen(false)
      setDetectionResult('Calculated metric created successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const handleCreateDimension = async (data: DimensionCreate) => {
    try {
      await createDimension(data)
      setIsCreateDimensionModalOpen(false)
      setDetectionResult('Dimension created successfully!')
    } catch (error) {
      setDetectionResult(`Error: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  const tabs = [
    { id: 'base' as const, label: 'Base Metrics', count: baseMetrics?.length || 0 },
    { id: 'calculated' as const, label: 'Calculated Metrics', count: calculatedMetrics?.length || 0 },
    { id: 'dimensions' as const, label: 'Dimensions', count: dimensions?.length || 0 },
  ]

  return (
    <div className="space-y-6">
      {/* Header with actions */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex justify-between items-start">
          <div>
            <h2 className="text-xl font-bold text-gray-900">Schema Management</h2>
            <p className="mt-1 text-sm text-gray-500">
              Configure metrics and dimensions for your data model
            </p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleDetectSchema}
              disabled={isDetecting}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isDetecting ? 'Detecting...' : 'Auto-Detect Schema'}
            </button>

            {/* Copy Schema Dropdown */}
            <div className="relative">
              <button
                onClick={() => setShowCopyMenu(!showCopyMenu)}
                className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-md hover:bg-purple-700"
              >
                <Copy size={16} />
                Copy Schema
              </button>
              {showCopyMenu && (
                <div className="absolute right-0 mt-2 w-64 bg-white rounded-md shadow-lg border z-10">
                  <div className="p-2">
                    <p className="text-xs text-gray-600 px-2 py-1">Copy from table:</p>
                    {tablesData?.tables.filter(t => !t.is_active).length === 0 ? (
                      <p className="text-sm text-gray-500 px-2 py-1">No other tables available</p>
                    ) : (
                      tablesData?.tables
                        .filter(t => !t.is_active)
                        .map(table => (
                          <button
                            key={table.table_id}
                            onClick={() => handleCopySchema(table.table_id)}
                            className="w-full text-left px-2 py-2 text-sm hover:bg-gray-100 rounded"
                          >
                            <div className="font-medium">{table.name}</div>
                            <div className="text-xs text-gray-500">{table.project_id}.{table.dataset}.{table.table}</div>
                          </button>
                        ))
                    )}
                  </div>
                </div>
              )}
            </div>

            {/* Template Dropdown */}
            <div className="relative">
              <button
                onClick={() => setShowTemplateMenu(!showTemplateMenu)}
                className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700"
              >
                <FileText size={16} />
                Apply Template
              </button>
              {showTemplateMenu && (
                <div className="absolute right-0 mt-2 w-56 bg-white rounded-md shadow-lg border z-10">
                  <div className="p-2">
                    <button
                      onClick={() => handleApplyTemplate('ecommerce')}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-gray-100 rounded"
                    >
                      <div className="font-medium">E-commerce</div>
                      <div className="text-xs text-gray-500">Search analytics metrics</div>
                    </button>
                    <button
                      onClick={() => handleApplyTemplate('saas')}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-gray-100 rounded"
                    >
                      <div className="font-medium">SaaS</div>
                      <div className="text-xs text-gray-500">Subscription & trials</div>
                    </button>
                    <button
                      onClick={() => handleApplyTemplate('marketing')}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-gray-100 rounded"
                    >
                      <div className="font-medium">Marketing</div>
                      <div className="text-xs text-gray-500">Ad campaigns & ROAS</div>
                    </button>
                  </div>
                </div>
              )}
            </div>

            <button
              onClick={handleResetSchema}
              className="px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700"
            >
              Reset to Defaults
            </button>
          </div>
        </div>

        {detectionResult && (
          <div className={`mt-4 p-4 rounded-md ${
            detectionResult.startsWith('Error')
              ? 'bg-red-50 text-red-700'
              : 'bg-green-50 text-green-700'
          }`}>
            {detectionResult}
          </div>
        )}
      </div>

      {/* Pivot Table Configuration */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="mb-4">
          <h3 className="text-lg font-semibold text-gray-900">Pivot Table Configuration</h3>
          <p className="mt-1 text-sm text-gray-500">
            Configure default settings for pivot table behavior
          </p>
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Primary Sort Metric
              </label>
              <select
                value={primarySortMetric}
                onChange={(e) => setPrimarySortMetric(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Select metric...</option>
                {metrics.map((metric) => (
                  <option key={metric.id} value={metric.id}>
                    {metric.label}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-xs text-gray-500">
                Default metric used for sorting pivot table rows
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Average Per Day Metric
              </label>
              <select
                value={avgPerDayMetric}
                onChange={(e) => setAvgPerDayMetric(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Select metric...</option>
                {metrics.map((metric) => (
                  <option key={metric.id} value={metric.id}>
                    {metric.label}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-xs text-gray-500">
                Metric used for average per day calculations
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Pagination Threshold
              </label>
              <input
                type="number"
                min="1"
                value={paginationThreshold}
                onChange={(e) => setPaginationThreshold(parseInt(e.target.value) || 100)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="mt-1 text-xs text-gray-500">
                Paginate when unique values exceed this number
              </p>
            </div>
          </div>

          <div className="flex items-center justify-between">
            <div>
              {configResult && (
                <div className={`text-sm ${
                  configResult.startsWith('Error')
                    ? 'text-red-700'
                    : 'text-green-700'
                }`}>
                  {configResult}
                </div>
              )}
            </div>
            <button
              onClick={handleSavePivotConfig}
              disabled={isUpdatingPivotConfig}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isUpdatingPivotConfig ? 'Saving...' : 'Save Configuration'}
            </button>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white rounded-lg shadow">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setSchemaTab(tab.id)}
                className={`
                  py-4 px-6 border-b-2 font-medium text-sm whitespace-nowrap
                  ${schemaTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }
                `}
              >
                {tab.label}
                <span className="ml-2 px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 text-xs">
                  {tab.count}
                </span>
              </button>
            ))}
          </nav>
        </div>

        <div className="p-6">
          {schemaTab === 'base' && (
            <BaseMetricsTab
              metrics={baseMetrics}
              isLoading={isLoadingBaseMetrics}
              onEdit={handleEditBaseMetric}
              onDelete={handleDeleteBaseMetric}
              onCreate={() => setIsCreateBaseMetricModalOpen(true)}
              calculatedMetrics={calculatedMetrics}
              onCreateDailyAverage={handleCreateDailyAverage}
            />
          )}
          {schemaTab === 'calculated' && (
            <CalculatedMetricsTab
              metrics={calculatedMetrics}
              isLoading={isLoadingCalculatedMetrics}
              onEdit={handleEditCalculatedMetric}
              onDelete={handleDeleteCalculatedMetric}
              onCreate={() => setIsCreateCalculatedMetricModalOpen(true)}
              onCreateDailyAverage={handleCreateDailyAverage}
            />
          )}
          {schemaTab === 'dimensions' && (
            <DimensionsTab
              dimensions={dimensions}
              isLoading={isLoadingDimensions}
              onEdit={handleEditDimension}
              onDelete={handleDeleteDimension}
              onCreate={() => setIsCreateDimensionModalOpen(true)}
            />
          )}
        </div>
      </div>

      {/* Edit Dimension Modal */}
      {isEditModalOpen && editingDimension && (
        <EditDimensionModal
          dimension={editingDimension}
          onSave={handleSaveDimension}
          onClose={() => {
            setIsEditModalOpen(false)
            setEditingDimension(null)
          }}
        />
      )}

      {/* Edit Base Metric Modal */}
      {isBaseMetricModalOpen && editingBaseMetric && (
        <EditBaseMetricModal
          metric={editingBaseMetric}
          onSave={handleSaveBaseMetric}
          onClose={() => {
            setIsBaseMetricModalOpen(false)
            setEditingBaseMetric(null)
          }}
        />
      )}

      {/* Edit Calculated Metric Modal */}
      {isCalculatedMetricModalOpen && editingCalculatedMetric && (
        <EditCalculatedMetricModal
          metric={editingCalculatedMetric}
          onSave={handleSaveCalculatedMetric}
          onClose={() => {
            setIsCalculatedMetricModalOpen(false)
            setEditingCalculatedMetric(null)
          }}
          baseMetrics={baseMetrics || []}
          calculatedMetrics={calculatedMetrics || []}
          dimensions={dimensions || []}
        />
      )}

      {/* Create Base Metric Modal */}
      {isCreateBaseMetricModalOpen && (
        <CreateBaseMetricModal
          onSave={handleCreateBaseMetric}
          onClose={() => setIsCreateBaseMetricModalOpen(false)}
        />
      )}

      {/* Create Calculated Metric Modal */}
      {isCreateCalculatedMetricModalOpen && (
        <CreateCalculatedMetricModal
          onSave={handleCreateCalculatedMetric}
          onClose={() => setIsCreateCalculatedMetricModalOpen(false)}
          baseMetrics={baseMetrics || []}
          calculatedMetrics={calculatedMetrics || []}
          dimensions={dimensions || []}
          editingMetricId={null}
        />
      )}

      {/* Create Dimension Modal */}
      {isCreateDimensionModalOpen && (
        <CreateDimensionModal
          onSave={handleCreateDimension}
          onClose={() => setIsCreateDimensionModalOpen(false)}
        />
      )}
    </div>
  )
}

interface BaseMetricsTabProps {
  metrics: BaseMetric[]
  isLoading: boolean
  onEdit: (metric: BaseMetric) => void
  onDelete: (id: string) => void
  onCreate: () => void
  calculatedMetrics?: CalculatedMetric[]
  onCreateDailyAverage: (metricId: string) => void
}

function BaseMetricsTab({ metrics, isLoading, onEdit, onDelete, onCreate, calculatedMetrics, onCreateDailyAverage }: BaseMetricsTabProps) {
  if (isLoading) {
    return <div className="text-center py-8 text-gray-500">Loading base metrics...</div>
  }

  if (!metrics || metrics.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No base metrics defined yet.</p>
        <p className="text-sm text-gray-400 mt-2">Use Auto-Detect to discover metrics from your BigQuery table or create them manually.</p>
        <button
          onClick={onCreate}
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Base Metric
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="flex justify-end mb-4">
        <button
          onClick={onCreate}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Base Metric
        </button>
      </div>
      <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead>
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Display Name</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Column</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Aggregation</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Format</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Visible</th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200">
          {metrics.map((metric) => (
            <tr key={metric.id} className="hover:bg-gray-50">
              <td className="px-4 py-3 text-sm font-mono text-gray-900">{metric.id}</td>
              <td className="px-4 py-3 text-sm text-gray-900">{metric.display_name}</td>
              <td className="px-4 py-3 text-sm font-mono text-gray-600">{metric.column_name}</td>
              <td className="px-4 py-3 text-sm">
                <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-xs">
                  {metric.aggregation}
                </span>
              </td>
              <td className="px-4 py-3 text-sm">
                <span className="px-2 py-1 bg-gray-100 text-gray-800 rounded text-xs">
                  {metric.format_type} ({metric.decimal_places} decimals)
                </span>
              </td>
              <td className="px-4 py-3 text-sm text-gray-600">{metric.category}</td>
              <td className="px-4 py-3 text-sm">
                {metric.is_visible_by_default ? (
                  <span className="text-green-600">Yes</span>
                ) : (
                  <span className="text-gray-400">No</span>
                )}
              </td>
              <td className="px-4 py-3 text-sm">
                <div className="flex gap-2">
                  {/* Create Daily Average button - only for volume metrics */}
                  {metric.category === 'volume' && !metric.is_system && (() => {
                    const dailyAvgId = `${metric.id}_per_day`
                    const dailyAvgExists = calculatedMetrics?.some(m => m.id === dailyAvgId)
                    return !dailyAvgExists ? (
                      <button
                        onClick={() => onCreateDailyAverage(metric.id)}
                        className="p-1 text-green-600 hover:bg-green-50 rounded transition-colors"
                        title="Create daily average (per-day) metric"
                      >
                        <Calendar className="h-4 w-4" />
                      </button>
                    ) : (
                      <span className="p-1 text-gray-300" title="Daily average already exists">
                        <Calendar className="h-4 w-4" />
                      </span>
                    )
                  })()}
                  <button
                    onClick={() => onEdit(metric)}
                    className="p-1 text-blue-600 hover:bg-blue-50 rounded transition-colors"
                    title="Edit metric"
                  >
                    <Pencil className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => onDelete(metric.id)}
                    className="p-1 text-red-600 hover:bg-red-50 rounded transition-colors"
                    title="Delete metric"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      </div>
    </div>
  )
}

interface CalculatedMetricsTabProps {
  metrics: CalculatedMetric[]
  isLoading: boolean
  onEdit: (metric: CalculatedMetric) => void
  onDelete: (id: string) => void
  onCreate: () => void
  onCreateDailyAverage: (metricId: string) => void
}

function CalculatedMetricsTab({ metrics, isLoading, onEdit, onDelete, onCreate, onCreateDailyAverage }: CalculatedMetricsTabProps) {
  if (isLoading) {
    return <div className="text-center py-8 text-gray-500">Loading calculated metrics...</div>
  }

  if (!metrics || metrics.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No calculated metrics defined yet.</p>
        <p className="text-sm text-gray-400 mt-2">Create calculated metrics using formulas like `{'{'}metric1{'}'} / {'{'}metric2{'}'}`</p>
        <button
          onClick={onCreate}
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Calculated Metric
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="flex justify-end mb-4">
        <button
          onClick={onCreate}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Calculated Metric
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Display Name</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Formula</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">SQL Expression</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Depends On</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Format</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Visible</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {metrics.map((metric) => (
              <tr key={metric.id} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-sm font-mono text-gray-900">{metric.id}</td>
                <td className="px-4 py-3 text-sm text-gray-900">{metric.display_name}</td>
                <td className="px-4 py-3 text-sm font-mono text-gray-600 max-w-xs truncate" title={metric.formula}>
                  {metric.formula}
                </td>
                <td className="px-4 py-3 text-sm font-mono text-gray-500 max-w-xs truncate" title={metric.sql_expression}>
                  {metric.sql_expression}
                </td>
                <td className="px-4 py-3 text-sm">
                  <div className="flex flex-wrap gap-1">
                    {metric.depends_on.map((dep) => (
                      <span key={dep} className="px-2 py-0.5 bg-purple-100 text-purple-800 rounded text-xs">
                        {dep}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm">
                  <span className="px-2 py-1 bg-gray-100 text-gray-800 rounded text-xs">
                    {metric.format_type} ({metric.decimal_places} decimals)
                  </span>
                </td>
                <td className="px-4 py-3 text-sm">
                  {metric.is_visible_by_default ? (
                    <span className="text-green-600">Yes</span>
                  ) : (
                    <span className="text-gray-400">No</span>
                  )}
                </td>
                <td className="px-4 py-3 text-sm">
                  <div className="flex gap-2">
                    {/* Create Daily Average button - only for volume metrics */}
                    {metric.category === 'volume' && (() => {
                      const dailyAvgId = `${metric.id}_per_day`
                      const dailyAvgExists = metrics.some(m => m.id === dailyAvgId)
                      return !dailyAvgExists ? (
                        <button
                          onClick={() => onCreateDailyAverage(metric.id)}
                          className="p-1 text-green-600 hover:bg-green-50 rounded transition-colors"
                          title="Create daily average (per-day) metric"
                        >
                          <Calendar className="h-4 w-4" />
                        </button>
                      ) : (
                        <span className="p-1 text-gray-300" title="Daily average already exists">
                          <Calendar className="h-4 w-4" />
                        </span>
                      )
                    })()}
                    <button
                      onClick={() => onEdit(metric)}
                      className="p-1 text-blue-600 hover:bg-blue-50 rounded transition-colors"
                      title="Edit metric"
                    >
                      <Pencil className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => onDelete(metric.id)}
                      className="p-1 text-red-600 hover:bg-red-50 rounded transition-colors"
                      title="Delete metric"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

interface DimensionsTabProps {
  dimensions: DimensionDef[]
  isLoading: boolean
  onEdit: (dimension: DimensionDef) => void
  onDelete: (id: string) => void
  onCreate: () => void
}

function DimensionsTab({ dimensions, isLoading, onEdit, onDelete, onCreate }: DimensionsTabProps) {
  if (isLoading) {
    return <div className="text-center py-8 text-gray-500">Loading dimensions...</div>
  }

  if (!dimensions || dimensions.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No dimensions defined yet.</p>
        <p className="text-sm text-gray-400 mt-2">Use Auto-Detect to discover dimensions from your BigQuery table or create them manually.</p>
        <button
          onClick={onCreate}
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Dimension
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="flex justify-end mb-4">
        <button
          onClick={onCreate}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Create Dimension
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Display Name</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Column</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Data Type</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Filter Type</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Filterable</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Groupable</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {dimensions.map((dimension) => (
              <tr key={dimension.id} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-sm font-mono text-gray-900">{dimension.id}</td>
                <td className="px-4 py-3 text-sm text-gray-900">{dimension.display_name}</td>
                <td className="px-4 py-3 text-sm font-mono text-gray-600">{dimension.column_name}</td>
                <td className="px-4 py-3 text-sm">
                  <span className="px-2 py-1 bg-green-100 text-green-800 rounded text-xs">
                    {dimension.data_type}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm">
                  <span className="px-2 py-1 bg-orange-100 text-orange-800 rounded text-xs">
                    {dimension.filter_type}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm">
                  {dimension.is_filterable ? (
                    <span className="text-green-600">Yes</span>
                  ) : (
                    <span className="text-gray-400">No</span>
                  )}
                </td>
                <td className="px-4 py-3 text-sm">
                  {dimension.is_groupable ? (
                    <span className="text-green-600">Yes</span>
                  ) : (
                    <span className="text-gray-400">No</span>
                  )}
                </td>
                <td className="px-4 py-3 text-sm">
                  <div className="flex gap-2">
                    <button
                      onClick={() => onEdit(dimension)}
                      className="p-1 text-blue-600 hover:bg-blue-50 rounded transition-colors"
                      title="Edit dimension"
                    >
                      <Pencil className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => onDelete(dimension.id)}
                      className="p-1 text-red-600 hover:bg-red-50 rounded transition-colors"
                      title="Delete dimension"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// CREATE MODALS

interface CreateBaseMetricModalProps {
  onSave: (data: MetricCreate) => void
  onClose: () => void
}

function CreateBaseMetricModal({ onSave, onClose }: CreateBaseMetricModalProps) {
  const [formData, setFormData] = useState<MetricCreate>({
    id: '',
    column_name: '',
    display_name: '',
    aggregation: 'SUM',
    data_type: 'INTEGER',
    format_type: 'number',
    decimal_places: 0,
    category: '',
    is_visible_by_default: true,
    sort_order: 0,
    description: '',
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200 sticky top-0 bg-white">
          <h3 className="text-lg font-semibold text-gray-900">Create Base Metric</h3>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                ID
              </label>
              <input
                type="text"
                value={formData.id}
                onChange={(e) => setFormData({ ...formData, id: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., revenue"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Column Name
              </label>
              <input
                type="text"
                value={formData.column_name}
                onChange={(e) => setFormData({ ...formData, column_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., gross_revenue"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Display Name
              </label>
              <input
                type="text"
                value={formData.display_name}
                onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Aggregation
              </label>
              <select
                value={formData.aggregation}
                onChange={(e) => setFormData({ ...formData, aggregation: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="SUM">SUM</option>
                <option value="COUNT">COUNT</option>
                <option value="AVG">AVG</option>
                <option value="MIN">MIN</option>
                <option value="MAX">MAX</option>
                <option value="COUNT_DISTINCT">COUNT_DISTINCT</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Data Type
              </label>
              <select
                value={formData.data_type}
                onChange={(e) => setFormData({ ...formData, data_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="INTEGER">INTEGER</option>
                <option value="FLOAT">FLOAT</option>
                <option value="NUMERIC">NUMERIC</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Format Type
              </label>
              <select
                value={formData.format_type}
                onChange={(e) => setFormData({ ...formData, format_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="number">Number</option>
                <option value="currency">Currency</option>
                <option value="percent">Percent</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Decimal Places
              </label>
              <input
                type="number"
                min="0"
                max="10"
                value={formData.decimal_places}
                onChange={(e) => {
                  const value = e.target.value === '' ? 0 : parseInt(e.target.value)
                  setFormData({ ...formData, decimal_places: isNaN(value) ? 0 : value })
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Category
              </label>
              <input
                type="text"
                value={formData.category}
                onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., revenue, volume, conversion"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Sort Order
              </label>
              <input
                type="number"
                value={formData.sort_order}
                onChange={(e) => setFormData({ ...formData, sort_order: parseInt(e.target.value) })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
              placeholder="Optional description of this metric"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="create_visible_by_default"
              checked={formData.is_visible_by_default}
              onChange={(e) => setFormData({ ...formData, is_visible_by_default: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="create_visible_by_default" className="text-sm font-medium text-gray-700">
              Visible by Default
            </label>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Create Metric
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

interface CreateCalculatedMetricModalProps {
  onSave: (data: CalculatedMetricCreate) => void
  onClose: () => void
  baseMetrics: BaseMetric[]
  calculatedMetrics: CalculatedMetric[]
  dimensions: DimensionDef[]
  editingMetricId?: string | null
}

function CreateCalculatedMetricModal({ onSave, onClose, baseMetrics, calculatedMetrics, dimensions, editingMetricId }: CreateCalculatedMetricModalProps) {
  const [formData, setFormData] = useState<CalculatedMetricCreate>({
    id: '',
    display_name: '',
    formula: '',
    format_type: 'number',
    decimal_places: 0,
    category: '',
    is_visible_by_default: true,
    sort_order: 0,
    description: '',
  })
  const [showFormulaBuilder, setShowFormulaBuilder] = useState(false)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  const handleFormulaApply = (formula: string) => {
    setFormData({ ...formData, formula })
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200 sticky top-0 bg-white">
          <h3 className="text-lg font-semibold text-gray-900">Create Calculated Metric</h3>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                ID
              </label>
              <input
                type="text"
                value={formData.id}
                onChange={(e) => setFormData({ ...formData, id: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., conversion_rate"
              />
            </div>

            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Display Name
              </label>
              <input
                type="text"
                value={formData.display_name}
                onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Formula
              </label>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={formData.formula}
                  onChange={(e) => setFormData({ ...formData, formula: e.target.value })}
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono"
                  required
                  placeholder="e.g., {purchases} / {queries}"
                />
                <button
                  type="button"
                  onClick={() => setShowFormulaBuilder(true)}
                  className="px-4 py-2 bg-purple-600 text-white rounded-md hover:bg-purple-700 whitespace-nowrap"
                >
                  Formula Builder
                </button>
              </div>
              <p className="mt-1 text-xs text-gray-500">Use curly braces to reference other metrics, or use Formula Builder</p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Format Type
              </label>
              <select
                value={formData.format_type}
                onChange={(e) => setFormData({ ...formData, format_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="number">Number</option>
                <option value="currency">Currency</option>
                <option value="percent">Percent</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Decimal Places
              </label>
              <input
                type="number"
                min="0"
                max="10"
                value={formData.decimal_places}
                onChange={(e) => {
                  const value = e.target.value === '' ? 0 : parseInt(e.target.value)
                  setFormData({ ...formData, decimal_places: isNaN(value) ? 0 : value })
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Category
              </label>
              <input
                type="text"
                value={formData.category}
                onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., conversion, revenue"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Sort Order
              </label>
              <input
                type="number"
                value={formData.sort_order}
                onChange={(e) => setFormData({ ...formData, sort_order: parseInt(e.target.value) })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
              placeholder="Optional description of this calculated metric"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="create_calc_visible_by_default"
              checked={formData.is_visible_by_default}
              onChange={(e) => setFormData({ ...formData, is_visible_by_default: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="create_calc_visible_by_default" className="text-sm font-medium text-gray-700">
              Visible by Default
            </label>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Create Metric
            </button>
          </div>
        </form>
      </div>

      {/* Formula Builder Modal */}
      <FormulaBuilderModal
        isOpen={showFormulaBuilder}
        onClose={() => setShowFormulaBuilder(false)}
        availableMetrics={baseMetrics || []}
        availableCalculatedMetrics={
          // Filter out the current metric being edited to prevent circular dependencies
          editingMetricId
            ? calculatedMetrics.filter(m => m.id !== editingMetricId)
            : calculatedMetrics
        }
        availableDimensions={dimensions || []}
        onApply={handleFormulaApply}
        initialFormula={formData.formula}
      />
    </div>
  )
}

interface CreateDimensionModalProps {
  onSave: (data: DimensionCreate) => void
  onClose: () => void
}

function CreateDimensionModal({ onSave, onClose }: CreateDimensionModalProps) {
  const [formData, setFormData] = useState<DimensionCreate>({
    id: '',
    column_name: '',
    display_name: '',
    data_type: 'STRING',
    filter_type: 'single_select',
    is_filterable: true,
    is_groupable: true,
    sort_order: 0,
    description: '',
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200 sticky top-0 bg-white">
          <h3 className="text-lg font-semibold text-gray-900">Create Dimension</h3>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                ID
              </label>
              <input
                type="text"
                value={formData.id}
                onChange={(e) => setFormData({ ...formData, id: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., country"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Column Name
              </label>
              <input
                type="text"
                value={formData.column_name}
                onChange={(e) => setFormData({ ...formData, column_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
                placeholder="e.g., country"
              />
            </div>

            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Display Name
              </label>
              <input
                type="text"
                value={formData.display_name}
                onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Data Type
              </label>
              <select
                value={formData.data_type}
                onChange={(e) => setFormData({ ...formData, data_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="STRING">STRING</option>
                <option value="INTEGER">INTEGER</option>
                <option value="BOOLEAN">BOOLEAN</option>
                <option value="DATE">DATE</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Filter Type
              </label>
              <select
                value={formData.filter_type}
                onChange={(e) => setFormData({ ...formData, filter_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="single_select">Single Select</option>
                <option value="multi_select">Multi Select</option>
                <option value="date_range">Date Range</option>
                <option value="boolean">Boolean</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Sort Order
              </label>
              <input
                type="number"
                value={formData.sort_order}
                onChange={(e) => setFormData({ ...formData, sort_order: parseInt(e.target.value) })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
              placeholder="Optional description of this dimension"
            />
          </div>

          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="create_dim_filterable"
                checked={formData.is_filterable}
                onChange={(e) => setFormData({ ...formData, is_filterable: e.target.checked })}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor="create_dim_filterable" className="text-sm font-medium text-gray-700">
                Filterable
              </label>
            </div>

            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="create_dim_groupable"
                checked={formData.is_groupable}
                onChange={(e) => setFormData({ ...formData, is_groupable: e.target.checked })}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor="create_dim_groupable" className="text-sm font-medium text-gray-700">
                Groupable
              </label>
            </div>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Create Dimension
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

// EDIT MODALS

interface EditDimensionModalProps {
  dimension: DimensionDef
  onSave: (data: DimensionUpdate) => void
  onClose: () => void
}

function EditDimensionModal({ dimension, onSave, onClose }: EditDimensionModalProps) {
  const [formData, setFormData] = useState({
    display_name: dimension.display_name,
    description: dimension.description || '',
    is_filterable: dimension.is_filterable,
    is_groupable: dimension.is_groupable,
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
        <div className="p-6 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900">Edit Dimension</h3>
          <p className="text-sm text-gray-500 mt-1">ID: {dimension.id}</p>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Display Name
            </label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="is_filterable"
              checked={formData.is_filterable}
              onChange={(e) => setFormData({ ...formData, is_filterable: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="is_filterable" className="text-sm font-medium text-gray-700">
              Filterable
            </label>
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="is_groupable"
              checked={formData.is_groupable}
              onChange={(e) => setFormData({ ...formData, is_groupable: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="is_groupable" className="text-sm font-medium text-gray-700">
              Groupable
            </label>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Save Changes
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

interface EditBaseMetricModalProps {
  metric: BaseMetric
  onSave: (data: MetricUpdate) => void
  onClose: () => void
}

function EditBaseMetricModal({ metric, onSave, onClose }: EditBaseMetricModalProps) {
  const [formData, setFormData] = useState({
    display_name: metric.display_name,
    aggregation: metric.aggregation,
    format_type: metric.format_type,
    decimal_places: metric.decimal_places,
    category: metric.category,
    is_visible_by_default: metric.is_visible_by_default,
    sort_order: metric.sort_order,
    description: metric.description || '',
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200 sticky top-0 bg-white">
          <h3 className="text-lg font-semibold text-gray-900">Edit Base Metric</h3>
          <p className="text-sm text-gray-500 mt-1">ID: {metric.id}</p>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Display Name
              </label>
              <input
                type="text"
                value={formData.display_name}
                onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Aggregation
              </label>
              <select
                value={formData.aggregation}
                onChange={(e) => setFormData({ ...formData, aggregation: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="SUM">SUM</option>
                <option value="COUNT">COUNT</option>
                <option value="AVG">AVG</option>
                <option value="MIN">MIN</option>
                <option value="MAX">MAX</option>
                <option value="COUNT_DISTINCT">COUNT_DISTINCT</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Format Type
              </label>
              <select
                value={formData.format_type}
                onChange={(e) => setFormData({ ...formData, format_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="number">Number</option>
                <option value="currency">Currency</option>
                <option value="percent">Percent</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Decimal Places
              </label>
              <input
                type="number"
                min="0"
                max="10"
                value={formData.decimal_places}
                onChange={(e) => {
                  const value = e.target.value === '' ? 0 : parseInt(e.target.value)
                  setFormData({ ...formData, decimal_places: isNaN(value) ? 0 : value })
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Category
              </label>
              <input
                type="text"
                value={formData.category}
                onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Sort Order
              </label>
              <input
                type="number"
                value={formData.sort_order}
                onChange={(e) => setFormData({ ...formData, sort_order: parseInt(e.target.value) })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="is_visible_by_default"
              checked={formData.is_visible_by_default}
              onChange={(e) => setFormData({ ...formData, is_visible_by_default: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="is_visible_by_default" className="text-sm font-medium text-gray-700">
              Visible by Default
            </label>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Save Changes
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

interface EditCalculatedMetricModalProps {
  metric: CalculatedMetric
  onSave: (data: CalculatedMetricUpdate) => void
  onClose: () => void
  baseMetrics: BaseMetric[]
  calculatedMetrics: CalculatedMetric[]
  dimensions: DimensionDef[]
}

function EditCalculatedMetricModal({ metric, onSave, onClose, baseMetrics, calculatedMetrics, dimensions }: EditCalculatedMetricModalProps) {
  const [formData, setFormData] = useState({
    display_name: metric.display_name,
    formula: metric.formula,
    format_type: metric.format_type,
    decimal_places: metric.decimal_places,
    category: metric.category,
    is_visible_by_default: metric.is_visible_by_default,
    sort_order: metric.sort_order,
    description: metric.description || '',
  })
  const [isFormulaBuilderOpen, setIsFormulaBuilderOpen] = useState(false)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave(formData)
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200 sticky top-0 bg-white">
          <h3 className="text-lg font-semibold text-gray-900">Edit Calculated Metric</h3>
          <p className="text-sm text-gray-500 mt-1">ID: {metric.id}</p>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Display Name
            </label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Formula
            </label>
            <div className="relative">
              <textarea
                value={formData.formula}
                onChange={(e) => setFormData({ ...formData, formula: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                rows={3}
                placeholder="{metric1} / {metric2}"
                required
              />
              <button
                type="button"
                onClick={() => setIsFormulaBuilderOpen(true)}
                className="absolute top-2 right-2 px-3 py-1 text-xs font-medium text-blue-600 bg-blue-50 border border-blue-200 rounded hover:bg-blue-100"
              >
                Open Formula Builder
              </button>
            </div>
            <p className="text-xs text-gray-500 mt-1">Use curly braces to reference other metrics, e.g., {'{'}queries{'}'}</p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Format Type
              </label>
              <select
                value={formData.format_type}
                onChange={(e) => setFormData({ ...formData, format_type: e.target.value as any })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              >
                <option value="number">Number</option>
                <option value="currency">Currency</option>
                <option value="percent">Percent</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Decimal Places
              </label>
              <input
                type="number"
                min="0"
                max="10"
                value={formData.decimal_places}
                onChange={(e) => {
                  const value = e.target.value === '' ? 0 : parseInt(e.target.value)
                  setFormData({ ...formData, decimal_places: isNaN(value) ? 0 : value })
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Category
              </label>
              <input
                type="text"
                value={formData.category}
                onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Sort Order
              </label>
              <input
                type="number"
                value={formData.sort_order}
                onChange={(e) => setFormData({ ...formData, sort_order: parseInt(e.target.value) })}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={3}
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="is_visible_by_default_calc"
              checked={formData.is_visible_by_default}
              onChange={(e) => setFormData({ ...formData, is_visible_by_default: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="is_visible_by_default_calc" className="text-sm font-medium text-gray-700">
              Visible by Default
            </label>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
            >
              Save Changes
            </button>
          </div>
        </form>

        {/* Formula Builder Modal */}
        <FormulaBuilderModal
          isOpen={isFormulaBuilderOpen}
          onClose={() => setIsFormulaBuilderOpen(false)}
          availableMetrics={baseMetrics}
          availableCalculatedMetrics={calculatedMetrics}
          availableDimensions={dimensions}
          initialFormula={formData.formula}
          onApply={(formula) => {
            setFormData({ ...formData, formula })
            setIsFormulaBuilderOpen(false)
          }}
        />
      </div>
    </div>
  )
}
