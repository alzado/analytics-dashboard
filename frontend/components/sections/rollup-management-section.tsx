'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchRollups,
  createRollup,
  deleteRollup,
  refreshRollup,
  previewRollupSql,
  refreshAllRollups,
  setDefaultRollupDataset,
  fetchSchema,
} from '@/lib/api'
import type {
  RollupDef,
  RollupCreate,
  RollupMetricDef,
  RollupStatus,
  BaseMetric,
  DimensionDef,
} from '@/lib/types'

interface RollupManagementSectionProps {
  tableId?: string
}

const STATUS_COLORS: Record<RollupStatus, string> = {
  pending: 'bg-yellow-100 text-yellow-800',
  building: 'bg-blue-100 text-blue-800',
  ready: 'bg-green-100 text-green-800',
  error: 'bg-red-100 text-red-800',
  stale: 'bg-orange-100 text-orange-800',
}

const STATUS_LABELS: Record<RollupStatus, string> = {
  pending: 'Pending',
  building: 'Building...',
  ready: 'Ready',
  error: 'Error',
  stale: 'Stale',
}

export function RollupManagementSection({ tableId }: RollupManagementSectionProps) {
  const queryClient = useQueryClient()
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [showSqlPreview, setShowSqlPreview] = useState<string | null>(null)
  const [sqlPreviewContent, setSqlPreviewContent] = useState<string>('')
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null)
  const [defaultDataset, setDefaultDataset] = useState<string>('')

  // Form state for creating a rollup
  const [formDisplayName, setFormDisplayName] = useState('')
  const [formDescription, setFormDescription] = useState('')
  const [formDimensions, setFormDimensions] = useState<string[]>(['date'])
  const [formMetrics, setFormMetrics] = useState<RollupMetricDef[]>([])
  const [formTargetDataset, setFormTargetDataset] = useState('')

  // Fetch rollups
  const { data: rollupsData, isLoading: rollupsLoading } = useQuery({
    queryKey: ['rollups', tableId],
    queryFn: () => fetchRollups(tableId),
  })

  // Fetch schema for dimension and metric options
  const { data: schema } = useQuery({
    queryKey: ['schema', tableId],
    queryFn: () => fetchSchema(tableId),
  })

  // Create rollup mutation
  const createMutation = useMutation({
    mutationFn: (data: RollupCreate) => createRollup(data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
      resetForm()
      setShowCreateForm(false)
    },
  })

  // Delete rollup mutation
  const deleteMutation = useMutation({
    mutationFn: ({ rollupId, dropTable }: { rollupId: string; dropTable: boolean }) =>
      deleteRollup(rollupId, dropTable, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
      setDeleteConfirm(null)
    },
  })

  // Refresh rollup mutation
  const refreshMutation = useMutation({
    mutationFn: (rollupId: string) => refreshRollup(rollupId, true, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
    },
  })

  // Refresh all mutation
  const refreshAllMutation = useMutation({
    mutationFn: () => refreshAllRollups(true, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
    },
  })

  // Set default dataset mutation
  const setDatasetMutation = useMutation({
    mutationFn: (dataset: string | null) => setDefaultRollupDataset(dataset, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
    },
  })

  const resetForm = () => {
    setFormDisplayName('')
    setFormDescription('')
    setFormDimensions(['date'])
    setFormMetrics([])
    setFormTargetDataset('')
  }

  const handleCreateRollup = () => {
    if (!formDisplayName || formDimensions.length === 0 || formMetrics.length === 0) {
      alert('Please fill in all required fields')
      return
    }

    const data: RollupCreate = {
      display_name: formDisplayName,
      description: formDescription || undefined,
      dimensions: formDimensions,
      metrics: formMetrics,
      target_dataset: formTargetDataset || undefined,
    }

    createMutation.mutate(data)
  }

  const handlePreviewSql = async (rollupId: string) => {
    try {
      const result = await previewRollupSql(rollupId, tableId)
      setSqlPreviewContent(result.sql)
      setShowSqlPreview(rollupId)
    } catch (error) {
      alert(`Failed to preview SQL: ${error}`)
    }
  }

  const toggleDimension = (dimId: string) => {
    if (dimId === 'date') return // date is always required
    setFormDimensions((prev) =>
      prev.includes(dimId) ? prev.filter((d) => d !== dimId) : [...prev, dimId]
    )
  }

  const toggleMetric = (metricId: string) => {
    setFormMetrics((prev) => {
      const exists = prev.find((m) => m.metric_id === metricId)
      if (exists) {
        return prev.filter((m) => m.metric_id !== metricId)
      }
      return [...prev, { metric_id: metricId, include_conditional: false }]
    })
  }

  const updateMetricConditional = (metricId: string, includeConditional: boolean, flagColumn?: string) => {
    setFormMetrics((prev) =>
      prev.map((m) =>
        m.metric_id === metricId
          ? { ...m, include_conditional: includeConditional, flag_column: flagColumn }
          : m
      )
    )
  }

  const formatBytes = (bytes: number | null | undefined): string => {
    if (!bytes) return '-'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
  }

  const formatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-'
    const date = new Date(dateStr)
    return date.toLocaleString()
  }

  const formatNumber = (num: number | null | undefined): string => {
    if (num === null || num === undefined) return '-'
    return num.toLocaleString()
  }

  if (rollupsLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-gray-200 rounded w-1/4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    )
  }

  const rollups = rollupsData?.rollups || []
  const defaultTargetDataset = rollupsData?.default_target_dataset
  const dimensions = schema?.dimensions || []
  const baseMetrics = schema?.base_metrics || []

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-xl font-semibold">Pre-Aggregation Rollups</h2>
          <p className="text-sm text-gray-500 mt-1">
            Create pre-computed tables for faster COUNT DISTINCT queries
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => refreshAllMutation.mutate()}
            disabled={refreshAllMutation.isPending || rollups.length === 0}
            className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
          >
            {refreshAllMutation.isPending ? 'Refreshing...' : 'Refresh All'}
          </button>
          <button
            onClick={() => setShowCreateForm(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            + Create Rollup
          </button>
        </div>
      </div>

      {/* Default Dataset Setting */}
      <div className="bg-gray-50 p-4 rounded-lg">
        <div className="flex items-center gap-4">
          <label className="text-sm font-medium">Default Target Dataset:</label>
          <input
            type="text"
            value={defaultDataset || defaultTargetDataset || ''}
            onChange={(e) => setDefaultDataset(e.target.value)}
            placeholder="e.g., analytics_rollups"
            className="px-3 py-1.5 border border-gray-300 rounded text-sm w-64"
          />
          <button
            onClick={() => setDatasetMutation.mutate(defaultDataset || null)}
            disabled={setDatasetMutation.isPending}
            className="px-3 py-1.5 text-sm bg-gray-200 rounded hover:bg-gray-300"
          >
            {setDatasetMutation.isPending ? 'Saving...' : 'Save'}
          </button>
          <span className="text-xs text-gray-500">
            Rollup tables will be created in this dataset
          </span>
        </div>
      </div>

      {/* Rollups List */}
      {rollups.length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg">
          <p className="text-gray-500 mb-4">No rollups configured yet</p>
          <button
            onClick={() => setShowCreateForm(true)}
            className="text-blue-600 hover:underline"
          >
            Create your first rollup
          </button>
        </div>
      ) : (
        <div className="space-y-4">
          {rollups.map((rollup) => (
            <div
              key={rollup.id}
              className="border border-gray-200 rounded-lg p-4 hover:border-gray-300"
            >
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <div className="flex items-center gap-3">
                    <h3 className="font-medium">{rollup.display_name}</h3>
                    <span
                      className={`px-2 py-0.5 text-xs rounded-full ${STATUS_COLORS[rollup.status]}`}
                    >
                      {STATUS_LABELS[rollup.status]}
                    </span>
                  </div>
                  {rollup.description && (
                    <p className="text-sm text-gray-500 mt-1">{rollup.description}</p>
                  )}
                  <div className="mt-3 text-sm text-gray-600 space-y-1">
                    <div>
                      <span className="font-medium">Dimensions:</span>{' '}
                      {rollup.dimensions.join(', ')}
                    </div>
                    <div>
                      <span className="font-medium">Metrics:</span>{' '}
                      {rollup.metrics.map((m) => m.metric_id).join(', ')}
                    </div>
                    {rollup.row_count && (
                      <div>
                        <span className="font-medium">Rows:</span>{' '}
                        {formatNumber(rollup.row_count)} | Size: {formatBytes(rollup.size_bytes)}
                      </div>
                    )}
                    {rollup.last_refresh_at && (
                      <div>
                        <span className="font-medium">Last Refresh:</span>{' '}
                        {formatDate(rollup.last_refresh_at)}
                      </div>
                    )}
                    {rollup.last_refresh_error && (
                      <div className="text-red-600">
                        <span className="font-medium">Error:</span> {rollup.last_refresh_error}
                      </div>
                    )}
                  </div>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => handlePreviewSql(rollup.id)}
                    className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                  >
                    Preview SQL
                  </button>
                  <button
                    onClick={() => refreshMutation.mutate(rollup.id)}
                    disabled={refreshMutation.isPending}
                    className="px-3 py-1.5 text-sm bg-blue-100 text-blue-700 rounded hover:bg-blue-200 disabled:opacity-50"
                  >
                    {refreshMutation.isPending ? 'Refreshing...' : 'Refresh'}
                  </button>
                  <button
                    onClick={() => setDeleteConfirm(rollup.id)}
                    className="px-3 py-1.5 text-sm text-red-600 border border-red-300 rounded hover:bg-red-50"
                  >
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create Rollup Modal */}
      {showCreateForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <h3 className="text-lg font-semibold mb-4">Create New Rollup</h3>

            <div className="space-y-4">
              {/* Display Name */}
              <div>
                <label className="block text-sm font-medium mb-1">Display Name *</label>
                <input
                  type="text"
                  value={formDisplayName}
                  onChange={(e) => setFormDisplayName(e.target.value)}
                  placeholder="e.g., Daily by Channel"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-medium mb-1">Description</label>
                <input
                  type="text"
                  value={formDescription}
                  onChange={(e) => setFormDescription(e.target.value)}
                  placeholder="Optional description"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>

              {/* Dimensions */}
              <div>
                <label className="block text-sm font-medium mb-1">
                  Dimensions * <span className="text-gray-500">(date is always included)</span>
                </label>
                <div className="flex flex-wrap gap-2">
                  {dimensions.map((dim) => (
                    <button
                      key={dim.id}
                      onClick={() => toggleDimension(dim.id)}
                      disabled={dim.id === 'date'}
                      className={`px-3 py-1.5 text-sm rounded-lg border ${
                        formDimensions.includes(dim.id)
                          ? 'bg-blue-100 border-blue-300 text-blue-700'
                          : 'bg-white border-gray-300 hover:bg-gray-50'
                      } ${dim.id === 'date' ? 'opacity-75 cursor-not-allowed' : ''}`}
                    >
                      {dim.display_name}
                    </button>
                  ))}
                </div>
              </div>

              {/* Metrics */}
              <div>
                <label className="block text-sm font-medium mb-1">Metrics *</label>
                <div className="space-y-2 max-h-60 overflow-y-auto border border-gray-200 rounded-lg p-3">
                  {baseMetrics.map((metric) => {
                    const selected = formMetrics.find((m) => m.metric_id === metric.id)
                    return (
                      <div key={metric.id} className="flex items-center gap-3">
                        <input
                          type="checkbox"
                          checked={!!selected}
                          onChange={() => toggleMetric(metric.id)}
                          className="rounded"
                        />
                        <span className="flex-1">{metric.display_name}</span>
                        {selected && (
                          <div className="flex items-center gap-2 text-sm">
                            <label className="flex items-center gap-1">
                              <input
                                type="checkbox"
                                checked={selected.include_conditional}
                                onChange={(e) =>
                                  updateMetricConditional(
                                    metric.id,
                                    e.target.checked,
                                    selected.flag_column ?? undefined
                                  )
                                }
                                className="rounded"
                              />
                              Conditional
                            </label>
                            {selected.include_conditional && (
                              <input
                                type="text"
                                value={selected.flag_column || ''}
                                onChange={(e) =>
                                  updateMetricConditional(metric.id, true, e.target.value)
                                }
                                placeholder="flag_column"
                                className="w-32 px-2 py-1 border border-gray-300 rounded text-sm"
                              />
                            )}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>

              {/* Target Dataset Override */}
              <div>
                <label className="block text-sm font-medium mb-1">
                  Target Dataset Override{' '}
                  <span className="text-gray-500">(optional)</span>
                </label>
                <input
                  type="text"
                  value={formTargetDataset}
                  onChange={(e) => setFormTargetDataset(e.target.value)}
                  placeholder={`Leave empty to use default (${defaultTargetDataset || 'source dataset'})`}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>
            </div>

            <div className="flex justify-end gap-3 mt-6">
              <button
                onClick={() => {
                  resetForm()
                  setShowCreateForm(false)
                }}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={handleCreateRollup}
                disabled={createMutation.isPending}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
              >
                {createMutation.isPending ? 'Creating...' : 'Create Rollup'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* SQL Preview Modal */}
      {showSqlPreview && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-3xl max-h-[90vh] overflow-y-auto">
            <h3 className="text-lg font-semibold mb-4">SQL Preview</h3>
            <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-x-auto text-sm whitespace-pre-wrap">
              {sqlPreviewContent}
            </pre>
            <div className="flex justify-end mt-4">
              <button
                onClick={() => {
                  setShowSqlPreview(null)
                  setSqlPreviewContent('')
                }}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {deleteConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold mb-4">Delete Rollup</h3>
            <p className="text-gray-600 mb-4">
              Are you sure you want to delete this rollup? This action cannot be undone.
            </p>
            <div className="flex items-center gap-2 mb-4">
              <input type="checkbox" id="dropTable" className="rounded" />
              <label htmlFor="dropTable" className="text-sm">
                Also drop the BigQuery table
              </label>
            </div>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setDeleteConfirm(null)}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={() => {
                  const dropTable = (document.getElementById('dropTable') as HTMLInputElement)?.checked
                  deleteMutation.mutate({ rollupId: deleteConfirm, dropTable })
                }}
                disabled={deleteMutation.isPending}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
              >
                {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
