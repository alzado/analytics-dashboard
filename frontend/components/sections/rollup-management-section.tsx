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
  setDefaultRollupProject,
  setDefaultRollupDataset,
  fetchSchema,
  fetchRollupStatus,
  // Optimized source API
  fetchOptimizedSourceStatus,
  analyzeOptimizedSource,
  createOptimizedSource,
  refreshOptimizedSource,
  deleteOptimizedSource,
  previewOptimizedSourceSql,
  type OptimizedSourceStatusResponse,
  type OptimizedSourceAnalysis,
} from '@/lib/api'
import type {
  RollupDef,
  RollupCreate,
  RollupStatus,
  RollupStatusResponse,
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
  const [defaultProject, setDefaultProject] = useState<string>('')
  const [defaultDataset, setDefaultDataset] = useState<string>('')
  const [refreshMode, setRefreshMode] = useState<'full' | 'incremental'>('incremental')
  const [rollupStatuses, setRollupStatuses] = useState<Record<string, RollupStatusResponse>>({})

  // Form state for creating a rollup
  const [formDisplayName, setFormDisplayName] = useState('')
  const [formDescription, setFormDescription] = useState('')
  const [formDimensions, setFormDimensions] = useState<string[]>(['date'])
  const [formTargetProject, setFormTargetProject] = useState('')
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

  // Optimized Source state
  const [showOptimizedSourceSql, setShowOptimizedSourceSql] = useState(false)
  const [optimizedSourceSqlContent, setOptimizedSourceSqlContent] = useState('')
  const [showOptimizedSourceForm, setShowOptimizedSourceForm] = useState(false)
  const [optSourceProject, setOptSourceProject] = useState('')
  const [optSourceDataset, setOptSourceDataset] = useState('')

  // Fetch optimized source status
  const { data: optimizedSourceStatus, isLoading: optimizedSourceLoading } = useQuery({
    queryKey: ['optimized-source-status', tableId],
    queryFn: () => fetchOptimizedSourceStatus(tableId),
  })

  // Analyze optimized source (what keys would be created)
  const { data: optimizedSourceAnalysis } = useQuery({
    queryKey: ['optimized-source-analysis', tableId],
    queryFn: () => analyzeOptimizedSource(tableId),
    enabled: !optimizedSourceStatus?.exists,
  })

  // Create optimized source mutation
  const createOptimizedSourceMutation = useMutation({
    mutationFn: (params: { targetProject?: string; targetDataset?: string }) =>
      createOptimizedSource({
        auto_detect_clustering: true,
        target_project: params.targetProject || null,
        target_dataset: params.targetDataset || null,
      }, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['optimized-source-status', tableId] })
      setShowOptimizedSourceForm(false)
      setOptSourceProject('')
      setOptSourceDataset('')
    },
  })

  // Refresh optimized source mutation
  const refreshOptimizedSourceMutation = useMutation({
    mutationFn: (incremental: boolean) => refreshOptimizedSource(incremental, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['optimized-source-status', tableId] })
    },
  })

  // Delete optimized source mutation
  const deleteOptimizedSourceMutation = useMutation({
    mutationFn: (dropTable: boolean) => deleteOptimizedSource(dropTable, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['optimized-source-status', tableId] })
    },
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
    mutationFn: ({ rollupId, incremental }: { rollupId: string; incremental: boolean }) =>
      refreshRollup(rollupId, incremental, false, tableId),
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

  // Set default project mutation
  const setProjectMutation = useMutation({
    mutationFn: (project: string | null) => setDefaultRollupProject(project, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rollups', tableId] })
    },
  })

  const resetForm = () => {
    setFormDisplayName('')
    setFormDescription('')
    setFormDimensions(['date'])
    setFormTargetProject('')
    setFormTargetDataset('')
  }

  const handleCreateRollup = () => {
    if (!formDisplayName || formDimensions.length === 0) {
      alert('Please fill in all required fields')
      return
    }

    const data: RollupCreate = {
      display_name: formDisplayName,
      description: formDescription || undefined,
      dimensions: formDimensions,
      target_project: formTargetProject || undefined,
      target_dataset: formTargetDataset || undefined,
    }

    createMutation.mutate(data)
  }

  const handleCheckStatus = async (rollupId: string) => {
    try {
      const status = await fetchRollupStatus(rollupId, tableId)
      setRollupStatuses((prev) => ({ ...prev, [rollupId]: status }))
    } catch (error) {
      console.error('Failed to check rollup status:', error)
    }
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

  const handlePreviewOptimizedSourceSql = async () => {
    try {
      const result = await previewOptimizedSourceSql(true, tableId)
      setOptimizedSourceSqlContent(result.sql)
      setShowOptimizedSourceSql(true)
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
  const defaultTargetProject = rollupsData?.default_target_project
  const defaultTargetDataset = rollupsData?.default_target_dataset
  const dimensions = schema?.dimensions || []

  // Count auto-included metrics for display (volume calculated metrics only)
  const autoIncludedVolumeMetrics = (schema?.calculated_metrics || []).filter(m => m.category === 'volume')
  const totalAutoMetrics = autoIncludedVolumeMetrics.length

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

      {/* Default Project & Dataset Settings */}
      <div className="bg-gray-50 p-4 rounded-lg space-y-3">
        <div className="flex items-center gap-4">
          <label className="text-sm font-medium w-40">Default Target Project:</label>
          <input
            type="text"
            value={defaultProject || defaultTargetProject || ''}
            onChange={(e) => setDefaultProject(e.target.value)}
            placeholder="e.g., my-analytics-project"
            className="px-3 py-1.5 border border-gray-300 rounded text-sm w-64"
          />
          <button
            onClick={() => setProjectMutation.mutate(defaultProject || null)}
            disabled={setProjectMutation.isPending}
            className="px-3 py-1.5 text-sm bg-gray-200 rounded hover:bg-gray-300"
          >
            {setProjectMutation.isPending ? 'Saving...' : 'Save'}
          </button>
          <span className="text-xs text-gray-500">
            Leave empty to use source project
          </span>
        </div>
        <div className="flex items-center gap-4">
          <label className="text-sm font-medium w-40">Default Target Dataset:</label>
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
            Leave empty to use source dataset
          </span>
        </div>
      </div>

      {/* Optimized Source Section */}
      <div className="border border-gray-200 rounded-lg">
        <div className="p-4 border-b border-gray-200 bg-gray-50">
          <div className="flex justify-between items-start">
            <div>
              <h3 className="font-medium flex items-center gap-2">
                Optimized Source Table
                {optimizedSourceStatus?.exists && (
                  <span
                    className={`px-2 py-0.5 text-xs rounded-full ${STATUS_COLORS[optimizedSourceStatus.config?.status || 'pending']}`}
                  >
                    {STATUS_LABELS[optimizedSourceStatus.config?.status || 'pending']}
                  </span>
                )}
              </h3>
              <p className="text-sm text-gray-500 mt-1">
                Precompute composite keys (CONCAT columns) once for faster rollup creation
              </p>
            </div>
          </div>
        </div>

        <div className="p-4">
          {optimizedSourceLoading ? (
            <div className="animate-pulse h-20 bg-gray-100 rounded"></div>
          ) : optimizedSourceStatus?.exists ? (
            // Optimized source exists - show status and actions
            <div className="space-y-4">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <span className="text-gray-500">Table:</span>
                  <p className="font-mono text-xs mt-1 truncate" title={optimizedSourceStatus.config?.optimized_table_name}>
                    {optimizedSourceStatus.config?.optimized_table_name}
                  </p>
                </div>
                <div>
                  <span className="text-gray-500">Rows:</span>
                  <p className="font-medium mt-1">{formatNumber(optimizedSourceStatus.config?.row_count)}</p>
                </div>
                <div>
                  <span className="text-gray-500">Size:</span>
                  <p className="font-medium mt-1">{formatBytes(optimizedSourceStatus.config?.size_bytes)}</p>
                </div>
                <div>
                  <span className="text-gray-500">Last Refresh:</span>
                  <p className="mt-1">{formatDate(optimizedSourceStatus.config?.last_refresh_at)}</p>
                </div>
              </div>

              {/* Composite Keys */}
              {optimizedSourceStatus.config?.composite_key_mappings && optimizedSourceStatus.config.composite_key_mappings.length > 0 && (
                <div>
                  <span className="text-sm text-gray-500">Precomputed Keys ({optimizedSourceStatus.config.composite_key_mappings.length}):</span>
                  <div className="flex flex-wrap gap-2 mt-2">
                    {optimizedSourceStatus.config.composite_key_mappings.map((mapping) => (
                      <span
                        key={mapping.key_column_name}
                        className="px-2 py-1 bg-purple-50 text-purple-700 text-xs rounded-lg"
                        title={`CONCAT(${mapping.source_columns.join(', ')})`}
                      >
                        {mapping.key_column_name}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Clustering */}
              {optimizedSourceStatus.config?.clustering?.columns && optimizedSourceStatus.config.clustering.columns.length > 0 && (
                <div>
                  <span className="text-sm text-gray-500">
                    Clustering Columns {optimizedSourceStatus.config.clustering.auto_detected && '(auto-detected)'}:
                  </span>
                  <div className="flex flex-wrap gap-2 mt-2">
                    {optimizedSourceStatus.config.clustering.columns.map((col) => (
                      <span key={col} className="px-2 py-1 bg-blue-50 text-blue-700 text-xs rounded-lg">
                        {col}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-2 pt-2">
                <button
                  onClick={handlePreviewOptimizedSourceSql}
                  className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                >
                  Preview SQL
                </button>
                <button
                  onClick={() => refreshOptimizedSourceMutation.mutate(true)}
                  disabled={refreshOptimizedSourceMutation.isPending}
                  className="px-3 py-1.5 text-sm bg-blue-100 text-blue-700 rounded hover:bg-blue-200 disabled:opacity-50"
                >
                  {refreshOptimizedSourceMutation.isPending ? 'Refreshing...' : 'Incremental Refresh'}
                </button>
                <button
                  onClick={() => refreshOptimizedSourceMutation.mutate(false)}
                  disabled={refreshOptimizedSourceMutation.isPending}
                  className="px-3 py-1.5 text-sm bg-orange-100 text-orange-700 rounded hover:bg-orange-200 disabled:opacity-50"
                >
                  Full Refresh
                </button>
                <button
                  onClick={() => {
                    if (confirm('Delete optimized source configuration and optionally drop the BigQuery table?')) {
                      const dropTable = confirm('Also drop the BigQuery table?')
                      deleteOptimizedSourceMutation.mutate(dropTable)
                    }
                  }}
                  disabled={deleteOptimizedSourceMutation.isPending}
                  className="px-3 py-1.5 text-sm text-red-600 border border-red-300 rounded hover:bg-red-50 disabled:opacity-50"
                >
                  {deleteOptimizedSourceMutation.isPending ? 'Deleting...' : 'Delete'}
                </button>
              </div>
            </div>
          ) : (
            // No optimized source - show analysis and create option
            <div className="space-y-4">
              {optimizedSourceAnalysis?.composite_keys && optimizedSourceAnalysis.composite_keys.length > 0 ? (
                <>
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <div className="flex items-start gap-3">
                      <svg className="w-5 h-5 text-yellow-600 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                      <div>
                        <p className="text-sm font-medium text-yellow-800">
                          {optimizedSourceAnalysis.composite_keys.length} composite keys detected
                        </p>
                        <p className="text-sm text-yellow-700 mt-1">
                          Your metrics use CONCAT operations that can be precomputed. Creating an optimized source table
                          will speed up rollup creation by avoiding runtime CONCAT calculations.
                        </p>
                      </div>
                    </div>
                  </div>

                  <div>
                    <span className="text-sm text-gray-500">Keys to precompute:</span>
                    <div className="mt-2 space-y-2">
                      {optimizedSourceAnalysis.composite_keys.map((key) => (
                        <div key={key.key_column_name} className="flex items-center gap-3 text-sm">
                          <span className="px-2 py-1 bg-purple-50 text-purple-700 rounded font-mono text-xs">
                            {key.key_column_name}
                          </span>
                          <span className="text-gray-400">=</span>
                          <span className="text-gray-600">
                            CONCAT({key.source_columns.join(', ')})
                          </span>
                          <span className="text-gray-400 text-xs">
                            used by {key.metric_ids.length} metric(s)
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>

                  {optimizedSourceAnalysis.recommended_clustering && optimizedSourceAnalysis.recommended_clustering.length > 0 && (
                    <div>
                      <span className="text-sm text-gray-500">Suggested clustering columns:</span>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {optimizedSourceAnalysis.recommended_clustering.map((col) => (
                          <span key={col} className="px-2 py-1 bg-blue-50 text-blue-700 text-xs rounded-lg">
                            {col}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}

                  {!showOptimizedSourceForm ? (
                    <div className="flex gap-2 pt-2">
                      <button
                        onClick={handlePreviewOptimizedSourceSql}
                        className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                      >
                        Preview SQL
                      </button>
                      <button
                        onClick={() => {
                          // Pre-fill with default values from rollup settings
                          setOptSourceProject(defaultTargetProject || '')
                          setOptSourceDataset(defaultTargetDataset || '')
                          setShowOptimizedSourceForm(true)
                        }}
                        className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700"
                      >
                        Create Optimized Source
                      </button>
                    </div>
                  ) : (
                    <div className="mt-4 p-4 border border-purple-200 rounded-lg bg-purple-50">
                      <h4 className="font-medium text-purple-900 mb-3">Create Optimized Source Table</h4>
                      <div className="space-y-3">
                        <div>
                          <label className="block text-sm text-purple-800 mb-1">
                            Target Project
                          </label>
                          <input
                            type="text"
                            value={optSourceProject}
                            onChange={(e) => setOptSourceProject(e.target.value)}
                            placeholder="e.g., my-analytics-project"
                            className="w-full px-3 py-2 border border-purple-300 rounded-lg text-sm"
                          />
                        </div>
                        <div>
                          <label className="block text-sm text-purple-800 mb-1">
                            Target Dataset
                          </label>
                          <input
                            type="text"
                            value={optSourceDataset}
                            onChange={(e) => setOptSourceDataset(e.target.value)}
                            placeholder="e.g., analytics_rollups"
                            className="w-full px-3 py-2 border border-purple-300 rounded-lg text-sm"
                          />
                        </div>
                        <div className="flex gap-2 pt-2">
                          <button
                            onClick={() => {
                              setShowOptimizedSourceForm(false)
                              setOptSourceProject('')
                              setOptSourceDataset('')
                            }}
                            className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                          >
                            Cancel
                          </button>
                          <button
                            onClick={() => createOptimizedSourceMutation.mutate({
                              targetProject: optSourceProject || undefined,
                              targetDataset: optSourceDataset || undefined,
                            })}
                            disabled={createOptimizedSourceMutation.isPending}
                            className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50"
                          >
                            {createOptimizedSourceMutation.isPending ? 'Creating...' : 'Create'}
                          </button>
                        </div>
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <div className="text-center py-6 text-gray-500">
                  <p>No composite keys detected in your metrics.</p>
                  <p className="text-sm mt-1">
                    Optimized source tables are useful when you have metrics using COUNT DISTINCT with CONCAT.
                  </p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Optimized Source SQL Preview Modal */}
      {showOptimizedSourceSql && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-3xl max-h-[90vh] overflow-y-auto">
            <h3 className="text-lg font-semibold mb-4">Optimized Source SQL Preview</h3>
            <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-x-auto text-sm whitespace-pre-wrap">
              {optimizedSourceSqlContent}
            </pre>
            <div className="flex justify-end mt-4">
              <button
                onClick={() => {
                  setShowOptimizedSourceSql(false)
                  setOptimizedSourceSqlContent('')
                }}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

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
                      <span className="text-gray-500 italic">Auto-included from schema ({totalAutoMetrics} metrics)</span>
                    </div>
                    {(rollup.target_project || rollup.target_dataset) && (
                      <div>
                        <span className="font-medium">Target:</span>{' '}
                        {rollup.target_project || '(default project)'}.
                        {rollup.target_dataset || '(default dataset)'}.
                        {rollup.target_table_name || rollup.id}
                      </div>
                    )}
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
                    {/* Rollup Status (missing dates/metrics) */}
                    {rollupStatuses[rollup.id] && (
                      <div className="mt-2 p-2 bg-gray-50 rounded text-xs">
                        {rollupStatuses[rollup.id].is_up_to_date ? (
                          <span className="text-green-600">Up to date</span>
                        ) : (
                          <div className="space-y-1">
                            {rollupStatuses[rollup.id].missing_dates_count > 0 && (
                              <div className="text-orange-600">
                                Missing {rollupStatuses[rollup.id].missing_dates_count} dates
                              </div>
                            )}
                            {rollupStatuses[rollup.id].missing_metrics_count > 0 && (
                              <div className="text-orange-600">
                                Missing {rollupStatuses[rollup.id].missing_metrics_count} metrics:{' '}
                                {rollupStatuses[rollup.id].missing_metrics.join(', ')}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </div>
                <div className="flex flex-col gap-2 items-end">
                  <div className="flex gap-2">
                    <button
                      onClick={() => handlePreviewSql(rollup.id)}
                      className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                    >
                      Preview SQL
                    </button>
                    <button
                      onClick={() => handleCheckStatus(rollup.id)}
                      className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50"
                    >
                      Check Status
                    </button>
                    <button
                      onClick={() => setDeleteConfirm(rollup.id)}
                      className="px-3 py-1.5 text-sm text-red-600 border border-red-300 rounded hover:bg-red-50"
                    >
                      Delete
                    </button>
                  </div>
                  <div className="flex gap-2 items-center">
                    <select
                      value={refreshMode}
                      onChange={(e) => setRefreshMode(e.target.value as 'full' | 'incremental')}
                      className="px-2 py-1.5 text-sm border border-gray-300 rounded"
                    >
                      <option value="incremental">Incremental</option>
                      <option value="full">Full Refresh</option>
                    </select>
                    <button
                      onClick={() => refreshMutation.mutate({
                        rollupId: rollup.id,
                        incremental: refreshMode === 'incremental'
                      })}
                      disabled={refreshMutation.isPending}
                      className="px-3 py-1.5 text-sm bg-blue-100 text-blue-700 rounded hover:bg-blue-200 disabled:opacity-50"
                    >
                      {refreshMutation.isPending ? 'Refreshing...' : 'Refresh'}
                    </button>
                  </div>
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

              {/* Auto-included Metrics Info */}
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <div className="flex items-start gap-3">
                  <svg className="w-5 h-5 text-blue-600 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <div>
                    <p className="text-sm font-medium text-blue-800">Metrics are auto-included</p>
                    <p className="text-sm text-blue-700 mt-1">
                      All {totalAutoMetrics} volume metrics from your schema will be automatically included in this rollup.
                    </p>
                    <p className="text-xs text-blue-600 mt-2">
                      When you add new metrics to your schema, use &quot;Incremental Refresh&quot; to automatically add them to existing rollups.
                    </p>
                  </div>
                </div>
              </div>

              {/* Target Project Override */}
              <div>
                <label className="block text-sm font-medium mb-1">
                  Target Project Override{' '}
                  <span className="text-gray-500">(optional)</span>
                </label>
                <input
                  type="text"
                  value={formTargetProject}
                  onChange={(e) => setFormTargetProject(e.target.value)}
                  placeholder={`Leave empty to use default (${defaultTargetProject || 'source project'})`}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
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
