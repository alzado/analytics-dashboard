'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchCacheStats,
  clearAllCache,
  clearCacheByTable,
  clearCacheByQueryType,
  fetchTables,
  type CacheStats
} from '@/lib/api'

const QUERY_TYPE_LABELS: Record<string, string> = {
  'pivot': 'Pivot Tables',
  'pivot_children': 'Pivot Drill-down',
  'pivot_totals': 'Pivot Totals',
  'pivot_date_range': 'Date Range',
  'pivot_count': 'Pagination Count',
  'kpi': 'KPI Overview',
  'trends': 'Trends',
  'dimension_values': 'Dimension Values',
  'filter_options': 'Filter Options',
  'significance': 'Significance Tests',
  'calculated_dimension_values': 'Calculated Dimensions'
}

export function CacheManagementSection() {
  const queryClient = useQueryClient()
  const [showClearAllConfirm, setShowClearAllConfirm] = useState(false)
  const [selectedTable, setSelectedTable] = useState<string>('')
  const [selectedQueryType, setSelectedQueryType] = useState<string>('')

  // Fetch cache stats
  const { data: stats, isLoading, refetch } = useQuery({
    queryKey: ['cache-stats'],
    queryFn: fetchCacheStats,
  })

  // Fetch tables for dropdown
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: () => fetchTables(),
  })

  // Clear mutations
  const clearAllMutation = useMutation({
    mutationFn: clearAllCache,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cache-stats'] })
      setShowClearAllConfirm(false)
    },
  })

  const clearTableMutation = useMutation({
    mutationFn: clearCacheByTable,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cache-stats'] })
      setSelectedTable('')
    },
  })

  const clearTypeMutation = useMutation({
    mutationFn: clearCacheByQueryType,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cache-stats'] })
      setSelectedQueryType('')
    },
  })

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
  }

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-'
    const date = new Date(dateStr)
    return date.toLocaleString()
  }

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-gray-200 rounded w-1/4"></div>
          <div className="grid grid-cols-4 gap-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-24 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-xl font-bold text-gray-900">Query Cache</h2>
          <p className="text-gray-600 mt-1 text-sm">
            Cached BigQuery results for faster dashboard loading
          </p>
        </div>
        <button
          onClick={() => refetch()}
          className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 flex items-center gap-2 text-sm"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh
        </button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-4 border border-gray-200">
          <div className="text-sm text-gray-600">Cached Entries</div>
          <div className="text-2xl font-bold text-gray-900">
            {stats?.total_entries.toLocaleString() || 0}
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border border-gray-200">
          <div className="text-sm text-gray-600">Cache Size</div>
          <div className="text-2xl font-bold text-gray-900">
            {stats?.total_size_mb.toFixed(2) || 0} MB
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border border-gray-200">
          <div className="text-sm text-gray-600">Total Hits</div>
          <div className="text-2xl font-bold text-green-600">
            {stats?.total_hits.toLocaleString() || 0}
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border border-gray-200">
          <div className="text-sm text-gray-600">Avg Hits/Entry</div>
          <div className="text-2xl font-bold text-gray-900">
            {stats?.avg_hits_per_entry.toFixed(1) || 0}
          </div>
        </div>
      </div>

      {/* Cache Age Info */}
      {(stats?.oldest_entry || stats?.newest_entry) && (
        <div className="bg-gray-50 rounded-lg p-4 text-sm text-gray-600">
          <div className="flex gap-8">
            <div>
              <span className="font-medium">Oldest entry:</span> {formatDate(stats?.oldest_entry || null)}
            </div>
            <div>
              <span className="font-medium">Newest entry:</span> {formatDate(stats?.newest_entry || null)}
            </div>
          </div>
        </div>
      )}

      {/* Clear Actions */}
      <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Clear Cache</h3>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Clear All */}
          <div className="border border-gray-200 rounded-lg p-4">
            <h4 className="font-medium text-gray-700 mb-2">Clear All</h4>
            <p className="text-sm text-gray-500 mb-3">
              Remove all {stats?.total_entries || 0} cached query results
            </p>
            <button
              onClick={() => setShowClearAllConfirm(true)}
              disabled={!stats?.total_entries}
              className="w-full px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-sm font-medium"
            >
              Clear All Cache
            </button>
          </div>

          {/* Clear by Table */}
          <div className="border border-gray-200 rounded-lg p-4">
            <h4 className="font-medium text-gray-700 mb-2">Clear by Table</h4>
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 mb-2 text-sm"
            >
              <option value="">Select table...</option>
              {tablesData?.tables.map((table) => (
                <option key={table.table_id} value={table.table_id}>
                  {table.name}
                </option>
              ))}
            </select>
            <button
              onClick={() => selectedTable && clearTableMutation.mutate(selectedTable)}
              disabled={!selectedTable || clearTableMutation.isPending}
              className="w-full px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-sm font-medium"
            >
              {clearTableMutation.isPending ? 'Clearing...' : 'Clear Table Cache'}
            </button>
          </div>

          {/* Clear by Query Type */}
          <div className="border border-gray-200 rounded-lg p-4">
            <h4 className="font-medium text-gray-700 mb-2">Clear by Query Type</h4>
            <select
              value={selectedQueryType}
              onChange={(e) => setSelectedQueryType(e.target.value)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 mb-2 text-sm"
            >
              <option value="">Select type...</option>
              {Object.entries(QUERY_TYPE_LABELS).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
            <button
              onClick={() => selectedQueryType && clearTypeMutation.mutate(selectedQueryType)}
              disabled={!selectedQueryType || clearTypeMutation.isPending}
              className="w-full px-4 py-2 bg-yellow-600 text-white rounded-lg hover:bg-yellow-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-sm font-medium"
            >
              {clearTypeMutation.isPending ? 'Clearing...' : 'Clear Type Cache'}
            </button>
          </div>
        </div>
      </div>

      {/* Breakdown Tables */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* By Table */}
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">By Table</h3>
          {stats?.by_table && stats.by_table.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-600 border-b">
                    <th className="py-2 font-medium">Table</th>
                    <th className="py-2 font-medium text-right">Entries</th>
                    <th className="py-2 font-medium text-right">Size</th>
                    <th className="py-2 font-medium text-right">Hits</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.by_table.map((item) => (
                    <tr key={item.table_id} className="border-b border-gray-100">
                      <td className="py-2 font-mono text-xs">{item.table_id}</td>
                      <td className="py-2 text-right">{item.entries.toLocaleString()}</td>
                      <td className="py-2 text-right">{formatBytes(item.size_bytes)}</td>
                      <td className="py-2 text-right text-green-600">{item.hits.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-gray-500 text-sm">No cached entries</p>
          )}
        </div>

        {/* By Query Type */}
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">By Query Type</h3>
          {stats?.by_query_type && stats.by_query_type.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-600 border-b">
                    <th className="py-2 font-medium">Type</th>
                    <th className="py-2 font-medium text-right">Entries</th>
                    <th className="py-2 font-medium text-right">Size</th>
                    <th className="py-2 font-medium text-right">Hits</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.by_query_type.map((item) => (
                    <tr key={item.query_type} className="border-b border-gray-100">
                      <td className="py-2">{QUERY_TYPE_LABELS[item.query_type] || item.query_type}</td>
                      <td className="py-2 text-right">{item.entries.toLocaleString()}</td>
                      <td className="py-2 text-right">{formatBytes(item.size_bytes)}</td>
                      <td className="py-2 text-right text-green-600">{item.hits.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-gray-500 text-sm">No cached entries</p>
          )}
        </div>
      </div>

      {/* Clear All Confirmation Modal */}
      {showClearAllConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Clear All Cache?</h3>
            <p className="text-gray-600 mb-6">
              This will remove all {stats?.total_entries?.toLocaleString() || 0} cached query results.
              Queries will need to be re-executed from BigQuery.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowClearAllConfirm(false)}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 text-sm font-medium"
              >
                Cancel
              </button>
              <button
                onClick={() => clearAllMutation.mutate()}
                disabled={clearAllMutation.isPending}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm font-medium"
              >
                {clearAllMutation.isPending ? 'Clearing...' : 'Clear All Cache'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
