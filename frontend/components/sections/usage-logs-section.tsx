'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchQueryLogs,
  fetchUsageStats,
  fetchTodayUsageStats,
  fetchUsageTimeSeries,
  clearQueryLogs
} from '@/lib/api'
import type { QueryLogEntry, UsageTimeSeries } from '@/lib/types'
import { getQueryTypeInfo, getCostLevelColor, getCostLevelText } from '@/lib/query-types'

export function UsageLogsSection() {
  const queryClient = useQueryClient()
  const [page, setPage] = useState(0)
  const [pageSize] = useState(50)
  const [queryTypeFilter, setQueryTypeFilter] = useState<string>('')
  const [dateRange, setDateRange] = useState<{ start: string; end: string } | null>(null)
  const [showClearConfirm, setShowClearConfirm] = useState(false)

  // Fetch usage stats
  const { data: stats } = useQuery({
    queryKey: ['usage-stats', dateRange],
    queryFn: () => fetchUsageStats(dateRange ? {
      start_date: dateRange.start,
      end_date: dateRange.end
    } : undefined),
  })

  // Fetch today's stats
  const { data: todayStats } = useQuery({
    queryKey: ['usage-stats-today'],
    queryFn: fetchTodayUsageStats,
  })

  // Fetch time series data
  const { data: timeSeries } = useQuery({
    queryKey: ['usage-timeseries', dateRange],
    queryFn: () => fetchUsageTimeSeries({
      granularity: 'daily',
      start_date: dateRange?.start,
      end_date: dateRange?.end,
    }),
  })

  // Fetch query logs
  const { data: logsData, isLoading } = useQuery({
    queryKey: ['query-logs', page, queryTypeFilter, dateRange],
    queryFn: () => fetchQueryLogs({
      limit: pageSize,
      offset: page * pageSize,
      query_type: queryTypeFilter || undefined,
      start_date: dateRange?.start,
      end_date: dateRange?.end,
    }),
  })

  // Clear logs mutation
  const clearMutation = useMutation({
    mutationFn: clearQueryLogs,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['query-logs'] })
      queryClient.invalidateQueries({ queryKey: ['usage-stats'] })
      queryClient.invalidateQueries({ queryKey: ['usage-stats-today'] })
      queryClient.invalidateQueries({ queryKey: ['usage-timeseries'] })
      setShowClearConfirm(false)
    },
  })

  const totalPages = logsData ? Math.ceil(logsData.total / pageSize) : 0

  // Format bytes to human readable
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
  }

  // Format duration
  const formatDuration = (ms: number): string => {
    if (ms < 1000) return `${ms}ms`
    return `${(ms / 1000).toFixed(2)}s`
  }

  // Export to CSV
  const exportToCSV = () => {
    if (!logsData?.logs) return

    const headers = ['ID', 'Timestamp', 'Endpoint', 'Query Type', 'Bytes Processed', 'Bytes Billed', 'Duration (ms)', 'Rows']
    const rows = logsData.logs.map(log => [
      log.id,
      log.timestamp,
      log.endpoint,
      log.query_type,
      log.bytes_processed,
      log.bytes_billed,
      log.execution_time_ms,
      log.row_count || 0,
    ])

    const csv = [headers, ...rows].map(row => row.join(',')).join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `bigquery-logs-${new Date().toISOString()}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">BigQuery Usage Logs</h2>
          <p className="text-gray-600 mt-1">Monitor your BigQuery usage and costs</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={exportToCSV}
            disabled={!logsData?.logs?.length}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
          >
            Export CSV
          </button>
          <button
            onClick={() => setShowClearConfirm(true)}
            className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700"
          >
            Clear Logs
          </button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Today's Queries */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600">Today's Queries</div>
          <div className="text-3xl font-bold text-gray-900 mt-2">
            {todayStats?.total_queries.toLocaleString() || 0}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            All Time: {stats?.total_queries.toLocaleString() || 0}
          </div>
        </div>

        {/* Data Scanned */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600">Data Scanned</div>
          <div className="text-3xl font-bold text-gray-900 mt-2">
            {stats?.total_gb_billed.toFixed(2) || 0} GB
          </div>
          <div className="text-xs text-gray-500 mt-1">
            Processed: {stats?.total_gb_processed.toFixed(2) || 0} GB
          </div>
        </div>

        {/* Estimated Cost */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600">Estimated Cost</div>
          <div className="text-3xl font-bold text-green-600 mt-2">
            ${stats?.estimated_cost_usd.toFixed(4) || '0.0000'}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            $5 per TB billed
          </div>
        </div>

        {/* Avg Query Time */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600">Avg Query Time</div>
          <div className="text-3xl font-bold text-gray-900 mt-2">
            {stats?.avg_execution_time_ms.toFixed(0) || 0}ms
          </div>
          <div className="text-xs text-gray-500 mt-1">
            Max: {stats?.max_execution_time_ms || 0}ms
          </div>
        </div>
      </div>

      {/* Query Type Breakdown */}
      {stats?.by_query_type && stats.by_query_type.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Queries by Type</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {stats.by_query_type.map((item) => {
              const info = getQueryTypeInfo(item.query_type)
              return (
                <div
                  key={item.query_type}
                  className="border rounded-lg p-4 relative group hover:shadow-md transition-shadow"
                  title={info.description}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-lg">{info.costIcon}</span>
                    <div className="text-sm font-medium text-gray-600">{item.query_type}</div>
                  </div>
                  <div className="text-2xl font-bold text-gray-900 mt-1">
                    {item.count.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {formatBytes(item.bytes_processed)}
                  </div>
                  <div className={`mt-2 inline-block px-2 py-0.5 rounded text-xs font-medium ${
                    getCostLevelColor(info.costLevel)
                  }`}>
                    {getCostLevelText(info.costLevel)}
                  </div>
                  {/* Hover tooltip */}
                  <div className="invisible group-hover:visible absolute z-10 w-64 p-3 bg-gray-900 text-white text-xs rounded-lg shadow-lg left-0 top-full mt-2">
                    <div className="font-bold mb-1">{info.label}</div>
                    <div className="text-gray-300 mb-2">{info.description}</div>
                    <div className="text-gray-400">{info.whatItDoes}</div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Time Series Chart */}
      {timeSeries && timeSeries.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Usage Over Time</h3>
          <div className="space-y-2">
            {timeSeries.slice(-10).map((item, idx) => (
              <div key={idx} className="flex items-center gap-4">
                <div className="text-sm text-gray-600 w-24">{item.date}</div>
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <div
                      className="bg-blue-500 h-6 rounded"
                      style={{ width: `${(item.queries / Math.max(...timeSeries.map(t => t.queries))) * 100}%` }}
                    />
                    <span className="text-sm text-gray-600">{item.queries} queries</span>
                  </div>
                </div>
                <div className="text-sm text-gray-600 w-32 text-right">
                  {item.gb_billed.toFixed(2)} GB (${item.estimated_cost_usd.toFixed(4)})
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex gap-4 items-end">
          <div className="flex-1">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Query Type
            </label>
            <select
              value={queryTypeFilter}
              onChange={(e) => {
                setQueryTypeFilter(e.target.value)
                setPage(0)
              }}
              className="w-full border border-gray-300 rounded-lg px-3 py-2"
            >
              <option value="">All Types</option>
              <option value="kpi">🟢 KPI - Low cost (single aggregation)</option>
              <option value="trends">🟡 Trends - Moderate cost (time-series)</option>
              <option value="breakdown">🟡 Breakdown - Moderate cost (dimension groups)</option>
              <option value="search_terms">🟠 Search Terms - High cost (many rows)</option>
              <option value="pivot">🟠 Pivot - High cost (complex grouping)</option>
              <option value="dimension_values">🟢 Dimension Values - Very low cost (1 column)</option>
              <option value="query_all">🔴 Export All - VERY HIGH cost (SELECT *)</option>
            </select>
          </div>

          <div className="flex-1">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Start Date
            </label>
            <input
              type="date"
              value={dateRange?.start || ''}
              onChange={(e) => {
                setDateRange(prev => ({ start: e.target.value, end: prev?.end || '' }))
                setPage(0)
              }}
              className="w-full border border-gray-300 rounded-lg px-3 py-2"
            />
          </div>

          <div className="flex-1">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              End Date
            </label>
            <input
              type="date"
              value={dateRange?.end || ''}
              onChange={(e) => {
                setDateRange(prev => ({ start: prev?.start || '', end: e.target.value }))
                setPage(0)
              }}
              className="w-full border border-gray-300 rounded-lg px-3 py-2"
            />
          </div>

          <button
            onClick={() => {
              setDateRange(null)
              setQueryTypeFilter('')
              setPage(0)
            }}
            className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
          >
            Reset
          </button>
        </div>
      </div>

      {/* Query Logs Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Endpoint</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Data Scanned</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Duration</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Rows</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {isLoading ? (
                <tr>
                  <td colSpan={7} className="px-6 py-12 text-center text-gray-500">
                    Loading logs...
                  </td>
                </tr>
              ) : logsData?.logs && logsData.logs.length > 0 ? (
                logsData.logs.map((log) => (
                  <tr key={log.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 text-sm text-gray-900">{log.id}</td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {new Date(log.timestamp).toLocaleString()}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600 font-mono text-xs">
                      {log.endpoint}
                    </td>
                    <td className="px-6 py-4">
                      <div
                        className="group relative inline-block"
                        title={(() => {
                          const info = getQueryTypeInfo(log.query_type)
                          return `${info.costIcon} ${info.label}\n${info.description}\n\nWhat it does: ${info.whatItDoes}`
                        })()}
                      >
                        <span className={`px-2 py-1 text-xs font-medium rounded border cursor-help ${
                          (() => {
                            const info = getQueryTypeInfo(log.query_type)
                            return getCostLevelColor(info.costLevel)
                          })()
                        }`}>
                          {(() => {
                            const info = getQueryTypeInfo(log.query_type)
                            return `${info.costIcon} ${log.query_type}`
                          })()}
                        </span>
                        {/* Hover Tooltip */}
                        <div className="invisible group-hover:visible absolute z-10 w-80 p-3 bg-gray-900 text-white text-xs rounded-lg shadow-lg -translate-y-full -translate-x-1/2 left-1/2 mb-2">
                          <div className="font-bold mb-1">
                            {getQueryTypeInfo(log.query_type).costIcon} {getQueryTypeInfo(log.query_type).label}
                          </div>
                          <div className="mb-2 text-gray-300">
                            {getQueryTypeInfo(log.query_type).description}
                          </div>
                          <div className="text-yellow-300 text-xs font-semibold mb-1">
                            Cost Level: {getCostLevelText(getQueryTypeInfo(log.query_type).costLevel)}
                          </div>
                          <div className="text-gray-400 text-xs">
                            What it does: {getQueryTypeInfo(log.query_type).whatItDoes}
                          </div>
                          <div className="text-gray-400 text-xs mt-1">
                            Example: {getQueryTypeInfo(log.query_type).example}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900">
                      {formatBytes(log.bytes_billed)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900">
                      {formatDuration(log.execution_time_ms)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {log.row_count?.toLocaleString() || '-'}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={7} className="px-6 py-12 text-center text-gray-500">
                    No logs found. Logs will appear after you run some queries.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-6 py-4 border-t bg-gray-50 flex items-center justify-between">
            <div className="text-sm text-gray-600">
              Showing {page * pageSize + 1} to {Math.min((page + 1) * pageSize, logsData?.total || 0)} of{' '}
              {logsData?.total || 0} logs
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setPage(p => Math.max(0, p - 1))}
                disabled={page === 0}
                className="px-4 py-2 bg-white border rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Previous
              </button>
              <div className="flex items-center gap-2">
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  let pageNum = i
                  if (totalPages > 5) {
                    if (page < 3) {
                      pageNum = i
                    } else if (page > totalPages - 3) {
                      pageNum = totalPages - 5 + i
                    } else {
                      pageNum = page - 2 + i
                    }
                  }
                  return (
                    <button
                      key={pageNum}
                      onClick={() => setPage(pageNum)}
                      className={`px-3 py-2 rounded-lg ${
                        page === pageNum
                          ? 'bg-blue-600 text-white'
                          : 'bg-white border hover:bg-gray-50'
                      }`}
                    >
                      {pageNum + 1}
                    </button>
                  )
                })}
              </div>
              <button
                onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                disabled={page >= totalPages - 1}
                className="px-4 py-2 bg-white border rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Clear Confirmation Modal */}
      {showClearConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Clear All Logs?</h3>
            <p className="text-gray-600 mb-6">
              This will permanently delete all query logs. This action cannot be undone.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowClearConfirm(false)}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
              >
                Cancel
              </button>
              <button
                onClick={() => clearMutation.mutate()}
                disabled={clearMutation.isPending}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
              >
                {clearMutation.isPending ? 'Clearing...' : 'Clear All Logs'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
