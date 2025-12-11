'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { X } from 'lucide-react'
import { addWidgetToDashboard, fetchTables, type WidgetCreateRequest } from '@/lib/api'

interface AddWidgetDialogProps {
  dashboardId: string
  onClose: () => void
}

export function AddWidgetDialog({ dashboardId, onClose }: AddWidgetDialogProps) {
  const [widgetType, setWidgetType] = useState<'table' | 'chart'>('table')
  const [tableId, setTableId] = useState('')
  const [title, setTitle] = useState('')
  const queryClient = useQueryClient()

  // Fetch available tables
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: () => fetchTables(),
  })

  const tables = tablesData?.tables || []

  const addWidgetMutation = useMutation({
    mutationFn: (request: WidgetCreateRequest) => addWidgetToDashboard(dashboardId, request),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })
      onClose()
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (title.trim() && tableId) {
      // Calculate next position (bottom of grid)
      const position = {
        x: 0,
        y: 999, // Will be placed at bottom
        w: widgetType === 'table' ? 12 : 6,
        h: widgetType === 'table' ? 4 : 3,
      }

      addWidgetMutation.mutate({
        type: widgetType,
        table_id: tableId,
        title: title.trim(),
        dimensions: [],
        table_dimensions: [],
        metrics: [],
        filters: {},
        start_date: null,
        end_date: null,
        chart_type: widgetType === 'chart' ? 'bar' : null,
        position,
      })
    }
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 max-w-md w-full shadow-xl">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold text-gray-900">Add Widget</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X className="h-6 w-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Widget Type *
            </label>
            <div className="grid grid-cols-2 gap-3">
              <button
                type="button"
                onClick={() => setWidgetType('table')}
                className={`p-4 border-2 rounded-lg text-left ${
                  widgetType === 'table'
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-300 hover:border-gray-400'
                }`}
              >
                <div className="font-semibold">Pivot Table</div>
                <div className="text-sm text-gray-600">Multi-dimensional data</div>
              </button>
              <button
                type="button"
                onClick={() => setWidgetType('chart')}
                className={`p-4 border-2 rounded-lg text-left ${
                  widgetType === 'chart'
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-300 hover:border-gray-400'
                }`}
              >
                <div className="font-semibold">Chart</div>
                <div className="text-sm text-gray-600">Visual representation</div>
              </button>
            </div>
          </div>

          <div className="mb-4">
            <label htmlFor="table" className="block text-sm font-medium text-gray-700 mb-2">
              Data Source *
            </label>
            <select
              id="table"
              value={tableId}
              onChange={(e) => setTableId(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            >
              <option value="">Select a table...</option>
              {tables.map((table) => (
                <option key={table.table_id} value={table.table_id}>
                  {table.name} ({table.project_id}.{table.dataset}.{table.table})
                </option>
              ))}
            </select>
          </div>

          <div className="mb-6">
            <label htmlFor="title" className="block text-sm font-medium text-gray-700 mb-2">
              Widget Title *
            </label>
            <input
              type="text"
              id="title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder={`My ${widgetType === 'table' ? 'Pivot Table' : 'Chart'}`}
              maxLength={100}
              required
            />
            <p className="mt-1 text-sm text-gray-500">
              You can configure dimensions, metrics, and filters after adding the widget.
            </p>
          </div>

          <div className="flex items-center justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={!title.trim() || !tableId || addWidgetMutation.isPending}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {addWidgetMutation.isPending ? 'Adding...' : 'Add Widget'}
            </button>
          </div>

          {addWidgetMutation.isError && (
            <div className="mt-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm">
              Failed to add widget. Please try again.
            </div>
          )}
        </form>
      </div>
    </div>
  )
}
