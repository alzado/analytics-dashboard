'use client'

import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createTable, type TableCreateRequest } from '@/lib/api'
import { X } from 'lucide-react'

interface CreateTableModalProps {
  isOpen: boolean
  onClose: () => void
}

export function CreateTableModal({ isOpen, onClose }: CreateTableModalProps) {
  const queryClient = useQueryClient()
  const [formData, setFormData] = useState<TableCreateRequest>({
    name: '',
    project_id: '',
    dataset: '',
    table_name: '',
    credentials_json: '',
    allowed_min_date: null,
    allowed_max_date: null,
  })
  const [useADC, setUseADC] = useState(true)

  const createMutation = useMutation({
    mutationFn: createTable,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tables'] })
      onClose()
      // Reset form
      setFormData({
        name: '',
        project_id: '',
        dataset: '',
        table_name: '',
        credentials_json: '',
        allowed_min_date: null,
        allowed_max_date: null,
      })
      setUseADC(true)
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const payload = { ...formData }
    if (useADC) {
      payload.credentials_json = ''
    }
    createMutation.mutate(payload)
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="sticky top-0 bg-white border-b px-6 py-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">Add New Table</h2>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded"
          >
            <X size={20} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Table Name */}
          <div>
            <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-1">
              Table Name *
            </label>
            <input
              type="text"
              id="name"
              required
              value={formData.name}
              onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              placeholder="My Analytics Table"
            />
            <p className="text-xs text-gray-500 mt-1">
              A friendly name to identify this table
            </p>
          </div>

          {/* BigQuery Connection Details */}
          <div className="border-t pt-4">
            <h3 className="text-sm font-medium text-gray-700 mb-3">BigQuery Connection</h3>

            <div className="space-y-4">
              <div>
                <label htmlFor="project_id" className="block text-sm font-medium text-gray-700 mb-1">
                  Project ID *
                </label>
                <input
                  type="text"
                  id="project_id"
                  required
                  value={formData.project_id}
                  onChange={(e) => setFormData(prev => ({ ...prev, project_id: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="my-gcp-project"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label htmlFor="dataset" className="block text-sm font-medium text-gray-700 mb-1">
                    Dataset *
                  </label>
                  <input
                    type="text"
                    id="dataset"
                    required
                    value={formData.dataset}
                    onChange={(e) => setFormData(prev => ({ ...prev, dataset: e.target.value }))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    placeholder="analytics"
                  />
                </div>

                <div>
                  <label htmlFor="table_name" className="block text-sm font-medium text-gray-700 mb-1">
                    Table *
                  </label>
                  <input
                    type="text"
                    id="table_name"
                    required
                    value={formData.table_name}
                    onChange={(e) => setFormData(prev => ({ ...prev, table_name: e.target.value }))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    placeholder="search_data"
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Date Limits */}
          <div className="border-t pt-4">
            <h3 className="text-sm font-medium text-gray-700 mb-3">Date Access Limits (Optional)</h3>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label htmlFor="allowed_min_date" className="block text-sm font-medium text-gray-700 mb-1">
                  Minimum Date
                </label>
                <input
                  type="date"
                  id="allowed_min_date"
                  value={formData.allowed_min_date || ''}
                  onChange={(e) => setFormData(prev => ({ ...prev, allowed_min_date: e.target.value || null }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <div>
                <label htmlFor="allowed_max_date" className="block text-sm font-medium text-gray-700 mb-1">
                  Maximum Date
                </label>
                <input
                  type="date"
                  id="allowed_max_date"
                  value={formData.allowed_max_date || ''}
                  onChange={(e) => setFormData(prev => ({ ...prev, allowed_max_date: e.target.value || null }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>
          </div>

          {/* Authentication */}
          <div className="border-t pt-4">
            <h3 className="text-sm font-medium text-gray-700 mb-3">Authentication</h3>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={useADC}
                onChange={(e) => setUseADC(e.target.checked)}
                className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm text-gray-700">
                Use Google Cloud credentials (gcloud auth)
              </span>
            </label>
            <p className="text-xs text-gray-500 mt-1 ml-6">
              Requires: <code className="bg-gray-100 px-1 rounded">gcloud auth application-default login</code>
            </p>

            {!useADC && (
              <div className="mt-4">
                <label htmlFor="credentials_json" className="block text-sm font-medium text-gray-700 mb-1">
                  Service Account JSON *
                </label>
                <textarea
                  id="credentials_json"
                  required={!useADC}
                  value={formData.credentials_json}
                  onChange={(e) => setFormData(prev => ({ ...prev, credentials_json: e.target.value }))}
                  rows={8}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 font-mono text-sm"
                  placeholder='{"type": "service_account", "project_id": "...", ...}'
                />
              </div>
            )}
          </div>

          {/* Error Message */}
          {createMutation.isError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3">
              <p className="text-sm text-red-700">
                {createMutation.error instanceof Error
                  ? createMutation.error.message
                  : 'Failed to create table'}
              </p>
            </div>
          )}

          {/* Buttons */}
          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 font-medium"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={createMutation.isPending}
              className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed font-medium"
            >
              {createMutation.isPending ? 'Creating...' : 'Create Table'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
