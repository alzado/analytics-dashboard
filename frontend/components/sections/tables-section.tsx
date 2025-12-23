'use client'

import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchTables, fetchAppSettings, updateAppSettings } from '@/lib/api'
import { BigQueryInfoSection } from './bigquery-info-section'
import { SchemaSection } from './schema-section'
import { CreateTableModal } from '@/components/modals/create-table-modal'
import { Database, Plus, Settings, CheckCircle } from 'lucide-react'

export function TablesSection() {
  const queryClient = useQueryClient()
  const [selectedTableId, setSelectedTableId] = useState<string>('')
  const [subTab, setSubTab] = useState<'info' | 'schema'>('info')
  const [globalBillingProject, setGlobalBillingProject] = useState<string>('')
  const [showSettingsSuccess, setShowSettingsSuccess] = useState(false)
  const [isCreateTableModalOpen, setIsCreateTableModalOpen] = useState(false)

  // Fetch available tables
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: () => fetchTables(),
  })

  // Fetch global app settings
  const { data: appSettings } = useQuery({
    queryKey: ['app-settings'],
    queryFn: () => fetchAppSettings(),
  })

  // Update global billing project when settings load
  useEffect(() => {
    if (appSettings?.default_billing_project) {
      setGlobalBillingProject(appSettings.default_billing_project)
    }
  }, [appSettings])

  // Mutation for updating settings
  const updateSettingsMutation = useMutation({
    mutationFn: updateAppSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['app-settings'] })
      setShowSettingsSuccess(true)
      setTimeout(() => setShowSettingsSuccess(false), 3000)
    },
  })

  const tables = tablesData?.tables || []

  // Set first table as selected if not set
  useEffect(() => {
    if (!selectedTableId && tables.length > 0) {
      setSelectedTableId(tables[0].table_id)
    }
  }, [tables, selectedTableId])

  const handleSaveGlobalSettings = () => {
    updateSettingsMutation.mutate({
      default_billing_project: globalBillingProject || null,
    })
  }

  // Show empty state if no tables
  if (tables.length === 0) {
    return (
      <>
        <div className="bg-white rounded-lg border border-gray-200 p-12">
          <div className="text-center max-w-md mx-auto">
            <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-100 rounded-full mb-4">
              <Database className="w-8 h-8 text-blue-600" />
            </div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">No Tables Configured</h2>
            <p className="text-gray-600 mb-6">
              Get started by creating your first BigQuery table connection. Once configured, you'll be able to use the Editor to query and visualize your data.
            </p>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
              <p className="text-sm text-blue-800">
                <strong>Note:</strong> The Editor and Dashboards will only work after you create at least one table connection.
              </p>
            </div>
            <button
              onClick={() => setIsCreateTableModalOpen(true)}
              className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2 mx-auto"
            >
              <Plus size={20} />
              Add Your First Table
            </button>
          </div>
        </div>
        <CreateTableModal
          isOpen={isCreateTableModalOpen}
          onClose={() => setIsCreateTableModalOpen(false)}
        />
      </>
    )
  }

  return (
    <div className="space-y-6">
      {/* Global Settings */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <Settings className="text-gray-600" size={20} />
          <h2 className="text-lg font-semibold">Global Settings</h2>
          {showSettingsSuccess && (
            <span className="flex items-center gap-1 text-green-600 text-sm ml-auto">
              <CheckCircle size={16} />
              Saved
            </span>
          )}
        </div>
        <div className="flex items-end gap-4">
          <div className="flex-1">
            <label htmlFor="global_billing_project" className="block text-sm font-medium text-gray-700 mb-1">
              Default Billing Project
            </label>
            <input
              type="text"
              id="global_billing_project"
              value={globalBillingProject}
              onChange={(e) => setGlobalBillingProject(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              placeholder="my-billing-project"
            />
            <p className="text-xs text-gray-500 mt-1">
              Default GCP project to bill queries to. Used when a table doesn't have its own billing project set.
            </p>
          </div>
          <button
            onClick={handleSaveGlobalSettings}
            disabled={updateSettingsMutation.isPending}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {updateSettingsMutation.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>

      {/* Table Selector */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-end gap-4">
          <div className="flex-1">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Select Table:
            </label>
            <select
              value={selectedTableId}
              onChange={(e) => setSelectedTableId(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              {tables.map((table) => (
                <option key={table.table_id} value={table.table_id}>
                  {table.name} ({table.project_id}.{table.dataset}.{table.table})
                </option>
              ))}
            </select>
          </div>
          <button
            onClick={() => setIsCreateTableModalOpen(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
          >
            <Plus size={18} />
            Add Table
          </button>
        </div>
      </div>

      {/* Sub-tabs for Info and Schema */}
      <div className="bg-white rounded-lg border border-gray-200">
        <div className="border-b border-gray-200">
          <nav className="flex -mb-px">
            <button
              onClick={() => setSubTab('info')}
              className={`
                px-6 py-3 border-b-2 font-medium text-sm
                ${subTab === 'info'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }
              `}
            >
              Connection & Info
            </button>
            <button
              onClick={() => setSubTab('schema')}
              className={`
                px-6 py-3 border-b-2 font-medium text-sm
                ${subTab === 'schema'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }
              `}
            >
              Schema Management
            </button>
          </nav>
        </div>

        <div className="p-6">
          {subTab === 'info' && <BigQueryInfoSection tableId={selectedTableId} />}
          {subTab === 'schema' && <SchemaSection tableId={selectedTableId} />}
        </div>
      </div>

      {/* Create Table Modal */}
      <CreateTableModal
        isOpen={isCreateTableModalOpen}
        onClose={() => setIsCreateTableModalOpen(false)}
      />
    </div>
  )
}
