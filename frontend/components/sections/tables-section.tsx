'use client'

import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchTables } from '@/lib/api'
import { BigQueryInfoSection } from './bigquery-info-section'
import { SchemaSection } from './schema-section'
import { Database, Plus } from 'lucide-react'

export function TablesSection() {
  const [selectedTableId, setSelectedTableId] = useState<string>('')
  const [subTab, setSubTab] = useState<'info' | 'schema'>('info')

  // Fetch available tables
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: fetchTables,
  })

  const tables = tablesData?.tables || []

  // Set first table as selected if not set
  useEffect(() => {
    if (!selectedTableId && tables.length > 0) {
      setSelectedTableId(tables[0].table_id)
    }
  }, [tables, selectedTableId])

  // Show empty state if no tables
  if (tables.length === 0) {
    return (
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
          <BigQueryInfoSection tableId={undefined} />
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Table Selector */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center justify-between gap-4">
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
              <option value="__new__">+ Add New Table</option>
            </select>
          </div>
        </div>
      </div>

      {/* Show configuration form when adding new table */}
      {selectedTableId === '__new__' ? (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-xl font-bold mb-4">Add New Table</h2>
          <BigQueryInfoSection tableId={undefined} />
        </div>
      ) : (
        /* Sub-tabs for Info and Schema */
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
      )}
    </div>
  )
}
