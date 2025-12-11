'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchBigQueryInfo, configureBigQuery, disconnectBigQuery, fetchTables, createTable } from '@/lib/api'
import { Database, Calendar, HardDrive, Table, CheckCircle, AlertCircle, Settings } from 'lucide-react'
import type { BigQueryConfig } from '@/lib/types'

interface BigQueryInfoSectionProps {
  tableId?: string
}

export function BigQueryInfoSection({ tableId }: BigQueryInfoSectionProps) {
  const router = useRouter()
  const queryClient = useQueryClient()
  const [formData, setFormData] = useState<BigQueryConfig>({
    project_id: '',
    dataset: '',
    table: '',
    use_adc: true,  // Default to using Google Cloud credentials
    credentials_json: '',
    billing_project: null,  // Project for query billing (defaults to project_id if not set)
    allowed_min_date: null,
    allowed_max_date: null
  })
  const [tableName, setTableName] = useState<string>('')
  const [showSuccess, setShowSuccess] = useState(false)

  // Fetch available tables for table management section
  const { data: tablesData } = useQuery({
    queryKey: ['tables'],
    queryFn: () => fetchTables(),
  })

  const { data: info, isLoading, error } = useQuery({
    queryKey: ['bigquery-info', tableId],
    queryFn: () => fetchBigQueryInfo(tableId || undefined),
    enabled: true,
  })

  const configureMutation = useMutation({
    mutationFn: configureBigQuery,
    onSuccess: (response) => {
      console.log('Configuration response:', response)
      if (response.success) {
        setShowSuccess(true)
        // Refresh both tables list and bigquery info
        queryClient.invalidateQueries({ queryKey: ['bigquery-info'] })
        queryClient.invalidateQueries({ queryKey: ['tables'] })
        // Clear only credentials, keep connection details
        setFormData(prev => ({
          ...prev,
          use_adc: true,
          credentials_json: '',
          billing_project: null,
          allowed_min_date: null,
          allowed_max_date: null
        }))
        // Hide success message after 3 seconds
        setTimeout(() => {
          setShowSuccess(false)
        }, 3000)
      }
    },
    onError: (error) => {
      console.error('Configuration error:', error)
    }
  })

  const createTableMutation = useMutation({
    mutationFn: createTable,
    onSuccess: () => {
      setShowSuccess(true)
      queryClient.invalidateQueries({ queryKey: ['tables'] })
      // Clear form
      setFormData({
        project_id: '',
        dataset: '',
        table: '',
        use_adc: true,
        credentials_json: '',
        billing_project: null,
        allowed_min_date: null,
        allowed_max_date: null
      })
      setTableName('')
      setTimeout(() => {
        setShowSuccess(false)
      }, 3000)
    },
    onError: (error) => {
      console.error('Create table error:', error)
    }
  })

  const disconnectMutation = useMutation({
    mutationFn: disconnectBigQuery,
    onSuccess: (response) => {
      console.log('Disconnect response:', response)
      if (response.success) {
        queryClient.invalidateQueries({ queryKey: ['bigquery-info'] })
        queryClient.invalidateQueries({ queryKey: ['tables'] })
      }
    },
    onError: (error) => {
      console.error('Disconnect error:', error)
    }
  })

  // Pre-fill form with saved connection details when not connected
  useEffect(() => {
    if (info && info.connection_status !== 'connected') {
      // Only pre-fill if there are saved values
      if (info.project_id || info.dataset || info.table) {
        setFormData(prev => ({
          ...prev,
          project_id: info.project_id || '',
          dataset: info.dataset || '',
          table: info.table || ''
        }))
      }
    }
  }, [info])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()

    // If tableId is undefined, we're creating a new table
    if (!tableId) {
      createTableMutation.mutate({
        name: tableName,
        project_id: formData.project_id,
        dataset: formData.dataset,
        table: formData.table,
        credentials_json: formData.use_adc ? '' : formData.credentials_json,
        billing_project: formData.billing_project || undefined,
        allowed_min_date: null,
        allowed_max_date: null
      })
    } else {
      // Otherwise, update existing table (using legacy endpoint)
      configureMutation.mutate(formData)
    }
  }

  const handleInputChange = (field: keyof BigQueryConfig, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const handleDisconnect = () => {
    if (!tableId) return
    if (confirm('Are you sure you want to disconnect and delete this table? This action cannot be undone.')) {
      disconnectMutation.mutate(tableId)
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-center gap-2 text-red-700">
          <AlertCircle size={20} />
          <p className="font-medium">Error loading BigQuery information</p>
        </div>
        <p className="text-red-600 mt-2 text-sm">{(error as Error).message}</p>
      </div>
    )
  }

  if (!info) {
    return null
  }

  const isConnected = info.connection_status === 'connected'
  const tables = tablesData?.tables || []

  // Show configuration form if not connected
  if (!isConnected) {
    return (
      <div className="space-y-6">
        {/* Connection Status - only show if no tables exist */}
        {tables.length === 0 && (
          <div className="rounded-lg border p-6 bg-yellow-50 border-yellow-200">
            <div className="flex items-center gap-3">
              <AlertCircle className="text-yellow-600" size={24} />
              <div>
                <h2 className="text-lg font-semibold">BigQuery Not Configured</h2>
                <p className="text-sm text-yellow-700">
                  {info.connection_status || 'Please configure your BigQuery connection below'}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Success Message */}
        {showSuccess && (
          <div className="rounded-lg border p-6 bg-green-50 border-green-200">
            <div className="flex items-center gap-3">
              <CheckCircle className="text-green-600" size={24} />
              <div>
                <h2 className="text-lg font-semibold">Configuration Successful</h2>
                <p className="text-sm text-green-700">
                  BigQuery has been configured successfully!
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Error Message */}
        {(configureMutation.isError || createTableMutation.isError) && (
          <div className="rounded-lg border p-6 bg-red-50 border-red-200">
            <div className="flex items-center gap-3">
              <AlertCircle className="text-red-600" size={24} />
              <div>
                <h2 className="text-lg font-semibold">
                  {!tableId ? 'Table Creation Failed' : 'Configuration Failed'}
                </h2>
                <p className="text-sm text-red-700">
                  {!tableId
                    ? (createTableMutation.error instanceof Error
                        ? createTableMutation.error.message
                        : 'An error occurred while creating the table')
                    : (configureMutation.error instanceof Error
                        ? configureMutation.error.message
                        : 'An error occurred while configuring BigQuery')}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Configuration Form */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-2 mb-6">
            <Settings className="text-blue-600" size={24} />
            <h2 className="text-lg font-semibold">Configure BigQuery Connection</h2>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Name field - only show when creating new table */}
            {!tableId && (
              <div>
                <label htmlFor="table_name" className="block text-sm font-medium text-gray-700 mb-1">
                  Table Name
                </label>
                <input
                  type="text"
                  id="table_name"
                  required
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="my-analytics-table"
                />
                <p className="text-xs text-gray-500 mt-1">
                  A friendly name to identify this table connection
                </p>
              </div>
            )}

            <div>
              <label htmlFor="project_id" className="block text-sm font-medium text-gray-700 mb-1">
                Project ID
              </label>
              <input
                type="text"
                id="project_id"
                required
                value={formData.project_id}
                onChange={(e) => handleInputChange('project_id', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="my-gcp-project"
              />
            </div>

            <div>
              <label htmlFor="dataset" className="block text-sm font-medium text-gray-700 mb-1">
                Dataset
              </label>
              <input
                type="text"
                id="dataset"
                required
                value={formData.dataset}
                onChange={(e) => handleInputChange('dataset', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="analytics"
              />
            </div>

            <div>
              <label htmlFor="table" className="block text-sm font-medium text-gray-700 mb-1">
                Table
              </label>
              <input
                type="text"
                id="table"
                required
                value={formData.table}
                onChange={(e) => handleInputChange('table', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="search_data"
              />
            </div>

            <div>
              <label htmlFor="billing_project" className="block text-sm font-medium text-gray-700 mb-1">
                Billing Project <span className="text-gray-400">(optional)</span>
              </label>
              <input
                type="text"
                id="billing_project"
                value={formData.billing_project || ''}
                onChange={(e) => handleInputChange('billing_project', e.target.value || '')}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="my-billing-project"
              />
              <p className="text-xs text-gray-500 mt-1">
                Project to bill queries to. Leave empty to use the data project.
              </p>
            </div>

            {/* Authentication Method */}
            <div className="border-t pt-4 mt-4">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={formData.use_adc}
                  onChange={(e) => setFormData(prev => ({ ...prev, use_adc: e.target.checked }))}
                  className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-gray-700">
                  Use my Google Cloud credentials (gcloud auth)
                </span>
              </label>
              <p className="text-xs text-gray-500 mt-1 ml-6">
                Requires running: <code className="bg-gray-100 px-1 rounded">gcloud auth application-default login</code>
              </p>
            </div>

            {/* Credentials JSON - only shown if not using ADC */}
            {!formData.use_adc && (
              <div>
                <label htmlFor="credentials_json" className="block text-sm font-medium text-gray-700 mb-1">
                  Service Account JSON
                </label>
                <textarea
                  id="credentials_json"
                  required={!formData.use_adc}
                  value={formData.credentials_json}
                  onChange={(e) => handleInputChange('credentials_json', e.target.value)}
                  rows={10}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 font-mono text-sm"
                  placeholder='{"type": "service_account", "project_id": "...", ...}'
                />
                <p className="text-xs text-gray-500 mt-1">
                  Paste the entire contents of your service account JSON file here
                </p>
              </div>
            )}

            <button
              type="submit"
              disabled={configureMutation.isPending || createTableMutation.isPending}
              className="w-full bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed font-medium"
            >
              {!tableId
                ? (createTableMutation.isPending ? 'Creating Table...' : 'Create Table')
                : (configureMutation.isPending ? 'Configuring...' : 'Configure BigQuery')}
            </button>
          </form>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Connection Status */}
      <div className={`rounded-lg border p-6 ${isConnected ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {isConnected ? (
              <CheckCircle className="text-green-600" size={24} />
            ) : (
              <AlertCircle className="text-red-600" size={24} />
            )}
            <div>
              <h2 className="text-lg font-semibold">Connection Status</h2>
              <p className={`text-sm ${isConnected ? 'text-green-700' : 'text-red-700'}`}>
                {isConnected ? 'Connected to BigQuery' : info.connection_status}
              </p>
            </div>
          </div>
          {isConnected && (
            <button
              onClick={handleDisconnect}
              disabled={disconnectMutation.isPending}
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:bg-gray-400 disabled:cursor-not-allowed font-medium text-sm"
            >
              {disconnectMutation.isPending ? 'Disconnecting...' : 'Disconnect'}
            </button>
          )}
        </div>
      </div>

      {/* Table Information */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <Database className="text-blue-600" size={24} />
          <h2 className="text-lg font-semibold">Table Information</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <p className="text-sm text-gray-600">Project ID</p>
            <p className="font-mono text-sm font-medium">{info.project_id}</p>
          </div>
          <div>
            <p className="text-sm text-gray-600">Dataset</p>
            <p className="font-mono text-sm font-medium">{info.dataset}</p>
          </div>
          <div>
            <p className="text-sm text-gray-600">Table</p>
            <p className="font-mono text-sm font-medium">{info.table}</p>
          </div>
          <div>
            <p className="text-sm text-gray-600">Full Path</p>
            <p className="font-mono text-sm font-medium break-all">{info.table_full_path}</p>
          </div>
          {info.billing_project && info.billing_project !== info.project_id && (
            <div>
              <p className="text-sm text-gray-600">Billing Project</p>
              <p className="font-mono text-sm font-medium">{info.billing_project}</p>
            </div>
          )}
        </div>
      </div>

      {/* Data Summary */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <Table className="text-blue-600" size={24} />
          <h2 className="text-lg font-semibold">Data Summary</h2>
        </div>
        <div className="grid grid-cols-1 gap-4">
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-sm text-gray-600">Total Rows</p>
            <p className="text-2xl font-bold text-gray-900">{info.total_rows.toLocaleString()}</p>
          </div>
        </div>
      </div>

      {/* Date Range */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <Calendar className="text-blue-600" size={24} />
          <h2 className="text-lg font-semibold">Date Range</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <p className="text-sm text-gray-600">Start Date</p>
            <p className="text-lg font-medium">{info.date_range.min || 'N/A'}</p>
          </div>
          <div>
            <p className="text-sm text-gray-600">End Date</p>
            <p className="text-lg font-medium">{info.date_range.max || 'N/A'}</p>
          </div>
        </div>
      </div>


      {/* Table Metadata */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <HardDrive className="text-blue-600" size={24} />
          <h2 className="text-lg font-semibold">Table Metadata</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <p className="text-sm text-gray-600">Table Size</p>
            <p className="text-lg font-medium">{info.table_size_mb.toLocaleString()} MB</p>
          </div>
          <div>
            <p className="text-sm text-gray-600">Last Modified</p>
            <p className="text-lg font-medium">{new Date(info.last_modified).toLocaleString()}</p>
          </div>
        </div>

        {/* Schema Columns */}
        <div>
          <p className="text-sm text-gray-600 mb-2">Schema Columns ({info.schema_columns.length})</p>
          <div className="bg-gray-50 rounded-lg p-4 max-h-64 overflow-y-auto">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
              {info.schema_columns.map((column) => (
                <span key={column} className="font-mono text-xs text-gray-700 px-2 py-1 bg-white rounded border border-gray-200">
                  {column}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
