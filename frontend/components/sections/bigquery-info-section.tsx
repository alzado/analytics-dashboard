'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchBigQueryInfo, configureBigQuery, disconnectBigQuery } from '@/lib/api'
import { Database, Calendar, HardDrive, Table, CheckCircle, AlertCircle, Settings } from 'lucide-react'
import type { BigQueryConfig } from '@/lib/types'

export function BigQueryInfoSection() {
  const router = useRouter()
  const queryClient = useQueryClient()
  const [formData, setFormData] = useState<BigQueryConfig>({
    project_id: '',
    dataset: '',
    table: '',
    use_adc: true,  // Default to using Google Cloud credentials
    credentials_json: '',
    allowed_min_date: null,
    allowed_max_date: null
  })
  const [showSuccess, setShowSuccess] = useState(false)

  const { data: info, isLoading, error } = useQuery({
    queryKey: ['bigquery-info'],
    queryFn: fetchBigQueryInfo,
    refetchInterval: 60000, // Refresh every minute
  })

  const configureMutation = useMutation({
    mutationFn: configureBigQuery,
    onSuccess: (response) => {
      console.log('Configuration response:', response)
      if (response.success) {
        setShowSuccess(true)
        queryClient.invalidateQueries({ queryKey: ['bigquery-info'] })
        // Clear only credentials and date limits, keep connection details
        setFormData(prev => ({
          ...prev,
          use_adc: true,
          credentials_json: '',
          allowed_min_date: null,
          allowed_max_date: null
        }))
        // Navigate to overview page after successful configuration
        setTimeout(() => {
          router.push('/')
        }, 1000)
      }
    },
    onError: (error) => {
      console.error('Configuration error:', error)
    }
  })

  const disconnectMutation = useMutation({
    mutationFn: disconnectBigQuery,
    onSuccess: (response) => {
      console.log('Disconnect response:', response)
      if (response.success) {
        queryClient.invalidateQueries({ queryKey: ['bigquery-info'] })
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
    configureMutation.mutate(formData)
  }

  const handleInputChange = (field: keyof BigQueryConfig, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const handleDisconnect = () => {
    if (confirm('Are you sure you want to disconnect BigQuery? This will clear your configuration.')) {
      disconnectMutation.mutate()
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

  // Show configuration form if not connected
  if (!isConnected) {
    return (
      <div className="space-y-6">
        {/* Connection Status */}
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
        {configureMutation.isError && (
          <div className="rounded-lg border p-6 bg-red-50 border-red-200">
            <div className="flex items-center gap-3">
              <AlertCircle className="text-red-600" size={24} />
              <div>
                <h2 className="text-lg font-semibold">Configuration Failed</h2>
                <p className="text-sm text-red-700">
                  {configureMutation.error instanceof Error
                    ? configureMutation.error.message
                    : 'An error occurred while configuring BigQuery'}
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

            {/* Date Limits - Optional */}
            <div className="border-t pt-4 mt-4">
              <h3 className="text-sm font-medium text-gray-700 mb-3">Date Access Limits (Optional)</h3>
              <p className="text-xs text-gray-500 mb-3">
                Restrict query access to specific date ranges. Leave empty for no restrictions.
              </p>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label htmlFor="allowed_min_date" className="block text-sm font-medium text-gray-700 mb-1">
                    Minimum Allowed Date
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
                    Maximum Allowed Date
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
              disabled={configureMutation.isPending}
              className="w-full bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed font-medium"
            >
              {configureMutation.isPending ? 'Configuring...' : 'Configure BigQuery'}
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
        </div>
      </div>

      {/* Data Summary */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-2 mb-4">
          <Table className="text-blue-600" size={24} />
          <h2 className="text-lg font-semibold">Data Summary</h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-sm text-gray-600">Total Rows</p>
            <p className="text-2xl font-bold text-gray-900">{info.total_rows.toLocaleString()}</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-sm text-gray-600">Total Searches</p>
            <p className="text-2xl font-bold text-gray-900">{info.total_searches.toLocaleString()}</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-sm text-gray-600">Unique Terms</p>
            <p className="text-2xl font-bold text-gray-900">{info.unique_search_terms.toLocaleString()}</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-sm text-gray-600">Total Revenue</p>
            <p className="text-2xl font-bold text-gray-900">${info.total_revenue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</p>
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

      {/* Available Filters */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h2 className="text-lg font-semibold mb-4">Available Filters</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <p className="text-sm text-gray-600 mb-2">Countries ({info.available_countries.length})</p>
            <div className="flex flex-wrap gap-2">
              {info.available_countries.slice(0, 10).map((country) => (
                <span key={country} className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-sm">
                  {country}
                </span>
              ))}
              {info.available_countries.length > 10 && (
                <span className="px-3 py-1 bg-gray-100 text-gray-600 rounded-full text-sm">
                  +{info.available_countries.length - 10} more
                </span>
              )}
            </div>
          </div>
          <div>
            <p className="text-sm text-gray-600 mb-2">Channels ({info.available_channels.length})</p>
            <div className="flex flex-wrap gap-2">
              {info.available_channels.map((channel) => (
                <span key={channel} className="px-3 py-1 bg-green-50 text-green-700 rounded-full text-sm">
                  {channel}
                </span>
              ))}
            </div>
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
