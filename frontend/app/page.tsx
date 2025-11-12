'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchBigQueryInfo } from '@/lib/api'
import { DashboardLayout } from '@/components/layout/dashboard-layout'
import { PivotTableSection } from '@/components/sections/pivot-table-section'
import { BigQueryInfoSection } from '@/components/sections/bigquery-info-section'
import { UsageLogsSection } from '@/components/sections/usage-logs-section'
import { SchemaSection } from '@/components/sections/schema-section'

export default function Home() {
  const [activeTab, setActiveTab] = useState('pivot')

  // Check BigQuery connection status
  const { data: bqInfo, isLoading } = useQuery({
    queryKey: ['bigquery-info'],
    queryFn: fetchBigQueryInfo,
    staleTime: 5 * 60 * 1000, // 5 minutes - don't refetch unless data is stale
    refetchInterval: false, // Disable automatic polling
    refetchOnWindowFocus: false, // Don't refetch when window regains focus
  })

  // Show loading state
  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading...</p>
        </div>
      </div>
    )
  }

  // If BigQuery is not connected, show only configuration screen
  const isConnected = bqInfo?.connection_status === 'connected'
  if (!isConnected) {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="mb-8">
            <h1 className="text-3xl font-bold text-gray-900">Search Analytics Dashboard</h1>
            <p className="mt-2 text-gray-600">Configure your BigQuery connection to get started</p>
          </div>
          <BigQueryInfoSection />
        </div>
      </div>
    )
  }

  // Show full dashboard if connected
  return (
    <DashboardLayout activeTab={activeTab} onTabChange={setActiveTab}>
      {activeTab === 'pivot' && <PivotTableSection />}
      {activeTab === 'schema' && <SchemaSection />}
      {activeTab === 'logs' && <UsageLogsSection />}
      {activeTab === 'info' && <BigQueryInfoSection />}
    </DashboardLayout>
  )
}
