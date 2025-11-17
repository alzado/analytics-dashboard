'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { LayoutGrid, Plus, ArrowLeft, Edit, Trash2, Settings } from 'lucide-react'
import { fetchDashboards, deleteDashboard, type Dashboard } from '@/lib/api'
import { useDashboard } from '@/lib/contexts/dashboard-context'
import { CreateDashboardDialog } from '@/components/dashboards/create-dashboard-dialog'
import { DashboardView } from '@/components/dashboards/dashboard-view'

interface DashboardsSectionProps {
  onTabChange?: (tab: string) => void
}

export default function DashboardsSection({ onTabChange }: DashboardsSectionProps = {}) {
  const queryClient = useQueryClient()
  const { currentDashboardId, setCurrentDashboardId } = useDashboard()
  const [showCreateDialog, setShowCreateDialog] = useState(false)

  // Fetch all dashboards
  const { data: dashboardsData, isLoading } = useQuery({
    queryKey: ['dashboards'],
    queryFn: fetchDashboards,
  })

  const dashboards = dashboardsData?.dashboards || []

  // Delete dashboard mutation
  const deleteMutation = useMutation({
    mutationFn: deleteDashboard,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboards'] })
      if (currentDashboardId) {
        setCurrentDashboardId(null)
      }
    },
  })

  const handleDeleteDashboard = (dashboardId: string) => {
    if (confirm('Are you sure you want to delete this dashboard? This action cannot be undone.')) {
      deleteMutation.mutate(dashboardId)
    }
  }

  // If a dashboard is selected, show dashboard view
  if (currentDashboardId) {
    return (
      <DashboardView
        dashboardId={currentDashboardId}
        onBack={() => setCurrentDashboardId(null)}
        onTabChange={onTabChange}
      />
    )
  }

  // Dashboard list view
  return (
    <div className="p-6">
      {/* Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboards</h1>
          <p className="text-sm text-gray-600 mt-1">
            Create custom dashboards with pivot tables and charts from multiple data sources
          </p>
        </div>
        <button
          onClick={() => setShowCreateDialog(true)}
          className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          <Plus className="h-5 w-5 mr-2" />
          Create Dashboard
        </button>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="text-center py-12 text-gray-500">
          Loading dashboards...
        </div>
      )}

      {/* Empty state */}
      {!isLoading && dashboards.length === 0 && (
        <div className="text-center py-12 border-2 border-dashed border-gray-300 rounded-lg">
          <LayoutGrid className="h-12 w-12 mx-auto text-gray-400 mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">No dashboards yet</h3>
          <p className="text-gray-600 mb-4">
            Create your first dashboard to start visualizing your data
          </p>
          <button
            onClick={() => setShowCreateDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            <Plus className="h-5 w-5 mr-2" />
            Create Dashboard
          </button>
        </div>
      )}

      {/* Dashboard grid */}
      {!isLoading && dashboards.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {dashboards.map((dashboard) => (
            <DashboardCard
              key={dashboard.id}
              dashboard={dashboard}
              onOpen={() => setCurrentDashboardId(dashboard.id)}
              onDelete={() => handleDeleteDashboard(dashboard.id)}
            />
          ))}
        </div>
      )}

      {/* Create dialog */}
      {showCreateDialog && (
        <CreateDashboardDialog
          onClose={() => setShowCreateDialog(false)}
          onCreated={(dashboardId) => setCurrentDashboardId(dashboardId)}
        />
      )}
    </div>
  )
}

function DashboardCard({
  dashboard,
  onOpen,
  onDelete,
}: {
  dashboard: Dashboard
  onOpen: () => void
  onDelete: () => void
}) {
  const widgetCount = dashboard.widgets.length
  const lastUpdated = new Date(dashboard.updated_at).toLocaleDateString()

  return (
    <div className="border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow cursor-pointer group">
      <div onClick={onOpen} className="mb-4">
        <div className="flex items-start justify-between mb-2">
          <h3 className="text-lg font-semibold text-gray-900 group-hover:text-blue-600">
            {dashboard.name}
          </h3>
          <LayoutGrid className="h-5 w-5 text-gray-400" />
        </div>
        {dashboard.description && (
          <p className="text-sm text-gray-600 mb-4 line-clamp-2">{dashboard.description}</p>
        )}
        <div className="flex items-center text-sm text-gray-500">
          <span className="mr-4">
            {widgetCount} {widgetCount === 1 ? 'widget' : 'widgets'}
          </span>
          <span>Updated {lastUpdated}</span>
        </div>
      </div>

      {/* Action buttons */}
      <div className="flex items-center gap-2 pt-4 border-t border-gray-200">
        <button
          onClick={(e) => {
            e.stopPropagation()
            onOpen()
          }}
          className="flex-1 flex items-center justify-center px-3 py-2 text-sm bg-blue-50 text-blue-600 rounded hover:bg-blue-100"
        >
          <Edit className="h-4 w-4 mr-1" />
          Open
        </button>
        <button
          onClick={(e) => {
            e.stopPropagation()
            onDelete()
          }}
          className="flex items-center justify-center px-3 py-2 text-sm bg-red-50 text-red-600 rounded hover:bg-red-100"
        >
          <Trash2 className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}
