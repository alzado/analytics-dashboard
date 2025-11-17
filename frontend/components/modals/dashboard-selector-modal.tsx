'use client'

import { useState, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, Plus, LayoutDashboard, Loader2 } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchDashboards, createDashboard, addWidgetToDashboard, type WidgetCreateRequest, type DashboardCreateRequest } from '@/lib/api'

interface DashboardSelectorModalProps {
  isOpen: boolean
  onClose: () => void
  widgetConfig: Omit<WidgetCreateRequest, 'position'>
  onSuccess?: (dashboardId: string, dashboardName: string) => void
}

export default function DashboardSelectorModal({
  isOpen,
  onClose,
  widgetConfig,
  onSuccess,
}: DashboardSelectorModalProps) {
  const [showCreateNew, setShowCreateNew] = useState(false)
  const [newDashboardName, setNewDashboardName] = useState('')
  const [newDashboardDescription, setNewDashboardDescription] = useState('')
  const [selectedDashboardId, setSelectedDashboardId] = useState<string | null>(null)
  const queryClient = useQueryClient()

  // Fetch dashboards
  const { data: dashboardsData, isLoading } = useQuery({
    queryKey: ['dashboards'],
    queryFn: fetchDashboards,
    enabled: isOpen,
  })

  // Create dashboard mutation
  const createDashboardMutation = useMutation({
    mutationFn: (request: DashboardCreateRequest) => createDashboard(request),
    onSuccess: (newDashboard) => {
      queryClient.invalidateQueries({ queryKey: ['dashboards'] })
      setSelectedDashboardId(newDashboard.id)
      setShowCreateNew(false)
      setNewDashboardName('')
      setNewDashboardDescription('')
    },
  })

  // Add widget mutation
  const addWidgetMutation = useMutation({
    mutationFn: ({ dashboardId, widget }: { dashboardId: string; widget: WidgetCreateRequest }) =>
      addWidgetToDashboard(dashboardId, widget),
    onSuccess: (updatedDashboard) => {
      queryClient.invalidateQueries({ queryKey: ['dashboards'] })
      queryClient.invalidateQueries({ queryKey: ['dashboard', updatedDashboard.id] })

      const dashboardName = dashboardsData?.dashboards.find(d => d.id === updatedDashboard.id)?.name || 'Dashboard'

      if (onSuccess) {
        onSuccess(updatedDashboard.id, dashboardName)
      }
      onClose()
    },
  })

  const handleCreateDashboard = () => {
    if (!newDashboardName.trim()) return

    createDashboardMutation.mutate({
      name: newDashboardName.trim(),
      description: newDashboardDescription.trim() || undefined,
    })
  }

  const handleSelectDashboard = (dashboardId: string) => {
    setSelectedDashboardId(dashboardId)
  }

  const handleAddWidget = () => {
    if (!selectedDashboardId) return

    // Calculate position - place at bottom of grid
    // We'll use a simple strategy: place at y = 100 (very bottom)
    const widgetWithPosition: WidgetCreateRequest = {
      ...widgetConfig,
      position: { x: 0, y: 100, w: 6, h: 4 }
    }

    addWidgetMutation.mutate({
      dashboardId: selectedDashboardId,
      widget: widgetWithPosition,
    })
  }

  const dashboards = dashboardsData?.dashboards || []

  if (!isOpen) return null

  const modalContent = (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <LayoutDashboard className="w-5 h-5 text-blue-600" />
            <h2 className="text-xl font-semibold text-gray-900">
              Add to Dashboard
            </h2>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto flex-1">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
            </div>
          ) : showCreateNew ? (
            <div className="space-y-4">
              <h3 className="text-sm font-medium text-gray-900">Create New Dashboard</h3>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Dashboard Name *
                </label>
                <input
                  type="text"
                  value={newDashboardName}
                  onChange={(e) => setNewDashboardName(e.target.value)}
                  placeholder="e.g., Sales Overview"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
                  autoFocus
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Description (optional)
                </label>
                <textarea
                  value={newDashboardDescription}
                  onChange={(e) => setNewDashboardDescription(e.target.value)}
                  placeholder="Brief description of this dashboard"
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
                />
              </div>

              <div className="flex gap-2">
                <button
                  onClick={handleCreateDashboard}
                  disabled={!newDashboardName.trim() || createDashboardMutation.isPending}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
                >
                  {createDashboardMutation.isPending ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Plus className="w-4 h-4" />
                  )}
                  Create Dashboard
                </button>
                <button
                  onClick={() => {
                    setShowCreateNew(false)
                    setNewDashboardName('')
                    setNewDashboardDescription('')
                  }}
                  className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              {dashboards.length === 0 ? (
                <div className="text-center py-12">
                  <LayoutDashboard className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                  <p className="text-gray-600 mb-4">No dashboards yet</p>
                  <button
                    onClick={() => setShowCreateNew(true)}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 inline-flex items-center gap-2"
                  >
                    <Plus className="w-4 h-4" />
                    Create Your First Dashboard
                  </button>
                </div>
              ) : (
                <>
                  <div>
                    <h3 className="text-sm font-medium text-gray-900 mb-3">
                      Select a dashboard
                    </h3>
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                      {dashboards.map((dashboard) => (
                        <button
                          key={dashboard.id}
                          onClick={() => handleSelectDashboard(dashboard.id)}
                          className={`w-full text-left p-4 rounded-lg border-2 transition-all ${
                            selectedDashboardId === dashboard.id
                              ? 'border-blue-500 bg-blue-50'
                              : 'border-gray-200 hover:border-gray-300 bg-white'
                          }`}
                        >
                          <div className="flex items-start justify-between">
                            <div className="flex-1">
                              <h4 className="font-medium text-gray-900">
                                {dashboard.name}
                              </h4>
                              {dashboard.description && (
                                <p className="text-sm text-gray-600 mt-1">
                                  {dashboard.description}
                                </p>
                              )}
                              <p className="text-xs text-gray-500 mt-2">
                                {dashboard.widgets?.length || 0} widget{dashboard.widgets?.length !== 1 ? 's' : ''}
                              </p>
                            </div>
                            {selectedDashboardId === dashboard.id && (
                              <div className="w-5 h-5 rounded-full bg-blue-600 flex items-center justify-center flex-shrink-0 ml-2">
                                <svg
                                  className="w-3 h-3 text-white"
                                  fill="none"
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth="2"
                                  viewBox="0 0 24 24"
                                  stroke="currentColor"
                                >
                                  <path d="M5 13l4 4L19 7"></path>
                                </svg>
                              </div>
                            )}
                          </div>
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="pt-4 border-t border-gray-200">
                    <button
                      onClick={() => setShowCreateNew(true)}
                      className="text-sm text-blue-600 hover:text-blue-700 flex items-center gap-1"
                    >
                      <Plus className="w-4 h-4" />
                      Create New Dashboard
                    </button>
                  </div>
                </>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {!showCreateNew && dashboards.length > 0 && (
          <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-2">
            <button
              onClick={onClose}
              className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              onClick={handleAddWidget}
              disabled={!selectedDashboardId || addWidgetMutation.isPending}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {addWidgetMutation.isPending ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Adding...
                </>
              ) : (
                <>
                  <Plus className="w-4 h-4" />
                  Add Widget
                </>
              )}
            </button>
          </div>
        )}
      </div>
    </div>
  )

  return createPortal(modalContent, document.body)
}
