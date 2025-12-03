'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Plus, Edit2, Trash2, Settings, LayoutGrid, Pen, Check, X } from 'lucide-react'
import { fetchDashboard, updateDashboard, deleteWidget, updateDashboardWidget, type Dashboard, type WidgetConfig } from '@/lib/api'
import { useDashboard } from '@/lib/contexts/dashboard-context'
import { AddWidgetDialog } from './add-widget-dialog'
import { DashboardSettingsDialog } from './dashboard-settings-dialog'
import { PivotTableWidget } from '../widgets/pivot-table-widget'
import { MultiTableWidget } from '../widgets/multi-table-widget'
import { ChartWidget } from '../widgets/chart-widget'
import GridLayout from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'

interface DashboardViewProps {
  dashboardId: string
  onBack: () => void
  onTabChange?: (tab: string) => void
}

export function DashboardView({ dashboardId, onBack, onTabChange }: DashboardViewProps) {
  const queryClient = useQueryClient()
  const { isEditMode, setIsEditMode, setEditingWidget, setCurrentDashboardId } = useDashboard()
  const [showAddWidgetDialog, setShowAddWidgetDialog] = useState(false)
  const [showSettings, setShowSettings] = useState(false)

  // Fetch dashboard data
  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['dashboard', dashboardId],
    queryFn: () => fetchDashboard(dashboardId),
  })

  // Delete widget mutation
  const deleteWidgetMutation = useMutation({
    mutationFn: ({ widgetId }: { widgetId: string }) => deleteWidget(dashboardId, widgetId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })
    },
  })

  // Rename widget mutation
  const renameWidgetMutation = useMutation({
    mutationFn: ({ widgetId, title }: { widgetId: string; title: string }) =>
      updateDashboardWidget(dashboardId, widgetId, { title }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })
    },
  })

  // Update layout mutation (for drag-and-drop)
  const updateLayoutMutation = useMutation({
    mutationFn: (widgets: WidgetConfig[]) => updateDashboard(dashboardId, { widgets }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard', dashboardId] })
    },
  })

  const handleLayoutChange = (layout: any[]) => {
    if (!dashboard || !isEditMode) return

    // Update widget positions based on new layout
    const updatedWidgets = dashboard.widgets.map((widget) => {
      const layoutItem = layout.find((l) => l.i === widget.id)
      if (layoutItem) {
        return {
          ...widget,
          position: {
            x: layoutItem.x,
            y: layoutItem.y,
            w: layoutItem.w,
            h: layoutItem.h,
          },
        }
      }
      return widget
    })

    updateLayoutMutation.mutate(updatedWidgets)
  }

  const handleDeleteWidget = (widgetId: string) => {
    if (confirm('Are you sure you want to delete this widget?')) {
      deleteWidgetMutation.mutate({ widgetId })
    }
  }

  const handleEditInEditor = (widget: WidgetConfig) => {
    // Set the widget in the dashboard context
    setEditingWidget(widget)
    setCurrentDashboardId(dashboardId)
    // Switch to Editor tab
    if (onTabChange) {
      onTabChange('pivot')
    }
  }

  const handleRenameWidget = (widgetId: string, newTitle: string) => {
    if (newTitle.trim()) {
      renameWidgetMutation.mutate({ widgetId, title: newTitle.trim() })
    }
  }

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="text-center py-12 text-gray-500">Loading dashboard...</div>
      </div>
    )
  }

  if (!dashboard) {
    return (
      <div className="p-6">
        <div className="text-center py-12 text-red-500">Dashboard not found</div>
      </div>
    )
  }

  // Prepare layout for react-grid-layout
  const layout = dashboard.widgets.map((widget) => ({
    i: widget.id,
    x: widget.position.x,
    y: widget.position.y,
    w: widget.position.w,
    h: widget.position.h,
    minW: 2,
    minH: 2,
  }))

  return (
    <div className="p-6">
      {/* Header */}
      <div className="mb-6">
        <button
          onClick={onBack}
          className="flex items-center text-gray-600 hover:text-gray-900 mb-4"
        >
          <ArrowLeft className="h-5 w-5 mr-2" />
          Back to Dashboards
        </button>

        <div className="flex items-start justify-between">
          <div className="flex-1">
            <h1 className="text-2xl font-bold text-gray-900">{dashboard.name}</h1>
            {dashboard.description && (
              <p className="text-gray-600 mt-1">{dashboard.description}</p>
            )}
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowSettings(true)}
              className="flex items-center px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200"
              title="Dashboard Settings"
            >
              <Settings className="h-4 w-4" />
            </button>

            <button
              onClick={() => setIsEditMode(!isEditMode)}
              className={`flex items-center px-4 py-2 rounded-lg ${
                isEditMode
                  ? 'bg-blue-600 text-white hover:bg-blue-700'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              <Edit2 className="h-4 w-4 mr-2" />
              {isEditMode ? 'Done Editing' : 'Edit'}
            </button>

            {isEditMode && (
              <button
                onClick={() => setShowAddWidgetDialog(true)}
                className="flex items-center px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                <Plus className="h-4 w-4 mr-2" />
                Add Widget
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Empty state */}
      {dashboard.widgets.length === 0 && (
        <div className="text-center py-12 border-2 border-dashed border-gray-300 rounded-lg">
          <LayoutGrid className="h-12 w-12 mx-auto text-gray-400 mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">No widgets yet</h3>
          <p className="text-gray-600 mb-4">
            Add your first widget to start visualizing data
          </p>
          <button
            onClick={() => {
              setIsEditMode(true)
              setShowAddWidgetDialog(true)
            }}
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            <Plus className="h-5 w-5 mr-2" />
            Add Widget
          </button>
        </div>
      )}

      {/* Grid layout with widgets */}
      {dashboard.widgets.length > 0 && (
        <GridLayout
          className="layout"
          layout={layout}
          cols={12}
          rowHeight={100}
          width={1200}
          isDraggable={isEditMode}
          isResizable={isEditMode}
          onLayoutChange={handleLayoutChange}
          draggableHandle=".widget-drag-handle"
        >
          {dashboard.widgets.map((widget) => (
            <div key={widget.id} className="bg-white border border-gray-200 rounded-lg shadow-sm">
              <WidgetCard
                widget={widget}
                isEditMode={isEditMode}
                onDelete={() => handleDeleteWidget(widget.id)}
                onEditInEditor={() => handleEditInEditor(widget)}
                onRename={(newTitle) => handleRenameWidget(widget.id, newTitle)}
              />
            </div>
          ))}
        </GridLayout>
      )}

      {/* Add Widget Dialog */}
      {showAddWidgetDialog && (
        <AddWidgetDialog
          dashboardId={dashboardId}
          onClose={() => setShowAddWidgetDialog(false)}
        />
      )}


      {/* Dashboard Settings */}
      {showSettings && (
        <DashboardSettingsDialog
          dashboard={dashboard}
          onClose={() => setShowSettings(false)}
        />
      )}
    </div>
  )
}

function WidgetCard({
  widget,
  isEditMode,
  onDelete,
  onEditInEditor,
  onRename,
}: {
  widget: WidgetConfig
  isEditMode: boolean
  onDelete: () => void
  onEditInEditor?: () => void
  onRename?: (newTitle: string) => void
}) {
  const [isEditing, setIsEditing] = useState(false)
  const [editedTitle, setEditedTitle] = useState(widget.title)

  const handleStartEditing = () => {
    setEditedTitle(widget.title)
    setIsEditing(true)
  }

  const handleSaveTitle = () => {
    if (editedTitle.trim() && editedTitle !== widget.title && onRename) {
      onRename(editedTitle.trim())
    }
    setIsEditing(false)
  }

  const handleCancelEditing = () => {
    setEditedTitle(widget.title)
    setIsEditing(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSaveTitle()
    } else if (e.key === 'Escape') {
      handleCancelEditing()
    }
  }

  return (
    <div className="h-full flex flex-col">
      {/* Widget header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-200 bg-gray-50">
        <div className="flex items-center gap-2 flex-1 min-w-0">
          {isEditMode && (
            <div className="widget-drag-handle cursor-move flex-shrink-0">
              <LayoutGrid className="h-4 w-4 text-gray-400" />
            </div>
          )}
          {isEditing ? (
            <div className="flex items-center gap-1 flex-1 min-w-0">
              <input
                type="text"
                value={editedTitle}
                onChange={(e) => setEditedTitle(e.target.value)}
                onKeyDown={handleKeyDown}
                className="flex-1 min-w-0 px-2 py-1 text-sm font-semibold border border-blue-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                autoFocus
                maxLength={100}
              />
              <button
                onClick={handleSaveTitle}
                className="p-1 text-green-600 hover:text-green-700 flex-shrink-0"
                title="Save"
              >
                <Check className="h-4 w-4" />
              </button>
              <button
                onClick={handleCancelEditing}
                className="p-1 text-gray-400 hover:text-gray-600 flex-shrink-0"
                title="Cancel"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          ) : (
            <>
              <h3
                className={`font-semibold text-gray-900 truncate ${isEditMode ? 'cursor-pointer hover:text-blue-600' : ''}`}
                onClick={isEditMode ? handleStartEditing : undefined}
                title={isEditMode ? 'Click to rename' : widget.title}
              >
                {widget.title}
              </h3>
              {isEditMode && (
                <button
                  onClick={handleStartEditing}
                  className="p-1 text-gray-400 hover:text-blue-600 flex-shrink-0"
                  title="Rename widget"
                >
                  <Edit2 className="h-3 w-3" />
                </button>
              )}
            </>
          )}
          <span className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded flex-shrink-0">
            {widget.type === 'table' ? 'Table' : 'Chart'}
          </span>
        </div>

        {isEditMode && !isEditing && (
          <div className="flex items-center gap-2 flex-shrink-0">
            {onEditInEditor && (
              <button
                onClick={onEditInEditor}
                className="text-purple-600 hover:text-purple-700"
                title="Edit in Editor"
              >
                <Pen className="h-4 w-4" />
              </button>
            )}
            <button
              onClick={onDelete}
              className="text-red-600 hover:text-red-700"
              title="Delete widget"
            >
              <Trash2 className="h-4 w-4" />
            </button>
          </div>
        )}
      </div>

      {/* Widget content */}
      <div className="flex-1 overflow-auto">
        {widget.display_mode === 'multi-table' ? (
          <MultiTableWidget widget={widget} />
        ) : widget.display_mode === 'single-metric-chart' ? (
          <ChartWidget widget={widget} />
        ) : widget.display_mode === 'pivot-table' ? (
          <PivotTableWidget widget={widget} />
        ) : (
          // Fallback for widgets without display_mode (backward compatibility)
          widget.type === 'table' ? (
            widget.table_dimensions && widget.table_dimensions.length > 0 ? (
              <MultiTableWidget widget={widget} />
            ) : (
              <PivotTableWidget widget={widget} />
            )
          ) : (
            <ChartWidget widget={widget} />
          )
        )}
      </div>
    </div>
  )
}
