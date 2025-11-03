'use client'

import { useState, useEffect } from 'react'
import { X, GripVertical, Plus, Settings, Database, Calendar, ChevronDown, ChevronRight } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { fetchBigQueryInfo, fetchTableDateRange } from '@/lib/api'
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core'
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { usePivotConfig } from '@/hooks/use-pivot-config'
import {
  AVAILABLE_METRICS,
  AVAILABLE_DIMENSIONS,
  getMetricById,
  getDimensionByValue,
  MetricDefinition,
} from '@/lib/pivot-metrics'

interface PivotConfigPanelProps {
  isOpen: boolean
  onClose: () => void
}

interface SortableMetricProps {
  metric: MetricDefinition
  onRemove: (id: string) => void
}

function SortableMetric({ metric, onRemove }: SortableMetricProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: metric.id })

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  }

  return (
    <div
      ref={setNodeRef}
      style={style}
      className="flex items-center gap-2 p-2 bg-white border border-gray-200 rounded hover:border-blue-300 transition-colors"
    >
      <button
        className="cursor-grab active:cursor-grabbing text-gray-400 hover:text-gray-600"
        {...attributes}
        {...listeners}
      >
        <GripVertical className="h-3 w-3" />
      </button>
      <div className="flex-1 min-w-0">
        <div className="text-xs font-medium text-gray-900 truncate">{metric.label}</div>
      </div>
      <button
        onClick={() => onRemove(metric.id)}
        className="text-gray-400 hover:text-red-600 transition-colors"
      >
        <X className="h-3 w-3" />
      </button>
    </div>
  )
}

interface AvailableMetricProps {
  metric: MetricDefinition
  onAdd: (id: string) => void
  isSelected: boolean
}

function AvailableMetric({ metric, onAdd, isSelected }: AvailableMetricProps) {
  return (
    <div
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData('type', 'metric')
        e.dataTransfer.setData('id', metric.id)
        e.dataTransfer.setData('label', metric.label)
      }}
      className="p-2 bg-orange-50 border-2 border-orange-300 rounded cursor-move hover:bg-orange-100 transition-colors"
    >
      <div className="flex items-center gap-2">
        <GripVertical className="h-3 w-3 text-orange-600" />
        <div className="flex-1">
          <div className="text-xs font-medium text-gray-900">
            {metric.label}
          </div>
          <div className="text-xs text-orange-600">Drag to table</div>
        </div>
      </div>
    </div>
  )
}

export function PivotConfigPanel({ isOpen, onClose }: PivotConfigPanelProps) {
  const {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    addDimension,
    removeDimension,
    reorderDimensions,
    updateMetrics,
    addMetric,
    removeMetric,
    reorderMetrics,
    addFilter,
    removeFilter,
    resetToDefaults,
  } = usePivotConfig()

  const [isDataSourceExpanded, setIsDataSourceExpanded] = useState(true)
  const [isDateRangeExpanded, setIsDateRangeExpanded] = useState(true)
  const [isDimensionsExpanded, setIsDimensionsExpanded] = useState(true)
  const [isMetricsExpanded, setIsMetricsExpanded] = useState(true)
  const [isFiltersExpanded, setIsFiltersExpanded] = useState(true)

  // Fetch BigQuery info to get the configured table
  const { data: bqInfo, isLoading: bqInfoLoading } = useQuery({
    queryKey: ['bigquery-info'],
    queryFn: fetchBigQueryInfo,
    retry: false,
  })

  // Auto-select the configured table if not already selected
  useEffect(() => {
    if (bqInfo && bqInfo.table && !config.selectedTable && bqInfo.connection_status === 'connected') {
      updateTable(bqInfo.table)
    }
  }, [bqInfo, config.selectedTable, updateTable])

  // Fetch date range for selected table
  const { data: dateRangeInfo, isLoading: dateRangeLoading } = useQuery({
    queryKey: ['table-dates', config.selectedTable],
    queryFn: () => fetchTableDateRange(config.selectedTable!),
    enabled: !!config.selectedTable,
    retry: false,
  })

  // Handle date range selection (use full available range)
  useEffect(() => {
    if (dateRangeInfo?.has_date_column && dateRangeInfo.min_date && dateRangeInfo.max_date) {
      // Auto-set date range to full available range
      if (!config.selectedDateRange) {
        updateDateRange({
          start_date: dateRangeInfo.min_date,
          end_date: dateRangeInfo.max_date,
        })
      }
    }
  }, [dateRangeInfo, config.selectedDateRange, updateDateRange])

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event

    if (over && active.id !== over.id) {
      const oldIndex = config.selectedMetrics.indexOf(active.id as string)
      const newIndex = config.selectedMetrics.indexOf(over.id as string)
      reorderMetrics(oldIndex, newIndex)
    }
  }

  const selectedMetricObjects = config.selectedMetrics
    .map((id) => getMetricById(id))
    .filter((m): m is MetricDefinition => m !== undefined)

  const availableMetricsToAdd = AVAILABLE_METRICS.filter(
    (m) => !config.selectedMetrics.includes(m.id)
  )

  // Group available metrics by category
  const metricsByCategory = availableMetricsToAdd.reduce((acc, metric) => {
    if (!acc[metric.category]) {
      acc[metric.category] = []
    }
    acc[metric.category].push(metric)
    return acc
  }, {} as Record<string, MetricDefinition[]>)

  if (!isOpen) return null

  return (
    <div className="w-80 bg-gray-50 border-r border-gray-200 flex flex-col h-[calc(100vh-8rem)] overflow-hidden">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-4 flex-shrink-0">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <Settings className="h-4 w-4 text-blue-600" />
            <h3 className="text-sm font-bold text-gray-900">Configuration</h3>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <p className="text-xs text-gray-600">
          Drag metrics to reorder
        </p>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Data Source Info */}
        <div>
          <button
            onClick={() => setIsDataSourceExpanded(!isDataSourceExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase flex items-center gap-1">
              <Database className="h-3 w-3" />
              Data Source
            </h4>
            {isDataSourceExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isDataSourceExpanded && (
            <>
              {bqInfoLoading ? (
                <div className="p-3 text-center bg-white border border-gray-200 rounded">
                  <p className="text-xs text-gray-500">Loading...</p>
                </div>
              ) : bqInfo?.connection_status === 'connected' && bqInfo.table ? (
                <div
                  draggable
                  onDragStart={(e) => {
                    e.dataTransfer.setData('type', 'datasource')
                    e.dataTransfer.setData('table', bqInfo.table)
                    e.dataTransfer.setData('tablePath', bqInfo.table_full_path || '')
                  }}
                  className="p-3 bg-blue-50 border-2 border-blue-300 rounded cursor-move hover:bg-blue-100 transition-colors"
                >
                  <div className="flex items-center gap-2 mb-2">
                    <GripVertical className="h-3 w-3 text-blue-600" />
                    <span className="text-green-600">✓</span>
                    <div className="text-xs font-medium text-gray-900">{bqInfo.table}</div>
                  </div>
                  {bqInfo.table_full_path && (
                    <div className="text-xs text-gray-500 mb-2">
                      {bqInfo.table_full_path}
                    </div>
                  )}
                  <div className="text-xs text-gray-400">
                    {bqInfo.total_rows?.toLocaleString()} rows • {bqInfo.table_size_mb} MB
                  </div>
                  <div className="text-xs text-blue-600 mt-2">Drag to table</div>
                </div>
              ) : (
                <div className="p-3 text-center bg-yellow-50 border border-yellow-200 rounded">
                  <p className="text-xs text-yellow-700">
                    No BigQuery connection configured
                  </p>
                  <p className="text-xs text-yellow-600 mt-1">
                    Configure BigQuery in Settings first
                  </p>
                </div>
              )}
            </>
          )}
        </div>

        {/* Date Range Selection */}
        {config.selectedTable && (
          <div>
            <button
              onClick={() => setIsDateRangeExpanded(!isDateRangeExpanded)}
              className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
            >
              <h4 className="text-xs font-semibold text-gray-700 uppercase flex items-center gap-1">
                <Calendar className="h-3 w-3" />
                Date Range
              </h4>
              {isDateRangeExpanded ? (
                <ChevronDown className="h-3 w-3 text-gray-500" />
              ) : (
                <ChevronRight className="h-3 w-3 text-gray-500" />
              )}
            </button>
            {isDateRangeExpanded && (
              <>
                {dateRangeLoading ? (
                  <div className="p-3 text-center bg-white border border-gray-200 rounded">
                    <p className="text-xs text-gray-500">Loading dates...</p>
                  </div>
                ) : dateRangeInfo?.has_date_column ? (
                  <div className="space-y-2">
                    <div className="p-3 bg-white border border-gray-200 rounded">
                      <div className="text-xs text-gray-600 mb-2">Available Range:</div>
                      <div className="text-xs text-gray-700">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-gray-500">From:</span>
                          <span className="font-medium">{dateRangeInfo.min_date}</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-gray-500">To:</span>
                          <span className="font-medium">{dateRangeInfo.max_date}</span>
                        </div>
                      </div>
                    </div>

                    {/* Date Pickers */}
                    <div className="p-3 bg-white border border-gray-200 rounded space-y-2">
                      <div>
                        <label className="text-xs text-gray-600 block mb-1">Start Date</label>
                        <input
                          type="date"
                          value={config.startDate || ''}
                          onChange={(e) => updateStartDate(e.target.value)}
                          min={dateRangeInfo.min_date || undefined}
                          max={config.endDate || dateRangeInfo.max_date || undefined}
                          className="w-full text-xs border border-gray-300 rounded px-2 py-1"
                        />
                      </div>
                      <div>
                        <label className="text-xs text-gray-600 block mb-1">End Date</label>
                        <input
                          type="date"
                          value={config.endDate || ''}
                          onChange={(e) => updateEndDate(e.target.value)}
                          min={config.startDate || dateRangeInfo.min_date || undefined}
                          max={dateRangeInfo.max_date || undefined}
                          className="w-full text-xs border border-gray-300 rounded px-2 py-1"
                        />
                      </div>
                    </div>

                    {/* Draggable Date Range Chip */}
                    {config.startDate && config.endDate && (
                      <div
                        draggable
                        onDragStart={(e) => {
                          e.dataTransfer.setData('type', 'daterange')
                          e.dataTransfer.setData('startDate', config.startDate)
                          e.dataTransfer.setData('endDate', config.endDate)
                        }}
                        className="p-2 bg-purple-50 border-2 border-purple-300 rounded cursor-move hover:bg-purple-100 transition-colors"
                      >
                        <div className="flex items-center gap-2">
                          <GripVertical className="h-3 w-3 text-purple-600" />
                          <div className="flex-1">
                            <div className="text-xs font-medium text-purple-900">
                              {config.startDate} → {config.endDate}
                            </div>
                            <div className="text-xs text-purple-600">Drag to table</div>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="p-3 text-center bg-yellow-50 border border-yellow-200 rounded">
                    <p className="text-xs text-yellow-700">This table has no date column</p>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {/* Dimension Selection */}
        <div>
          <button
            onClick={() => setIsDimensionsExpanded(!isDimensionsExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Dimensions
            </h4>
            {isDimensionsExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isDimensionsExpanded && (
          <div className="space-y-2">
            {AVAILABLE_DIMENSIONS.map((dimension) => (
              <div
                key={dimension.value}
                draggable
                onDragStart={(e) => {
                  e.dataTransfer.setData('type', 'dimension')
                  e.dataTransfer.setData('value', dimension.value)
                  e.dataTransfer.setData('label', dimension.label)
                }}
                className="p-2 bg-green-50 border-2 border-green-300 rounded cursor-move hover:bg-green-100 transition-colors"
              >
                <div className="flex items-center gap-2">
                  <GripVertical className="h-3 w-3 text-green-600" />
                  <div className="flex-1">
                    <div className="text-xs font-medium text-gray-900">
                      {dimension.label}
                    </div>
                    <div className="text-xs text-green-600">Drag to table</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
          )}
        </div>

        {/* Filters */}
        <div>
          <button
            onClick={() => setIsFiltersExpanded(!isFiltersExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Filters
            </h4>
            {isFiltersExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isFiltersExpanded && (
            <>
              {config.selectedFilters.length === 0 ? (
                <div className="p-3 text-center bg-pink-50 border border-pink-200 rounded">
                  <p className="text-xs text-pink-700">
                    No filters applied yet
                  </p>
                  <p className="text-xs text-gray-500 mt-1">
                    Filters will appear here once created
                  </p>
                </div>
              ) : (
                <div className="space-y-2">
                  {config.selectedFilters.map((filter, index) => (
                    <div
                      key={`${filter.dimension}-${filter.value}-${index}`}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.setData('type', 'filter')
                        e.dataTransfer.setData('dimension', filter.dimension)
                        e.dataTransfer.setData('value', filter.value)
                        e.dataTransfer.setData('label', filter.label)
                        e.dataTransfer.setData('index', String(index))
                      }}
                      className="p-2 bg-pink-50 border-2 border-pink-300 rounded cursor-move hover:bg-pink-100 transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        <GripVertical className="h-3 w-3 text-pink-600" />
                        <div className="flex-1">
                          <div className="text-xs font-medium text-gray-900">
                            {filter.label}
                          </div>
                          <div className="text-xs text-pink-600">Drag to table</div>
                        </div>
                        <button
                          onClick={() => removeFilter(index)}
                          className="text-gray-400 hover:text-red-600 transition-colors"
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>

        {/* Metrics */}
        <div>
          <button
            onClick={() => setIsMetricsExpanded(!isMetricsExpanded)}
            className="w-full flex items-center justify-between mb-2 hover:opacity-70 transition-opacity"
          >
            <h4 className="text-xs font-semibold text-gray-700 uppercase">
              Metrics
            </h4>
            {isMetricsExpanded ? (
              <ChevronDown className="h-3 w-3 text-gray-500" />
            ) : (
              <ChevronRight className="h-3 w-3 text-gray-500" />
            )}
          </button>
          {isMetricsExpanded && (
            <div className="space-y-4">
              {/* Selected Metrics */}
              <div>
                <h5 className="text-xs font-semibold text-gray-700 uppercase mb-2">
                  Selected ({selectedMetricObjects.length})
                </h5>
                {selectedMetricObjects.length === 0 ? (
                  <div className="p-4 text-center bg-white border-2 border-dashed border-gray-300 rounded">
                    <p className="text-xs text-gray-500">
                      Add metrics below
                    </p>
                  </div>
                ) : (
                  <DndContext
                    sensors={sensors}
                    collisionDetection={closestCenter}
                    onDragEnd={handleDragEnd}
                  >
                    <SortableContext
                      items={config.selectedMetrics}
                      strategy={verticalListSortingStrategy}
                    >
                      <div className="space-y-1">
                        {selectedMetricObjects.map((metric) => (
                          <SortableMetric
                            key={metric.id}
                            metric={metric}
                            onRemove={removeMetric}
                          />
                        ))}
                      </div>
                    </SortableContext>
                  </DndContext>
                )}
              </div>

              {/* Available Metrics */}
              <div>
                <h5 className="text-xs font-semibold text-gray-700 uppercase mb-2">
                  Available ({availableMetricsToAdd.length})
                </h5>
                {availableMetricsToAdd.length === 0 ? (
                  <div className="p-4 text-center bg-white border border-gray-200 rounded">
                    <p className="text-xs text-gray-500">All selected</p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {Object.entries(metricsByCategory).map(([category, metrics]) => (
                      <div key={category}>
                        <h6 className="text-xs font-medium text-gray-600 mb-1">
                          {category}
                        </h6>
                        <div className="space-y-1">
                          {metrics.map((metric) => (
                            <AvailableMetric
                              key={metric.id}
                              metric={metric}
                              onAdd={addMetric}
                              isSelected={false}
                            />
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="bg-white border-t border-gray-200 p-4">
        <button
          onClick={resetToDefaults}
          className="w-full text-xs text-gray-600 hover:text-gray-900 transition-colors py-1"
        >
          Reset to Defaults
        </button>
      </div>
    </div>
  )
}
