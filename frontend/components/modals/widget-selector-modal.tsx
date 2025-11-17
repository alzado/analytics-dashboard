'use client'

import { useState } from 'react'
import { X, Table, BarChart3 } from 'lucide-react'
import type { MetricDefinition } from '@/hooks/use-pivot-metrics'

export type WidgetSelection =
  | { type: 'table' }
  | { type: 'multi-table' }
  | { type: 'chart'; metricId: string }

interface WidgetSelectorModalProps {
  metrics: MetricDefinition[]
  hasTableDimensions: boolean // Whether we're in multi-table mode
  onSelect: (selection: WidgetSelection) => void
  onCancel: () => void
}

export function WidgetSelectorModal({ metrics, hasTableDimensions, onSelect, onCancel }: WidgetSelectorModalProps) {
  const [hoveredOption, setHoveredOption] = useState<string | null>(null)

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full mx-4 max-h-[80vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900">Select Widget Type</h2>
          <button
            onClick={onCancel}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X size={24} />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto">
          <p className="text-sm text-gray-600 mb-6">
            {hasTableDimensions
              ? 'Choose which visualization to add to your dashboard. You can add the multi-table view or individual metric charts.'
              : 'Add this pivot table to your dashboard.'}
          </p>

          <div className="space-y-3">
            {hasTableDimensions ? (
              <>
                {/* Multi-Table Option */}
                <button
                  onClick={() => onSelect({ type: 'multi-table' })}
                  onMouseEnter={() => setHoveredOption('multi-table')}
                  onMouseLeave={() => setHoveredOption(null)}
                  className={`w-full flex items-center gap-4 p-4 border-2 rounded-lg transition-all text-left ${
                    hoveredOption === 'multi-table'
                      ? 'border-purple-500 bg-purple-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className={`p-3 rounded-lg ${
                    hoveredOption === 'multi-table' ? 'bg-purple-100' : 'bg-gray-100'
                  }`}>
                    <Table className={`h-6 w-6 ${
                      hoveredOption === 'multi-table' ? 'text-purple-600' : 'text-gray-600'
                    }`} />
                  </div>
                  <div className="flex-1">
                    <h3 className="font-semibold text-gray-900">Multi-Table View</h3>
                    <p className="text-sm text-gray-600">
                      Table showing all metrics across table dimensions ({metrics.length} {metrics.length === 1 ? 'metric' : 'metrics'})
                    </p>
                  </div>
                </button>

                {/* Divider */}
                <div className="flex items-center gap-3 py-2">
                  <div className="flex-1 border-t border-gray-200"></div>
                  <span className="text-xs text-gray-500 font-medium">OR CHOOSE A METRIC CHART</span>
                  <div className="flex-1 border-t border-gray-200"></div>
                </div>

                {/* Individual Metric Charts */}
                {metrics.map((metric) => (
                  <button
                    key={metric.id}
                    onClick={() => onSelect({ type: 'chart', metricId: metric.id })}
                    onMouseEnter={() => setHoveredOption(metric.id)}
                    onMouseLeave={() => setHoveredOption(null)}
                    className={`w-full flex items-center gap-4 p-4 border-2 rounded-lg transition-all text-left ${
                      hoveredOption === metric.id
                        ? 'border-green-500 bg-green-50'
                        : 'border-gray-200 hover:border-gray-300'
                    }`}
                  >
                    <div className={`p-3 rounded-lg ${
                      hoveredOption === metric.id ? 'bg-green-100' : 'bg-gray-100'
                    }`}>
                      <BarChart3 className={`h-6 w-6 ${
                        hoveredOption === metric.id ? 'text-green-600' : 'text-gray-600'
                      }`} />
                    </div>
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900">{metric.label}</h3>
                      <p className="text-sm text-gray-600">
                        Chart showing {metric.label.toLowerCase()} across table dimensions
                      </p>
                    </div>
                  </button>
                ))}
              </>
            ) : (
              /* Pivot Table Option - Only in pivot mode */
              <button
                onClick={() => onSelect({ type: 'table' })}
                onMouseEnter={() => setHoveredOption('table')}
                onMouseLeave={() => setHoveredOption(null)}
                className={`w-full flex items-center gap-4 p-4 border-2 rounded-lg transition-all text-left ${
                  hoveredOption === 'table'
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300'
                }`}
              >
                <div className={`p-3 rounded-lg ${
                  hoveredOption === 'table' ? 'bg-blue-100' : 'bg-gray-100'
                }`}>
                  <Table className={`h-6 w-6 ${
                    hoveredOption === 'table' ? 'text-blue-600' : 'text-gray-600'
                  }`} />
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold text-gray-900">Pivot Table</h3>
                  <p className="text-sm text-gray-600">
                    Standard pivot table with all selected metrics ({metrics.length} {metrics.length === 1 ? 'metric' : 'metrics'})
                  </p>
                </div>
              </button>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-3 p-6 border-t bg-gray-50">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  )
}
