'use client'

import { useState } from 'react'
import { createPortal } from 'react-dom'
import { X, Download, FileSpreadsheet, FileCode, BarChart3, Image } from 'lucide-react'

export type ExportFormat = 'csv' | 'html' | 'png'

export interface ExportOptions {
  format: ExportFormat
  rowLimit?: number  // For PNG: limit number of dimension value rows
}

export interface ExportData {
  metadata: {
    exportDate: string
    dataSource?: string
    dateRange: string
    dimensions: string
    tableDimensions?: string
    metrics: string
    filters: { label: string; values: string }[]
  }
  headers: string[]
  rows: (string | number)[][]
  dimensionRowCount?: number  // Total unique dimension values (for multi-table: rows / metrics count)
}

interface ExportModalProps {
  isOpen: boolean
  onClose: () => void
  onExport: (options: ExportOptions) => void
  data: ExportData | null
}

export default function ExportModal({
  isOpen,
  onClose,
  onExport,
  data,
}: ExportModalProps) {
  const [selectedFormat, setSelectedFormat] = useState<ExportFormat>('csv')
  const [pngRowLimit, setPngRowLimit] = useState<string>('')  // Empty = all rows

  if (!isOpen) return null

  // Calculate dimension row count from data
  const dimensionRowCount = data?.dimensionRowCount ?? data?.rows.length ?? 0

  const handleExport = () => {
    const options: ExportOptions = { format: selectedFormat }
    if (selectedFormat === 'png' && pngRowLimit) {
      const limit = parseInt(pngRowLimit, 10)
      if (!isNaN(limit) && limit > 0) {
        options.rowLimit = limit
      }
    }
    onExport(options)
    onClose()
  }

  const modalContent = (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Download className="w-5 h-5 text-blue-600" />
            <h2 className="text-xl font-semibold text-gray-900">
              Export Data
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
        <div className="p-6">
          <h3 className="text-sm font-medium text-gray-900 mb-4">Select export format</h3>

          <div className="space-y-3">
            {/* CSV Option */}
            <label
              className={`flex items-start gap-4 p-4 border rounded-lg cursor-pointer transition-colors ${
                selectedFormat === 'csv'
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <input
                type="radio"
                name="format"
                value="csv"
                checked={selectedFormat === 'csv'}
                onChange={() => setSelectedFormat('csv')}
                className="mt-1"
              />
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <FileSpreadsheet className="w-5 h-5 text-green-600" />
                  <span className="font-medium text-gray-900">CSV</span>
                </div>
                <p className="text-sm text-gray-600 mt-1">
                  Spreadsheet format. Opens in Excel, Google Sheets, etc.
                  Includes metadata header with filters and configuration.
                </p>
              </div>
            </label>

            {/* HTML Option */}
            <label
              className={`flex items-start gap-4 p-4 border rounded-lg cursor-pointer transition-colors ${
                selectedFormat === 'html'
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <input
                type="radio"
                name="format"
                value="html"
                checked={selectedFormat === 'html'}
                onChange={() => setSelectedFormat('html')}
                className="mt-1"
              />
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <FileCode className="w-5 h-5 text-orange-600" />
                  <span className="font-medium text-gray-900">HTML</span>
                  <span className="px-2 py-0.5 text-xs bg-blue-100 text-blue-700 rounded-full flex items-center gap-1">
                    <BarChart3 className="w-3 h-3" />
                    Interactive
                  </span>
                </div>
                <p className="text-sm text-gray-600 mt-1">
                  Standalone HTML file with styled table and interactive chart.
                  Opens in any browser, no internet required.
                </p>
              </div>
            </label>

            {/* PNG Option */}
            <label
              className={`flex items-start gap-4 p-4 border rounded-lg cursor-pointer transition-colors ${
                selectedFormat === 'png'
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <input
                type="radio"
                name="format"
                value="png"
                checked={selectedFormat === 'png'}
                onChange={() => setSelectedFormat('png')}
                className="mt-1"
              />
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <Image className="w-5 h-5 text-purple-600" />
                  <span className="font-medium text-gray-900">PNG Image</span>
                </div>
                <p className="text-sm text-gray-600 mt-1">
                  High-resolution image of the complete table.
                  Perfect for presentations, reports, or sharing.
                </p>
                {/* Row limit input - only shown when PNG is selected */}
                {selectedFormat === 'png' && (
                  <div className="mt-3 flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
                    <label htmlFor="pngRowLimit" className="text-sm text-gray-700">
                      Rows to export:
                    </label>
                    <input
                      id="pngRowLimit"
                      type="number"
                      min="1"
                      max={dimensionRowCount}
                      value={pngRowLimit}
                      onChange={(e) => setPngRowLimit(e.target.value)}
                      placeholder={`All (${dimensionRowCount})`}
                      className="w-28 px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                    <span className="text-xs text-gray-500">of {dimensionRowCount}</span>
                  </div>
                )}
              </div>
            </label>
          </div>

          {/* Preview info */}
          {data && (
            <div className="mt-6 p-4 bg-gray-50 rounded-lg">
              <h4 className="text-sm font-medium text-gray-700 mb-2">Export preview</h4>
              <div className="text-sm text-gray-600 space-y-1">
                <p><span className="font-medium">{data.rows.length}</span> rows</p>
                <p><span className="font-medium">{data.headers.length - 1}</span> metrics</p>
                {data.metadata.dimensions && (
                  <p>Grouped by: <span className="font-medium">{data.metadata.dimensions}</span></p>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleExport}
            disabled={!data}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
          >
            <Download className="w-4 h-4" />
            Export {selectedFormat.toUpperCase()}
          </button>
        </div>
      </div>
    </div>
  )

  // Use portal to render modal at document body level
  if (typeof window !== 'undefined') {
    return createPortal(modalContent, document.body)
  }

  return null
}
