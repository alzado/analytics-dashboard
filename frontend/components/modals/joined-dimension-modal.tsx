'use client'

import { useState, useCallback, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, Upload, Trash2, FileSpreadsheet, ChevronLeft, Check } from 'lucide-react'
import { parseJoinedDimensionPreview, createJoinedDimensionSource } from '@/lib/api'
import type {
  FilePreview,
  FilePreviewColumn,
  DimensionDef,
  DimensionDataType,
  FilterType,
  JoinedDimensionColumnCreate
} from '@/lib/types'

interface JoinedDimensionModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: () => void
  tableId?: string
  existingDimensions: DimensionDef[]
  defaultBqProject?: string
  defaultBqDataset?: string
}

interface ColumnMapping {
  source_column_name: string
  dimension_id: string
  display_name: string
  data_type: DimensionDataType
  is_filterable: boolean
  is_groupable: boolean
  filter_type: FilterType
  selected: boolean
}

export default function JoinedDimensionModal({
  isOpen,
  onClose,
  onSave,
  tableId,
  existingDimensions,
  defaultBqProject = '',
  defaultBqDataset = ''
}: JoinedDimensionModalProps) {
  const [step, setStep] = useState<'upload' | 'configure'>('upload')
  const [file, setFile] = useState<File | null>(null)
  const [preview, setPreview] = useState<FilePreview | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isDragging, setIsDragging] = useState(false)

  // Configuration state
  const [name, setName] = useState('')
  const [joinKeyColumn, setJoinKeyColumn] = useState('')
  const [targetDimensionId, setTargetDimensionId] = useState('')
  const [bqProject, setBqProject] = useState(defaultBqProject)
  const [bqDataset, setBqDataset] = useState(defaultBqDataset)
  const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>([])

  // Reset state when modal opens/closes
  useEffect(() => {
    if (isOpen) {
      setStep('upload')
      setFile(null)
      setPreview(null)
      setError(null)
      setName('')
      setJoinKeyColumn('')
      setTargetDimensionId('')
      setBqProject(defaultBqProject)
      setBqDataset(defaultBqDataset)
      setColumnMappings([])
    }
  }, [isOpen, defaultBqProject, defaultBqDataset])

  const handleFileSelect = useCallback(async (selectedFile: File) => {
    // Validate file type
    const filename = selectedFile.name.toLowerCase()
    if (!filename.endsWith('.csv') && !filename.endsWith('.xlsx') && !filename.endsWith('.xls')) {
      setError('File must be CSV (.csv) or Excel (.xlsx)')
      return
    }

    // Validate file size (10MB max)
    if (selectedFile.size > 10 * 1024 * 1024) {
      setError('File size must be under 10MB')
      return
    }

    setFile(selectedFile)
    setIsLoading(true)
    setError(null)

    try {
      const previewData = await parseJoinedDimensionPreview(selectedFile, tableId)
      setPreview(previewData)
      setName(selectedFile.name.replace(/\.(csv|xlsx|xls)$/i, ''))
      setStep('configure')

      // Initialize column mappings - all columns as potential dimensions
      const mappings: ColumnMapping[] = previewData.columns.map((col: FilePreviewColumn, idx: number) => ({
        source_column_name: col.name,
        dimension_id: `joined_${col.name.toLowerCase().replace(/\s+/g, '_').replace(/[^\w]/g, '')}`,
        display_name: col.name,
        data_type: (col.inferred_type as DimensionDataType) || 'STRING',
        is_filterable: true,
        is_groupable: true,
        filter_type: col.inferred_type === 'BOOLEAN' ? 'boolean' : 'multi' as FilterType,
        selected: idx > 0  // Select all except first (likely join key)
      }))
      setColumnMappings(mappings)

      // Default join key to first column
      if (previewData.columns.length > 0) {
        setJoinKeyColumn(previewData.columns[0].name)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setIsLoading(false)
    }
  }, [tableId])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)

    const droppedFile = e.dataTransfer.files[0]
    if (droppedFile) {
      handleFileSelect(droppedFile)
    }
  }, [handleFileSelect])

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
  }, [])

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      handleFileSelect(selectedFile)
    }
  }, [handleFileSelect])

  const handleSave = async () => {
    // Validation
    if (!file || !name.trim()) {
      setError('Please provide a name for this dimension source')
      return
    }

    if (!joinKeyColumn) {
      setError('Please select a join key column from the file')
      return
    }

    if (!targetDimensionId) {
      setError('Please select which schema dimension to join against')
      return
    }

    if (!bqProject.trim() || !bqDataset.trim()) {
      setError('Please provide BigQuery project and dataset')
      return
    }

    const selectedColumns = columnMappings.filter(col => col.selected && col.source_column_name !== joinKeyColumn)
    if (selectedColumns.length === 0) {
      setError('Please select at least one dimension column')
      return
    }

    setIsLoading(true)
    setError(null)

    try {
      const formData = new FormData()
      formData.append('file', file)
      formData.append('name', name.trim())
      formData.append('join_key_column', joinKeyColumn)
      formData.append('target_dimension_id', targetDimensionId)
      formData.append('bq_project', bqProject.trim())
      formData.append('bq_dataset', bqDataset.trim())

      const columnsToSend: JoinedDimensionColumnCreate[] = selectedColumns.map(col => ({
        source_column_name: col.source_column_name,
        dimension_id: col.dimension_id,
        display_name: col.display_name,
        data_type: col.data_type,
        is_filterable: col.is_filterable,
        is_groupable: col.is_groupable,
        filter_type: col.filter_type
      }))
      formData.append('columns', JSON.stringify(columnsToSend))

      await createJoinedDimensionSource(formData, tableId)
      onSave()
      onClose()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Save failed')
    } finally {
      setIsLoading(false)
    }
  }

  const toggleColumnSelection = (index: number) => {
    setColumnMappings(prev =>
      prev.map((col, i) => i === index ? { ...col, selected: !col.selected } : col)
    )
  }

  const updateColumnMapping = (index: number, updates: Partial<ColumnMapping>) => {
    setColumnMappings(prev =>
      prev.map((col, i) => i === index ? { ...col, ...updates } : col)
    )
  }

  if (!isOpen) return null

  const modalContent = (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b">
          <div className="flex items-center gap-3">
            <FileSpreadsheet className="w-6 h-6 text-blue-600" />
            <h2 className="text-xl font-semibold">Add Joined Dimension</h2>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={24} />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
              <p className="text-red-600 text-sm">{error}</p>
            </div>
          )}

          {step === 'upload' && (
            <div
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              className={`border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-colors ${
                isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
              }`}
              onClick={() => document.getElementById('file-input')?.click()}
            >
              <input
                id="file-input"
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={handleFileInputChange}
                className="hidden"
              />
              <Upload className="w-12 h-12 mx-auto mb-4 text-gray-400" />
              <p className="text-lg font-medium text-gray-700">
                {isDragging ? 'Drop the file here' : 'Drag & drop a CSV or Excel file'}
              </p>
              <p className="text-sm text-gray-500 mt-2">
                or click to browse (max 10MB)
              </p>
              {isLoading && (
                <p className="text-sm text-blue-600 mt-4">Processing file...</p>
              )}
            </div>
          )}

          {step === 'configure' && preview && (
            <div className="space-y-6">
              {/* Source Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Source Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., Seller Attributes"
                />
              </div>

              {/* BigQuery Location */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    BigQuery Project <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={bqProject}
                    onChange={(e) => setBqProject(e.target.value)}
                    className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="my-project"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    BigQuery Dataset <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={bqDataset}
                    onChange={(e) => setBqDataset(e.target.value)}
                    className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="my_dataset"
                  />
                </div>
              </div>

              {/* Join Configuration */}
              <div className="bg-gray-50 p-4 rounded-lg">
                <h4 className="text-sm font-medium text-gray-700 mb-3">Join Configuration</h4>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm text-gray-600 mb-2">
                      Join Key Column (from file) <span className="text-red-500">*</span>
                    </label>
                    <select
                      value={joinKeyColumn}
                      onChange={(e) => setJoinKeyColumn(e.target.value)}
                      className="w-full px-4 py-2 border rounded-lg bg-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    >
                      <option value="">Select column...</option>
                      {preview.columns.map(col => (
                        <option key={col.name} value={col.name}>{col.name}</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm text-gray-600 mb-2">
                      Join To Dimension <span className="text-red-500">*</span>
                    </label>
                    <select
                      value={targetDimensionId}
                      onChange={(e) => setTargetDimensionId(e.target.value)}
                      className="w-full px-4 py-2 border rounded-lg bg-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    >
                      <option value="">Select dimension...</option>
                      {existingDimensions.map(dim => (
                        <option key={dim.id} value={dim.id}>{dim.display_name}</option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>

              {/* Column Selection */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-3">
                  Select Dimension Columns
                </h4>
                <p className="text-sm text-gray-500 mb-3">
                  Choose which columns from the file should become new dimensions.
                </p>
                <div className="space-y-2 max-h-64 overflow-y-auto border rounded-lg p-2">
                  {columnMappings.map((col, idx) => (
                    <div
                      key={col.source_column_name}
                      className={`flex items-center gap-3 p-3 rounded-lg transition-colors ${
                        col.source_column_name === joinKeyColumn
                          ? 'bg-gray-100 opacity-50'
                          : col.selected
                          ? 'bg-blue-50 border border-blue-200'
                          : 'bg-gray-50 hover:bg-gray-100'
                      }`}
                    >
                      <button
                        type="button"
                        disabled={col.source_column_name === joinKeyColumn}
                        onClick={() => toggleColumnSelection(idx)}
                        className={`w-5 h-5 rounded border flex items-center justify-center ${
                          col.source_column_name === joinKeyColumn
                            ? 'bg-gray-200 border-gray-300 cursor-not-allowed'
                            : col.selected
                            ? 'bg-blue-600 border-blue-600'
                            : 'bg-white border-gray-300 hover:border-gray-400'
                        }`}
                      >
                        {col.selected && col.source_column_name !== joinKeyColumn && (
                          <Check className="w-3 h-3 text-white" />
                        )}
                      </button>

                      <div className="flex-1 min-w-0">
                        <span className="text-sm font-medium text-gray-700">
                          {col.source_column_name}
                        </span>
                        {col.source_column_name === joinKeyColumn && (
                          <span className="ml-2 text-xs text-gray-500">(Join Key)</span>
                        )}
                      </div>

                      {col.selected && col.source_column_name !== joinKeyColumn && (
                        <>
                          <input
                            type="text"
                            value={col.display_name}
                            onChange={(e) => updateColumnMapping(idx, { display_name: e.target.value })}
                            className="w-32 px-2 py-1 border rounded text-sm"
                            placeholder="Display name"
                          />
                          <select
                            value={col.data_type}
                            onChange={(e) => updateColumnMapping(idx, {
                              data_type: e.target.value as DimensionDataType,
                              filter_type: e.target.value === 'BOOLEAN' ? 'boolean' : 'multi' as FilterType
                            })}
                            className="px-2 py-1 border rounded text-sm bg-white"
                          >
                            <option value="STRING">String</option>
                            <option value="INTEGER">Integer</option>
                            <option value="FLOAT">Float</option>
                            <option value="BOOLEAN">Boolean</option>
                          </select>
                        </>
                      )}
                    </div>
                  ))}
                </div>
              </div>

              {/* Preview */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-2">
                  Data Preview ({preview.row_count.toLocaleString()} rows)
                </h4>
                <div className="border rounded-lg overflow-x-auto max-h-48">
                  <table className="min-w-full text-sm">
                    <thead className="bg-gray-50 sticky top-0">
                      <tr>
                        {preview.columns.map(col => (
                          <th key={col.name} className="px-3 py-2 text-left font-medium text-gray-600 whitespace-nowrap">
                            {col.name}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {preview.preview_rows.slice(0, 5).map((row, idx) => (
                        <tr key={idx} className="border-t">
                          {preview.columns.map(col => (
                            <td key={col.name} className="px-3 py-2 whitespace-nowrap">
                              {String(row[col.name] ?? '')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t bg-gray-50">
          <div>
            {step === 'configure' && (
              <button
                onClick={() => {
                  setStep('upload')
                  setFile(null)
                  setPreview(null)
                }}
                className="flex items-center gap-2 text-gray-600 hover:text-gray-800"
              >
                <ChevronLeft className="w-4 h-4" />
                Choose different file
              </button>
            )}
          </div>
          <div className="flex gap-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-gray-700 bg-white border rounded-lg hover:bg-gray-50"
            >
              Cancel
            </button>
            {step === 'configure' && (
              <button
                onClick={handleSave}
                disabled={isLoading}
                className="px-4 py-2 text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isLoading ? 'Saving...' : 'Save Dimension'}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )

  return typeof window !== 'undefined' ? createPortal(modalContent, document.body) : null
}
