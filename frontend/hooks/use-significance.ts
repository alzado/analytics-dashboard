import { useState, useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  calculateSignificance,
  SignificanceRequest,
  SignificanceResponse,
  SignificanceResultItem,
  ColumnDefinition,
  RowDefinition
} from '@/lib/api'
import type { FilterParams, PivotResponse, PivotRow } from '@/lib/types'

interface TableCombination {
  [dimensionId: string]: string
}

interface UseSignificanceOptions {
  /** All column data from pivot table */
  allColumnData: Record<number, PivotResponse> | null
  /** Table combinations that define each column's dimension filters */
  tableCombinations: TableCombination[]
  /** Column order (first column is the control/starred column) */
  columnOrder: number[]
  /** Selected metrics to analyze */
  selectedMetrics: string[]
  /** Table dimensions used for columns */
  selectedTableDimensions: string[]
  /** Row dimensions used for grouping */
  selectedRowDimensions: string[]
  /** Rows from the pivot table (for per-row testing) */
  pivotRows: PivotRow[]
  /** Base filters (date range, etc.) */
  filters: FilterParams
  /** Table ID for multi-table support */
  tableId?: string
  /** Whether significance testing is enabled */
  enabled?: boolean
}

interface UseSignificanceReturn {
  /** Whether significance results are available */
  hasResults: boolean
  /** Whether significance test is currently running */
  isLoading: boolean
  /** Error message if test failed */
  error: string | null
  /** Run the significance test */
  runSignificanceTest: () => void
  /** Clear significance results */
  clearResults: () => void
  /** Get significance result for a specific cell (with optional rowId for per-row results) */
  getSignificanceForCell: (
    columnIndex: number,
    metricId: string,
    rowId?: string
  ) => SignificanceResultItem | null
  /** Get all results for a metric */
  getResultsForMetric: (metricId: string) => SignificanceResultItem[]
  /** Control column index */
  controlColumnIndex: number | null
  /** Full significance response */
  significanceResults: SignificanceResponse | null
}

/**
 * Hook for managing Bayesian significance testing between pivot table columns.
 *
 * Usage:
 * 1. Pass pivot table state (allColumnData, tableCombinations, columnOrder, pivotRows, etc.)
 * 2. Call runSignificanceTest() when user clicks "Run Significance Test" button
 * 3. Use getSignificanceForCell(columnIndex, metricId, rowId) to get results for rendering indicators
 */
export function useSignificance({
  allColumnData,
  tableCombinations,
  columnOrder,
  selectedMetrics,
  selectedTableDimensions,
  selectedRowDimensions,
  pivotRows,
  filters,
  tableId,
  enabled = true
}: UseSignificanceOptions): UseSignificanceReturn {
  // Track whether user has requested a significance test
  const [testRequested, setTestRequested] = useState(false)
  // Store snapshot of the request when user clicks "Run Test" (for stable query keys)
  const [fetchedRequest, setFetchedRequest] = useState<SignificanceRequest | null>(null)

  // Build the significance request from pivot table state
  const significanceRequest = useMemo((): SignificanceRequest | null => {
    // Need at least 2 columns to compare
    if (columnOrder.length < 2) return null
    if (!tableCombinations.length) return null
    if (!selectedMetrics.length) return null
    if (!selectedTableDimensions.length) return null

    // Control column is the first column in columnOrder (starred column)
    const controlColIndex = columnOrder[0]
    const controlCombination = tableCombinations[controlColIndex]
    if (!controlCombination) return null

    // Build control column definition
    const controlColumn: ColumnDefinition = {
      column_index: controlColIndex,
      dimension_filters: {}
    }
    selectedTableDimensions.forEach(dim => {
      if (controlCombination[dim]) {
        controlColumn.dimension_filters[dim] = [controlCombination[dim]]
      }
    })

    // Build treatment columns (all other columns)
    const treatmentColumns: ColumnDefinition[] = []
    for (let i = 1; i < columnOrder.length; i++) {
      const colIndex = columnOrder[i]
      const combination = tableCombinations[colIndex]
      if (!combination) continue

      const treatmentColumn: ColumnDefinition = {
        column_index: colIndex,
        dimension_filters: {}
      }
      selectedTableDimensions.forEach(dim => {
        if (combination[dim]) {
          treatmentColumn.dimension_filters[dim] = [combination[dim]]
        }
      })
      treatmentColumns.push(treatmentColumn)
    }

    if (treatmentColumns.length === 0) return null

    // Build rows array for per-row testing
    // Each row is defined by its dimension value(s) from the first row dimension
    const rows: RowDefinition[] = []
    if (pivotRows.length > 0 && selectedRowDimensions.length > 0) {
      const primaryRowDimension = selectedRowDimensions[0]

      for (const row of pivotRows) {
        // Skip "Other" merged rows - they can't be tested individually
        if (row.dimension_value === 'Other') continue

        rows.push({
          row_id: row.dimension_value,
          dimension_filters: {
            [primaryRowDimension]: [row.dimension_value]
          }
        })
      }
    }

    return {
      control_column: controlColumn,
      treatment_columns: treatmentColumns,
      metric_ids: selectedMetrics,
      filters: filters,
      rows: rows.length > 0 ? rows : undefined
    }
  }, [columnOrder, tableCombinations, selectedMetrics, selectedTableDimensions, selectedRowDimensions, pivotRows, filters])

  // Query for significance results
  // Uses fetchedRequest (snapshot) for stable query keys - only refetches when user clicks "Run Test"
  const {
    data: significanceResults,
    isLoading,
    error,
    refetch
  } = useQuery({
    queryKey: ['significance', fetchedRequest, tableId],
    queryFn: async () => {
      if (!fetchedRequest) {
        throw new Error('Invalid request')
      }
      return calculateSignificance(fetchedRequest, tableId)
    },
    enabled: enabled && testRequested && fetchedRequest !== null,
    staleTime: Infinity, // Don't refetch automatically
    retry: false
  })

  // Run the significance test
  const runSignificanceTest = useCallback(() => {
    if (!significanceRequest) return
    // Store current request as snapshot for stable query keys
    setFetchedRequest(significanceRequest)
    if (!testRequested) {
      // First time - enable the query
      setTestRequested(true)
    } else {
      // Already enabled - force refetch (will use new fetchedRequest)
      refetch()
    }
  }, [significanceRequest, testRequested, refetch])

  // Clear results
  const clearResults = useCallback(() => {
    setTestRequested(false)
    setFetchedRequest(null)
  }, [])

  // Get significance result for a specific cell (with optional rowId for per-row results)
  const getSignificanceForCell = useCallback(
    (columnIndex: number, metricId: string, rowId?: string): SignificanceResultItem | null => {
      if (!significanceResults) return null

      const metricResults = significanceResults.results[metricId]
      if (!metricResults) return null

      // Find result for this column and row
      // If rowId is provided, find the per-row result
      // If rowId is not provided or null, find the total-level result (where row_id is null/undefined)
      return metricResults.find(r => {
        const columnMatch = r.column_index === columnIndex
        if (rowId) {
          // Looking for a specific row
          return columnMatch && r.row_id === rowId
        } else {
          // Looking for totals (no row_id)
          return columnMatch && (r.row_id === null || r.row_id === undefined)
        }
      }) || null
    },
    [significanceResults]
  )

  // Get all results for a metric
  const getResultsForMetric = useCallback(
    (metricId: string): SignificanceResultItem[] => {
      if (!significanceResults) return []
      return significanceResults.results[metricId] || []
    },
    [significanceResults]
  )

  // Control column index
  const controlColumnIndex = columnOrder.length > 0 ? columnOrder[0] : null

  return {
    hasResults: !!significanceResults,
    isLoading,
    error: error ? (error as Error).message : null,
    runSignificanceTest,
    clearResults,
    getSignificanceForCell,
    getResultsForMetric,
    controlColumnIndex,
    significanceResults: significanceResults || null
  }
}
