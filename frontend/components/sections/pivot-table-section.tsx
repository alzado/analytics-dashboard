'use client'

import React, { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchPivotData, fetchPivotChildren } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import type { PivotRow, PivotChildRow } from '@/lib/types'
import { ChevronRight, ChevronDown, Settings2 } from 'lucide-react'
import { usePivotConfig } from '@/hooks/use-pivot-config'
import { getMetricById, getDimensionByValue } from '@/lib/pivot-metrics'
import { PivotConfigPanel } from '@/components/pivot/pivot-config-panel'

export function PivotTableSection() {
  const { filters } = useFilters()
  const {
    config,
    updateTable,
    updateStartDate,
    updateEndDate,
    setDataSourceDropped,
    setDateRangeDropped,
    addDimension,
    removeDimension,
    addMetric,
    removeMetric,
    removeFilter,
    reorderMetrics,
  } = usePivotConfig()
  const selectedDimensions = config.selectedDimensions
  const selectedMetrics = config.selectedMetrics
  const selectedTable = config.selectedTable
  const selectedDateRange = config.selectedDateRange
  const [isConfigOpen, setIsConfigOpen] = useState(true)
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const [childrenCache, setChildrenCache] = useState<Record<string, PivotChildRow[]>>({})
  const [currentPage, setCurrentPage] = useState<Record<string, number>>({})
  const [loadingRows, setLoadingRows] = useState<Set<string>>(new Set())
  const [cumulativePercentageCache, setCumulativePercentageCache] = useState<Record<string, number>>({}) // Tracks cumulative % at end of each page
  const [mergeThreshold, setMergeThreshold] = useState(0) // 0 means disabled
  const [isDragOver, setIsDragOver] = useState(false)
  const [dragOverColumn, setDragOverColumn] = useState<string | null>(null)
  const CHILDREN_PAGE_SIZE = 10

  // Check if we have required configuration
  const isConfigured = !!(config.isDataSourceDropped && config.isDateRangeDropped)

  // Only query the first dimension for hierarchical drill-down
  const firstDimension = selectedDimensions.length > 0 ? [selectedDimensions[0]] : []

  const { data: pivotData, isLoading, error } = useQuery({
    queryKey: ['pivot', firstDimension, filters, selectedTable, selectedDateRange],
    queryFn: () => {
      // If no dimensions, create a single "All Data" row manually
      if (selectedDimensions.length === 0) {
        return fetchPivotData([], filters, 1).then(data => {
          // Return single row representing all data
          return {
            ...data,
            rows: data.rows.length > 0 ? [{
              ...data.total,
              dimension_value: 'All Data',
              has_children: true, // Allow expanding to see search terms
            }] : []
          }
        })
      }
      return fetchPivotData(firstDimension, filters, 50)
    },
    enabled: isConfigured, // Only fetch when data source and date range are configured
  })

  // Apply merge threshold logic - must be before conditional returns
  const processedRows = React.useMemo(() => {
    const rows = pivotData?.rows || []

    // Mark rows as having children if there are more dimensions to drill into
    // Also convert backend percentage (0-100) to decimal (0-1) for consistent formatting
    const hasMoreDimensions = selectedDimensions.length > 1
    const rowsWithChildren = rows.map(row => ({
      ...row,
      has_children: hasMoreDimensions || row.has_children,
      percentage_of_total: row.percentage_of_total / 100, // Convert from 0-100 to 0-1
    }))

    if (!pivotData || mergeThreshold <= 0) {
      return rowsWithChildren
    }

    const mainRows: PivotRow[] = []
    const otherRows: PivotRow[] = []
    let cumulativePercentage = 0

    // Iterate through rows in order, tracking cumulative percentage
    rowsWithChildren.forEach(row => {
      // Check if adding this row would exceed the threshold
      if (cumulativePercentage < mergeThreshold) {
        mainRows.push(row)
        cumulativePercentage += row.percentage_of_total
      } else {
        // This row and all subsequent rows go into "Other"
        otherRows.push(row)
      }
    })

    // If there are rows to merge, create an "Other" row
    if (otherRows.length > 0) {
      const otherRow: PivotRow = {
        dimension_value: 'Other',
        queries: otherRows.reduce((sum, r) => sum + r.queries, 0),
        queries_pdp: otherRows.reduce((sum, r) => sum + r.queries_pdp, 0),
        queries_a2c: otherRows.reduce((sum, r) => sum + r.queries_a2c, 0),
        purchases: otherRows.reduce((sum, r) => sum + r.purchases, 0),
        revenue: otherRows.reduce((sum, r) => sum + r.revenue, 0),
        ctr: 0, // Will calculate after
        a2c_rate: 0, // Will calculate after
        conversion_rate: 0, // Will calculate after
        revenue_per_query: 0, // Will calculate after
        aov: 0, // Will calculate after
        percentage_of_total: otherRows.reduce((sum, r) => sum + r.percentage_of_total, 0),
        search_term_count: otherRows.reduce((sum, r) => sum + r.search_term_count, 0),
        has_children: false
      }

      // Calculate derived metrics
      if (otherRow.queries > 0) {
        otherRow.ctr = otherRow.queries_pdp / otherRow.queries
        otherRow.a2c_rate = otherRow.queries_a2c / otherRow.queries
        otherRow.conversion_rate = otherRow.purchases / otherRow.queries
        otherRow.revenue_per_query = otherRow.revenue / otherRow.queries
      }
      if (otherRow.purchases > 0) {
        otherRow.aov = otherRow.revenue / otherRow.purchases
      }

      return [...mainRows, otherRow]
    }

    return mainRows
  }, [pivotData, mergeThreshold])

  // Clear cache when dimension changes
  React.useEffect(() => {
    setExpandedRows(new Set())
    setChildrenCache({})
    setCurrentPage({})
    setLoadingRows(new Set())
  }, [selectedDimensions])

  // Handle drop events
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)

    const type = e.dataTransfer.getData('type')

    switch (type) {
      case 'datasource':
        const table = e.dataTransfer.getData('table')
        updateTable(table)
        setDataSourceDropped(true)
        break

      case 'daterange':
        const startDate = e.dataTransfer.getData('startDate')
        const endDate = e.dataTransfer.getData('endDate')
        updateStartDate(startDate)
        updateEndDate(endDate)
        setDateRangeDropped(true)
        break

      case 'dimension':
        const dimensionValue = e.dataTransfer.getData('value')
        addDimension(dimensionValue)
        break

      case 'metric':
        const metricId = e.dataTransfer.getData('id')
        addMetric(metricId)
        break

      default:
        break
    }
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(true)
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
  }

  // Helper functions to remove items
  const removeDataSource = () => {
    updateTable(null)
    setDataSourceDropped(false)
  }

  const removeDateRange = () => {
    updateStartDate(null)
    updateEndDate(null)
    setDateRangeDropped(false)
  }

  const toggleRow = async (dimensionPath: string, depth: number, grandTotalQueries: number) => {
    const key = `depth${depth}:${dimensionPath}`

    if (expandedRows.has(key)) {
      // Collapse row
      const newExpanded = new Set(expandedRows)
      newExpanded.delete(key)
      setExpandedRows(newExpanded)
    } else {
      // Expand row - show immediately and set loading state
      const newExpanded = new Set(expandedRows)
      newExpanded.add(key)
      setExpandedRows(newExpanded)

      const newLoading = new Set(loadingRows)
      newLoading.add(key)
      setLoadingRows(newLoading)

      // Fetch children if not cached for page 0
      const pageKey = `${key}:0`
      if (!childrenCache[pageKey]) {
        try {
          // Special case: No dimensions selected, fetch all search terms
          if (selectedDimensions.length === 0) {
            const children = await fetchPivotChildren('', '', filters, CHILDREN_PAGE_SIZE, 0)
            setChildrenCache(prev => ({ ...prev, [pageKey]: children }))
            // Store cumulative percentage at end of this page
            const cumulative = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
            setCumulativePercentageCache(prev => ({ ...prev, [key]: cumulative }))
          } else {
            // Check if there are more dimensions to drill into
            const nextDepth = depth + 1
            if (nextDepth < selectedDimensions.length) {
              // Fetch next dimension level
              const dimensionsToFetch = selectedDimensions.slice(0, nextDepth + 1)
              const pivotChildren = await fetchPivotData(dimensionsToFetch, filters, 1000)

              // Filter to only children of this parent
              const prefix = `${dimensionPath} - `
              const childRows = pivotChildren.rows.filter(row => row.dimension_value.startsWith(prefix))

              // Calculate percentage relative to grand total (whole dataset)
              const filteredChildren = childRows.map(row => ({
                search_term: row.dimension_value.replace(prefix, ''),
                queries: row.queries,
                queries_pdp: row.queries_pdp,
                queries_a2c: row.queries_a2c,
                purchases: row.purchases,
                revenue: row.revenue,
                ctr: row.ctr,
                a2c_rate: row.a2c_rate,
                conversion_rate: row.conversion_rate,
                revenue_per_query: row.revenue_per_query,
                aov: row.aov,
                percentage_of_total: grandTotalQueries > 0 ? (row.queries / grandTotalQueries) : 0,
              }))

              setChildrenCache(prev => ({ ...prev, [pageKey]: filteredChildren }))
            } else {
              // No more dimensions - fetch search terms
              // Extract the value at each depth level from the path
              const pathParts = dimensionPath.split(' - ')
              const dimensionValue = pathParts[pathParts.length - 1]
              const dimension = selectedDimensions[depth]

              const children = await fetchPivotChildren(dimension, dimensionValue, filters, CHILDREN_PAGE_SIZE, 0)
              setChildrenCache(prev => ({ ...prev, [pageKey]: children }))
              // Store cumulative percentage at end of this page
              const cumulative = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
              setCumulativePercentageCache(prev => ({ ...prev, [key]: cumulative }))
            }
          }
          setCurrentPage(prev => ({ ...prev, [key]: 0 }))
        } catch (error) {
          console.error('Failed to fetch children:', error)
          // Collapse on error
          const errorExpanded = new Set(expandedRows)
          errorExpanded.delete(key)
          setExpandedRows(errorExpanded)
        } finally {
          // Remove loading state after fetch completes
          const doneLoading = new Set(loadingRows)
          doneLoading.delete(key)
          setLoadingRows(doneLoading)
        }
      } else {
        setCurrentPage(prev => ({ ...prev, [key]: 0 }))
        // Remove loading state immediately if data is cached
        const doneLoading = new Set(loadingRows)
        doneLoading.delete(key)
        setLoadingRows(doneLoading)
      }
    }
  }

  const goToPage = async (dimensionPath: string, depth: number, page: number) => {
    const key = `depth${depth}:${dimensionPath}`
    const pageKey = `${key}:${page}`

    const newLoading = new Set(loadingRows)
    newLoading.add(key)
    setLoadingRows(newLoading)

    try {
      if (!childrenCache[pageKey]) {
        const offset = page * CHILDREN_PAGE_SIZE

        // Special case: No dimensions selected, fetch all search terms
        if (selectedDimensions.length === 0) {
          const children = await fetchPivotChildren('', '', filters, CHILDREN_PAGE_SIZE, offset)
          setChildrenCache(prev => ({ ...prev, [pageKey]: children }))

          // Calculate cumulative percentage including previous pages
          let previousCumulative = 0
          if (page > 0) {
            // Sum up cumulative percentages from all previous pages
            for (let p = 0; p < page; p++) {
              const prevPageKey = `${key}:${p}`
              const prevChildren = childrenCache[prevPageKey] || []
              previousCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
            }
          }
          const currentPageTotal = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
          const totalCumulative = previousCumulative + currentPageTotal
          setCumulativePercentageCache(prev => ({ ...prev, [key]: totalCumulative }))
        } else {
          // Extract the value at the current depth level from the path
          const pathParts = dimensionPath.split(' - ')
          const dimensionValue = pathParts[pathParts.length - 1]
          const dimension = selectedDimensions[depth]

          const children = await fetchPivotChildren(dimension, dimensionValue, filters, CHILDREN_PAGE_SIZE, offset)
          setChildrenCache(prev => ({ ...prev, [pageKey]: children }))

          // Calculate cumulative percentage including previous pages
          let previousCumulative = 0
          if (page > 0) {
            // Sum up cumulative percentages from all previous pages
            for (let p = 0; p < page; p++) {
              const prevPageKey = `${key}:${p}`
              const prevChildren = childrenCache[prevPageKey] || []
              previousCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
            }
          }
          const currentPageTotal = children.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
          const totalCumulative = previousCumulative + currentPageTotal
          setCumulativePercentageCache(prev => ({ ...prev, [key]: totalCumulative }))
        }
      }
      setCurrentPage(prev => ({ ...prev, [key]: page }))
    } catch (error) {
      console.error('Failed to fetch page:', error)
    } finally {
      const doneLoading = new Set(loadingRows)
      doneLoading.delete(key)
      setLoadingRows(doneLoading)
    }
  }

  const formatNumber = (num: number) => num.toLocaleString()
  const formatCurrency = (num: number) => `$${num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  const formatPercent = (num: number) => `${(num * 100).toFixed(2)}%`

  // Helper to format value based on metric format
  const formatMetricValue = (value: number, metricId: string): string => {
    const metric = getMetricById(metricId)
    if (!metric) return formatNumber(value)

    switch (metric.format) {
      case 'currency':
        return formatCurrency(value)
      case 'percent':
        return formatPercent(value)
      default:
        return formatNumber(value)
    }
  }

  // Helper to get value from row by metric ID
  const getRowValue = (row: PivotRow | typeof pivotData.total, metricId: string): number => {
    return (row as any)[metricId] ?? 0
  }

  // Recursive helper to render a row and its children
  const renderRow = (
    child: PivotChildRow,
    parentPath: string,
    depth: number,
    indentLevel: number,
    siblings: PivotChildRow[],
    indexInSiblings: number,
    grandTotalQueries: number,
    allSiblingsUpToThis: PivotChildRow[]
  ): React.ReactNode => {
    const dimensionPath = parentPath ? `${parentPath} - ${child.search_term}` : child.search_term
    const rowKey = `depth${depth}:${dimensionPath}`
    const isExpanded = expandedRows.has(rowKey)
    const isLoading = loadingRows.has(rowKey)
    const currentPageNum = currentPage[rowKey] || 0
    const pageKey = `${rowKey}:${currentPageNum}`
    const children = childrenCache[pageKey] || []

    // Check if this row can have children (more dimensions or search terms)
    const canHaveChildren = depth < selectedDimensions.length
    const bgColor = indentLevel % 2 === 1 ? 'bg-blue-50' : 'bg-blue-100'

    // Calculate cumulative percentage relative to grand total (all siblings up to and including this row)
    // Get cumulative from previous pages using the parent's row key
    const parentRowKey = `depth${depth}:${parentPath}`
    const parentPageNum = currentPage[parentRowKey] || 0
    let previousPagesCumulative = 0
    if (parentPageNum > 0) {
      // Sum up all items from previous pages
      for (let p = 0; p < parentPageNum; p++) {
        const prevPageKey = `${parentRowKey}:${p}`
        const prevChildren = childrenCache[prevPageKey] || []
        previousPagesCumulative += prevChildren.reduce((sum, child) => sum + (child.percentage_of_total || 0), 0)
      }
    }
    const currentPageCumulative = allSiblingsUpToThis
      .reduce((sum, sib) => sum + ((sib as any).percentage_of_total || 0), 0)
    const cumulativePercentage = previousPagesCumulative + currentPageCumulative

    return (
      <React.Fragment key={dimensionPath}>
        <tr
          className={`${bgColor} cursor-pointer hover:opacity-80`}
          onClick={() => canHaveChildren && toggleRow(dimensionPath, depth, grandTotalQueries)}
        >
          <td className={`px-6 py-4 whitespace-nowrap text-sm text-gray-700`} style={{paddingLeft: `${indentLevel * 2 + 1.5}rem`}}>
            <div className="flex items-center gap-2">
              {canHaveChildren && (
                isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
              )}
              <span>{child.search_term}</span>
            </div>
          </td>
          {selectedMetrics.map((metricId) => {
            let value = (child as any)[metricId]

            // Handle cumulative percentage specially
            if (metricId === 'cumulative_percentage') {
              value = cumulativePercentage
            }

            return (
              <td
                key={metricId}
                className={`px-6 py-4 whitespace-nowrap text-sm text-gray-700 text-right`}
              >
                {value != null && value !== undefined ? formatMetricValue(value, metricId) : '-'}
              </td>
            )
          })}
        </tr>
        {isExpanded && (
          <>
            {isLoading && children.length === 0 ? (
              <tr className={bgColor}>
                <td colSpan={selectedMetrics.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                  Loading...
                </td>
              </tr>
            ) : (
              <>
                {children.map((grandchild, idx) => {
                  // Get all children from previous pages
                  let allPreviousPagesChildren: PivotChildRow[] = []
                  if (currentPageNum > 0) {
                    for (let p = 0; p < currentPageNum; p++) {
                      const prevPageKey = `${rowKey}:${p}`
                      const prevChildren = childrenCache[prevPageKey] || []
                      allPreviousPagesChildren = [...allPreviousPagesChildren, ...prevChildren]
                    }
                  }
                  // Add children from current page up to this index
                  const allChildrenUpToThis = [...allPreviousPagesChildren, ...children.slice(0, idx + 1)]
                  return renderRow(grandchild, dimensionPath, depth + 1, indentLevel + 1, children, idx, grandTotalQueries, allChildrenUpToThis)
                })}
                {children.length > 0 && children.length === CHILDREN_PAGE_SIZE && !canHaveChildren && (
                  <tr className={bgColor}>
                    <td colSpan={selectedMetrics.length + 1} className="px-6 py-4">
                      {isLoading ? (
                        <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                          Loading...
                        </div>
                      ) : (
                        <div className="flex items-center justify-center gap-4">
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              const currentPageNum = currentPage[rowKey] || 0
                              if (currentPageNum > 0) {
                                goToPage(dimensionPath, depth, currentPageNum - 1)
                              }
                            }}
                            disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                          >
                            Previous
                          </button>
                          <span className="text-sm text-gray-600">
                            Page {(currentPage[rowKey] || 0) + 1}
                          </span>
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              const currentPageNum = currentPage[rowKey] || 0
                              goToPage(dimensionPath, depth, currentPageNum + 1)
                            }}
                            disabled={children.length < CHILDREN_PAGE_SIZE}
                            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                          >
                            Next
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </>
            )}
          </>
        )}
      </React.Fragment>
    )
  }

  // Show empty state if not configured
  if (!isConfigured) {
    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Pivot Table</h2>
            <p className="text-sm text-gray-600 mt-1">Configure your data source to get started</p>
          </div>
          <button
            onClick={() => setIsConfigOpen(!isConfigOpen)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isConfigOpen
                ? 'bg-gray-200 text-gray-700'
                : 'bg-blue-600 text-white hover:bg-blue-700'
            }`}
          >
            <Settings2 className="h-4 w-4" />
            {isConfigOpen ? 'Hide' : 'Configure'}
          </button>
        </div>

        <div className="flex gap-4">
          <PivotConfigPanel isOpen={isConfigOpen} onClose={() => setIsConfigOpen(false)} />

          <div
            className={`flex-1 bg-white shadow overflow-hidden rounded-lg transition-all ${
              isDragOver ? 'ring-4 ring-blue-400 bg-blue-50' : ''
            }`}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
          >
            <div className="flex flex-col items-center justify-center h-96 text-center p-8">
              <div className={`mb-4 transition-colors ${isDragOver ? 'text-blue-600' : 'text-gray-400'}`}>
                <Settings2 className="h-16 w-16 mx-auto" />
              </div>
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                {isDragOver ? 'Drop here to configure' : 'Drop to Configure Pivot Table'}
              </h3>
              <p className="text-sm text-gray-600 max-w-md mb-4">
                {isDragOver
                  ? 'Release to add this configuration'
                  : 'Drag and drop data source, date range, dimensions, and metrics from the sidebar'}
              </p>
              <div className="text-xs text-gray-500 space-y-2">
                <div className="flex items-center gap-2 justify-center">
                  <span className={config.isDataSourceDropped ? 'text-green-600' : 'text-gray-400'}>
                    {config.isDataSourceDropped ? '✓' : '○'}
                  </span>
                  <span>Data Source</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={config.isDateRangeDropped ? 'text-green-600' : 'text-gray-400'}>
                    {config.isDateRangeDropped ? '✓' : '○'}
                  </span>
                  <span>Date Range</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={selectedDimensions.length > 0 ? 'text-green-600' : 'text-gray-400'}>
                    {selectedDimensions.length > 0 ? '✓' : '○'}
                  </span>
                  <span>Dimensions ({selectedDimensions.length})</span>
                </div>
                <div className="flex items-center gap-2 justify-center">
                  <span className={selectedMetrics.length > 0 ? 'text-green-600' : 'text-gray-400'}>
                    {selectedMetrics.length > 0 ? '✓' : '○'}
                  </span>
                  <span>Metrics ({selectedMetrics.length})</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading pivot table
      </div>
    )
  }

  if (!pivotData) {
    return null
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Pivot Table</h2>
          <p className="text-sm text-gray-600 mt-1">
            {selectedDimensions.length > 0 ? (
              <>
                Grouped by: <span className="font-medium">
                  {selectedDimensions.map(dim => getDimensionByValue(dim)?.label || dim).join(' > ')}
                </span>
              </>
            ) : (
              <span className="font-medium">Showing aggregated totals</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Show top:</label>
            <input
              type="number"
              min="0"
              max="100"
              step="1"
              value={mergeThreshold}
              onChange={(e) => setMergeThreshold(parseFloat(e.target.value) || 0)}
              className="w-20 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="100"
            />
            <span className="text-sm text-gray-600">%</span>
            {mergeThreshold > 0 && mergeThreshold < 100 && (
              <button
                onClick={() => setMergeThreshold(0)}
                className="text-sm text-blue-600 hover:text-blue-800"
              >
                Clear
              </button>
            )}
          </div>
          <button
            onClick={() => setIsConfigOpen(!isConfigOpen)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isConfigOpen
                ? 'bg-gray-200 text-gray-700'
                : 'bg-blue-600 text-white hover:bg-blue-700'
            }`}
          >
            <Settings2 className="h-4 w-4" />
            {isConfigOpen ? 'Hide' : 'Configure'}
          </button>
        </div>
      </div>

      <div className="flex gap-4">
        <PivotConfigPanel isOpen={isConfigOpen} onClose={() => setIsConfigOpen(false)} />

        <div className="flex-1 space-y-4">
          {/* Configuration Bar */}
          {isConfigured && (
            <div className="bg-white shadow rounded-lg p-4">
              <div className="flex flex-wrap gap-2">
                {/* Data Source Chip */}
                {config.isDataSourceDropped && (
                  <div className="flex items-center gap-1 px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
                    <span>{config.selectedTable}</span>
                    <button
                      onClick={removeDataSource}
                      className="ml-1 hover:bg-blue-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                )}

                {/* Date Range Chip */}
                {config.isDateRangeDropped && config.startDate && config.endDate && (
                  <div className="flex items-center gap-1 px-3 py-1 bg-purple-100 text-purple-800 rounded-full text-sm">
                    <span>{config.startDate} → {config.endDate}</span>
                    <button
                      onClick={removeDateRange}
                      className="ml-1 hover:bg-purple-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                )}

                {/* Dimension Chips */}
                {selectedDimensions.map((dim) => (
                  <div key={dim} className="flex items-center gap-1 px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm">
                    <span>{getDimensionByValue(dim)?.label || dim}</span>
                    <button
                      onClick={() => removeDimension(dim)}
                      className="ml-1 hover:bg-green-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ))}

                {/* Metric Chips */}
                {selectedMetrics.map((metricId) => {
                  const metric = getMetricById(metricId)
                  return (
                    <div key={metricId} className="flex items-center gap-1 px-3 py-1 bg-orange-100 text-orange-800 rounded-full text-sm">
                      <span>{metric?.label || metricId}</span>
                      <button
                        onClick={() => removeMetric(metricId)}
                        className="ml-1 hover:bg-orange-200 rounded-full p-0.5"
                      >
                        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  )
                })}

                {/* Filter Chips */}
                {config.selectedFilters.map((filter, index) => (
                  <div key={`${filter.dimension}-${filter.value}-${index}`} className="flex items-center gap-1 px-3 py-1 bg-pink-100 text-pink-800 rounded-full text-sm">
                    <span>{filter.label}</span>
                    <button
                      onClick={() => removeFilter(index)}
                      className="ml-1 hover:bg-pink-200 rounded-full p-0.5"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Table with drop zone */}
          <div
            className={`bg-white shadow overflow-hidden rounded-lg transition-all ${
              isDragOver ? 'ring-4 ring-blue-400 bg-blue-50' : ''
            }`}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
          >
            <div className="overflow-auto max-h-[calc(100vh-16rem)]">
            <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  {selectedDimensions.length === 0
                    ? 'Summary'
                    : (getDimensionByValue(selectedDimensions[0])?.label || 'Dimension')}
                </th>
                {selectedMetrics.map((metricId, index) => {
                  const metric = getMetricById(metricId)
                  return (
                    <th
                      key={metricId}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.effectAllowed = 'move'
                        e.dataTransfer.setData('text/plain', metricId)
                        e.dataTransfer.setData('columnIndex', String(index))
                      }}
                      onDragOver={(e) => {
                        e.preventDefault()
                        e.dataTransfer.dropEffect = 'move'
                        setDragOverColumn(metricId)
                      }}
                      onDragLeave={() => {
                        setDragOverColumn(null)
                      }}
                      onDrop={(e) => {
                        e.preventDefault()
                        const draggedIndex = parseInt(e.dataTransfer.getData('columnIndex'))
                        const targetIndex = index
                        if (draggedIndex !== targetIndex) {
                          reorderMetrics(draggedIndex, targetIndex)
                        }
                        setDragOverColumn(null)
                      }}
                      className={`px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-move transition-colors ${
                        dragOverColumn === metricId ? 'bg-blue-100' : ''
                      }`}
                      title={metric?.description}
                    >
                      {metric?.label || metricId}
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {processedRows.map((row, index) => {
                const rowKey = `depth0:${row.dimension_value}`
                const isExpanded = expandedRows.has(rowKey)
                const isLoading = loadingRows.has(rowKey)
                const currentPageNum = currentPage[rowKey] || 0
                const pageKey = `${rowKey}:${currentPageNum}`
                const children = childrenCache[pageKey] || []

                // Get grand total queries from the whole dataset
                const grandTotalQueries = pivotData.total.queries

                // Calculate cumulative percentage (already in decimal 0-1 format)
                const cumulativePercentage = processedRows
                  .slice(0, index + 1)
                  .reduce((sum, r) => sum + r.percentage_of_total, 0)

                // Calculate cumulative search term count
                const cumulativeTerms = processedRows
                  .slice(0, index + 1)
                  .reduce((sum, r) => sum + r.search_term_count, 0)

                return (
                  <React.Fragment key={row.dimension_value}>
                    <tr
                      className="hover:bg-gray-50 cursor-pointer"
                      onClick={() => row.has_children && toggleRow(row.dimension_value, 0, grandTotalQueries)}
                    >
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        <div className="flex items-center gap-2">
                          {row.has_children && (
                            isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
                          )}
                          <span>{row.dimension_value}</span>
                        </div>
                      </td>
                      {selectedMetrics.map((metricId) => {
                        let value: number

                        // Handle special computed metrics
                        if (metricId === 'cumulative_percentage') {
                          value = cumulativePercentage // Already in decimal format (0-1)
                        } else if (metricId === 'cumulative_terms') {
                          value = cumulativeTerms
                        } else {
                          value = getRowValue(row, metricId)
                        }

                        return (
                          <td
                            key={metricId}
                            className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right"
                          >
                            {formatMetricValue(value, metricId)}
                          </td>
                        )
                      })}
                    </tr>
                    {isExpanded && (
                      <>
                        {isLoading && children.length === 0 ? (
                          <tr className="bg-blue-50">
                            <td colSpan={selectedMetrics.length + 1} className="px-6 py-4 text-center text-sm text-gray-600">
                              Loading...
                            </td>
                          </tr>
                        ) : (
                          <>
                            {children.map((child, idx) => {
                              // Get all children from previous pages
                              let allPreviousPagesChildren: PivotChildRow[] = []
                              if (currentPageNum > 0) {
                                for (let p = 0; p < currentPageNum; p++) {
                                  const prevPageKey = `${rowKey}:${p}`
                                  const prevChildren = childrenCache[prevPageKey] || []
                                  allPreviousPagesChildren = [...allPreviousPagesChildren, ...prevChildren]
                                }
                              }
                              // Add children from current page up to this index
                              const allChildrenUpToThis = [...allPreviousPagesChildren, ...children.slice(0, idx + 1)]
                              return renderRow(child, row.dimension_value, 1, 1, children, idx, grandTotalQueries, allChildrenUpToThis)
                            })}
                            {children.length > 0 && children.length === CHILDREN_PAGE_SIZE && (
                              <tr className="bg-blue-50">
                                <td colSpan={selectedMetrics.length + 1} className="px-6 py-4">
                                  {isLoading ? (
                                    <div className="flex items-center justify-center gap-2 text-sm text-gray-600">
                                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-700"></div>
                                      Loading...
                                    </div>
                                  ) : (
                                    <div className="flex items-center justify-center gap-4">
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          const currentPageNum = currentPage[rowKey] || 0
                                          if (currentPageNum > 0) {
                                            goToPage(row.dimension_value, 0, currentPageNum - 1)
                                          }
                                        }}
                                        disabled={!currentPage[rowKey] || currentPage[rowKey] === 0}
                                        className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                      >
                                        Previous
                                      </button>
                                      <span className="text-sm text-gray-600">
                                        Page {(currentPage[rowKey] || 0) + 1}
                                      </span>
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          const currentPageNum = currentPage[rowKey] || 0
                                          goToPage(row.dimension_value, 0, currentPageNum + 1)
                                        }}
                                        disabled={children.length < CHILDREN_PAGE_SIZE}
                                        className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                      >
                                        Next
                                      </button>
                                    </div>
                                  )}
                                </td>
                              </tr>
                            )}
                          </>
                        )}
                      </>
                    )}
                  </React.Fragment>
                )
              })}
              <tr className="bg-gray-100 font-semibold">
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  Total
                </td>
                {selectedMetrics.map((metricId) => {
                  let value: number

                  // Handle special computed metrics for total row
                  if (metricId === 'percentage_of_total' || metricId === 'cumulative_percentage') {
                    value = 1.0 // 100%
                  } else if (metricId === 'cumulative_terms') {
                    value = pivotData.total.search_term_count
                  } else {
                    value = getRowValue(pivotData.total, metricId)
                  }

                  return (
                    <td
                      key={metricId}
                      className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right"
                    >
                      {formatMetricValue(value, metricId)}
                    </td>
                  )
                })}
              </tr>
            </tbody>
          </table>
        </div>
          </div>
        </div>
      </div>
    </div>
  )
}
