#!/usr/bin/env python3
"""Fix pivot table state persistence"""
import re

file_path = 'components/sections/pivot-table-section.tsx'

with open(file_path, 'r') as f:
    content = f.read()

# 1. Remove the local useState declarations
content = re.sub(
    r'  const \[isConfigOpen, setIsConfigOpen\] = useState\(true\)\n',
    '',
    content
)

content = re.sub(
    r'  const \[expandedRows, setExpandedRows\] = useState<Set<string>>\(new Set\(\)\)\n',
    '',
    content
)

content = re.sub(
    r'  const \[selectedDisplayMetric, setSelectedDisplayMetric\] = useState\(\'queries\'\)\n',
    '',
    content
)

content = re.sub(
    r'  const \[sortConfig, setSortConfig\] = useState<\{\n    column: string \| number  // metricId for single-table, columnIndex for multi-table\n    subColumn\?: \'value\' \| \'diff\' \| \'pctDiff\'  // for multi-table mode\n    direction: \'asc\' \| \'desc\'\n.*?\}>\(\{ column: \'queries\', direction: \'desc\' \}\)\n',
    '',
    content,
    flags=re.DOTALL
)

# 2. Add the new hook methods to the destructuring
old_destructure = '''  const {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    setDataSourceDropped,
    setDateRangeDropped,
    addDimension,
    removeDimension,
    addTableDimension,
    removeTableDimension,
    addMetric,
    removeMetric,
    addFilter,
    removeFilter,
    resetToDefaults,
    reorderMetrics,
  } = usePivotConfig()'''

new_destructure = '''  const {
    config,
    updateTable,
    updateDateRange,
    updateStartDate,
    updateEndDate,
    setDataSourceDropped,
    setDateRangeDropped,
    addDimension,
    removeDimension,
    addTableDimension,
    removeTableDimension,
    addMetric,
    removeMetric,
    addFilter,
    removeFilter,
    resetToDefaults,
    reorderMetrics,
    setConfigOpen,
    setExpandedRows,
    setSelectedDisplayMetric,
    setSortConfig,
  } = usePivotConfig()

  // Get UI state from config (with defaults)
  const isConfigOpen = config.isConfigOpen ?? true
  const expandedRowsArray = config.expandedRows ?? []
  const expandedRows = new Set(expandedRowsArray)
  const selectedDisplayMetric = config.selectedDisplayMetric ?? 'queries'
  const sortConfig = config.sortColumn !== undefined ? {
    column: config.sortColumn,
    subColumn: config.sortSubColumn,
    direction: config.sortDirection ?? 'desc'
  } : { column: 'queries', direction: 'desc' as const }'''

content = content.replace(old_destructure, new_destructure)

# 3. Replace setExpandedRows calls to convert Set to Array
# Pattern: setExpandedRows(new Set()) -> setExpandedRows([])
content = re.sub(
    r'setExpandedRows\(new Set\(\)\)',
    'setExpandedRows([])',
    content
)

# Pattern: setExpandedRows(newExpanded) where newExpanded is a Set
# Need to add Array.from() conversion
# This is tricky - let me find the specific occurrences

with open(file_path, 'w') as f:
    f.write(content)

print("Fixed pivot table state persistence")
print("Note: Some setExpandedRows calls may need manual Array.from() conversion")
