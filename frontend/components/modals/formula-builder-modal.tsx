'use client'

import { useState } from 'react'
import { X, Plus, Minus, Divide, Asterisk, Trash2 } from 'lucide-react'
import type { BaseMetric, CalculatedMetric, DimensionDef } from '@/lib/types'

interface FormulaBuilderModalProps {
  isOpen: boolean
  onClose: () => void
  availableMetrics: BaseMetric[]
  availableCalculatedMetrics?: CalculatedMetric[]
  availableDimensions: DimensionDef[]
  onApply: (formula: string) => void
  initialFormula?: string
}

type FormulaToken = {
  id: string
  type: 'metric' | 'dimension' | 'operator' | 'parenthesis' | 'number' | 'function' | 'wildcard'
  value: string
  displayName?: string
}

export function FormulaBuilderModal({
  isOpen,
  onClose,
  availableMetrics,
  availableCalculatedMetrics = [],
  availableDimensions,
  onApply,
  initialFormula = '',
}: FormulaBuilderModalProps) {
  const [tokens, setTokens] = useState<FormulaToken[]>(() => {
    // Parse initial formula if provided
    if (initialFormula) {
      return parseFormulaToTokens(initialFormula, availableMetrics, availableCalculatedMetrics, availableDimensions)
    }
    return []
  })

  const addMetric = (metric: BaseMetric | CalculatedMetric) => {
    // Check if this is a calculated metric
    if ('formula' in metric) {
      // For calculated metrics, expand their formula instead of adding reference
      const expandedTokens = parseFormulaToTokens(
        metric.formula,
        availableMetrics,
        availableCalculatedMetrics,
        availableDimensions
      )
      setTokens([...tokens, ...expandedTokens])
    } else {
      // For base metrics, add as a reference
      const newToken: FormulaToken = {
        id: `token-${Date.now()}-${Math.random()}`,
        type: 'metric',
        value: metric.id,
        displayName: metric.display_name,
      }
      setTokens([...tokens, newToken])
    }
  }

  const addDimension = (dimension: DimensionDef) => {
    const newToken: FormulaToken = {
      id: `token-${Date.now()}-${Math.random()}`,
      type: 'dimension',
      value: dimension.id,
      displayName: dimension.display_name,
    }
    setTokens([...tokens, newToken])
  }

  const addFunction = (funcName: 'COUNT_DISTINCT' | 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX') => {
    const newToken: FormulaToken = {
      id: `token-${Date.now()}-${Math.random()}`,
      type: 'function',
      value: funcName,
    }
    setTokens([...tokens, newToken])
  }

  const addWildcard = () => {
    const newToken: FormulaToken = {
      id: `token-${Date.now()}-${Math.random()}`,
      type: 'wildcard',
      value: '*',
    }
    setTokens([...tokens, newToken])
  }

  const addOperator = (operator: '+' | '-' | '*' | '/') => {
    const newToken: FormulaToken = {
      id: `token-${Date.now()}-${Math.random()}`,
      type: 'operator',
      value: operator,
    }
    setTokens([...tokens, newToken])
  }

  const addParenthesis = (paren: '(' | ')') => {
    const newToken: FormulaToken = {
      id: `token-${Date.now()}-${Math.random()}`,
      type: 'parenthesis',
      value: paren,
    }
    setTokens([...tokens, newToken])
  }

  const addNumber = () => {
    const number = prompt('Enter a number:')
    if (number && !isNaN(Number(number))) {
      const newToken: FormulaToken = {
        id: `token-${Date.now()}-${Math.random()}`,
        type: 'number',
        value: number,
      }
      setTokens([...tokens, newToken])
    }
  }

  const removeToken = (tokenId: string) => {
    setTokens(tokens.filter(t => t.id !== tokenId))
  }

  const clearFormula = () => {
    if (confirm('Clear entire formula?')) {
      setTokens([])
    }
  }

  const buildFormula = (): string => {
    return tokens
      .map(token => {
        if (token.type === 'metric') {
          return `{${token.value}}`
        } else if (token.type === 'dimension') {
          return `[${token.value}]`
        } else if (token.type === 'function') {
          return token.value
        }
        return token.value
      })
      .join(' ')
  }

  const handleApply = () => {
    const formula = buildFormula()
    if (!formula.trim()) {
      alert('Formula cannot be empty')
      return
    }
    onApply(formula)
    onClose()
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="sticky top-0 bg-white border-b px-6 py-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">Formula Builder</h2>
          <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded">
            <X size={20} />
          </button>
        </div>

        <div className="p-6 space-y-6">
          {/* Formula Preview */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Formula Preview
            </label>
            <div className="min-h-[60px] p-3 bg-gray-50 border border-gray-300 rounded-lg font-mono text-sm">
              {tokens.length === 0 ? (
                <span className="text-gray-400">Add metrics and operators to build your formula...</span>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {tokens.map((token) => (
                    <div
                      key={token.id}
                      className={`inline-flex items-center gap-1 px-2 py-1 rounded ${
                        token.type === 'metric'
                          ? 'bg-blue-100 text-blue-800'
                          : token.type === 'dimension'
                          ? 'bg-orange-100 text-orange-800'
                          : token.type === 'function'
                          ? 'bg-indigo-100 text-indigo-800'
                          : token.type === 'operator'
                          ? 'bg-purple-100 text-purple-800'
                          : token.type === 'number'
                          ? 'bg-green-100 text-green-800'
                          : token.type === 'wildcard'
                          ? 'bg-yellow-100 text-yellow-800'
                          : 'bg-gray-200 text-gray-800'
                      }`}
                    >
                      {token.type === 'metric' || token.type === 'dimension' ? token.displayName : token.value}
                      <button
                        onClick={() => removeToken(token.id)}
                        className="ml-1 hover:bg-white hover:bg-opacity-50 rounded"
                      >
                        <X size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
            {tokens.length > 0 && (
              <button
                onClick={clearFormula}
                className="mt-2 text-sm text-red-600 hover:text-red-700"
              >
                Clear All
              </button>
            )}
          </div>

          {/* Raw Formula */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Raw Formula (for reference)
            </label>
            <div className="p-3 bg-gray-50 border border-gray-300 rounded-lg font-mono text-sm text-gray-600">
              {buildFormula() || '(empty)'}
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {/* Base Metrics */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Base Metrics</h3>
              <div className="space-y-2 max-h-64 overflow-y-auto border rounded-lg p-2">
                {availableMetrics.map((metric) => (
                  <button
                    key={metric.id}
                    onClick={() => addMetric(metric)}
                    className="w-full text-left px-3 py-2 text-sm hover:bg-blue-50 rounded border border-transparent hover:border-blue-200 transition-colors"
                  >
                    <div className="font-medium">{metric.display_name}</div>
                    <div className="text-xs text-gray-500">{metric.id}</div>
                  </button>
                ))}
              </div>
            </div>

            {/* Calculated Metrics */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Calculated Metrics</h3>
              <div className="space-y-2 max-h-64 overflow-y-auto border rounded-lg p-2">
                {availableCalculatedMetrics.length > 0 ? (
                  availableCalculatedMetrics.map((metric) => (
                    <button
                      key={metric.id}
                      onClick={() => addMetric(metric)}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-cyan-50 rounded border border-transparent hover:border-cyan-200 transition-colors"
                    >
                      <div className="font-medium">{metric.display_name}</div>
                      <div className="text-xs text-gray-500">{metric.id}</div>
                    </button>
                  ))
                ) : (
                  <div className="text-xs text-gray-400 p-3 text-center">
                    No calculated metrics available
                  </div>
                )}
              </div>
            </div>

            {/* Available Dimensions */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Dimensions</h3>
              <div className="space-y-2 max-h-64 overflow-y-auto border rounded-lg p-2">
                {availableDimensions.map((dimension) => (
                  <button
                    key={dimension.id}
                    onClick={() => addDimension(dimension)}
                    className="w-full text-left px-3 py-2 text-sm hover:bg-orange-50 rounded border border-transparent hover:border-orange-200 transition-colors"
                  >
                    <div className="font-medium">{dimension.display_name}</div>
                    <div className="text-xs text-gray-500">{dimension.id}</div>
                  </button>
                ))}
              </div>
            </div>

            {/* Operators, Functions & Numbers */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Operators & Functions</h3>
              <div className="space-y-3">
                <div>
                  <p className="text-xs text-gray-600 mb-2">Basic Operators</p>
                  <div className="grid grid-cols-4 gap-2">
                    <button
                      onClick={() => addOperator('+')}
                      className="flex items-center justify-center gap-1 px-3 py-2 bg-purple-100 text-purple-800 rounded hover:bg-purple-200 font-medium"
                    >
                      <Plus size={16} />
                      +
                    </button>
                    <button
                      onClick={() => addOperator('-')}
                      className="flex items-center justify-center gap-1 px-3 py-2 bg-purple-100 text-purple-800 rounded hover:bg-purple-200 font-medium"
                    >
                      <Minus size={16} />
                      -
                    </button>
                    <button
                      onClick={() => addOperator('*')}
                      className="flex items-center justify-center gap-1 px-3 py-2 bg-purple-100 text-purple-800 rounded hover:bg-purple-200 font-medium"
                    >
                      <Asterisk size={16} />
                      ×
                    </button>
                    <button
                      onClick={() => addOperator('/')}
                      className="flex items-center justify-center gap-1 px-3 py-2 bg-purple-100 text-purple-800 rounded hover:bg-purple-200 font-medium"
                    >
                      <Divide size={16} />
                      ÷
                    </button>
                  </div>
                </div>

                <div>
                  <p className="text-xs text-gray-600 mb-2">Parentheses</p>
                  <div className="grid grid-cols-2 gap-2">
                    <button
                      onClick={() => addParenthesis('(')}
                      className="px-3 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300 font-medium"
                    >
                      (
                    </button>
                    <button
                      onClick={() => addParenthesis(')')}
                      className="px-3 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300 font-medium"
                    >
                      )
                    </button>
                  </div>
                </div>

                <div>
                  <p className="text-xs text-gray-600 mb-2">Functions</p>
                  <div className="space-y-2">
                    <button
                      onClick={() => addFunction('COUNT')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      COUNT
                    </button>
                    <button
                      onClick={() => addFunction('COUNT_DISTINCT')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      COUNT_DISTINCT
                    </button>
                    <button
                      onClick={() => addFunction('SUM')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      SUM
                    </button>
                    <button
                      onClick={() => addFunction('AVG')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      AVG
                    </button>
                    <button
                      onClick={() => addFunction('MIN')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      MIN
                    </button>
                    <button
                      onClick={() => addFunction('MAX')}
                      className="w-full px-3 py-2 bg-indigo-100 text-indigo-800 rounded hover:bg-indigo-200 font-medium text-sm"
                    >
                      MAX
                    </button>
                  </div>
                </div>

                <div>
                  <p className="text-xs text-gray-600 mb-2">Special</p>
                  <div className="space-y-2">
                    <button
                      onClick={addWildcard}
                      className="w-full px-3 py-2 bg-yellow-100 text-yellow-800 rounded hover:bg-yellow-200 font-medium"
                    >
                      * (Wildcard)
                    </button>
                    <button
                      onClick={addNumber}
                      className="w-full px-3 py-2 bg-green-100 text-green-800 rounded hover:bg-green-200 font-medium"
                    >
                      Add Number
                    </button>
                  </div>
                </div>
              </div>

              {/* Help Text */}
              <div className="mt-4 p-3 bg-blue-50 rounded-lg text-xs text-blue-800">
                <p className="font-medium mb-1">Tips:</p>
                <ul className="list-disc list-inside space-y-1">
                  <li>Click metrics/dimensions to add them</li>
                  <li>Use COUNT_DISTINCT with dimensions for unique counts</li>
                  <li>Example: COUNT_DISTINCT ( [user_id] )</li>
                  <li>Division uses SAFE_DIVIDE (no errors)</li>
                  <li>Click X on tokens to remove them</li>
                </ul>
              </div>
            </div>
          </div>

          {/* Buttons */}
          <div className="flex gap-3 pt-4 border-t">
            <button
              onClick={onClose}
              className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 font-medium"
            >
              Cancel
            </button>
            <button
              onClick={handleApply}
              disabled={tokens.length === 0}
              className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed font-medium"
            >
              Apply Formula
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

// Helper function to parse existing formula back to tokens
function parseFormulaToTokens(
  formula: string,
  availableMetrics: BaseMetric[],
  availableCalculatedMetrics: CalculatedMetric[],
  availableDimensions: DimensionDef[]
): FormulaToken[] {
  const tokens: FormulaToken[] = []
  const parts = formula.split(/(\{[^}]+\}|\[[^\]]+\]|COUNT_DISTINCT|COUNT|SUM|AVG|MIN|MAX|[+\-*/()]|\s+)/).filter(p => p.trim())

  parts.forEach((part, index) => {
    const metricMatch = part.match(/^\{([^}]+)\}$/)
    const dimensionMatch = part.match(/^\[([^\]]+)\]$/)

    if (metricMatch) {
      const metricId = metricMatch[1]
      // Check both base and calculated metrics
      const metric = availableMetrics.find(m => m.id === metricId) ||
                     availableCalculatedMetrics.find(m => m.id === metricId)
      tokens.push({
        id: `token-init-${index}`,
        type: 'metric',
        value: metricId,
        displayName: metric?.display_name || metricId,
      })
    } else if (dimensionMatch) {
      const dimensionId = dimensionMatch[1]
      const dimension = availableDimensions.find(d => d.id === dimensionId)
      tokens.push({
        id: `token-init-${index}`,
        type: 'dimension',
        value: dimensionId,
        displayName: dimension?.display_name || dimensionId,
      })
    } else if (['COUNT_DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(part)) {
      tokens.push({
        id: `token-init-${index}`,
        type: 'function',
        value: part,
      })
    } else if (part === '*' && (index === 0 || parts[index - 1] === '(')) {
      // Wildcard * (not multiplication operator)
      tokens.push({
        id: `token-init-${index}`,
        type: 'wildcard',
        value: part,
      })
    } else if (['+', '-', '*', '/'].includes(part)) {
      tokens.push({
        id: `token-init-${index}`,
        type: 'operator',
        value: part,
      })
    } else if (['(', ')'].includes(part)) {
      tokens.push({
        id: `token-init-${index}`,
        type: 'parenthesis',
        value: part,
      })
    } else if (!isNaN(Number(part))) {
      tokens.push({
        id: `token-init-${index}`,
        type: 'number',
        value: part,
      })
    }
  })

  return tokens
}
