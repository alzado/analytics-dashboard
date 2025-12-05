'use client'

import React, { useState, useRef } from 'react'
import { createPortal } from 'react-dom'
import type { SignificanceResultItem } from '@/lib/api'

interface SignificanceIndicatorProps {
  /** Significance result for this cell */
  result: SignificanceResultItem | null
  /** Whether to show compact version (just arrow + percentage) */
  compact?: boolean
  /** Format type for the metric (to format credible interval correctly) */
  formatType?: 'number' | 'currency' | 'percent'
}

/**
 * Custom tooltip component with detailed significance information
 * Uses React Portal to render outside overflow containers
 */
function SignificanceTooltip({
  result,
  formatType,
  children
}: {
  result: SignificanceResultItem
  formatType: 'number' | 'currency' | 'percent'
  children: React.ReactNode
}) {
  const [isVisible, setIsVisible] = useState(false)
  const triggerRef = useRef<HTMLDivElement>(null)

  // Calculate position directly when rendering (no state delay)
  const getPosition = () => {
    if (!triggerRef.current) return { top: 0, left: 0 }

    const rect = triggerRef.current.getBoundingClientRect()
    const tooltipWidth = 288 // w-72 = 18rem = 288px

    // Position above the trigger, centered
    let left = rect.left + rect.width / 2 - tooltipWidth / 2
    let top = rect.top - 8 // 8px gap above trigger

    // Keep tooltip within viewport
    if (left < 8) left = 8
    if (left + tooltipWidth > window.innerWidth - 8) {
      left = window.innerWidth - tooltipWidth - 8
    }

    return { top, left }
  }

  const { prob_beat_control, is_significant, direction, warning } = result

  // Format probability as percentage with 2 decimals
  const probPercent = (prob_beat_control * 100).toFixed(2)

  // Format values based on type
  const formatValue = (value: number, showSign = true): string => {
    const sign = showSign && value >= 0 ? '+' : ''
    if (formatType === 'percent') {
      return `${sign}${(value * 100).toFixed(2)}%`
    } else if (formatType === 'currency') {
      return `${sign}$${value.toFixed(2)}`
    } else {
      return `${sign}${value.toFixed(2)}`
    }
  }

  // Format mean values (no sign needed)
  const formatMean = (value: number): string => {
    if (formatType === 'percent') {
      return `${(value * 100).toFixed(2)}%`
    } else if (formatType === 'currency') {
      return `$${value.toFixed(2)}`
    } else {
      return value.toFixed(2)
    }
  }

  const position = getPosition()

  const tooltipContent = (
    <div
      className="fixed z-[9999] w-72 bg-gray-900 text-white text-xs rounded-lg shadow-xl p-3"
      style={{
        top: position.top,
        left: position.left,
        transform: 'translateY(-100%)'
      }}
    >
      {/* Arrow pointing down */}
      <div
        className="absolute top-full left-1/2 -translate-x-1/2 border-8 border-transparent border-t-gray-900"
      />

      {/* Header */}
      <div className="font-semibold text-sm mb-2 pb-2 border-b border-gray-700">
        Proportion Significance Test
      </div>

      {/* Main probability */}
      <div className="mb-3">
        <div className="flex justify-between items-center">
          <span className="text-gray-400">Probability beats control:</span>
          <span className={`font-bold ${
            is_significant
              ? direction === 'better' ? 'text-green-400' : 'text-red-400'
              : 'text-gray-300'
          }`}>
            {probPercent}%
          </span>
        </div>
        <div className="text-[10px] text-gray-500 mt-0.5">
          {prob_beat_control >= 0.95
            ? 'Very high confidence this column outperforms the starred column'
            : prob_beat_control >= 0.80
              ? 'Likely better than the starred column, but not conclusive'
              : prob_beat_control <= 0.05
                ? 'Very high confidence this column underperforms the starred column'
                : prob_beat_control <= 0.20
                  ? 'Likely worse than the starred column, but not conclusive'
                  : 'No clear difference from the starred column'}
        </div>
      </div>

      {/* Status */}
      <div className="mb-3">
        <div className="flex justify-between items-center">
          <span className="text-gray-400">Status:</span>
          <span className={`font-medium ${
            is_significant
              ? direction === 'better' ? 'text-green-400' : 'text-red-400'
              : 'text-yellow-400'
          }`}>
            {is_significant
              ? `Significantly ${direction}`
              : 'Not significant'}
          </span>
        </div>
        <div className="text-[10px] text-gray-500 mt-0.5">
          {is_significant
            ? 'The difference is statistically meaningful (95% confidence)'
            : 'The difference could be due to random variation'}
        </div>
      </div>

      {/* Detailed metrics */}
      <div className="space-y-2 pt-2 border-t border-gray-700">
        {/* Mean values */}
        <div className="flex justify-between">
          <span className="text-gray-400">Control (starred) mean:</span>
          <span className="text-gray-200">{formatMean(result.control_mean)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-400">This column mean:</span>
          <span className="text-gray-200">{formatMean(result.treatment_mean)}</span>
        </div>

        {/* Difference */}
        <div className="flex justify-between">
          <span className="text-gray-400">Absolute difference:</span>
          <span className={result.mean_difference >= 0 ? 'text-green-400' : 'text-red-400'}>
            {formatValue(result.mean_difference)}
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-400">Relative difference:</span>
          <span className={result.relative_difference >= 0 ? 'text-green-400' : 'text-red-400'}>
            {result.relative_difference >= 0 ? '+' : ''}{(result.relative_difference * 100).toFixed(1)}%
          </span>
        </div>

        {/* Confidence interval */}
        <div className="pt-1">
          <div className="flex justify-between">
            <span className="text-gray-400">95% Confidence Interval:</span>
            <span className="text-gray-200">
              [{formatValue(result.credible_interval_lower)}, {formatValue(result.credible_interval_upper)}]
            </span>
          </div>
          <div className="text-[10px] text-gray-500 mt-0.5">
            The true difference is 95% likely to be within this range
          </div>
        </div>

        {/* Formula explanation */}
        <div className="pt-2 mt-2 border-t border-gray-700">
          <div className="text-gray-400 text-[11px] font-medium mb-2">Two-Proportion Z-Test</div>

          {/* Control calculation */}
          <div className="mb-2 p-2 bg-gray-800 rounded text-[10px] font-mono">
            <div className="text-gray-400 mb-1">Control (starred):</div>
            <div className="text-gray-200">
              p₁ = {result.control_successes?.toLocaleString() ?? '?'} / {result.n_control_events?.toLocaleString() ?? '?'} = {(result.control_mean * 100).toFixed(4)}%
            </div>
          </div>

          {/* Treatment calculation */}
          <div className="mb-2 p-2 bg-gray-800 rounded text-[10px] font-mono">
            <div className="text-gray-400 mb-1">Treatment (this column):</div>
            <div className="text-gray-200">
              p₂ = {result.treatment_successes?.toLocaleString() ?? '?'} / {result.n_treatment_events?.toLocaleString() ?? '?'} = {(result.treatment_mean * 100).toFixed(4)}%
            </div>
          </div>

          {/* Difference and CI calculation */}
          <div className="p-2 bg-gray-800 rounded text-[10px] font-mono">
            <div className="text-gray-400 mb-1">Difference:</div>
            <div className={result.mean_difference >= 0 ? 'text-green-400' : 'text-red-400'}>
              Δ = p₂ - p₁ = {result.mean_difference >= 0 ? '+' : ''}{(result.mean_difference * 100).toFixed(4)}%
            </div>
            <div className="text-gray-500 mt-2 text-[9px]">
              <div>p̂ = ({result.control_successes?.toLocaleString() ?? '?'} + {result.treatment_successes?.toLocaleString() ?? '?'}) / ({result.n_control_events?.toLocaleString() ?? '?'} + {result.n_treatment_events?.toLocaleString() ?? '?'})</div>
              <div className="mt-0.5">SE = √(p̂(1-p̂)(1/n₁ + 1/n₂))</div>
              <div className="mt-0.5">95% CI = Δ ± 1.96 × SE</div>
            </div>
          </div>

          {/* Confidence Interval result */}
          <div className="mt-2 p-2 bg-gray-800 rounded text-[10px] font-mono">
            <div className="text-gray-400 mb-1">95% Confidence Interval:</div>
            <div className="text-gray-200">
              [{(result.credible_interval_lower * 100).toFixed(4)}%, {(result.credible_interval_upper * 100).toFixed(4)}%]
            </div>
            <div className="text-gray-500 mt-1 text-[9px]">
              {result.credible_interval_lower > 0
                ? 'Interval excludes 0 → Significant positive effect'
                : result.credible_interval_upper < 0
                  ? 'Interval excludes 0 → Significant negative effect'
                  : 'Interval includes 0 → Not statistically significant'}
            </div>
          </div>
        </div>
      </div>

      {/* Warning if present */}
      {warning && (
        <div className="mt-2 pt-2 border-t border-gray-700">
          <div className="flex items-start gap-1 text-yellow-400">
            <span>⚠️</span>
            <span>{warning}</span>
          </div>
        </div>
      )}

      {/* Legend explanation */}
      <div className="mt-3 pt-2 border-t border-gray-700 text-[10px] text-gray-500">
        <div className="font-medium text-gray-400 mb-1">How to read:</div>
        <div>▲ = This column has a higher rate than control</div>
        <div>▼ = This column has a lower rate than control</div>
        <div>Green/Red = Statistically significant (p &lt; 0.05)</div>
        <div>Gray = Not significant (may be random variation)</div>
        <div className="mt-1 italic">Uses two-proportion z-test with event counts</div>
      </div>
    </div>
  )

  return (
    <div
      ref={triggerRef}
      className="relative inline-flex"
      onMouseEnter={() => setIsVisible(true)}
      onMouseLeave={() => setIsVisible(false)}
    >
      {children}

      {isVisible && typeof window !== 'undefined' && createPortal(tooltipContent, document.body)}
    </div>
  )
}

/**
 * Visual indicator showing statistical significance between treatment and control.
 *
 * Display variants:
 * - Significantly better: Green up arrow (▲) with probability
 * - Significantly worse: Red down arrow (▼) with probability
 * - Not significant: Gray dash (—)
 *
 * Includes tooltip with full details on hover.
 */
export function SignificanceIndicator({
  result,
  compact = true,
  formatType = 'number'
}: SignificanceIndicatorProps) {
  if (!result) return null

  const { prob_beat_control, is_significant, direction, warning } = result

  // Format probability as percentage with 2 decimals
  const probPercent = (prob_beat_control * 100).toFixed(2)
  const inverseProbPercent = ((1 - prob_beat_control) * 100).toFixed(2)

  // Determine display based on probability and significance
  // Always show arrow direction and probability, but color indicates significance
  let icon: React.ReactNode
  let colorClass: string
  let label: string

  // Determine if treatment is likely better or worse based on probability
  const likelyBetter = prob_beat_control > 0.5
  const likelyWorse = prob_beat_control < 0.5

  if (is_significant) {
    // Significant result - bold colors
    if (direction === 'better') {
      icon = '▲'
      colorClass = 'text-green-600'
      label = `${probPercent}%`
    } else if (direction === 'worse') {
      icon = '▼'
      colorClass = 'text-red-600'
      label = `${inverseProbPercent}%`  // Show probability of being worse
    } else {
      icon = '~'
      colorClass = 'text-gray-500'
      label = `${probPercent}%`
    }
  } else {
    // Not significant (< 95% confidence) - gray only, no green/red
    if (likelyBetter) {
      icon = '▲'
      colorClass = 'text-gray-400'  // Gray - not significant
      label = `${probPercent}%`
    } else if (likelyWorse) {
      icon = '▼'
      colorClass = 'text-gray-400'  // Gray - not significant
      label = `${inverseProbPercent}%`  // Show probability of being worse
    } else {
      // Exactly 50%
      icon = '~'
      colorClass = 'text-gray-400'
      label = '50%'
    }
  }

  // Format credible interval values
  const formatCI = (value: number): string => {
    if (formatType === 'percent') {
      return `${value >= 0 ? '+' : ''}${(value * 100).toFixed(2)}%`
    } else if (formatType === 'currency') {
      return `${value >= 0 ? '+' : ''}$${value.toFixed(2)}`
    } else {
      return `${value >= 0 ? '+' : ''}${value.toFixed(2)}`
    }
  }

  if (compact) {
    return (
      <SignificanceTooltip result={result} formatType={formatType}>
        <span
          className={`inline-flex items-center gap-0.5 ml-1 text-xs font-medium ${colorClass} cursor-help`}
        >
          <span>{icon}</span>
          <span className="text-[10px]">{label}</span>
          {warning && <span className="text-yellow-500 text-[10px]">⚠</span>}
        </span>
      </SignificanceTooltip>
    )
  }

  // Full display (for expanded view or tooltip)
  return (
    <SignificanceTooltip result={result} formatType={formatType}>
      <div
        className={`inline-flex flex-col items-start gap-0.5 ${colorClass} cursor-help`}
      >
        <div className="flex items-center gap-1">
          <span className="text-sm">{icon}</span>
          {label && <span className="text-xs font-medium">{label}</span>}
        </div>
        <div className="text-[10px] text-gray-500">
          CI: [{formatCI(result.credible_interval_lower)}, {formatCI(result.credible_interval_upper)}]
        </div>
        {warning && (
          <div className="text-[10px] text-yellow-600">⚠️ {warning}</div>
        )}
      </div>
    </SignificanceTooltip>
  )
}

/**
 * Badge showing significance test button or status
 */
interface SignificanceButtonProps {
  onClick: (rowLimit?: number) => void
  isLoading: boolean
  hasResults: boolean
  disabled?: boolean
  columnCount: number
  totalRows: number
}

export function SignificanceButton({
  onClick,
  isLoading,
  hasResults,
  disabled = false,
  columnCount,
  totalRows
}: SignificanceButtonProps) {
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [rowLimit, setRowLimit] = useState<string>('')
  const canRun = columnCount >= 2 && !disabled

  const handleRunTest = () => {
    const limit = rowLimit ? parseInt(rowLimit, 10) : undefined
    if (limit && !isNaN(limit) && limit > 0) {
      onClick(limit)
    } else {
      onClick(undefined)
    }
    setIsModalOpen(false)
  }

  const modalContent = isModalOpen && typeof window !== 'undefined' ? createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={() => setIsModalOpen(false)}
      />

      {/* Modal */}
      <div className="relative bg-white rounded-lg shadow-xl w-full max-w-sm mx-4">
        {/* Header */}
        <div className="px-5 py-3 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            📊 Run Significance Test
          </h2>
        </div>

        {/* Content */}
        <div className="p-5">
          <p className="text-sm text-gray-600 mb-4">
            Run statistical significance test comparing treatment columns against the control (starred) column.
          </p>

          <div className="mb-4">
            <label htmlFor="sigRowLimit" className="block text-sm font-medium text-gray-700 mb-1">
              Number of rows to test
            </label>
            <div className="flex items-center gap-2">
              <input
                id="sigRowLimit"
                type="number"
                min="1"
                max={totalRows}
                value={rowLimit}
                onChange={(e) => setRowLimit(e.target.value)}
                placeholder={`All (${totalRows})`}
                className="flex-1 px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
              <span className="text-sm text-gray-500">of {totalRows}</span>
            </div>
            <p className="mt-1 text-xs text-gray-500">
              Leave empty to test all rows. Limiting rows can speed up the test.
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-gray-200 flex justify-end gap-2">
          <button
            onClick={() => setIsModalOpen(false)}
            className="px-4 py-2 text-sm border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleRunTest}
            className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors flex items-center gap-1.5"
          >
            📊 Run Test
          </button>
        </div>
      </div>
    </div>,
    document.body
  ) : null

  return (
    <>
      <button
        onClick={() => setIsModalOpen(true)}
        disabled={!canRun || isLoading}
        className={`
          px-3 py-1.5 text-xs font-medium rounded-md transition-colors
          flex items-center gap-1.5
          ${canRun
            ? hasResults
              ? 'bg-green-100 text-green-700 hover:bg-green-200 border border-green-300'
              : 'bg-blue-100 text-blue-700 hover:bg-blue-200 border border-blue-300'
            : 'bg-gray-100 text-gray-400 cursor-not-allowed border border-gray-200'
          }
        `}
        title={
          !canRun
            ? 'Need at least 2 columns to run significance test'
            : 'Run significance test for rate metrics (only percent-format metrics with simple A/B formulas)'
        }
      >
        {isLoading ? (
          <>
            <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24">
              <circle
                className="opacity-25"
                cx="12"
                cy="12"
                r="10"
                stroke="currentColor"
                strokeWidth="4"
                fill="none"
              />
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              />
            </svg>
            <span>Testing...</span>
          </>
        ) : (
          <>
            <span>{hasResults ? '✓' : '📊'}</span>
            <span>Run Significance Test</span>
          </>
        )}
      </button>
      {modalContent}
    </>
  )
}
