import { useMemo } from 'react'
import { useSchema } from './use-schema'
import type { BaseMetric, CalculatedMetric, DimensionDef } from '@/lib/types'

export type MetricFormat = 'number' | 'currency' | 'percent'

export interface MetricDefinition {
  id: string
  label: string
  format: MetricFormat
  description: string
  category: 'volume' | 'conversion' | 'revenue' | 'other'
  decimalPlaces?: number
}

export interface DimensionDefinition {
  value: string
  label: string
  description: string
  is_groupable?: boolean
  is_filterable?: boolean
}

interface UsePivotMetricsReturn {
  metrics: MetricDefinition[]
  dimensions: DimensionDefinition[]
  isLoading: boolean
  getMetricById: (id: string) => MetricDefinition | undefined
  getDimensionByValue: (value: string) => DimensionDefinition | undefined
  getMetricsByCategory: (category: string) => MetricDefinition[]
  getDefaultMetrics: () => string[]
}

// Map schema format_type to pivot table format
function mapFormatType(formatType: string): MetricFormat {
  switch (formatType) {
    case 'currency':
      return 'currency'
    case 'percent':
      return 'percent'
    case 'number':
    default:
      return 'number'
  }
}

// Transform BaseMetric to MetricDefinition
function transformBaseMetric(metric: BaseMetric): MetricDefinition {
  return {
    id: metric.id,
    label: metric.display_name,
    format: mapFormatType(metric.format_type),
    description: metric.description || `${metric.display_name} (${metric.aggregation})`,
    category: metric.category as 'volume' | 'conversion' | 'revenue' | 'other',
    decimalPlaces: metric.decimal_places,
  }
}

// Transform CalculatedMetric to MetricDefinition
function transformCalculatedMetric(metric: CalculatedMetric): MetricDefinition {
  return {
    id: metric.id,
    label: metric.display_name,
    format: mapFormatType(metric.format_type),
    description: metric.description || `Calculated: ${metric.formula}`,
    category: metric.category as 'volume' | 'conversion' | 'revenue' | 'other',
    decimalPlaces: metric.decimal_places,
  }
}

// Transform DimensionDef to DimensionDefinition
function transformDimension(dimension: DimensionDef): DimensionDefinition {
  return {
    value: dimension.id,
    label: dimension.display_name,
    description: dimension.description || `Group by ${dimension.display_name}`,
    is_groupable: dimension.is_groupable,
    is_filterable: dimension.is_filterable,
  }
}

export function usePivotMetrics(tableId?: string): UsePivotMetricsReturn {
  const {
    baseMetrics,
    calculatedMetrics,
    dimensions,
    groupableDimensions,
    isLoadingBaseMetrics,
    isLoadingCalculatedMetrics,
    isLoadingDimensions,
  } = useSchema(tableId)

  const isLoading = isLoadingBaseMetrics || isLoadingCalculatedMetrics || isLoadingDimensions

  // Transform and combine base metrics and calculated metrics
  const metrics = useMemo(() => {
    const allMetrics: MetricDefinition[] = []

    // Add base metrics
    if (baseMetrics) {
      allMetrics.push(...baseMetrics.map(transformBaseMetric))

      // Add _pct variants for volume metrics
      baseMetrics.forEach((metric) => {
        if (metric.category === 'volume') {
          allMetrics.push({
            id: `${metric.id}_pct`,
            label: `${metric.display_name} %`,
            format: 'percent',
            description: `Percentage of total ${metric.display_name}`,
            category: 'volume',
            decimalPlaces: 0,
          })
        }
      })
    }

    // Add calculated metrics
    if (calculatedMetrics) {
      allMetrics.push(...calculatedMetrics.map(transformCalculatedMetric))

      // Add _pct variants for volume calculated metrics
      calculatedMetrics.forEach((metric) => {
        if (metric.category === 'volume') {
          allMetrics.push({
            id: `${metric.id}_pct`,
            label: `${metric.display_name} %`,
            format: 'percent',
            description: `Percentage of total ${metric.display_name}`,
            category: 'volume',
            decimalPlaces: 0,
          })
        }
      })
    }

    return allMetrics
  }, [baseMetrics, calculatedMetrics])

  // Transform all dimensions (not just groupable ones)
  const transformedDimensions = useMemo(() => {
    return dimensions.map(transformDimension)
  }, [dimensions])

  // Helper function to get metric by ID
  // Also handles dynamically computed _pct suffix metrics that aren't in the schema
  const getMetricById = useMemo(
    () => (id: string): MetricDefinition | undefined => {
      // First check if it exists in the schema
      const existingMetric = metrics.find((m) => m.id === id)
      if (existingMetric) {
        return existingMetric
      }

      // Handle dynamically computed _pct metrics (percent over total)
      if (id.endsWith('_pct')) {
        const baseMetricId = id.replace(/_pct$/, '')
        const baseMetric = metrics.find((m) => m.id === baseMetricId)
        if (baseMetric) {
          return {
            id,
            label: `${baseMetric.label} %`,
            format: 'percent',
            description: `Percentage of total ${baseMetric.label}`,
            category: baseMetric.category,
            decimalPlaces: 2,
          }
        }
      }

      return undefined
    },
    [metrics]
  )

  // Helper function to get dimension by value
  const getDimensionByValue = useMemo(
    () => (value: string) => transformedDimensions.find((d) => d.value === value),
    [transformedDimensions]
  )

  // Helper to get metrics by category
  const getMetricsByCategory = useMemo(
    () => (category: string) => metrics.filter((m) => m.category === category),
    [metrics]
  )

  // Get default metrics - first 6 visible base metrics
  const getDefaultMetrics = useMemo(() => {
    return () => {
      if (!baseMetrics || baseMetrics.length === 0) {
        return []
      }
      return baseMetrics
        .filter((m) => m.is_visible_by_default)
        .slice(0, 6)
        .map((m) => m.id)
    }
  }, [baseMetrics])

  return {
    metrics,
    dimensions: transformedDimensions,
    isLoading,
    getMetricById,
    getDimensionByValue,
    getMetricsByCategory,
    getDefaultMetrics,
  }
}
