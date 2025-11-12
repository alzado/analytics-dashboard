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

export function usePivotMetrics(): UsePivotMetricsReturn {
  const {
    baseMetrics,
    calculatedMetrics,
    dimensions,
    groupableDimensions,
    isLoadingBaseMetrics,
    isLoadingCalculatedMetrics,
    isLoadingDimensions,
  } = useSchema()

  const isLoading = isLoadingBaseMetrics || isLoadingCalculatedMetrics || isLoadingDimensions

  // Transform and combine base metrics and calculated metrics
  const metrics = useMemo(() => {
    const allMetrics: MetricDefinition[] = []

    // Add base metrics
    if (baseMetrics) {
      allMetrics.push(...baseMetrics.map(transformBaseMetric))
    }

    // Add calculated metrics
    if (calculatedMetrics) {
      allMetrics.push(...calculatedMetrics.map(transformCalculatedMetric))
    }

    return allMetrics
  }, [baseMetrics, calculatedMetrics])

  // Transform all dimensions (not just groupable ones)
  const transformedDimensions = useMemo(() => {
    return dimensions.map(transformDimension)
  }, [dimensions])

  // Helper function to get metric by ID
  const getMetricById = useMemo(
    () => (id: string) => metrics.find((m) => m.id === id),
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
