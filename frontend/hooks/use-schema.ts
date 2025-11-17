import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchSchema,
  detectSchema,
  resetSchema,
  fetchBaseMetrics,
  fetchCalculatedMetrics,
  fetchDimensions,
  createBaseMetric,
  updateBaseMetric,
  deleteBaseMetric,
  createCalculatedMetric,
  updateCalculatedMetric,
  deleteCalculatedMetric,
  createDimension,
  updateDimension,
  deleteDimension,
  validateFormula,
  fetchFilterableDimensions,
  fetchGroupableDimensions,
  updatePivotConfig,
} from '@/lib/api'
import type {
  SchemaConfig,
  BaseMetric,
  CalculatedMetric,
  DimensionDef,
  MetricCreate,
  CalculatedMetricCreate,
  DimensionCreate,
  MetricUpdate,
  CalculatedMetricUpdate,
  DimensionUpdate,
  SchemaDetectionResult,
  FormulaValidationResult,
} from '@/lib/types'

export function useSchema(tableId?: string) {
  const queryClient = useQueryClient()

  const schemaQuery = useQuery<SchemaConfig>({
    queryKey: ['schema', tableId],
    queryFn: () => fetchSchema(tableId),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })

  const baseMetricsQuery = useQuery<BaseMetric[]>({
    queryKey: ['base-metrics', tableId],
    queryFn: () => fetchBaseMetrics(tableId),
    staleTime: 5 * 60 * 1000,
  })

  const calculatedMetricsQuery = useQuery<CalculatedMetric[]>({
    queryKey: ['calculated-metrics', tableId],
    queryFn: () => fetchCalculatedMetrics(tableId),
    staleTime: 5 * 60 * 1000,
  })

  const dimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['dimensions', tableId],
    queryFn: () => fetchDimensions(tableId),
    staleTime: 5 * 60 * 1000,
  })

  const filterableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['filterable-dimensions', tableId],
    queryFn: () => fetchFilterableDimensions(tableId),
    staleTime: 5 * 60 * 1000,
  })

  const groupableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['groupable-dimensions', tableId],
    queryFn: () => fetchGroupableDimensions(tableId),
    staleTime: 5 * 60 * 1000,
  })

  // Schema-level mutations
  const detectSchemaMutation = useMutation<SchemaDetectionResult, Error>({
    mutationFn: () => detectSchema(tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
    },
  })

  const resetSchemaMutation = useMutation<SchemaConfig, Error>({
    mutationFn: () => resetSchema(tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId] })
    },
  })

  // Base metric mutations
  const createBaseMetricMutation = useMutation<BaseMetric, Error, MetricCreate>({
    mutationFn: (data) => createBaseMetric(data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics', tableId] })
    },
  })

  const updateBaseMetricMutation = useMutation<
    { metric: BaseMetric; cascade_updated_count: number; cascade_updated_metrics: string[] },
    Error,
    { id: string; data: MetricUpdate }
  >({
    mutationFn: ({ id, data }) => updateBaseMetric(id, data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['base-metrics', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId], exact: true })
    },
  })

  const deleteBaseMetricMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: (metricId) => deleteBaseMetric(metricId, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics', tableId] })
    },
  })

  // Calculated metric mutations
  const createCalculatedMetricMutation = useMutation<CalculatedMetric, Error, CalculatedMetricCreate>({
    mutationFn: (data) => createCalculatedMetric(data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
    },
  })

  const updateCalculatedMetricMutation = useMutation<
    { metric: CalculatedMetric; cascade_updated_count: number; cascade_updated_metrics: string[] },
    Error,
    { id: string; data: CalculatedMetricUpdate }
  >({
    mutationFn: ({ id, data }) => updateCalculatedMetric(id, data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId], exact: true })
    },
  })

  const deleteCalculatedMetricMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: (metricId) => deleteCalculatedMetric(metricId, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
    },
  })

  // Dimension mutations
  const createDimensionMutation = useMutation<DimensionDef, Error, DimensionCreate>({
    mutationFn: (data) => createDimension(data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId] })
    },
  })

  const updateDimensionMutation = useMutation<DimensionDef, Error, { id: string; data: DimensionUpdate }>({
    mutationFn: ({ id, data }) => updateDimension(id, data, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId], exact: true })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId], exact: true })
    },
  })

  const deleteDimensionMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: (dimensionId) => deleteDimension(dimensionId, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId] })
    },
  })

  // Formula validation mutation
  const validateFormulaMutation = useMutation<FormulaValidationResult, Error, string>({
    mutationFn: validateFormula,
  })

  // Pivot config mutation
  const updatePivotConfigMutation = useMutation<
    SchemaConfig,
    Error,
    { primary_sort_metric?: string; avg_per_day_metric?: string; pagination_threshold?: number }
  >({
    mutationFn: (config) => updatePivotConfig(config, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
    },
  })

  return {
    // Query states
    schema: schemaQuery.data,
    isLoadingSchema: schemaQuery.isLoading,
    schemaError: schemaQuery.error,

    baseMetrics: baseMetricsQuery.data ?? [],
    isLoadingBaseMetrics: baseMetricsQuery.isLoading,
    baseMetricsError: baseMetricsQuery.error,

    calculatedMetrics: calculatedMetricsQuery.data ?? [],
    isLoadingCalculatedMetrics: calculatedMetricsQuery.isLoading,
    calculatedMetricsError: calculatedMetricsQuery.error,

    dimensions: dimensionsQuery.data ?? [],
    isLoadingDimensions: dimensionsQuery.isLoading,
    dimensionsError: dimensionsQuery.error,

    filterableDimensions: filterableDimensionsQuery.data ?? [],
    isLoadingFilterableDimensions: filterableDimensionsQuery.isLoading,

    groupableDimensions: groupableDimensionsQuery.data ?? [],
    isLoadingGroupableDimensions: groupableDimensionsQuery.isLoading,

    // Schema-level actions
    detectSchema: detectSchemaMutation.mutateAsync,
    isDetectingSchema: detectSchemaMutation.isPending,
    detectSchemaError: detectSchemaMutation.error,

    resetSchema: resetSchemaMutation.mutateAsync,
    isResettingSchema: resetSchemaMutation.isPending,
    resetSchemaError: resetSchemaMutation.error,

    // Base metric actions
    createBaseMetric: createBaseMetricMutation.mutateAsync,
    isCreatingBaseMetric: createBaseMetricMutation.isPending,
    createBaseMetricError: createBaseMetricMutation.error,

    updateBaseMetric: updateBaseMetricMutation.mutateAsync,
    isUpdatingBaseMetric: updateBaseMetricMutation.isPending,
    updateBaseMetricError: updateBaseMetricMutation.error,

    deleteBaseMetric: deleteBaseMetricMutation.mutateAsync,
    isDeletingBaseMetric: deleteBaseMetricMutation.isPending,
    deleteBaseMetricError: deleteBaseMetricMutation.error,

    // Calculated metric actions
    createCalculatedMetric: createCalculatedMetricMutation.mutateAsync,
    isCreatingCalculatedMetric: createCalculatedMetricMutation.isPending,
    createCalculatedMetricError: createCalculatedMetricMutation.error,

    updateCalculatedMetric: updateCalculatedMetricMutation.mutateAsync,
    isUpdatingCalculatedMetric: updateCalculatedMetricMutation.isPending,
    updateCalculatedMetricError: updateCalculatedMetricMutation.error,

    deleteCalculatedMetric: deleteCalculatedMetricMutation.mutateAsync,
    isDeletingCalculatedMetric: deleteCalculatedMetricMutation.isPending,
    deleteCalculatedMetricError: deleteCalculatedMetricMutation.error,

    // Dimension actions
    createDimension: createDimensionMutation.mutateAsync,
    isCreatingDimension: createDimensionMutation.isPending,
    createDimensionError: createDimensionMutation.error,

    updateDimension: updateDimensionMutation.mutateAsync,
    isUpdatingDimension: updateDimensionMutation.isPending,
    updateDimensionError: updateDimensionMutation.error,

    deleteDimension: deleteDimensionMutation.mutateAsync,
    isDeletingDimension: deleteDimensionMutation.isPending,
    deleteDimensionError: deleteDimensionMutation.error,

    // Formula validation
    validateFormula: validateFormulaMutation.mutateAsync,
    isValidatingFormula: validateFormulaMutation.isPending,
    validateFormulaError: validateFormulaMutation.error,

    // Pivot config
    updatePivotConfig: updatePivotConfigMutation.mutateAsync,
    isUpdatingPivotConfig: updatePivotConfigMutation.isPending,
    updatePivotConfigError: updatePivotConfigMutation.error,
  }
}
