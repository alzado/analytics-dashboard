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

export function useSchema() {
  const queryClient = useQueryClient()

  const schemaQuery = useQuery<SchemaConfig>({
    queryKey: ['schema'],
    queryFn: fetchSchema,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })

  const baseMetricsQuery = useQuery<BaseMetric[]>({
    queryKey: ['base-metrics'],
    queryFn: fetchBaseMetrics,
    staleTime: 5 * 60 * 1000,
  })

  const calculatedMetricsQuery = useQuery<CalculatedMetric[]>({
    queryKey: ['calculated-metrics'],
    queryFn: fetchCalculatedMetrics,
    staleTime: 5 * 60 * 1000,
  })

  const dimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['dimensions'],
    queryFn: fetchDimensions,
    staleTime: 5 * 60 * 1000,
  })

  const filterableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['filterable-dimensions'],
    queryFn: fetchFilterableDimensions,
    staleTime: 5 * 60 * 1000,
  })

  const groupableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['groupable-dimensions'],
    queryFn: fetchGroupableDimensions,
    staleTime: 5 * 60 * 1000,
  })

  // Schema-level mutations
  const detectSchemaMutation = useMutation<SchemaDetectionResult, Error>({
    mutationFn: detectSchema,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics'] })
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
    },
  })

  const resetSchemaMutation = useMutation<SchemaConfig, Error>({
    mutationFn: resetSchema,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics'] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics'] })
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions'] })
    },
  })

  // Base metric mutations
  const createBaseMetricMutation = useMutation<BaseMetric, Error, MetricCreate>({
    mutationFn: createBaseMetric,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics'] })
    },
  })

  const updateBaseMetricMutation = useMutation<BaseMetric, Error, { id: string; data: MetricUpdate }>({
    mutationFn: ({ id, data }) => updateBaseMetric(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics'] })
    },
  })

  const deleteBaseMetricMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: deleteBaseMetric,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['base-metrics'] })
    },
  })

  // Calculated metric mutations
  const createCalculatedMetricMutation = useMutation<CalculatedMetric, Error, CalculatedMetricCreate>({
    mutationFn: createCalculatedMetric,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics'] })
    },
  })

  const updateCalculatedMetricMutation = useMutation<
    CalculatedMetric,
    Error,
    { id: string; data: CalculatedMetricUpdate }
  >({
    mutationFn: ({ id, data }) => updateCalculatedMetric(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics'] })
    },
  })

  const deleteCalculatedMetricMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: deleteCalculatedMetric,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics'] })
    },
  })

  // Dimension mutations
  const createDimensionMutation = useMutation<DimensionDef, Error, DimensionCreate>({
    mutationFn: createDimension,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions'] })
    },
  })

  const updateDimensionMutation = useMutation<DimensionDef, Error, { id: string; data: DimensionUpdate }>({
    mutationFn: ({ id, data }) => updateDimension(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions'] })
    },
  })

  const deleteDimensionMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: deleteDimension,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions'] })
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
    mutationFn: updatePivotConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema'] })
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
