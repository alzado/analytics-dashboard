import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchSchema,
  detectSchema,
  resetSchema,
  clearSchema,
  fetchCalculatedMetrics,
  fetchDimensions,
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
  fetchJoinedDimensionSources,
  createJoinedDimensionSource,
  deleteJoinedDimensionSource,
  reuploadJoinedDimensionSource,
} from '@/lib/api'
import type {
  SchemaConfig,
  CalculatedMetric,
  DimensionDef,
  CalculatedMetricCreate,
  DimensionCreate,
  CalculatedMetricUpdate,
  DimensionUpdate,
  SchemaDetectionResult,
  FormulaValidationResult,
  JoinedDimensionSource,
} from '@/lib/types'

export function useSchema(tableId?: string) {
  const queryClient = useQueryClient()

  const schemaQuery = useQuery<SchemaConfig>({
    queryKey: ['schema', tableId],
    queryFn: () => fetchSchema(tableId),
  })

  const calculatedMetricsQuery = useQuery<CalculatedMetric[]>({
    queryKey: ['calculated-metrics', tableId],
    queryFn: () => fetchCalculatedMetrics(tableId),
  })

  const dimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['dimensions', tableId],
    queryFn: () => fetchDimensions(tableId),
  })

  const filterableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['filterable-dimensions', tableId],
    queryFn: () => fetchFilterableDimensions(tableId),
  })

  const groupableDimensionsQuery = useQuery<DimensionDef[]>({
    queryKey: ['groupable-dimensions', tableId],
    queryFn: () => fetchGroupableDimensions(tableId),
  })

  const joinedDimensionSourcesQuery = useQuery<JoinedDimensionSource[]>({
    queryKey: ['joined-dimension-sources', tableId],
    queryFn: () => fetchJoinedDimensionSources(tableId),
  })

  // Schema-level mutations
  const detectSchemaMutation = useMutation<SchemaDetectionResult, Error>({
    mutationFn: () => detectSchema(tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
    },
  })

  const resetSchemaMutation = useMutation<SchemaConfig, Error>({
    mutationFn: () => resetSchema(tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId] })
    },
  })

  const clearSchemaMutation = useMutation<{ status: string; message: string }, Error>({
    mutationFn: () => clearSchema(tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schema', tableId] })
      queryClient.invalidateQueries({ queryKey: ['calculated-metrics', tableId] })
      queryClient.invalidateQueries({ queryKey: ['dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['filterable-dimensions', tableId] })
      queryClient.invalidateQueries({ queryKey: ['groupable-dimensions', tableId] })
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
    mutationFn: (formula) => validateFormula(formula, tableId),
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

  // Joined dimension source mutations
  const createJoinedDimensionSourceMutation = useMutation<JoinedDimensionSource, Error, FormData>({
    mutationFn: (formData) => createJoinedDimensionSource(formData, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['joined-dimension-sources', tableId] })
    },
  })

  const deleteJoinedDimensionSourceMutation = useMutation<{ success: boolean; message: string }, Error, string>({
    mutationFn: (sourceId) => deleteJoinedDimensionSource(sourceId, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['joined-dimension-sources', tableId] })
    },
  })

  const reuploadJoinedDimensionSourceMutation = useMutation<
    JoinedDimensionSource,
    Error,
    { sourceId: string; file: File }
  >({
    mutationFn: ({ sourceId, file }) => reuploadJoinedDimensionSource(sourceId, file, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['joined-dimension-sources', tableId] })
    },
  })

  return {
    // Query states
    schema: schemaQuery.data,
    isLoadingSchema: schemaQuery.isLoading,
    schemaError: schemaQuery.error,

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

    clearSchema: clearSchemaMutation.mutateAsync,
    isClearingSchema: clearSchemaMutation.isPending,
    clearSchemaError: clearSchemaMutation.error,

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

    // Joined dimension sources
    joinedDimensionSources: joinedDimensionSourcesQuery.data ?? [],
    isLoadingJoinedDimensionSources: joinedDimensionSourcesQuery.isLoading,
    joinedDimensionSourcesError: joinedDimensionSourcesQuery.error,

    createJoinedDimensionSource: createJoinedDimensionSourceMutation.mutateAsync,
    isCreatingJoinedDimensionSource: createJoinedDimensionSourceMutation.isPending,
    createJoinedDimensionSourceError: createJoinedDimensionSourceMutation.error,

    deleteJoinedDimensionSource: deleteJoinedDimensionSourceMutation.mutateAsync,
    isDeletingJoinedDimensionSource: deleteJoinedDimensionSourceMutation.isPending,
    deleteJoinedDimensionSourceError: deleteJoinedDimensionSourceMutation.error,

    reuploadJoinedDimensionSource: reuploadJoinedDimensionSourceMutation.mutateAsync,
    isReuploadingJoinedDimensionSource: reuploadJoinedDimensionSourceMutation.isPending,
    reuploadJoinedDimensionSourceError: reuploadJoinedDimensionSourceMutation.error,
  }
}
