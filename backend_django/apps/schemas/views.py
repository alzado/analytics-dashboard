"""
Schema views for metrics and dimensions management.
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404

from apps.tables.models import BigQueryTable
from apps.core.permissions import IsTableOwnerOrOrganizationMember
from .models import (
    SchemaConfig, CalculatedMetric, Dimension,
    CalculatedDimension, CustomDimension
)
from .serializers import (
    SchemaConfigSerializer, SchemaConfigUpdateSerializer,
    CalculatedMetricSerializer, CalculatedMetricCreateSerializer,
    CalculatedMetricUpdateSerializer, CalculatedMetricListSerializer,
    DimensionSerializer, DimensionCreateSerializer, DimensionUpdateSerializer,
    DimensionListSerializer,
    CalculatedDimensionSerializer, CalculatedDimensionCreateSerializer,
    CalculatedDimensionUpdateSerializer,
    CustomDimensionSerializer, CustomDimensionCreateSerializer,
    FormulaValidationRequestSerializer, FormulaValidationResponseSerializer,
    SchemaDetectionResponseSerializer
)
from .services import SchemaService, MetricService, DimensionService


class SchemaConfigViewSet(viewsets.ModelViewSet):
    """ViewSet for schema configuration."""
    permission_classes = [IsTableOwnerOrOrganizationMember]
    serializer_class = SchemaConfigSerializer
    lookup_field = 'bigquery_table_id'
    lookup_url_kwarg = 'table_id'  # URL uses table_id, maps to bigquery_table_id
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Return schemas for tables the user has access to."""
        user = self.request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        return SchemaConfig.objects.filter(
            bigquery_table__owner=user
        ).select_related('bigquery_table') | SchemaConfig.objects.filter(
            bigquery_table__organization_id__in=org_ids
        ).select_related('bigquery_table')

    def get_serializer_class(self):
        if self.action in ['update', 'partial_update']:
            return SchemaConfigUpdateSerializer
        return SchemaConfigSerializer

    @action(detail=True, methods=['post'])
    def detect(self, request, table_id=None):
        """Auto-detect schema from BigQuery table."""
        table = get_object_or_404(BigQueryTable, id=table_id)

        try:
            schema_service = SchemaService(table)
            result = schema_service.detect_and_create_schema()

            return Response({
                'status': 'success',
                'schema_id': str(result.id),
                'metrics_count': result.calculated_metrics.count(),
                'dimensions_count': result.dimensions.count()
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'])
    def reset(self, request, table_id=None):
        """Reset schema to defaults."""
        table = get_object_or_404(BigQueryTable, id=table_id)

        try:
            schema_service = SchemaService(table)
            schema_config = schema_service.get_or_create_schema(auto_detect=False)

            # Clear and re-detect
            schema_config.calculated_metrics.all().delete()
            schema_config.dimensions.all().delete()
            schema_config.calculated_dimensions.all().delete()

            # Create default metrics
            schema_service.create_default_metrics(schema_config)

            return Response({
                'status': 'success',
                'message': 'Schema reset to defaults'
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'])
    def clear(self, request, table_id=None):
        """Clear all metrics and dimensions without recreating defaults."""
        table = get_object_or_404(BigQueryTable, id=table_id)

        try:
            schema_config = SchemaConfig.objects.get(bigquery_table=table)

            # Delete all metrics and dimensions
            metrics_count = schema_config.calculated_metrics.count()
            dims_count = schema_config.dimensions.count()
            calc_dims_count = schema_config.calculated_dimensions.count()

            schema_config.calculated_metrics.all().delete()
            schema_config.dimensions.all().delete()
            schema_config.calculated_dimensions.all().delete()

            return Response({
                'status': 'success',
                'message': f'Cleared {metrics_count} metrics, {dims_count} dimensions, {calc_dims_count} calculated dimensions'
            })
        except SchemaConfig.DoesNotExist:
            return Response({
                'status': 'error',
                'message': 'No schema configuration found for this table'
            }, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'])
    def copy(self, request, table_id=None):
        """Copy schema from another table."""
        source_table_id = request.data.get('source_table_id')
        if not source_table_id:
            return Response({
                'error': 'source_table_id is required'
            }, status=status.HTTP_400_BAD_REQUEST)

        target_table = get_object_or_404(BigQueryTable, id=table_id)
        source_table = get_object_or_404(BigQueryTable, id=source_table_id)

        try:
            source_schema = source_table.schema_config

            schema_service = SchemaService(target_table)
            target_schema = schema_service.copy_schema_from(source_schema)

            return Response({
                'status': 'success',
                'schema_id': str(target_schema.id),
                'message': 'Schema copied successfully'
            })
        except SchemaConfig.DoesNotExist:
            return Response({
                'error': 'Source table has no schema configuration'
            }, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)


class CalculatedMetricViewSet(viewsets.ModelViewSet):
    """ViewSet for calculated metrics."""
    permission_classes = []
    serializer_class = CalculatedMetricSerializer
    lookup_field = 'metric_id'
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Filter metrics by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return CalculatedMetric.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).order_by('sort_order')
        return CalculatedMetric.objects.none()

    def get_serializer_class(self):
        if self.action == 'create':
            return CalculatedMetricCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return CalculatedMetricUpdateSerializer
        elif self.action == 'list':
            return CalculatedMetricListSerializer
        return CalculatedMetricSerializer

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table_id = self.kwargs.get('table_id')
        table = get_object_or_404(BigQueryTable, id=table_id)
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def create(self, request, *args, **kwargs):
        """Create a new calculated metric."""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        metric_service = MetricService(schema_config)

        try:
            metric = metric_service.create_metric(
                display_name=serializer.validated_data['display_name'],
                formula=serializer.validated_data['formula'],
                metric_id=serializer.validated_data.get('metric_id'),
                format_type=serializer.validated_data.get('format_type', 'number'),
                decimal_places=serializer.validated_data.get('decimal_places', 2),
                category=serializer.validated_data.get('category', 'other'),
                is_visible_by_default=serializer.validated_data.get(
                    'is_visible_by_default', True
                ),
                sort_order=serializer.validated_data.get('sort_order', 999),
                description=serializer.validated_data.get('description')
            )

            return Response(
                CalculatedMetricSerializer(metric).data,
                status=status.HTTP_201_CREATED
            )
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def update(self, request, *args, **kwargs):
        """Update a calculated metric."""
        partial = kwargs.pop('partial', False)
        metric_id = self.kwargs.get('metric_id')

        serializer = self.get_serializer(data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        metric_service = MetricService(schema_config)

        try:
            metric = metric_service.update_metric(
                metric_id=metric_id,
                **serializer.validated_data
            )

            # Cascade update dependents
            metric_service.cascade_update_dependents(metric_id)

            return Response(CalculatedMetricSerializer(metric).data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def destroy(self, request, *args, **kwargs):
        """Delete a calculated metric."""
        metric_id = self.kwargs.get('metric_id')

        schema_config = self.get_schema_config()
        metric_service = MetricService(schema_config)

        try:
            metric_service.delete_metric(metric_id)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )

    @action(detail=False, methods=['post'])
    def validate_formula(self, request, *args, **kwargs):
        """Validate a formula without saving it."""
        serializer = FormulaValidationRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        metric_service = MetricService(schema_config)

        result = metric_service.validate_formula(
            serializer.validated_data['formula']
        )

        return Response(result)

    @action(detail=True, methods=['get'])
    def dependents(self, request, *args, **kwargs):
        """Get metrics that depend on this metric."""
        metric_id = self.kwargs.get('metric_id')

        schema_config = self.get_schema_config()
        metric_service = MetricService(schema_config)

        dependents = metric_service.get_dependents(metric_id)

        return Response({
            'metric_id': metric_id,
            'dependents': dependents
        })


class DimensionViewSet(viewsets.ModelViewSet):
    """ViewSet for dimensions."""
    permission_classes = []
    serializer_class = DimensionSerializer
    lookup_field = 'dimension_id'
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Filter dimensions by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return Dimension.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).order_by('sort_order')
        return Dimension.objects.none()

    def get_serializer_class(self):
        if self.action == 'create':
            return DimensionCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return DimensionUpdateSerializer
        elif self.action == 'list':
            return DimensionListSerializer
        return DimensionSerializer

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table_id = self.kwargs.get('table_id')
        table = get_object_or_404(BigQueryTable, id=table_id)
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def create(self, request, *args, **kwargs):
        """Create a new dimension."""
        import re
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        # Auto-generate dimension_id and column_name from display_name if not provided
        data = serializer.validated_data.copy()
        display_name = data.get('display_name', '')

        if not data.get('dimension_id'):
            # Generate ID from display name: lowercase, replace spaces with underscores
            generated_id = re.sub(r'[^\w\s-]', '', display_name.lower())
            generated_id = re.sub(r'[-\s]+', '_', generated_id)
            data['dimension_id'] = generated_id

        if not data.get('column_name'):
            # Use dimension_id as column_name by default
            data['column_name'] = data['dimension_id']

        try:
            dimension = dimension_service.create_dimension(**data)

            return Response(
                DimensionSerializer(dimension).data,
                status=status.HTTP_201_CREATED
            )
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def update(self, request, *args, **kwargs):
        """Update a dimension."""
        partial = kwargs.pop('partial', False)
        dimension_id = self.kwargs.get('dimension_id')

        serializer = self.get_serializer(data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        try:
            dimension = dimension_service.update_dimension(
                dimension_id=dimension_id,
                **serializer.validated_data
            )

            return Response(DimensionSerializer(dimension).data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def destroy(self, request, *args, **kwargs):
        """Delete a dimension."""
        dimension_id = self.kwargs.get('dimension_id')

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        try:
            dimension_service.delete_dimension(dimension_id)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )

    @action(detail=False, methods=['get'])
    def filterable(self, request, *args, **kwargs):
        """Get only filterable dimensions."""
        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        dimensions = dimension_service.list_filterable_dimensions()
        serializer = DimensionListSerializer(dimensions, many=True)

        return Response(serializer.data)

    @action(detail=False, methods=['get'])
    def groupable(self, request, *args, **kwargs):
        """Get only groupable dimensions."""
        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        dimensions = dimension_service.list_groupable_dimensions()
        serializer = DimensionListSerializer(dimensions, many=True)

        return Response(serializer.data)


class CalculatedDimensionViewSet(viewsets.ModelViewSet):
    """ViewSet for calculated dimensions."""
    permission_classes = []
    serializer_class = CalculatedDimensionSerializer
    lookup_field = 'dimension_id'
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Filter calculated dimensions by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return CalculatedDimension.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).order_by('sort_order')
        return CalculatedDimension.objects.none()

    def get_serializer_class(self):
        if self.action == 'create':
            return CalculatedDimensionCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return CalculatedDimensionUpdateSerializer
        return CalculatedDimensionSerializer

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table_id = self.kwargs.get('table_id')
        table = get_object_or_404(BigQueryTable, id=table_id)
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def create(self, request, *args, **kwargs):
        """Create a new calculated dimension."""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        try:
            calc_dim = dimension_service.create_calculated_dimension(
                display_name=serializer.validated_data['display_name'],
                sql_expression=serializer.validated_data['sql_expression'],
                dimension_id=serializer.validated_data.get('dimension_id'),
                data_type=serializer.validated_data.get('data_type', 'STRING'),
                is_filterable=serializer.validated_data.get('is_filterable', True),
                is_groupable=serializer.validated_data.get('is_groupable', True),
                sort_order=serializer.validated_data.get('sort_order', 999),
                filter_type=serializer.validated_data.get('filter_type', 'multi'),
                description=serializer.validated_data.get('description')
            )

            return Response(
                CalculatedDimensionSerializer(calc_dim).data,
                status=status.HTTP_201_CREATED
            )
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def update(self, request, *args, **kwargs):
        """Update a calculated dimension."""
        partial = kwargs.pop('partial', False)
        dimension_id = self.kwargs.get('dimension_id')

        serializer = self.get_serializer(data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        try:
            calc_dim = dimension_service.update_calculated_dimension(
                dimension_id=dimension_id,
                **serializer.validated_data
            )

            return Response(CalculatedDimensionSerializer(calc_dim).data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def destroy(self, request, *args, **kwargs):
        """Delete a calculated dimension."""
        dimension_id = self.kwargs.get('dimension_id')

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        try:
            dimension_service.delete_calculated_dimension(dimension_id)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )

    @action(detail=False, methods=['post'])
    def validate_expression(self, request, *args, **kwargs):
        """Validate a SQL expression without saving it."""
        sql_expression = request.data.get('sql_expression')
        if not sql_expression:
            return Response(
                {'error': 'sql_expression is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        result = dimension_service.validate_expression(sql_expression)

        return Response(result)


class CustomDimensionViewSet(viewsets.ModelViewSet):
    """ViewSet for custom dimensions (date ranges, metric conditions)."""
    permission_classes = []
    serializer_class = CustomDimensionSerializer
    lookup_field = 'id'
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Filter custom dimensions by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return CustomDimension.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).order_by('name')
        return CustomDimension.objects.none()

    def get_serializer_class(self):
        if self.action == 'create':
            return CustomDimensionCreateSerializer
        return CustomDimensionSerializer

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table_id = self.kwargs.get('table_id')
        table = get_object_or_404(BigQueryTable, id=table_id)
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def perform_create(self, serializer):
        """Create custom dimension with schema config."""
        schema_config = self.get_schema_config()
        serializer.save(schema_config=schema_config)
