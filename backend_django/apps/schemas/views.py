"""
Schema views for metrics and dimensions management.
"""
import json
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from django.shortcuts import get_object_or_404
from google.cloud import bigquery

from apps.tables.models import BigQueryTable
from apps.core.permissions import IsTableOwnerOrOrganizationMember
from .models import (
    SchemaConfig, CalculatedMetric, Dimension,
    CalculatedDimension, CustomDimension, CustomMetric,
    JoinedDimensionSource, JoinedDimensionStatus
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
    CustomDimensionUpdateSerializer,
    CustomMetricSerializer, CustomMetricCreateSerializer, CustomMetricUpdateSerializer,
    FormulaValidationRequestSerializer, FormulaValidationResponseSerializer,
    SchemaDetectionResponseSerializer,
    JoinedDimensionSourceSerializer, JoinedDimensionSourceCreateSerializer,
    FilePreviewSerializer
)
from .services import SchemaService, MetricService, DimensionService, JoinedDimensionService


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

    def _get_joined_dimensions_as_dicts(self, schema_config, is_filterable=None, is_groupable=None):
        """Get joined dimensions formatted as dimension dicts."""
        joined_dims = []
        for source in schema_config.joined_dimension_sources.filter(
            status=JoinedDimensionStatus.READY
        ).prefetch_related('columns'):
            for col in source.columns.all():
                # Apply filters if specified
                if is_filterable is not None and col.is_filterable != is_filterable:
                    continue
                if is_groupable is not None and col.is_groupable != is_groupable:
                    continue

                joined_dims.append({
                    'id': col.dimension_id,
                    'column_name': col.source_column_name,
                    'display_name': col.display_name,
                    'data_type': col.data_type,
                    'is_filterable': col.is_filterable,
                    'is_groupable': col.is_groupable,
                    'filter_type': col.filter_type,
                    'is_joined': True,
                    'source_id': str(source.id),
                    'source_name': source.name,
                })
        return joined_dims

    def list(self, request, *args, **kwargs):
        """List all dimensions including joined dimensions."""
        # Get regular dimensions
        queryset = self.filter_queryset(self.get_queryset())
        serializer = DimensionListSerializer(queryset, many=True)
        regular_dims = serializer.data

        # Get joined dimensions
        schema_config = self.get_schema_config()
        joined_dims = self._get_joined_dimensions_as_dicts(schema_config)

        # Combine and return
        return Response(regular_dims + joined_dims)

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

        # Get regular filterable dimensions
        dimensions = dimension_service.list_filterable_dimensions()
        serializer = DimensionListSerializer(dimensions, many=True)
        regular_dims = serializer.data

        # Get joined filterable dimensions
        joined_dims = self._get_joined_dimensions_as_dicts(schema_config, is_filterable=True)

        return Response(regular_dims + joined_dims)

    @action(detail=False, methods=['get'])
    def groupable(self, request, *args, **kwargs):
        """Get only groupable dimensions."""
        schema_config = self.get_schema_config()
        dimension_service = DimensionService(schema_config)

        # Get regular groupable dimensions
        dimensions = dimension_service.list_groupable_dimensions()
        serializer = DimensionListSerializer(dimensions, many=True)
        regular_dims = serializer.data

        # Get joined groupable dimensions
        joined_dims = self._get_joined_dimensions_as_dicts(schema_config, is_groupable=True)

        return Response(regular_dims + joined_dims)


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
        elif self.action in ['update', 'partial_update']:
            return CustomDimensionUpdateSerializer
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


class CustomMetricViewSet(viewsets.ModelViewSet):
    """ViewSet for custom metrics (re-aggregation metrics)."""
    permission_classes = []
    serializer_class = CustomMetricSerializer
    lookup_field = 'id'
    pagination_class = None  # Disable pagination - return plain array

    def get_queryset(self):
        """Filter custom metrics by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return CustomMetric.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).order_by('name')
        return CustomMetric.objects.none()

    def get_serializer_class(self):
        if self.action == 'create':
            return CustomMetricCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return CustomMetricUpdateSerializer
        return CustomMetricSerializer

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table_id = self.kwargs.get('table_id')
        table = get_object_or_404(BigQueryTable, id=table_id)
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def get_serializer_context(self):
        """Add schema_config to serializer context for validation."""
        context = super().get_serializer_context()
        table_id = self.kwargs.get('table_id')
        if table_id:
            table = get_object_or_404(BigQueryTable, id=table_id)
            schema_service = SchemaService(table)
            context['schema_config'] = schema_service.get_or_create_schema()
        return context

    def perform_create(self, serializer):
        """Create custom metric with schema config."""
        schema_config = self.get_schema_config()
        serializer.save(schema_config=schema_config)


class JoinedDimensionSourceViewSet(viewsets.ModelViewSet):
    """ViewSet for joined dimension sources (file uploads)."""
    permission_classes = []
    serializer_class = JoinedDimensionSourceSerializer
    lookup_field = 'id'
    pagination_class = None
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def get_queryset(self):
        """Filter joined dimension sources by table."""
        table_id = self.kwargs.get('table_id')
        if table_id:
            return JoinedDimensionSource.objects.filter(
                schema_config__bigquery_table_id=table_id
            ).prefetch_related('columns').order_by('name')
        return JoinedDimensionSource.objects.none()

    def get_table(self):
        """Get the BigQuery table for this request."""
        table_id = self.kwargs.get('table_id')
        return get_object_or_404(BigQueryTable, id=table_id)

    def get_schema_config(self):
        """Get or create schema config for the table."""
        table = self.get_table()
        schema_service = SchemaService(table)
        return schema_service.get_or_create_schema()

    def get_bigquery_client(self, project: str = None):
        """Get BigQuery client for the current user."""
        user = self.request.user
        credentials = None

        # Try user's OAuth credentials
        if user and user.is_authenticated:
            from apps.users.gcp_oauth_service import GCPOAuthService
            credentials = GCPOAuthService.get_valid_credentials(user)

        if credentials:
            return bigquery.Client(project=project, credentials=credentials)
        else:
            # Fall back to ADC
            return bigquery.Client(project=project)

    def create(self, request, *args, **kwargs):
        """Handle file upload and data processing."""
        # Get file from request
        file = request.FILES.get('file')
        if not file:
            return Response(
                {'error': 'File is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate file type
        filename = file.name.lower()
        if not (filename.endswith('.csv') or filename.endswith('.xlsx') or filename.endswith('.xls')):
            return Response(
                {'error': 'File must be CSV (.csv) or Excel (.xlsx)'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse JSON data from form
        try:
            name = request.data.get('name')
            join_key_column = request.data.get('join_key_column')
            target_dimension_id = request.data.get('target_dimension_id')
            bq_project = request.data.get('bq_project')
            bq_dataset = request.data.get('bq_dataset')

            # Parse columns JSON
            columns_data = request.data.get('columns')
            if isinstance(columns_data, str):
                columns = json.loads(columns_data)
            else:
                columns = columns_data

        except (json.JSONDecodeError, TypeError) as e:
            return Response(
                {'error': f'Invalid JSON data: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate required fields
        if not all([name, join_key_column, target_dimension_id, bq_project, bq_dataset, columns]):
            return Response(
                {'error': 'Missing required fields: name, join_key_column, target_dimension_id, bq_project, bq_dataset, columns'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Get schema config and BigQuery client
        schema_config = self.get_schema_config()
        client = self.get_bigquery_client(project=bq_project)

        # Process the upload
        service = JoinedDimensionService(client, schema_config)

        try:
            source = service.process_upload(
                file=file,
                name=name,
                join_key_column=join_key_column,
                target_dimension_id=target_dimension_id,
                columns=columns,
                bq_project=bq_project,
                bq_dataset=bq_dataset
            )

            return Response(
                JoinedDimensionSourceSerializer(source).data,
                status=status.HTTP_201_CREATED
            )
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': f'Upload failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def destroy(self, request, *args, **kwargs):
        """Delete joined dimension source and BigQuery table."""
        source = self.get_object()
        client = self.get_bigquery_client(project=source.bq_project)
        service = JoinedDimensionService(client, source.schema_config)

        try:
            service.delete_source(source)
            return Response(
                {'success': True, 'message': 'Joined dimension source deleted'},
                status=status.HTTP_200_OK
            )
        except Exception as e:
            return Response(
                {'error': f'Delete failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=True, methods=['post'])
    def reupload(self, request, *args, **kwargs):
        """Re-upload file data (replaces existing data)."""
        source = self.get_object()

        file = request.FILES.get('file')
        if not file:
            return Response(
                {'error': 'File is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        client = self.get_bigquery_client(project=source.bq_project)
        service = JoinedDimensionService(client, source.schema_config)

        try:
            result = service.reupload(source, file)
            return Response(JoinedDimensionSourceSerializer(result).data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': f'Reupload failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=True, methods=['get'])
    def preview(self, request, *args, **kwargs):
        """Preview data rows from BigQuery lookup table."""
        source = self.get_object()
        limit = int(request.query_params.get('limit', 10))

        client = self.get_bigquery_client(project=source.bq_project)
        service = JoinedDimensionService(client, source.schema_config)

        result = service.get_preview_data(source, limit)
        return Response(result)

    @action(detail=False, methods=['post'])
    def parse_preview(self, request, *args, **kwargs):
        """Parse uploaded file and return column preview (before committing)."""
        file = request.FILES.get('file')
        if not file:
            return Response(
                {'error': 'File is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate file type
        filename = file.name.lower()
        if not (filename.endswith('.csv') or filename.endswith('.xlsx') or filename.endswith('.xls')):
            return Response(
                {'error': 'File must be CSV (.csv) or Excel (.xlsx)'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse file locally - no BigQuery client needed
        service = JoinedDimensionService()

        try:
            preview = service.parse_file_preview(file)
            return Response(FilePreviewSerializer(preview).data)
        except Exception as e:
            return Response(
                {'error': f'Failed to parse file: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
