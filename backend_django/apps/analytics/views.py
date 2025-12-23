"""
Analytics views for pivot tables and data queries.
"""
import logging
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from django.db.models import Q
from django.utils import timezone
import json

from apps.tables.models import BigQueryTable, Visibility
from apps.tables.serializers import BigQueryTableSerializer, BigQueryTableCreateSerializer
from .services.data_service import DataService
from .services import StatisticalService
from .serializers import (
    PivotResponseSerializer,
    PivotChildRowSerializer,
    TableInfoSerializer,
    SignificanceRequestSerializer,
    SignificanceResponseSerializer,
)

logger = logging.getLogger(__name__)


def parse_dimension_filters(request) -> dict:
    """
    Parse dimension filters from query parameters.
    Supports multi-select: ?country=USA&country=Canada
    """
    dimension_filters = {}
    skip_params = {
        'dimensions', 'dimension_values', 'start_date', 'end_date',
        'date_range_type', 'relative_date_preset', 'limit', 'offset',
        'table_id', 'skip_count', 'metrics', 'require_rollup', 'pivot_dimensions',
        '_t', '_'  # Cache-busting parameters
    }

    for key, values in request.query_params.lists():
        if key not in skip_params:
            dimension_filters[key] = values

    return dimension_filters


def get_table_and_service(request, table_id=None):
    """Get BigQueryTable and DataService for a request."""
    # Get table_id from query param or path param
    if not table_id:
        table_id = request.query_params.get('table_id')

    if not table_id:
        return None, None, Response(
            {'error': 'table_id is required'},
            status=status.HTTP_400_BAD_REQUEST
        )

    # Get the table (permission checks disabled for debugging)
    table = get_object_or_404(BigQueryTable, id=table_id)

    data_service = DataService(table, request.user)
    return table, data_service, None


class PivotView(APIView):
    """Pivot table endpoint."""
    permission_classes = []

    def get(self, request):
        """
        Get pivot table data grouped by specified dimensions.

        Query params:
        - dimensions: List of dimension columns to group by
        - dimension_values: Specific dimension values to fetch
        - start_date, end_date: Date range (YYYY-MM-DD)
        - date_range_type: "absolute" or "relative"
        - relative_date_preset: Preset like "last_7_days"
        - limit: Max rows (default 50)
        - offset: Skip rows (default 0)
        - table_id: BigQuery table ID
        - skip_count: Skip count query (default False)
        - metrics: List of metric IDs to calculate
        - require_rollup: Require rollup availability (default True)
        - Dynamic dimension filters: ?country=USA&channel=Web
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            # Parse parameters
            dimensions = request.query_params.getlist('dimensions', [])
            dimension_values = request.query_params.getlist('dimension_values')
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')
            limit = int(request.query_params.get('limit', 50))
            offset = int(request.query_params.get('offset', 0))
            skip_count = request.query_params.get('skip_count', '').lower() == 'true'
            metrics = request.query_params.getlist('metrics') or None
            require_rollup = request.query_params.get('require_rollup', '').lower() != 'false'

            # Parse dimension filters
            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_pivot_data(
                dimensions=dimensions,
                filters=filters,
                limit=limit,
                offset=offset,
                dimension_values=dimension_values if dimension_values else None,
                skip_count=skip_count,
                metrics=metrics,
                require_rollup=require_rollup
            )

            serializer = PivotResponseSerializer(result)
            return Response(serializer.data)

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.exception(f"Pivot error: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PivotChildrenView(APIView):
    """DISABLED: Pivot drill-down to search terms is no longer supported."""
    permission_classes = []

    def get(self, request, dimension=None, value=None):
        """
        DISABLED: This feature has been removed.

        Pivot drill-down to search terms required raw table access which is no longer
        supported. All data queries must now use rollup tables.
        """
        return Response(
            {
                'error': 'Pivot drill-down to search terms is disabled. This feature requires raw table access which is no longer supported. All data queries must use rollup tables.',
                'error_type': 'feature_disabled',
                'rows': []
            },
            status=status.HTTP_400_BAD_REQUEST
        )


class DimensionValuesView(APIView):
    """Get distinct values for a dimension."""
    permission_classes = []

    def get(self, request, dimension):
        """
        Get distinct values for a given dimension.

        Path params:
        - dimension: Dimension column name

        Query params:
        - table_id: BigQuery table ID
        - pivot_dimensions: List of dimensions in current pivot context (for rollup routing)
        - Filters: start_date, end_date, date_range_type, relative_date_preset
        - Dynamic dimension filters
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')

            # Get pivot context dimensions (for rollup routing)
            pivot_dimensions = request.query_params.getlist('pivot_dimensions', [])

            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_dimension_values(
                dimension=dimension,
                filters=filters,
                pivot_dimensions=pivot_dimensions,
                require_rollup=False  # Dimension values are simple SELECT DISTINCT - don't need rollups
            )

            # If there's an error (no rollup found), return the full error response
            if 'error' in result:
                return Response(result)

            # Otherwise return just the values list for backward compatibility
            return Response(result.get('values', []))

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TableInfoView(APIView):
    """Get BigQuery table info."""
    permission_classes = []

    def get(self, request):
        """Get table info including date range and row count."""
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            info = data_service.bq_service.get_table_info()
            serializer = TableInfoSerializer(info)
            return Response(serializer.data)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# BigQuery Compatibility Views
# These provide backward compatibility with the FastAPI /api/bigquery/* routes
# =============================================================================

def get_user_tables(user):
    """Get tables accessible by user."""
    org_ids = user.memberships.values_list('organization_id', flat=True)
    return BigQueryTable.objects.filter(
        Q(owner=user) |
        Q(organization_id__in=org_ids) |
        Q(visibility=Visibility.PUBLIC)
    ).select_related('owner', 'organization').distinct()


class BigQueryInfoView(APIView):
    """Get BigQuery connection info (compatibility with FastAPI /api/bigquery/info)."""
    permission_classes = []

    def get(self, request):
        """
        Get BigQuery connection info for a table or the user's default table.

        Query params:
        - table_id: Optional specific table ID
        """
        table_id = request.query_params.get('table_id')

        # Get user's tables
        tables = get_user_tables(request.user)

        if table_id:
            table = tables.filter(id=table_id).first()
            if not table:
                return Response(
                    {'error': 'Table not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
        else:
            # Return first table or indicate no tables configured
            table = tables.order_by('-last_used_at').first()

        if not table:
            return Response({
                'configured': False,
                'message': 'No BigQuery tables configured'
            })

        # Update last used
        table.last_used_at = timezone.now()
        table.save(update_fields=['last_used_at'])

        try:
            from .services.bigquery_service import BigQueryService
            bq_service = BigQueryService(table, request.user)
            info = bq_service.get_table_info()

            return Response({
                'configured': True,
                'table_id': str(table.id),
                'name': table.name,
                'project_id': table.project_id,
                'dataset': table.dataset,
                'table': table.table_name,
                'table_full_path': table.full_table_path,
                'connection_status': 'connected',
                'date_range': info.get('date_range', {'min': None, 'max': None}),
                'total_rows': info.get('total_rows', 0),
                'table_size_mb': info.get('table_size_mb', 0),
                'last_modified': info.get('last_modified', ''),
                'schema_columns': info.get('schema_columns', []),
                'allowed_min_date': str(table.allowed_min_date) if table.allowed_min_date else None,
                'allowed_max_date': str(table.allowed_max_date) if table.allowed_max_date else None,
            })
        except Exception as e:
            return Response({
                'configured': True,
                'table_id': str(table.id),
                'name': table.name,
                'project_id': table.project_id,
                'dataset': table.dataset,
                'table': table.table_name,
                'table_full_path': table.full_table_path,
                'connection_status': f'error: {str(e)}',
                'date_range': {'min': None, 'max': None},
                'total_rows': 0,
                'table_size_mb': 0,
                'last_modified': '',
                'schema_columns': []
            })


class BigQueryTablesListView(APIView):
    """List all tables in BigQuery project/dataset."""
    permission_classes = []

    def get(self, request):
        """
        List available BigQuery tables (user's configured tables).
        """
        tables = get_user_tables(request.user)
        result = []

        for table in tables:
            result.append({
                'table_id': str(table.id),
                'name': table.name,
                'project_id': table.project_id,
                'dataset': table.dataset,
                'table': table.table_name,
                'full_table_path': table.full_table_path,
                'created_at': table.created_at.isoformat() if table.created_at else None,
                'last_used_at': table.last_used_at.isoformat() if table.last_used_at else None,
                'is_active': True
            })

        return Response(result)


class BigQueryTableDatesView(APIView):
    """Get date range for a BigQuery table."""
    permission_classes = []

    def get(self, request):
        """
        Get the date range available in a table.

        Query params:
        - table_id: Table ID (required)
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            info = data_service.bq_service.get_table_info()
            date_range = info.get('date_range', {})

            # Check if table has a date column
            has_date_column = date_range.get('min') is not None or date_range.get('max') is not None

            return Response({
                'min_date': date_range.get('min'),
                'max_date': date_range.get('max'),
                'allowed_min_date': str(table.allowed_min_date) if table.allowed_min_date else None,
                'allowed_max_date': str(table.allowed_max_date) if table.allowed_max_date else None,
                'has_date_column': has_date_column,
            })
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BigQueryConfigureView(APIView):
    """Configure BigQuery connection (create or update table)."""
    permission_classes = []

    def post(self, request):
        """
        Configure BigQuery connection.

        Body: {
            name: string,
            project_id: string,
            dataset: string,
            table: string,
            credentials_json?: string,
            billing_project?: string,
            allowed_min_date?: string,
            allowed_max_date?: string,
            date_column?: string
        }
        """
        data = request.data
        from apps.credentials.models import GCPCredential

        # Check if this is an update to an existing table
        table_id = data.get('table_id')
        if table_id:
            tables = get_user_tables(request.user)
            table = tables.filter(id=table_id).first()
            if table:
                # Update existing table
                if 'project_id' in data:
                    table.project_id = data['project_id']
                if 'dataset' in data:
                    table.dataset = data['dataset']
                if 'table' in data:
                    table.table_name = data['table']
                if 'name' in data:
                    table.name = data['name']
                if 'billing_project' in data:
                    table.billing_project = data['billing_project']
                if 'date_column' in data:
                    table.date_column = data['date_column']
                if 'allowed_min_date' in data:
                    table.allowed_min_date = data['allowed_min_date']
                if 'allowed_max_date' in data:
                    table.allowed_max_date = data['allowed_max_date']

                # Handle credentials update
                if data.get('credentials_json'):
                    creds_json = data['credentials_json']
                    creds_dict = json.loads(creds_json)
                    credential = GCPCredential(
                        user=request.user,
                        name=f"Credential for {table.name}",
                        project_id=creds_dict.get('project_id', table.project_id),
                    )
                    credential.set_credentials(creds_json)
                    credential.save()
                    table.gcp_credential = credential

                table.save()
                return Response({
                    'success': True,
                    'message': 'BigQuery table updated',
                    'table_id': str(table.id)
                })

        # Create new table
        name = data.get('name', f"{data.get('dataset', 'dataset')}.{data.get('table', 'table')}")

        # Handle credential
        credential = None
        if data.get('credentials_json'):
            creds_json = data['credentials_json']
            creds_dict = json.loads(creds_json)
            credential = GCPCredential(
                user=request.user,
                name=f"Credential for {name}",
                project_id=creds_dict.get('project_id', data.get('project_id')),
            )
            credential.set_credentials(creds_json)
            credential.save()

        table = BigQueryTable.objects.create(
            owner=request.user,
            name=name,
            project_id=data.get('project_id'),
            dataset=data.get('dataset'),
            table_name=data.get('table'),
            billing_project=data.get('billing_project'),
            date_column=data.get('date_column', 'date'),
            allowed_min_date=data.get('allowed_min_date'),
            allowed_max_date=data.get('allowed_max_date'),
            gcp_credential=credential
        )

        return Response({
            'success': True,
            'message': 'BigQuery table configured',
            'table_id': str(table.id)
        })


class BigQueryDisconnectView(APIView):
    """Disconnect from BigQuery (delete table configuration)."""
    permission_classes = []

    def post(self, request):
        """
        Disconnect/delete a BigQuery table configuration.

        Query params:
        - table_id: Table ID to delete
        """
        table_id = request.query_params.get('table_id')
        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        tables = get_user_tables(request.user)
        table = tables.filter(id=table_id).first()

        if not table:
            return Response(
                {'error': 'Table not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Only owner can delete
        if table.owner != request.user:
            return Response(
                {'error': 'Only the table owner can delete it'},
                status=status.HTTP_403_FORBIDDEN
            )

        table.delete()
        return Response({
            'success': True,
            'message': 'BigQuery table disconnected'
        })


class BigQueryCancelView(APIView):
    """Cancel running BigQuery queries."""
    permission_classes = []

    def post(self, request):
        """Cancel running queries (placeholder - not fully implemented)."""
        return Response({
            'success': True,
            'message': 'Query cancellation not yet implemented in Django',
            'cancelled_count': 0
        })


class BigQueryLogsView(APIView):
    """Query logs endpoint."""
    permission_classes = []

    def get(self, request):
        """Get query logs (placeholder)."""
        return Response({
            'logs': [],
            'total_count': 0
        })


class BigQueryLogsClearView(APIView):
    """Clear query logs."""
    permission_classes = []

    def post(self, request):
        """Clear query logs (placeholder)."""
        return Response({
            'success': True,
            'message': 'Logs cleared',
            'deleted_count': 0
        })


class BigQueryUsageStatsView(APIView):
    """Usage statistics."""
    permission_classes = []

    def get(self, request):
        """Get usage stats (placeholder)."""
        return Response({
            'total_queries': 0,
            'total_bytes_processed': 0,
            'avg_duration_ms': 0
        })


class BigQueryUsageTodayView(APIView):
    """Today's usage statistics."""
    permission_classes = []

    def get(self, request):
        """Get today's usage stats (placeholder)."""
        return Response({
            'total_queries': 0,
            'total_bytes_processed': 0,
            'avg_duration_ms': 0
        })


class BigQueryUsageTimeSeriesView(APIView):
    """Usage time series."""
    permission_classes = []

    def get(self, request):
        """Get usage time series (placeholder)."""
        return Response([])


# =============================================================================
# Optimized Source Views
# These provide endpoints for managing optimized source tables with precomputed
# composite keys for improved COUNT(DISTINCT CONCAT(...)) performance.
# =============================================================================

def get_optimized_source_service(request):
    """Get OptimizedSourceService for a request."""
    from .services.optimized_source_service import OptimizedSourceService
    from .services.bigquery_service import BigQueryService

    table_id = request.query_params.get('table_id')
    if not table_id:
        return None, None, None, Response(
            {'error': 'table_id is required'},
            status=status.HTTP_400_BAD_REQUEST
        )

    tables = get_user_tables(request.user)
    table = tables.filter(id=table_id).first()

    if not table:
        return None, None, None, Response(
            {'error': 'Table not found'},
            status=status.HTTP_404_NOT_FOUND
        )

    try:
        bq_service = BigQueryService(table, request.user)
        optimized_service = OptimizedSourceService(bq_service.client, table)
        return table, bq_service, optimized_service, None
    except Exception as e:
        return None, None, None, Response(
            {'error': str(e)},
            status=status.HTTP_400_BAD_REQUEST
        )


class OptimizedSourceStatusView(APIView):
    """Get status of optimized source table."""
    permission_classes = []

    def get(self, request):
        """Get current status of optimized source table for a BigQuery table."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            # Get schema config
            try:
                schema_config = table.schema_config
            except Exception:
                return Response(
                    {'error': 'Schema not configured'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            source_table_path = table.full_table_path
            result = optimized_service.get_status(source_table_path, schema_config)
            return Response(result)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OptimizedSourceAnalyzeView(APIView):
    """Analyze schema for potential optimized source table creation."""
    permission_classes = []

    def get(self, request):
        """Analyze schema to show what composite keys would be created."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            try:
                schema_config = table.schema_config
            except Exception:
                return Response(
                    {'error': 'Schema not configured'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            source_table_path = table.full_table_path
            result = optimized_service.analyze(source_table_path, schema_config)
            return Response(result)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OptimizedSourcePreviewSqlView(APIView):
    """Preview SQL for optimized source table creation."""
    permission_classes = []

    def get(self, request):
        """Preview the SQL that would be generated for optimized source table."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            try:
                schema_config = table.schema_config
            except Exception:
                return Response(
                    {'error': 'Schema not configured'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            auto_detect_clustering = request.query_params.get('auto_detect_clustering', 'true').lower() == 'true'
            target_project = request.query_params.get('target_project')
            target_dataset = request.query_params.get('target_dataset')

            source_table_path = table.full_table_path
            result = optimized_service.preview_sql(
                source_table_path,
                schema_config,
                auto_detect_clustering=auto_detect_clustering,
                target_project=target_project,
                target_dataset=target_dataset
            )
            return Response(result)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OptimizedSourceCreateView(APIView):
    """Create optimized source table."""
    permission_classes = []

    def post(self, request):
        """Create optimized source table with precomputed composite keys."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            try:
                schema_config = table.schema_config
            except Exception:
                return Response(
                    {'error': 'Schema not configured'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            data = request.data
            auto_detect_clustering = data.get('auto_detect_clustering', True)
            clustering_columns = data.get('clustering_columns')
            target_project = data.get('target_project')
            target_dataset = data.get('target_dataset')

            source_table_path = table.full_table_path
            result = optimized_service.create_optimized_source(
                source_table_path,
                schema_config,
                auto_detect_clustering=auto_detect_clustering,
                clustering_columns=clustering_columns,
                target_project=target_project,
                target_dataset=target_dataset
            )
            return Response(result)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OptimizedSourceRefreshView(APIView):
    """Refresh optimized source table."""
    permission_classes = []

    def post(self, request):
        """Refresh the optimized source table."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            try:
                schema_config = table.schema_config
            except Exception:
                return Response(
                    {'error': 'Schema not configured'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            incremental = request.query_params.get('incremental', 'true').lower() == 'true'
            source_table_path = table.full_table_path

            result = optimized_service.refresh_optimized_source(
                source_table_path,
                schema_config,
                incremental=incremental
            )
            return Response(result)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OptimizedSourceDeleteView(APIView):
    """Delete optimized source table."""
    permission_classes = []

    def delete(self, request):
        """Delete optimized source configuration and optionally the table."""
        table, bq_service, optimized_service, error = get_optimized_source_service(request)
        if error:
            return error

        try:
            drop_table = request.query_params.get('drop_table', 'false').lower() == 'true'
            success, message = optimized_service.delete_optimized_source(drop_table=drop_table)

            if success:
                return Response({'success': True, 'message': message})
            else:
                return Response(
                    {'error': message},
                    status=status.HTTP_400_BAD_REQUEST
                )

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# Statistical Significance Testing
# =============================================================================

class SignificanceView(APIView):
    """
    Calculate statistical significance for rate metrics using two-proportion z-test.

    Only percent-format calculated metrics with simple {A}/{B} formulas are eligible.
    Uses event counts (e.g., queries, clicks) as the sample size, not days.
    """
    permission_classes = []

    def post(self, request):
        """
        Calculate significance for control vs treatment columns.

        Request body:
        - control_column: Reference column definition with dimension filters
        - treatment_columns: List of treatment columns to compare against control
        - metric_ids: List of metric IDs to analyze (only eligible percent metrics will be tested)
        - filters: Base filters (date range, etc.)
        - rows: Optional list of rows to test (for per-row significance)

        Query params:
        - table_id: BigQuery table ID
        """
        # Validate request data
        serializer = SignificanceRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        request_data = serializer.validated_data

        # Get table and services
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            # Initialize services
            stat_service = StatisticalService()
            bq_service = data_service.bq_service

            # Get metric service from schema config
            from apps.schemas.services.metric_service import MetricService
            schema_config = table.schema_config
            metric_service = MetricService(schema_config)

            # Extract request components
            control_column = request_data['control_column']
            treatment_columns = request_data['treatment_columns']
            metric_ids = request_data['metric_ids']
            filters = request_data['filters']
            rows = request_data.get('rows')

            # Convert filters to dict format expected by BQ service
            filter_dict = {
                'start_date': str(filters.get('start_date')) if filters.get('start_date') else None,
                'end_date': str(filters.get('end_date')) if filters.get('end_date') else None,
                'date_range_type': filters.get('date_range_type', 'absolute'),
                'relative_date_preset': filters.get('relative_date_preset'),
                'dimension_filters': filters.get('dimension_filters', {})
            }

            # Filter metrics to only eligible ones:
            # 1. Percent format calculated metrics with simple A/B formula
            # 2. _pct metrics (percentage of total for volume metrics)
            eligible_metrics = {}
            pct_metrics = {}  # Separate handling for _pct metrics

            for metric_id in metric_ids:
                # Check if it's a _pct metric (e.g., queries_pct)
                if metric_id.endswith('_pct'):
                    base_metric_id = metric_id[:-4]  # Remove '_pct' suffix
                    # Verify the base metric exists
                    if schema_config.calculated_metrics.filter(metric_id=base_metric_id).exists():
                        pct_metrics[metric_id] = {
                            'base_metric_id': base_metric_id,
                            'is_pct_metric': True
                        }
                else:
                    # Check if it's a calculated metric with simple A/B formula
                    components = metric_service.extract_formula_components(metric_id)
                    if components and components.get('is_simple_ratio'):
                        eligible_metrics[metric_id] = components

            if not eligible_metrics and not pct_metrics:
                # No eligible metrics - return empty results
                return Response({
                    'control_column_index': control_column['column_index'],
                    'results': {}
                })

            # Collect all base metrics needed (numerators and denominators)
            base_metrics_needed = set()
            for metric_id, components in eligible_metrics.items():
                base_metrics_needed.add(components['numerator_metric_id'])
                base_metrics_needed.add(components['denominator_metric_id'])
            for metric_id, pct_info in pct_metrics.items():
                base_metrics_needed.add(pct_info['base_metric_id'])

            # Merge dimension filters safely (handle None cases)
            base_dim_filters = filter_dict.get('dimension_filters') or {}
            control_dim_filters = control_column.get('dimension_filters') or {}

            # === ROLLUP ROUTING ===
            # Collect ALL dimensions used in any filters (table dims + row dims)
            # A rollup must have ALL these dimensions to be usable
            filter_dimensions = set()
            # From base filters
            filter_dimensions.update(base_dim_filters.keys())
            # From control column filters (table dimensions)
            filter_dimensions.update(control_dim_filters.keys())
            # From treatment column filters
            for treatment in treatment_columns:
                if treatment.get('dimension_filters'):
                    filter_dimensions.update(treatment['dimension_filters'].keys())
            # From row dimension filters
            if rows:
                for row in rows:
                    if row.get('dimension_filters'):
                        filter_dimensions.update(row['dimension_filters'].keys())

            # Get route decision - need rollup with ALL filter dimensions
            from apps.analytics.services.query_router_service import QueryRouterService
            from apps.rollups.models import RollupConfig

            rollup_config = RollupConfig.objects.filter(bigquery_table=table).first()
            use_rollup = False
            rollup_table_path = None

            # Rollup is REQUIRED for significance testing
            if not rollup_config:
                return Response({
                    'error': 'No rollup configuration found. Significance testing requires rollup tables.',
                    'error_type': 'rollup_required',
                    'control_column_index': control_column['column_index'],
                    'results': {}
                }, status=status.HTTP_400_BAD_REQUEST)

            router = QueryRouterService(
                rollup_config=rollup_config,
                schema_config=schema_config,
                source_project_id=table.project_id,
                source_dataset=table.dataset
            )

            # Create routing filter dict (values don't matter for routing, just keys)
            routing_filters = {d: [] for d in filter_dimensions} if filter_dimensions else None

            route_decision = router.route_query(
                query_dimensions=[],  # No GROUP BY needed (we're aggregating totals)
                query_metrics=list(base_metrics_needed),
                query_filters=routing_filters,
                require_rollup=True  # Rollup is required
            )

            # Check if rollup is available
            if not route_decision.use_rollup:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=[],
                    query_metrics=list(base_metrics_needed),
                    query_filters=routing_filters
                )
                return Response({
                    'error': f'No suitable rollup found for significance testing. Filter dimensions: {list(filter_dimensions)}. Reason: {route_decision.reason}',
                    'error_type': 'rollup_required',
                    'control_column_index': control_column['column_index'],
                    'results': {},
                    'available_rollups': available_rollups
                }, status=status.HTTP_400_BAD_REQUEST)

            rollup_table_path = route_decision.rollup_table_path
            logger.info(f"Significance test routing: use_rollup=True, rollup={rollup_table_path}")

            # Helper function to fetch aggregated totals with combined filters
            def fetch_aggregated_totals(extra_filters=None):
                combined_filters = dict(base_dim_filters)
                if extra_filters:
                    combined_filters.update(extra_filters)

                # Always use rollup table (raw table access is not allowed)
                return bq_service.query_rollup_aggregates(
                    rollup_table_path=rollup_table_path,
                    metric_ids=list(base_metrics_needed),
                    start_date=filter_dict.get('start_date'),
                    end_date=filter_dict.get('end_date'),
                    dimension_filters=combined_filters,
                    date_range_type=filter_dict.get('date_range_type', 'absolute'),
                    relative_date_preset=filter_dict.get('relative_date_preset')
                )

            # Helper function to run proportion test for a specific row
            def run_test_for_row(row_filters=None, row_id=None):
                # Fetch control aggregated totals
                control_combined = dict(control_dim_filters)
                if row_filters:
                    control_combined.update(row_filters)
                control_totals = fetch_aggregated_totals(control_combined)

                # Build results for each treatment column
                all_metric_results = {}

                for treatment_col in treatment_columns:
                    # Fetch treatment aggregated totals
                    treatment_combined = dict(treatment_col.get('dimension_filters') or {})
                    if row_filters:
                        treatment_combined.update(row_filters)
                    treatment_totals = fetch_aggregated_totals(treatment_combined)

                    # Run proportion test for each eligible metric
                    for metric_id, components in eligible_metrics.items():
                        numerator_id = components['numerator_metric_id']
                        denominator_id = components['denominator_metric_id']

                        # Get counts
                        control_successes = int(control_totals.get(numerator_id, 0))
                        control_trials = int(control_totals.get(denominator_id, 0))
                        treatment_successes = int(treatment_totals.get(numerator_id, 0))
                        treatment_trials = int(treatment_totals.get(denominator_id, 0))

                        # Skip if no trials (avoid division by zero)
                        if control_trials == 0 and treatment_trials == 0:
                            continue

                        # Get direction preference
                        higher_is_better = stat_service.get_higher_is_better(metric_id)

                        # Run proportion significance test
                        result = stat_service.analyze_proportion_metric(
                            metric_id=metric_id,
                            control_successes=control_successes,
                            control_trials=control_trials,
                            treatment_successes=treatment_successes,
                            treatment_trials=treatment_trials,
                            column_index=treatment_col['column_index'],
                            higher_is_better=higher_is_better
                        )

                        # Convert to dict for serializer
                        result_item = {
                            'metric_id': result.metric_id,
                            'column_index': result.column_index,
                            'row_id': row_id,
                            'prob_beat_control': result.prob_beat_control,
                            'credible_interval_lower': result.credible_interval_lower,
                            'credible_interval_upper': result.credible_interval_upper,
                            'mean_difference': result.mean_difference,
                            'relative_difference': result.relative_difference,
                            'is_significant': result.is_significant,
                            'direction': result.direction,
                            'control_mean': result.control_mean,
                            'treatment_mean': result.treatment_mean,
                            'n_control_events': result.n_control_events,
                            'n_treatment_events': result.n_treatment_events,
                            'control_successes': result.control_successes,
                            'treatment_successes': result.treatment_successes,
                            'warning': result.warning
                        }

                        if metric_id not in all_metric_results:
                            all_metric_results[metric_id] = []
                        all_metric_results[metric_id].append(result_item)

                    # Fetch column totals ONCE per treatment column (for _pct metrics)
                    # These don't change per metric, so fetch outside the loop
                    if pct_metrics:
                        control_column_totals = fetch_aggregated_totals(control_dim_filters)
                        treatment_column_totals = fetch_aggregated_totals(
                            treatment_col.get('dimension_filters') or {}
                        )

                    # Run proportion test for _pct metrics
                    for metric_id, pct_info in pct_metrics.items():
                        base_metric_id = pct_info['base_metric_id']

                        # Get counts
                        control_successes = int(control_totals.get(base_metric_id, 0))
                        control_trials = int(control_column_totals.get(base_metric_id, 0))
                        treatment_successes = int(treatment_totals.get(base_metric_id, 0))
                        treatment_trials = int(treatment_column_totals.get(base_metric_id, 0))

                        # Skip if no trials
                        if control_trials == 0 and treatment_trials == 0:
                            continue

                        # For _pct metrics, higher percentage is typically better
                        higher_is_better = True

                        # Run proportion significance test
                        result = stat_service.analyze_proportion_metric(
                            metric_id=metric_id,
                            control_successes=control_successes,
                            control_trials=control_trials,
                            treatment_successes=treatment_successes,
                            treatment_trials=treatment_trials,
                            column_index=treatment_col['column_index'],
                            higher_is_better=higher_is_better
                        )

                        result_item = {
                            'metric_id': result.metric_id,
                            'column_index': result.column_index,
                            'row_id': row_id,
                            'prob_beat_control': result.prob_beat_control,
                            'credible_interval_lower': result.credible_interval_lower,
                            'credible_interval_upper': result.credible_interval_upper,
                            'mean_difference': result.mean_difference,
                            'relative_difference': result.relative_difference,
                            'is_significant': result.is_significant,
                            'direction': result.direction,
                            'control_mean': result.control_mean,
                            'treatment_mean': result.treatment_mean,
                            'n_control_events': result.n_control_events,
                            'n_treatment_events': result.n_treatment_events,
                            'control_successes': result.control_successes,
                            'treatment_successes': result.treatment_successes,
                            'warning': result.warning
                        }

                        if metric_id not in all_metric_results:
                            all_metric_results[metric_id] = []
                        all_metric_results[metric_id].append(result_item)

                return all_metric_results

            # Check if per-row testing is requested
            if rows:
                # Run per-row significance tests
                all_results = {}
                for row in rows:
                    row_results = run_test_for_row(
                        row.get('dimension_filters'),
                        row.get('row_id')
                    )
                    # Merge results
                    for metric_id, metric_results in row_results.items():
                        if metric_id not in all_results:
                            all_results[metric_id] = []
                        all_results[metric_id].extend(metric_results)
            else:
                # Run totals-only significance test
                all_results = run_test_for_row(None, None)

            return Response({
                'control_column_index': control_column['column_index'],
                'results': all_results
            })

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.exception(f"Error calculating significance: {e}")
            return Response(
                {'error': f"Error calculating significance: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# Cache Management Views
# =============================================================================

class CacheStatsView(APIView):
    """Get query cache statistics."""
    permission_classes = []

    def get(self, request):
        """Get cache statistics."""
        from .services import get_query_cache

        try:
            cache_service = get_query_cache()
            stats = cache_service.get_stats()
            return Response(stats)
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CacheClearView(APIView):
    """Clear query cache."""
    permission_classes = []

    def post(self, request):
        """Clear entire cache."""
        from .services import get_query_cache

        try:
            cache_service = get_query_cache()
            deleted_count = cache_service.clear_all()
            return Response({
                'success': True,
                'message': f'Cleared {deleted_count} cache entries',
                'deleted_count': deleted_count
            })
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CacheClearByTableView(APIView):
    """Clear cache for a specific table."""
    permission_classes = []

    def post(self, request, table_id):
        """Clear cache for a specific table."""
        from .services import get_query_cache

        try:
            cache_service = get_query_cache()
            deleted_count = cache_service.clear_by_table(table_id)
            return Response({
                'success': True,
                'message': f'Cleared {deleted_count} cache entries for table {table_id}',
                'deleted_count': deleted_count,
                'table_id': table_id
            })
        except Exception as e:
            logger.error(f"Error clearing cache for table {table_id}: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CacheClearByTypeView(APIView):
    """Clear cache for a specific query type."""
    permission_classes = []

    def post(self, request, query_type):
        """Clear cache for a specific query type."""
        from .services import get_query_cache

        try:
            cache_service = get_query_cache()
            deleted_count = cache_service.clear_by_query_type(query_type)
            return Response({
                'success': True,
                'message': f'Cleared {deleted_count} cache entries for query type {query_type}',
                'deleted_count': deleted_count,
                'query_type': query_type
            })
        except Exception as e:
            logger.error(f"Error clearing cache for query type {query_type}: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# Core Analytics Endpoints (matching FastAPI /api/overview, /api/trends, etc.)
# =============================================================================

class OverviewView(APIView):
    """Get overview KPI metrics."""
    permission_classes = []

    def get(self, request):
        """
        Get aggregated KPI metrics.

        Query params:
        - table_id: BigQuery table ID
        - start_date, end_date: Date range
        - date_range_type: 'absolute' or 'relative'
        - relative_date_preset: Preset for relative dates
        - Dynamic dimension filters
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')
            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_overview_metrics(filters)

            # Check for rollup-required error
            if isinstance(result, dict) and result.get('error_type') == 'rollup_required':
                return Response(result, status=status.HTTP_400_BAD_REQUEST)

            return Response(result)

        except Exception as e:
            logger.exception(f"Error getting overview metrics: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TrendsView(APIView):
    """Get time-series trends data."""
    permission_classes = []

    def get(self, request):
        """
        Get time-series data for trends visualization.

        Query params:
        - table_id: BigQuery table ID
        - granularity: 'daily', 'weekly', or 'monthly' (default: daily)
        - start_date, end_date: Date range
        - date_range_type: 'absolute' or 'relative'
        - relative_date_preset: Preset for relative dates
        - Dynamic dimension filters
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            granularity = request.query_params.get('granularity', 'daily')
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')
            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_trends_data(filters, granularity)

            # Check for rollup-required error
            if isinstance(result, dict) and result.get('error_type') == 'rollup_required':
                return Response(result, status=status.HTTP_400_BAD_REQUEST)

            return Response(result)

        except Exception as e:
            logger.exception(f"Error getting trends data: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BreakdownView(APIView):
    """Get breakdown by dimension."""
    permission_classes = []

    def get(self, request, dimension):
        """
        Get breakdown data for a specific dimension.

        Path params:
        - dimension: Dimension to break down by

        Query params:
        - table_id: BigQuery table ID
        - limit: Max rows (default: 20)
        - start_date, end_date: Date range
        - date_range_type: 'absolute' or 'relative'
        - relative_date_preset: Preset for relative dates
        - Dynamic dimension filters
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            limit = int(request.query_params.get('limit', 20))
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')
            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_dimension_breakdown(dimension, filters, limit)

            # Check for rollup-required error
            if isinstance(result, dict) and result.get('error_type') == 'rollup_required':
                return Response(result, status=status.HTTP_400_BAD_REQUEST)

            return Response(result)

        except Exception as e:
            logger.exception(f"Error getting breakdown data: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchTermsView(APIView):
    """Get search terms data."""
    permission_classes = []

    def get(self, request):
        """
        Get top search terms with metrics.

        Query params:
        - table_id: BigQuery table ID
        - limit: Max rows (default: 100)
        - sort_by: Metric to sort by (default: queries)
        - start_date, end_date: Date range
        - date_range_type: 'absolute' or 'relative'
        - relative_date_preset: Preset for relative dates
        - Dynamic dimension filters
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            limit = int(request.query_params.get('limit', 100))
            sort_by = request.query_params.get('sort_by', 'queries')
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')
            dimension_filters = parse_dimension_filters(request)

            filters = {
                'start_date': start_date,
                'end_date': end_date,
                'date_range_type': date_range_type,
                'relative_date_preset': relative_date_preset,
                'dimension_filters': dimension_filters
            }

            result = data_service.get_search_terms(filters, limit, sort_by)

            # Check for rollup-required error
            if isinstance(result, dict) and result.get('error_type') == 'rollup_required':
                return Response(result, status=status.HTTP_400_BAD_REQUEST)

            return Response(result)

        except Exception as e:
            logger.exception(f"Error getting search terms: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class FilterOptionsView(APIView):
    """Get filter options for dimensions."""
    permission_classes = []

    def get(self, request):
        """
        Get available filter options for all filterable dimensions.

        Query params:
        - table_id: BigQuery table ID
        - start_date, end_date: Date range (optional, to limit options)
        - date_range_type: 'absolute' or 'relative'
        - relative_date_preset: Preset for relative dates
        """
        table, data_service, error = get_table_and_service(request)
        if error:
            return error

        try:
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            date_range_type = request.query_params.get('date_range_type', 'absolute')
            relative_date_preset = request.query_params.get('relative_date_preset')

            filters = None
            if start_date or end_date:
                filters = {
                    'start_date': start_date,
                    'end_date': end_date,
                    'date_range_type': date_range_type,
                    'relative_date_preset': relative_date_preset
                }

            result = data_service.get_filter_options(filters)
            return Response(result)

        except Exception as e:
            logger.exception(f"Error getting filter options: {e}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DatePresetsView(APIView):
    """Get available date presets."""
    permission_classes = []

    def get(self, request):
        """Get list of available date presets."""
        from apps.core.services import DateResolver

        presets = DateResolver.get_available_presets()
        return Response(presets)
