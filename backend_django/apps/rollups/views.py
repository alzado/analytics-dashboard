"""
Rollup views for pre-aggregation management.
"""
import logging
from django.db.models import Q
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from apps.core.permissions import IsAuthenticatedOrAuthDisabled
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404

from google.cloud import bigquery

from apps.tables.models import BigQueryTable
from .models import Rollup, RollupConfig, RollupStatus
from .serializers import (
    RollupSerializer,
    RollupCreateSerializer,
    RollupListSerializer,
    RollupConfigSerializer,
    RollupRefreshResponseSerializer,
    RollupPreviewSqlSerializer,
    RollupStatusResponseSerializer
)
from .services import RollupService

logger = logging.getLogger(__name__)


class RollupViewSet(viewsets.ModelViewSet):
    """ViewSet for rollup management."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]
    lookup_field = 'id'

    def get_queryset(self):
        """Return rollups for tables the user has access to."""
        user = self.request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        # Get table_id from query params if provided
        table_id = self.request.query_params.get('table_id')

        queryset = Rollup.objects.filter(
            Q(bigquery_table__owner=user) |
            Q(bigquery_table__organization_id__in=org_ids)
        ).select_related('bigquery_table').distinct()

        if table_id:
            queryset = queryset.filter(bigquery_table_id=table_id)

        return queryset

    def get_serializer_class(self):
        if self.action == 'list':
            return RollupListSerializer
        elif self.action == 'create':
            return RollupCreateSerializer
        return RollupSerializer

    def list(self, request, *args, **kwargs):
        """Override list to wrap response in expected format with config."""
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True)

        # Get table_id to fetch config
        table_id = request.query_params.get('table_id')
        default_target_project = None
        default_target_dataset = None

        if table_id:
            try:
                config = RollupConfig.objects.filter(bigquery_table_id=table_id).first()
                if config:
                    default_target_project = config.default_project or None
                    default_target_dataset = config.default_dataset or None
            except Exception:
                pass

        return Response({
            'rollups': serializer.data,
            'default_target_project': default_target_project,
            'default_target_dataset': default_target_dataset,
        })

    def perform_create(self, serializer):
        """Create a new rollup."""
        import re
        import uuid

        # Get table_id from query params (frontend sends it there) or request body
        table_id = self.request.query_params.get('table_id') or self.request.data.get('table_id')
        if not table_id:
            raise ValueError("table_id is required")

        table = get_object_or_404(BigQueryTable, id=table_id)

        # Get project/dataset: prefer frontend values, fall back to RollupConfig defaults
        rollup_project = serializer.validated_data.get('rollup_project', '')
        rollup_dataset = serializer.validated_data.get('rollup_dataset', '')

        # If not provided by frontend, use RollupConfig defaults
        if not rollup_project or not rollup_dataset:
            try:
                config = RollupConfig.objects.filter(bigquery_table=table).first()
                if config:
                    if not rollup_project:
                        rollup_project = config.default_project or ''
                    if not rollup_dataset:
                        rollup_dataset = config.default_dataset or ''
            except Exception:
                pass

        # Get dimensions for ID generation
        dimensions = serializer.validated_data.get('dimensions', [])
        sorted_dims = sorted(dimensions)

        # Generate rollup_id using FastAPI convention: {table_id}_rollup_{sorted_dims}
        # Use short table ID (first 8 chars of UUID)
        short_table_id = str(table.id).split('-')[0]
        dim_suffix = '_'.join(sorted_dims) if sorted_dims else 'nodims'
        rollup_id = f"{short_table_id}_rollup_{dim_suffix}"

        # Table name is the same as rollup_id (FastAPI convention)
        rollup_table = rollup_id

        serializer.save(
            bigquery_table=table,
            rollup_id=rollup_id,
            rollup_table=rollup_table,
            rollup_project=rollup_project,
            rollup_dataset=rollup_dataset
        )

    @action(detail=True, methods=['post'])
    def refresh(self, request, id=None):
        """Refresh a rollup by re-running the aggregation query."""
        rollup = self.get_object()
        table = rollup.bigquery_table

        # Get BigQuery client from table credentials
        try:
            bq_client = self._get_bigquery_client(table)
        except Exception as e:
            return Response({
                'success': False,
                'message': f'Failed to create BigQuery client: {str(e)}',
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Get query params for refresh options
        incremental = request.query_params.get('incremental', 'true').lower() == 'true'
        force = request.query_params.get('force', 'false').lower() == 'true'

        try:
            service = RollupService(bq_client, table)
            result = service.refresh_rollup(rollup, incremental=incremental, force=force)

            if result['success']:
                return Response(result)
            else:
                return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.exception(f"Rollup refresh failed: {e}")
            rollup.mark_error(str(e))
            return Response({
                'success': False,
                'message': f'Rollup refresh failed: {str(e)}',
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _get_bigquery_client(self, table: BigQueryTable) -> bigquery.Client:
        """Get a BigQuery client for the table's credentials."""
        # Try to get credentials from table's credential config
        try:
            cred_config = table.credential_config
            if cred_config and cred_config.credentials_json:
                from google.oauth2 import service_account
                import json
                credentials = service_account.Credentials.from_service_account_info(
                    json.loads(cred_config.credentials_json)
                )
                return bigquery.Client(project=table.project_id, credentials=credentials)
        except Exception:
            pass

        # Fall back to default credentials (ADC)
        return bigquery.Client(project=table.project_id)

    @action(detail=True, methods=['get'], url_path='preview-sql')
    def preview_sql(self, request, id=None):
        """Preview the SQL that would be used to create/refresh the rollup."""
        rollup = self.get_object()
        table = rollup.bigquery_table

        # Use RollupService to generate proper SQL with schema
        try:
            bq_client = self._get_bigquery_client(table)
            service = RollupService(bq_client, table)
            return Response(service.preview_sql(rollup))
        except Exception as e:
            # Fallback to basic SQL if service fails
            logger.warning(f"Failed to generate SQL via service: {e}")
            dim_columns = ", ".join(rollup.dimensions) if rollup.dimensions else "1 as _dummy"
            fallback_sql = f"""-- CREATE TABLE SQL
CREATE OR REPLACE TABLE `{rollup.full_rollup_path}` AS
SELECT
    {dim_columns},
    COUNT(*) as row_count
FROM `{table.full_table_path}`
GROUP BY {dim_columns}"""
            return Response({
                'rollup_id': str(rollup.id),
                'sql': fallback_sql,
                'target_table_path': rollup.full_rollup_path
            })

    @action(detail=True, methods=['get'])
    def status(self, request, id=None):
        """Get detailed status of a rollup."""
        rollup = self.get_object()

        return Response({
            'rollup_id': str(rollup.id),
            'name': rollup.name,
            'status': rollup.status,
            'last_refresh_at': rollup.last_refresh_at,
            'row_count': rollup.row_count,
            'size_bytes': rollup.size_bytes,
            'error_message': rollup.error_message
        })

    def destroy(self, request, *args, **kwargs):
        """
        Delete a rollup and optionally drop the BigQuery table.

        Query params:
        - drop_table: If 'true', also drop the BigQuery table (default: true)
        """
        rollup = self.get_object()
        table = rollup.bigquery_table

        # Check if we should drop the BigQuery table (default: True)
        drop_table = request.query_params.get('drop_table', 'true').lower() == 'true'

        if drop_table:
            # Use RollupService to delete both DB record and BigQuery table
            try:
                bq_client = self._get_bigquery_client(table)
                service = RollupService(bq_client, table)
                result = service.delete_rollup(rollup, drop_table=True)

                if result['success']:
                    return Response({
                        'success': True,
                        'message': result['message'],
                        'dropped_table': True
                    }, status=status.HTTP_200_OK)
                else:
                    return Response({
                        'success': False,
                        'message': result['message'],
                        'dropped_table': False
                    }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            except Exception as e:
                logger.exception(f"Failed to delete rollup with table drop: {e}")
                return Response({
                    'success': False,
                    'message': f'Failed to delete rollup: {str(e)}',
                    'dropped_table': False
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            # Just delete the DB record without dropping BigQuery table
            rollup.delete()
            return Response({
                'success': True,
                'message': 'Rollup deleted (BigQuery table preserved)',
                'dropped_table': False
            }, status=status.HTTP_200_OK)


class RefreshAllRollupsView(APIView):
    """Refresh all rollups for a table."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def _get_bigquery_client(self, table: BigQueryTable) -> bigquery.Client:
        """Get a BigQuery client for the table's credentials."""
        try:
            cred_config = table.credential_config
            if cred_config and cred_config.credentials_json:
                from google.oauth2 import service_account
                import json
                credentials = service_account.Credentials.from_service_account_info(
                    json.loads(cred_config.credentials_json)
                )
                return bigquery.Client(project=table.project_id, credentials=credentials)
        except Exception:
            pass
        return bigquery.Client(project=table.project_id)

    def post(self, request):
        """
        Refresh all rollups for a table.

        Query params or request body:
        - table_id: UUID of the table
        """
        # Accept table_id from query params (frontend) or request body
        table_id = request.query_params.get('table_id') or request.data.get('table_id')
        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        table = get_object_or_404(BigQueryTable, id=table_id)

        # Check permissions
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)
        if not (
            table.owner == user or
            table.organization_id in org_ids
        ):
            return Response(
                {'error': 'Permission denied'},
                status=status.HTTP_403_FORBIDDEN
            )

        # Get BigQuery client
        try:
            bq_client = self._get_bigquery_client(table)
        except Exception as e:
            return Response({
                'success': False,
                'message': f'Failed to create BigQuery client: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Use RollupService to refresh all rollups
        service = RollupService(bq_client, table)
        result = service.refresh_all_rollups(incremental=True, only_pending_or_stale=False)

        # Format response for frontend
        refresh_results = []
        for r in result.get('results', []):
            refresh_results.append({
                'rollup_id': r.get('rollup_id'),
                'name': r.get('message', ''),
                'status': r.get('status', 'unknown')
            })

        return Response({
            'success': result.get('success', True),
            'message': f'Refreshed {result.get("total", len(refresh_results))} rollups '
                       f'({result.get("successful", 0)} succeeded, {result.get("failed", 0)} failed)',
            'rollups': refresh_results
        })


class RollupConfigView(APIView):
    """Manage rollup configuration for a table."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def get(self, request):
        """Get rollup config for a table."""
        table_id = request.query_params.get('table_id')
        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        table = get_object_or_404(BigQueryTable, id=table_id)

        config, _ = RollupConfig.objects.get_or_create(bigquery_table=table)
        serializer = RollupConfigSerializer(config)
        return Response(serializer.data)

    def put(self, request):
        """Update rollup config for a table."""
        table_id = request.data.get('table_id')
        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        table = get_object_or_404(BigQueryTable, id=table_id)

        config, _ = RollupConfig.objects.get_or_create(bigquery_table=table)
        serializer = RollupConfigSerializer(config, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(serializer.data)


class DefaultProjectView(APIView):
    """Update default project for rollups."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def put(self, request):
        """Set default project for rollup tables."""
        # Accept table_id from query params or body
        table_id = request.query_params.get('table_id') or request.data.get('table_id')
        # Accept project from query params (frontend sends 'project') or body (default_project)
        default_project = (
            request.query_params.get('project') or
            request.data.get('project') or
            request.data.get('default_project')
        )

        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        table = get_object_or_404(BigQueryTable, id=table_id)
        config, _ = RollupConfig.objects.get_or_create(bigquery_table=table)
        config.default_project = default_project or ''
        config.save()

        return Response({
            'success': True,
            'default_project': config.default_project
        })


class DefaultDatasetView(APIView):
    """Update default dataset for rollups."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def put(self, request):
        """Set default dataset for rollup tables."""
        # Accept table_id from query params or body
        table_id = request.query_params.get('table_id') or request.data.get('table_id')
        # Accept dataset from query params (frontend sends 'dataset') or body (default_dataset)
        default_dataset = (
            request.query_params.get('dataset') or
            request.data.get('dataset') or
            request.data.get('default_dataset')
        )

        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        table = get_object_or_404(BigQueryTable, id=table_id)
        config, _ = RollupConfig.objects.get_or_create(bigquery_table=table)
        config.default_dataset = default_dataset or ''
        config.save()

        return Response({
            'success': True,
            'default_dataset': config.default_dataset
        })
