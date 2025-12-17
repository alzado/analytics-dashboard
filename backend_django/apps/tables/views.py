"""
BigQuery Table views.
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db.models import Q
from django.utils import timezone

from .models import BigQueryTable, Visibility
from .serializers import (
    BigQueryTableSerializer,
    BigQueryTableCreateSerializer,
    BigQueryTableUpdateSerializer,
    BigQueryConfigUpdateSerializer
)
from apps.core.permissions import IsTableOwnerOrOrganizationMember
from apps.credentials.models import GCPCredential


class BigQueryTableViewSet(viewsets.ModelViewSet):
    """ViewSet for BigQuery tables."""
    permission_classes = [IsAuthenticated, IsTableOwnerOrOrganizationMember]
    lookup_field = 'id'

    def get_queryset(self):
        """Return tables the user has access to."""
        user = self.request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        return BigQueryTable.objects.filter(
            Q(owner=user) |
            Q(organization_id__in=org_ids) |
            Q(visibility=Visibility.PUBLIC)
        ).select_related('owner', 'organization').distinct()

    def list(self, request):
        """
        List tables with FastAPI-compatible response format.

        Response: {"tables": [...], "active_table_id": "..."}
        """
        queryset = self.get_queryset()
        serializer = BigQueryTableSerializer(queryset, many=True)

        # Get active_table_id from query params or use most recently used
        active_table_id = request.query_params.get('active_table_id')
        if not active_table_id:
            most_recent = queryset.order_by('-last_used_at').first()
            active_table_id = str(most_recent.id) if most_recent else None

        # Transform to match frontend expected format
        tables = []
        for table_data in serializer.data:
            tables.append({
                'table_id': str(table_data['id']),
                'name': table_data['name'],
                'project_id': table_data['project_id'],
                'dataset': table_data['dataset'],
                'table': table_data['table_name'],
                'created_at': table_data['created_at'],
                'last_used_at': table_data['last_used_at'],
                'is_active': str(table_data['id']) == active_table_id
            })

        return Response({
            'tables': tables,
            'active_table_id': active_table_id
        })

    def get_serializer_class(self):
        if self.action == 'create':
            return BigQueryTableCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return BigQueryTableUpdateSerializer
        elif self.action == 'update_config':
            return BigQueryConfigUpdateSerializer
        return BigQueryTableSerializer

    def perform_create(self, serializer):
        """Create a new table configuration."""
        data = serializer.validated_data

        # Handle credential
        credential = None
        if 'credential_id' in data:
            try:
                credential = GCPCredential.objects.get(id=data.pop('credential_id'))
            except GCPCredential.DoesNotExist:
                pass
        elif 'credentials_json' in data and data['credentials_json']:
            # Create inline credential
            import json
            creds_json = data.pop('credentials_json')
            creds_dict = json.loads(creds_json)

            credential = GCPCredential(
                user=self.request.user,
                name=f"Credential for {data.get('name', 'table')}",
                project_id=creds_dict.get('project_id', data.get('project_id')),
            )
            credential.set_credentials(creds_json)
            credential.save()
        else:
            data.pop('credentials_json', None)

        # Remove credential fields before save
        data.pop('credential_id', None)

        serializer.save(
            owner=self.request.user,
            gcp_credential=credential
        )

    @action(detail=True, methods=['put'])
    def config(self, request, id=None):
        """Update BigQuery connection configuration."""
        table = self.get_object()
        serializer = BigQueryConfigUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Update fields
        if 'project_id' in serializer.validated_data:
            table.project_id = serializer.validated_data['project_id']
        if 'dataset' in serializer.validated_data:
            table.dataset = serializer.validated_data['dataset']
        if 'table_name' in serializer.validated_data:
            table.table_name = serializer.validated_data['table_name']
        if 'billing_project' in serializer.validated_data:
            table.billing_project = serializer.validated_data['billing_project']

        # Handle credential update
        if 'credential_id' in serializer.validated_data:
            try:
                credential = GCPCredential.objects.get(
                    id=serializer.validated_data['credential_id']
                )
                table.gcp_credential = credential
            except GCPCredential.DoesNotExist:
                return Response(
                    {'error': 'Credential not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
        elif 'credentials_json' in serializer.validated_data:
            import json
            creds_json = serializer.validated_data['credentials_json']
            if creds_json:
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
        return Response(BigQueryTableSerializer(table).data)

    @action(detail=True, methods=['get'])
    def info(self, request, id=None):
        """Get BigQuery table info and date range."""
        table = self.get_object()

        # Update last used timestamp
        table.last_used_at = timezone.now()
        table.save(update_fields=['last_used_at'])

        try:
            from apps.analytics.services.bigquery_service import BigQueryService

            bq_service = BigQueryService(table, request.user)
            info = bq_service.get_table_info()

            return Response({
                'id': str(table.id),
                'name': table.name,
                'full_table_path': table.full_table_path,
                'connection_status': 'connected',
                **info
            })
        except Exception as e:
            return Response({
                'id': str(table.id),
                'name': table.name,
                'full_table_path': table.full_table_path,
                'connection_status': f'error: {str(e)}',
                'date_range': {'min': None, 'max': None},
                'total_rows': 0
            })

    @action(detail=True, methods=['post'])
    def detect_schema(self, request, id=None):
        """Auto-detect schema from BigQuery table."""
        table = self.get_object()

        try:
            from apps.schemas.services.schema_service import SchemaService

            schema_service = SchemaService(table)
            schema = schema_service.detect_and_create_schema()

            return Response({
                'status': 'success',
                'schema_id': str(schema.id),
                'metrics_count': schema.calculated_metrics.count(),
                'dimensions_count': schema.dimensions.count()
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)
