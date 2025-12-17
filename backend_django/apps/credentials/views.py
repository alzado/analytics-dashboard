"""
Credentials views.
"""
import json
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from apps.core.permissions import IsAuthenticatedOrAuthDisabled
from django.db.models import Q

from .models import GCPCredential, CredentialType
from .serializers import (
    GCPCredentialSerializer,
    GCPCredentialCreateSerializer,
    GCPCredentialUpdateSerializer
)
from apps.organizations.models import Organization


class GCPCredentialViewSet(viewsets.ModelViewSet):
    """ViewSet for GCP credentials."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]
    lookup_field = 'id'

    def get_queryset(self):
        """Return credentials the user has access to."""
        user = self.request.user
        # User's own credentials + organization credentials
        org_ids = user.memberships.values_list('organization_id', flat=True)
        return GCPCredential.objects.filter(
            Q(user=user) | Q(organization_id__in=org_ids)
        ).distinct()

    def get_serializer_class(self):
        if self.action == 'create':
            return GCPCredentialCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return GCPCredentialUpdateSerializer
        return GCPCredentialSerializer

    def create(self, request):
        """Create a new GCP credential."""
        serializer = GCPCredentialCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Parse credentials to get project_id
        creds = json.loads(serializer.validated_data['credentials_json'])
        project_id = creds.get('project_id')

        # Create credential
        credential = GCPCredential(
            user=request.user,
            name=serializer.validated_data['name'],
            credential_type=CredentialType.SERVICE_ACCOUNT,
            project_id=project_id,
            is_default=serializer.validated_data.get('is_default', False)
        )

        # Handle organization
        org_id = serializer.validated_data.get('organization_id')
        if org_id:
            try:
                org = Organization.objects.get(id=org_id)
                # Verify user is member
                if not org.memberships.filter(user=request.user).exists():
                    return Response(
                        {'error': 'Not a member of this organization'},
                        status=status.HTTP_403_FORBIDDEN
                    )
                credential.organization = org
                credential.user = None  # Org credential, not user credential
            except Organization.DoesNotExist:
                return Response(
                    {'error': 'Organization not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        # Encrypt and save credentials
        credential.set_credentials(serializer.validated_data['credentials_json'])
        credential.save()

        return Response(
            GCPCredentialSerializer(credential).data,
            status=status.HTTP_201_CREATED
        )

    def update(self, request, id=None):
        """Update a credential."""
        credential = self.get_object()
        serializer = GCPCredentialUpdateSerializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)

        if 'name' in serializer.validated_data:
            credential.name = serializer.validated_data['name']
        if 'is_default' in serializer.validated_data:
            credential.is_default = serializer.validated_data['is_default']

        credential.save()
        return Response(GCPCredentialSerializer(credential).data)

    @action(detail=True, methods=['post'])
    def set_default(self, request, id=None):
        """Set credential as default."""
        credential = self.get_object()
        credential.is_default = True
        credential.save()
        return Response(GCPCredentialSerializer(credential).data)

    @action(detail=True, methods=['post'])
    def verify(self, request, id=None):
        """Verify credential can connect to BigQuery."""
        credential = self.get_object()

        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account

            creds_dict = credential.get_credentials()
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(credentials=credentials, project=credential.project_id)

            # Try a simple query
            client.query("SELECT 1").result()

            return Response({
                'status': 'success',
                'message': 'Credentials verified successfully'
            })
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)
