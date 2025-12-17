"""
Library views for table discovery and sharing.
"""
from django.db.models import Q, Count
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from apps.tables.models import BigQueryTable, Visibility
from apps.tables.serializers import BigQueryTableSerializer


class LibraryListView(APIView):
    """List public and shared tables available in the library."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Get tables available in the library.

        Returns tables that are:
        - Public
        - Owned by the user
        - Shared with user's organizations
        """
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        tables = BigQueryTable.objects.filter(
            Q(visibility=Visibility.PUBLIC) |
            Q(owner=user) |
            Q(organization_id__in=org_ids)
        ).select_related(
            'owner', 'organization', 'gcp_credential'
        ).annotate(
            dashboard_count=Count('dashboards'),
            rollup_count=Count('rollups')
        ).distinct().order_by('-last_used_at', '-created_at')

        # Group by category
        result = {
            'my_tables': [],
            'organization_tables': [],
            'public_tables': []
        }

        for table in tables:
            table_data = BigQueryTableSerializer(table).data
            table_data['dashboard_count'] = table.dashboard_count
            table_data['rollup_count'] = table.rollup_count

            if table.owner == user:
                result['my_tables'].append(table_data)
            elif table.organization_id in org_ids:
                result['organization_tables'].append(table_data)
            else:
                result['public_tables'].append(table_data)

        return Response(result)


class LibrarySearchView(APIView):
    """Search tables in the library."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Search tables by name or description.

        Query params:
        - q: Search query
        - visibility: Filter by visibility (public, private, organization)
        - owner: Filter by owner email
        - limit: Max results (default 50)
        """
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        query = request.query_params.get('q', '')
        visibility = request.query_params.get('visibility')
        owner_email = request.query_params.get('owner')
        limit = int(request.query_params.get('limit', 50))

        # Base queryset: tables user has access to
        tables = BigQueryTable.objects.filter(
            Q(visibility=Visibility.PUBLIC) |
            Q(owner=user) |
            Q(organization_id__in=org_ids)
        )

        # Apply search filter
        if query:
            tables = tables.filter(
                Q(name__icontains=query) |
                Q(project_id__icontains=query) |
                Q(dataset__icontains=query) |
                Q(table_name__icontains=query)
            )

        # Apply visibility filter
        if visibility:
            tables = tables.filter(visibility=visibility)

        # Apply owner filter
        if owner_email:
            tables = tables.filter(owner__email__icontains=owner_email)

        # Limit results
        tables = tables.select_related(
            'owner', 'organization'
        ).distinct()[:limit]

        serializer = BigQueryTableSerializer(tables, many=True)
        return Response(serializer.data)


class LibraryStatsView(APIView):
    """Get library statistics."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get statistics about the table library."""
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        # Count tables by category
        my_tables = BigQueryTable.objects.filter(owner=user).count()
        org_tables = BigQueryTable.objects.filter(
            organization_id__in=org_ids
        ).exclude(owner=user).count()
        public_tables = BigQueryTable.objects.filter(
            visibility=Visibility.PUBLIC
        ).exclude(owner=user).exclude(organization_id__in=org_ids).count()

        return Response({
            'my_tables': my_tables,
            'organization_tables': org_tables,
            'public_tables': public_tables,
            'total_accessible': my_tables + org_tables + public_tables
        })
