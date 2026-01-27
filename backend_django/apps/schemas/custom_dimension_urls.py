"""
Custom dimension URLs at root level (/api/custom-dimensions/).
FastAPI compatibility - provides root-level custom dimension endpoints.
"""
from django.urls import path
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status

from .models import CustomDimension
from .serializers import (
    CustomDimensionSerializer,
    CustomDimensionCreateSerializer,
    CustomDimensionUpdateSerializer
)


class RootCustomDimensionListView(APIView):
    """List all custom dimensions for the current user's tables."""
    permission_classes = []

    def get(self, request):
        """List all custom dimensions the user has access to."""
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        # Get custom dimensions from all tables the user owns or has org access to
        custom_dims = CustomDimension.objects.filter(
            schema_config__bigquery_table__owner=user
        ) | CustomDimension.objects.filter(
            schema_config__bigquery_table__organization_id__in=org_ids
        )

        serializer = CustomDimensionSerializer(custom_dims.order_by('name'), many=True)
        return Response(serializer.data)

    def post(self, request):
        """Create a new custom dimension."""
        serializer = CustomDimensionCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        custom_dim = serializer.save()
        return Response(
            CustomDimensionSerializer(custom_dim).data,
            status=status.HTTP_201_CREATED
        )


class RootCustomDimensionDetailView(APIView):
    """Get/update/delete a custom dimension by ID."""
    permission_classes = []

    def get_object(self, request, dimension_id):
        """Get custom dimension with permission check."""
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        try:
            return CustomDimension.objects.get(
                id=dimension_id,
                schema_config__bigquery_table__owner=user
            )
        except CustomDimension.DoesNotExist:
            try:
                return CustomDimension.objects.get(
                    id=dimension_id,
                    schema_config__bigquery_table__organization_id__in=org_ids
                )
            except CustomDimension.DoesNotExist:
                return None

    def get(self, request, dimension_id):
        """Get a custom dimension by ID."""
        custom_dim = self.get_object(request, dimension_id)
        if not custom_dim:
            return Response(
                {'error': 'Custom dimension not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response(CustomDimensionSerializer(custom_dim).data)

    def put(self, request, dimension_id):
        """Update a custom dimension."""
        custom_dim = self.get_object(request, dimension_id)
        if not custom_dim:
            return Response(
                {'error': 'Custom dimension not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = CustomDimensionUpdateSerializer(custom_dim, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(CustomDimensionSerializer(custom_dim).data)

    def delete(self, request, dimension_id):
        """Delete a custom dimension."""
        custom_dim = self.get_object(request, dimension_id)
        if not custom_dim:
            return Response(
                {'error': 'Custom dimension not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        custom_dim.delete()
        return Response({'success': True, 'message': 'Custom dimension deleted'})


class RootCustomDimensionDuplicateView(APIView):
    """Duplicate a custom dimension."""
    permission_classes = []

    def post(self, request, dimension_id):
        """Duplicate a custom dimension."""
        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        try:
            custom_dim = CustomDimension.objects.get(
                id=dimension_id,
                schema_config__bigquery_table__owner=user
            )
        except CustomDimension.DoesNotExist:
            try:
                custom_dim = CustomDimension.objects.get(
                    id=dimension_id,
                    schema_config__bigquery_table__organization_id__in=org_ids
                )
            except CustomDimension.DoesNotExist:
                return Response(
                    {'error': 'Custom dimension not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        # Create a copy
        new_custom_dim = CustomDimension.objects.create(
            schema_config=custom_dim.schema_config,
            name=f"{custom_dim.name} (Copy)",
            dimension_type=custom_dim.dimension_type,
            metric=custom_dim.metric,
            values_json=custom_dim.values_json.copy() if custom_dim.values_json else []
        )

        return Response(
            CustomDimensionSerializer(new_custom_dim).data,
            status=status.HTTP_201_CREATED
        )


urlpatterns = [
    path('', RootCustomDimensionListView.as_view(), name='custom-dimensions-root'),
    path('<uuid:dimension_id>/', RootCustomDimensionDetailView.as_view(), name='custom-dimension-detail-root'),
    path('<uuid:dimension_id>/duplicate/', RootCustomDimensionDuplicateView.as_view(), name='custom-dimension-duplicate-root'),
]
