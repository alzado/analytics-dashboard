"""
Custom metric URLs at root level (/api/custom-metrics/).
Provides root-level custom metric endpoints for easier frontend access.
"""
from django.urls import path
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .models import CustomMetric
from .serializers import CustomMetricSerializer, CustomMetricCreateSerializer, CustomMetricUpdateSerializer


class RootCustomMetricListView(APIView):
    """List all custom metrics for the current user's tables."""
    permission_classes = []

    def get(self, request):
        """List all custom metrics the user has access to."""
        user = request.user

        # Handle anonymous users
        if not user.is_authenticated:
            custom_metrics = CustomMetric.objects.all().order_by('name')
        else:
            org_ids = user.memberships.values_list('organization_id', flat=True)
            # Get custom metrics from all tables the user owns or has org access to
            custom_metrics = CustomMetric.objects.filter(
                schema_config__bigquery_table__owner=user
            ) | CustomMetric.objects.filter(
                schema_config__bigquery_table__organization_id__in=org_ids
            )
            custom_metrics = custom_metrics.order_by('name')

        serializer = CustomMetricSerializer(custom_metrics, many=True)
        return Response(serializer.data)

    def post(self, request):
        """Create a new custom metric."""
        serializer = CustomMetricCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        custom_metric = serializer.save()
        return Response(
            CustomMetricSerializer(custom_metric).data,
            status=status.HTTP_201_CREATED
        )


class RootCustomMetricDetailView(APIView):
    """Get/update/delete a custom metric by ID."""
    permission_classes = []

    def get_object(self, request, metric_id):
        """Get custom metric with permission check."""
        user = request.user

        # Handle anonymous users
        if not user.is_authenticated:
            try:
                return CustomMetric.objects.get(id=metric_id)
            except CustomMetric.DoesNotExist:
                return None

        org_ids = user.memberships.values_list('organization_id', flat=True)

        try:
            return CustomMetric.objects.get(
                id=metric_id,
                schema_config__bigquery_table__owner=user
            )
        except CustomMetric.DoesNotExist:
            try:
                return CustomMetric.objects.get(
                    id=metric_id,
                    schema_config__bigquery_table__organization_id__in=org_ids
                )
            except CustomMetric.DoesNotExist:
                return None

    def get(self, request, metric_id):
        """Get a custom metric by ID."""
        custom_metric = self.get_object(request, metric_id)
        if not custom_metric:
            return Response(
                {'error': 'Custom metric not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response(CustomMetricSerializer(custom_metric).data)

    def put(self, request, metric_id):
        """Update a custom metric."""
        custom_metric = self.get_object(request, metric_id)
        if not custom_metric:
            return Response(
                {'error': 'Custom metric not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = CustomMetricUpdateSerializer(
            custom_metric,
            data=request.data,
            partial=True
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(CustomMetricSerializer(custom_metric).data)

    def delete(self, request, metric_id):
        """Delete a custom metric."""
        custom_metric = self.get_object(request, metric_id)
        if not custom_metric:
            return Response(
                {'error': 'Custom metric not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        custom_metric.delete()
        return Response(
            {'success': True, 'message': 'Custom metric deleted'},
            status=status.HTTP_200_OK
        )


urlpatterns = [
    path('', RootCustomMetricListView.as_view(), name='custom-metrics-root'),
    path('<uuid:metric_id>/', RootCustomMetricDetailView.as_view(), name='custom-metric-detail'),
]
