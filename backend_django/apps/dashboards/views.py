"""
Dashboard views.
"""
from django.db.models import Count, Q
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from .models import Dashboard, Widget
from .serializers import (
    DashboardSerializer,
    DashboardCreateSerializer,
    DashboardUpdateSerializer,
    DashboardListSerializer,
    WidgetSerializer,
    WidgetCreateSerializer,
    WidgetUpdateSerializer,
    StandaloneWidgetCreateSerializer
)


class DashboardViewSet(viewsets.ModelViewSet):
    """ViewSet for dashboards."""
    permission_classes = []  # Temporarily disabled for testing
    lookup_field = 'id'

    def get_queryset(self):
        """Return dashboards the user has access to."""
        user = self.request.user
        # If no authenticated user, return all dashboards (for testing)
        if not user or not user.is_authenticated:
            return Dashboard.objects.all().annotate(
                widget_count=Count('widgets')
            ).select_related('owner').distinct()

        org_ids = user.memberships.values_list('organization_id', flat=True)

        return Dashboard.objects.filter(
            Q(owner=user) | Q(organization_id__in=org_ids)
        ).annotate(
            widget_count=Count('widgets')
        ).select_related('owner').distinct()

    def get_serializer_class(self):
        if self.action == 'list':
            return DashboardListSerializer
        elif self.action == 'create':
            return DashboardCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return DashboardUpdateSerializer
        return DashboardSerializer

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)

    @action(detail=True, methods=['post'])
    def widgets(self, request, id=None):
        """Add a widget to the dashboard."""
        dashboard = self.get_object()
        serializer = WidgetCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save(dashboard=dashboard)

        # Return updated dashboard
        dashboard_serializer = DashboardSerializer(dashboard)
        return Response(dashboard_serializer.data, status=status.HTTP_201_CREATED)


class WidgetViewSet(viewsets.ModelViewSet):
    """ViewSet for widgets within a dashboard."""
    permission_classes = []
    lookup_field = 'id'

    def get_queryset(self):
        """Return widgets from dashboards the user has access to."""
        user = self.request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        return Widget.objects.filter(
            Q(dashboard__owner=user) | Q(dashboard__organization_id__in=org_ids)
        ).select_related('dashboard', 'table').distinct()

    def get_serializer_class(self):
        if self.action == 'create':
            return WidgetCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return WidgetUpdateSerializer
        return WidgetSerializer

    def perform_create(self, serializer):
        dashboard_id = self.request.data.get('dashboard_id')
        if not dashboard_id:
            raise ValueError("dashboard_id is required")

        dashboard = Dashboard.objects.get(id=dashboard_id)
        serializer.save(dashboard=dashboard)


class DashboardWidgetViewSet(viewsets.ViewSet):
    """Nested ViewSet for managing widgets within a specific dashboard."""
    permission_classes = []

    def get_dashboard(self, dashboard_id):
        """Get dashboard with permission check."""
        user = self.request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        return Dashboard.objects.filter(
            Q(owner=user) | Q(organization_id__in=org_ids),
            id=dashboard_id
        ).first()

    def create(self, request, dashboard_id=None):
        """Create a widget in the dashboard."""
        dashboard = self.get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                {'error': 'Dashboard not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = WidgetCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save(dashboard=dashboard)

        # Return updated dashboard
        dashboard_serializer = DashboardSerializer(dashboard)
        return Response(dashboard_serializer.data, status=status.HTTP_201_CREATED)

    def update(self, request, dashboard_id=None, widget_id=None):
        """Update a widget in the dashboard."""
        dashboard = self.get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                {'error': 'Dashboard not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        try:
            widget = dashboard.widgets.get(id=widget_id)
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = WidgetUpdateSerializer(widget, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        # Return updated dashboard
        dashboard_serializer = DashboardSerializer(dashboard)
        return Response(dashboard_serializer.data)

    def destroy(self, request, dashboard_id=None, widget_id=None):
        """Delete a widget from the dashboard."""
        dashboard = self.get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                {'error': 'Dashboard not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        try:
            widget = dashboard.widgets.get(id=widget_id)
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        widget.delete()

        # Return updated dashboard
        dashboard_serializer = DashboardSerializer(dashboard)
        return Response(dashboard_serializer.data)


class StandaloneWidgetViewSet(viewsets.ViewSet):
    """ViewSet for standalone widgets (not attached to a dashboard)."""
    permission_classes = []

    def list(self, request):
        """List standalone widgets owned by the user."""
        include_drafts = request.query_params.get('include_drafts', 'true').lower() == 'true'

        # Get standalone widgets (no dashboard) owned by the user
        queryset = Widget.objects.filter(
            owner=request.user,
            dashboard__isnull=True
        )

        if not include_drafts:
            queryset = queryset.filter(is_draft=False)

        serializer = WidgetSerializer(queryset, many=True)
        return Response(serializer.data)

    def retrieve(self, request, widget_id=None):
        """Get a specific widget by ID."""
        # Can retrieve any widget the user owns (standalone or dashboard-attached)
        try:
            widget = Widget.objects.get(
                Q(owner=request.user) | Q(dashboard__owner=request.user),
                id=widget_id
            )
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = WidgetSerializer(widget)
        return Response(serializer.data)

    def create(self, request):
        """Create a new standalone widget."""
        serializer = StandaloneWidgetCreateSerializer(
            data=request.data,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        widget = serializer.save()

        return Response(
            WidgetSerializer(widget).data,
            status=status.HTTP_201_CREATED
        )

    def destroy(self, request, widget_id=None):
        """Delete a standalone widget."""
        try:
            widget = Widget.objects.get(
                owner=request.user,
                dashboard__isnull=True,
                id=widget_id
            )
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        widget.delete()
        return Response({'success': True, 'message': 'Widget deleted'})

    @action(detail=True, methods=['post'])
    def attach(self, request, widget_id=None):
        """Attach a standalone widget to a dashboard."""
        dashboard_id = request.query_params.get('dashboard_id')
        if not dashboard_id:
            return Response(
                {'error': 'dashboard_id query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            widget = Widget.objects.get(
                owner=request.user,
                dashboard__isnull=True,
                id=widget_id
            )
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        try:
            dashboard = Dashboard.objects.get(
                Q(owner=request.user) | Q(organization_id__in=request.user.memberships.values_list('organization_id', flat=True)),
                id=dashboard_id
            )
        except Dashboard.DoesNotExist:
            return Response(
                {'error': 'Dashboard not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Attach widget to dashboard
        widget.dashboard = dashboard
        widget.is_draft = False
        widget.save()

        return Response(DashboardSerializer(dashboard).data)

    @action(detail=True, methods=['post'])
    def clone(self, request, widget_id=None):
        """Clone a widget as a new standalone widget."""
        try:
            widget = Widget.objects.get(
                Q(owner=request.user) | Q(dashboard__owner=request.user),
                id=widget_id
            )
        except Widget.DoesNotExist:
            return Response(
                {'error': 'Widget not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Create a new widget with same config
        new_widget = Widget.objects.create(
            owner=request.user,
            name=f"{widget.name} (Copy)",
            widget_type=widget.widget_type,
            table=widget.table,
            config=widget.config.copy() if widget.config else {},
            position=widget.position.copy() if widget.position else {},
            sort_order=widget.sort_order,
            is_draft=True
        )

        return Response(
            WidgetSerializer(new_widget).data,
            status=status.HTTP_201_CREATED
        )
