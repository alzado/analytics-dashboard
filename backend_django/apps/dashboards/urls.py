"""
URL configuration for dashboards app.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DashboardViewSet, DashboardWidgetViewSet

router = DefaultRouter()
router.register(r'', DashboardViewSet, basename='dashboard')

urlpatterns = [
    # Dashboard CRUD
    path('', include(router.urls)),

    # Widget management within dashboard
    path(
        '<uuid:dashboard_id>/widgets/',
        DashboardWidgetViewSet.as_view({'post': 'create'}),
        name='dashboard-widgets-create'
    ),
    path(
        '<uuid:dashboard_id>/widgets/<uuid:widget_id>/',
        DashboardWidgetViewSet.as_view({
            'put': 'update',
            'patch': 'update',
            'delete': 'destroy'
        }),
        name='dashboard-widgets-detail'
    ),
]
