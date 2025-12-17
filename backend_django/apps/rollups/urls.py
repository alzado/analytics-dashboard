"""
URL configuration for rollups app.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    RollupViewSet,
    RefreshAllRollupsView,
    RollupConfigView,
    DefaultProjectView,
    DefaultDatasetView
)

router = DefaultRouter()
router.register(r'', RollupViewSet, basename='rollup')

urlpatterns = [
    # Non-router endpoints MUST come before router to avoid being caught by viewset
    # Refresh all rollups for a table
    path('refresh-all/', RefreshAllRollupsView.as_view(), name='refresh-all-rollups'),

    # Configuration endpoints
    path('config/', RollupConfigView.as_view(), name='rollup-config'),
    path('config/default-project/', DefaultProjectView.as_view(), name='rollup-default-project'),
    path('config/default-dataset/', DefaultDatasetView.as_view(), name='rollup-default-dataset'),

    # Rollup CRUD via router (must be last)
    path('', include(router.urls)),
]
