"""
Schema URL configuration.

The schema endpoints are nested under tables:
/api/tables/{table_id}/schema/
/api/tables/{table_id}/metrics/
/api/tables/{table_id}/dimensions/
/api/tables/{table_id}/calculated-dimensions/
/api/tables/{table_id}/custom-dimensions/
/api/tables/{table_id}/custom-metrics/
/api/tables/{table_id}/joined-dimensions/
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    SchemaConfigViewSet,
    CalculatedMetricViewSet,
    DimensionViewSet,
    CalculatedDimensionViewSet,
    CustomDimensionViewSet,
    CustomMetricViewSet,
    JoinedDimensionSourceViewSet
)


# Create a router for table-nested routes
table_router = DefaultRouter()
table_router.register(r'metrics', CalculatedMetricViewSet, basename='metric')
table_router.register(r'dimensions', DimensionViewSet, basename='dimension')
table_router.register(
    r'calculated-dimensions',
    CalculatedDimensionViewSet,
    basename='calculated-dimension'
)
table_router.register(
    r'custom-dimensions',
    CustomDimensionViewSet,
    basename='custom-dimension'
)
table_router.register(
    r'custom-metrics',
    CustomMetricViewSet,
    basename='custom-metric'
)
table_router.register(
    r'joined-dimensions',
    JoinedDimensionSourceViewSet,
    basename='joined-dimension'
)


# URL patterns for schema operations on a specific table
# These will be included in the tables app urls
table_schema_urlpatterns = [
    # Schema configuration
    path(
        'schema/',
        SchemaConfigViewSet.as_view({
            'get': 'retrieve',
            'put': 'update',
            'patch': 'partial_update'
        }),
        name='schema-detail'
    ),
    path(
        'schema/detect/',
        SchemaConfigViewSet.as_view({'post': 'detect'}),
        name='schema-detect'
    ),
    path(
        'schema/reset/',
        SchemaConfigViewSet.as_view({'post': 'reset'}),
        name='schema-reset'
    ),
    path(
        'schema/clear/',
        SchemaConfigViewSet.as_view({'post': 'clear'}),
        name='schema-clear'
    ),
    path(
        'schema/copy/',
        SchemaConfigViewSet.as_view({'post': 'copy'}),
        name='schema-copy'
    ),

    # Include router URLs for metrics/dimensions
    path('', include(table_router.urls)),
]


# Standalone URL patterns for direct access (e.g., /api/schemas/)
app_name = 'schemas'

urlpatterns = [
    # List all schemas (for admin/debugging)
    path(
        '',
        SchemaConfigViewSet.as_view({'get': 'list'}),
        name='schema-list'
    ),
]
