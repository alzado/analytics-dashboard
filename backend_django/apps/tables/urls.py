"""
BigQuery Table URLs.

Includes nested schema URLs:
/api/tables/{table_id}/schema/
/api/tables/{table_id}/metrics/
/api/tables/{table_id}/dimensions/
/api/tables/{table_id}/calculated-dimensions/
/api/tables/{table_id}/custom-dimensions/
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register('', views.BigQueryTableViewSet, basename='table')

# Import schema URLs for nesting under tables
from apps.schemas.urls import table_schema_urlpatterns

urlpatterns = [
    # Schema utility routes at /api/tables/schema/* (must come before router)
    path('schema/', include('apps.tables.schema_utils_urls')),

    path('', include(router.urls)),
    # Nested schema routes under each table
    path('<uuid:table_id>/', include(table_schema_urlpatterns)),
]
