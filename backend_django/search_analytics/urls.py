"""
URL configuration for search_analytics project.
"""
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),

    # API endpoints
    path('api/', include('apps.core.urls')),
    path('api/auth/', include('apps.users.urls')),
    path('api/organizations/', include('apps.organizations.urls')),
    path('api/credentials/', include('apps.credentials.urls')),
    path('api/tables/', include('apps.tables.urls')),
    path('api/schemas/', include('apps.schemas.urls')),
    path('api/dashboards/', include('apps.dashboards.urls')),
    path('api/library/', include('apps.library.urls')),
    path('api/rollups/', include('apps.rollups.urls')),
    path('api/analytics/', include('apps.analytics.urls')),
    path('api/audit/', include('apps.audit.urls')),

    # BigQuery compatibility routes (for FastAPI frontend compatibility)
    path('api/bigquery/', include('apps.analytics.bigquery_urls')),

    # Optimized source routes (for FastAPI frontend compatibility)
    path('api/optimized-source/', include('apps.analytics.optimized_source_urls')),

    # Pivot endpoint at root /api/pivot/ (FastAPI compatibility)
    path('api/', include('apps.analytics.pivot_urls')),

    # Standalone widgets at /api/widgets/ (FastAPI compatibility)
    path('api/widgets/', include('apps.dashboards.widget_urls')),

    # Root-level endpoint aliases (FastAPI compatibility)
    path('api/significance/', include('apps.analytics.significance_urls')),
    path('api/cache/', include('apps.analytics.cache_urls')),
    path('api/custom-dimensions/', include('apps.schemas.custom_dimension_urls')),
    path('api/custom-metrics/', include('apps.schemas.custom_metric_urls')),
]

# Serve static files in development mode
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
