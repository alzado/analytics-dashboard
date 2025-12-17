"""
URL configuration for analytics app.
"""
from django.urls import path
from .views import (
    PivotView,
    PivotChildrenView,
    DimensionValuesView,
    TableInfoView,
    SignificanceView,
    CacheStatsView,
    CacheClearView,
    CacheClearByTableView,
    CacheClearByTypeView,
    OverviewView,
    TrendsView,
    BreakdownView,
    SearchTermsView,
    FilterOptionsView,
    DatePresetsView,
)

urlpatterns = [
    # Core analytics endpoints (matching FastAPI /api/*)
    path('overview/', OverviewView.as_view(), name='overview'),
    path('trends/', TrendsView.as_view(), name='trends'),
    path('breakdown/<str:dimension>/', BreakdownView.as_view(), name='breakdown'),
    path('search-terms/', SearchTermsView.as_view(), name='search-terms'),
    path('filters/options/', FilterOptionsView.as_view(), name='filter-options'),
    path('date-presets/', DatePresetsView.as_view(), name='date-presets'),

    # Pivot table endpoints
    path('pivot/', PivotView.as_view(), name='pivot'),
    path('pivot/children/', PivotChildrenView.as_view(), name='pivot-children-all'),
    path(
        'pivot/<str:dimension>/<str:value>/children/',
        PivotChildrenView.as_view(),
        name='pivot-children'
    ),

    # Dimension values endpoint
    path(
        'pivot/dimension/<str:dimension>/values/',
        DimensionValuesView.as_view(),
        name='dimension-values'
    ),

    # Table info endpoint
    path('info/', TableInfoView.as_view(), name='table-info'),

    # Statistical significance endpoint
    path('significance/', SignificanceView.as_view(), name='significance'),

    # Cache management endpoints
    path('cache/stats/', CacheStatsView.as_view(), name='cache-stats'),
    path('cache/clear/', CacheClearView.as_view(), name='cache-clear'),
    path('cache/clear/table/<str:table_id>/', CacheClearByTableView.as_view(), name='cache-clear-table'),
    path('cache/clear/type/<str:query_type>/', CacheClearByTypeView.as_view(), name='cache-clear-type'),
]
