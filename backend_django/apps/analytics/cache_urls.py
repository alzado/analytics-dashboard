"""
Cache endpoint URLs at root level (/api/cache/).
FastAPI compatibility alias for /api/analytics/cache/
"""
from django.urls import path
from .views import (
    CacheStatsView,
    CacheClearView,
    CacheClearByTableView,
    CacheClearByTypeView
)

urlpatterns = [
    path('stats/', CacheStatsView.as_view(), name='cache-stats-root'),
    path('clear/', CacheClearView.as_view(), name='cache-clear-root'),
    path('clear/table/<str:table_id>/', CacheClearByTableView.as_view(), name='cache-clear-table-root'),
    path('clear/type/<str:query_type>/', CacheClearByTypeView.as_view(), name='cache-clear-type-root'),
]
