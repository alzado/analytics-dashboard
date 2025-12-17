"""
URL configuration for audit app.
"""
from django.urls import path
from .views import (
    QueryLogListView,
    QueryLogClearView,
    UsageStatsView,
    TodayUsageStatsView,
    UsageTimeSeriesView,
    CacheStatsView,
    CacheClearView,
    CacheClearByTableView,
    CacheClearByTypeView
)

urlpatterns = [
    # Query logging endpoints
    path('bigquery/logs/', QueryLogListView.as_view(), name='query-logs'),
    path('bigquery/logs/clear/', QueryLogClearView.as_view(), name='query-logs-clear'),

    # Usage statistics endpoints
    path('bigquery/usage/stats/', UsageStatsView.as_view(), name='usage-stats'),
    path('bigquery/usage/stats/today/', TodayUsageStatsView.as_view(), name='today-usage-stats'),
    path('bigquery/usage/timeseries/', UsageTimeSeriesView.as_view(), name='usage-timeseries'),

    # Cache management endpoints
    path('cache/stats/', CacheStatsView.as_view(), name='cache-stats'),
    path('cache/clear/', CacheClearView.as_view(), name='cache-clear'),
    path('cache/clear/table/<uuid:table_id>/', CacheClearByTableView.as_view(), name='cache-clear-table'),
    path('cache/clear/type/<str:query_type>/', CacheClearByTypeView.as_view(), name='cache-clear-type'),
]
