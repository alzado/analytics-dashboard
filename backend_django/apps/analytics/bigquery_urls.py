"""
BigQuery compatibility URLs.

These routes provide backward compatibility with the FastAPI frontend
by mapping /api/bigquery/* endpoints to the Django views.
"""
from django.urls import path
from . import views

urlpatterns = [
    # /api/bigquery/info - Get BigQuery connection info for a table
    path('info/', views.BigQueryInfoView.as_view(), name='bigquery-info'),

    # /api/bigquery/tables - List tables in the BigQuery project/dataset
    path('tables/', views.BigQueryTablesListView.as_view(), name='bigquery-tables'),

    # /api/bigquery/tables/_/dates - Get date range for a table
    path('tables/_/dates/', views.BigQueryTableDatesView.as_view(), name='bigquery-table-dates'),

    # /api/bigquery/configure - Configure BigQuery connection (create/update table)
    path('configure/', views.BigQueryConfigureView.as_view(), name='bigquery-configure'),

    # /api/bigquery/disconnect - Disconnect from BigQuery (delete table config)
    path('disconnect/', views.BigQueryDisconnectView.as_view(), name='bigquery-disconnect'),

    # /api/bigquery/cancel - Cancel running queries
    path('cancel/', views.BigQueryCancelView.as_view(), name='bigquery-cancel'),

    # /api/bigquery/logs - Query logs
    path('logs/', views.BigQueryLogsView.as_view(), name='bigquery-logs'),
    path('logs/clear/', views.BigQueryLogsClearView.as_view(), name='bigquery-logs-clear'),

    # /api/bigquery/usage - Usage statistics
    path('usage/stats/', views.BigQueryUsageStatsView.as_view(), name='bigquery-usage-stats'),
    path('usage/stats/today/', views.BigQueryUsageTodayView.as_view(), name='bigquery-usage-today'),
    path('usage/timeseries/', views.BigQueryUsageTimeSeriesView.as_view(), name='bigquery-usage-timeseries'),
]
