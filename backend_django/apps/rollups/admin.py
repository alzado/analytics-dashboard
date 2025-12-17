"""
Admin configuration for rollups app.
"""
from django.contrib import admin
from .models import Rollup, RollupConfig


@admin.register(Rollup)
class RollupAdmin(admin.ModelAdmin):
    """Admin for Rollup model."""

    list_display = ('name', 'rollup_id', 'bigquery_table', 'status', 'is_searchable', 'row_count', 'last_refresh_at')
    list_filter = ('status', 'is_searchable', 'created_at')
    search_fields = ('name', 'rollup_id', 'bigquery_table__name', 'rollup_table')
    autocomplete_fields = ['bigquery_table']
    readonly_fields = ('id', 'created_at', 'updated_at', 'last_refresh_at', 'row_count', 'size_bytes', 'refresh_duration_seconds')
    date_hierarchy = 'created_at'

    fieldsets = (
        (None, {'fields': ('id', 'bigquery_table', 'name', 'rollup_id')}),
        ('BigQuery Location', {'fields': ('rollup_project', 'rollup_dataset', 'rollup_table')}),
        ('Configuration', {'fields': ('dimensions', 'metrics', 'is_searchable')}),
        ('Status', {'fields': ('status', 'error_message')}),
        ('Statistics', {'fields': ('row_count', 'size_bytes', 'refresh_duration_seconds', 'last_refresh_at')}),
        ('Date Range', {'fields': ('min_date', 'max_date')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


@admin.register(RollupConfig)
class RollupConfigAdmin(admin.ModelAdmin):
    """Admin for RollupConfig model."""

    list_display = ('bigquery_table', 'default_project', 'default_dataset', 'auto_refresh_enabled', 'updated_at')
    list_filter = ('auto_refresh_enabled', 'created_at')
    search_fields = ('bigquery_table__name', 'default_project', 'default_dataset')
    autocomplete_fields = ['bigquery_table']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'bigquery_table')}),
        ('Defaults', {'fields': ('default_project', 'default_dataset')}),
        ('Auto-Refresh', {'fields': ('auto_refresh_enabled', 'refresh_schedule_cron')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )
