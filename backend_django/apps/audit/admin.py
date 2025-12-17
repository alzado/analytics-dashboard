"""
Admin configuration for audit app.
"""
from django.contrib import admin
from .models import QueryLog, CacheEntry


@admin.register(QueryLog)
class QueryLogAdmin(admin.ModelAdmin):
    """Admin for QueryLog model."""

    list_display = ('query_type', 'endpoint', 'bigquery_table', 'user', 'execution_time_ms', 'is_success', 'cache_hit', 'created_at')
    list_filter = ('query_type', 'is_success', 'cache_hit', 'created_at')
    search_fields = ('endpoint', 'sql_query', 'bigquery_table__name', 'user__email')
    autocomplete_fields = ['bigquery_table', 'user']
    readonly_fields = ('id', 'created_at', 'query_type', 'endpoint', 'sql_query', 'filters',
                       'execution_time_ms', 'bytes_processed', 'bytes_billed', 'row_count',
                       'is_success', 'error', 'cache_hit')
    date_hierarchy = 'created_at'

    fieldsets = (
        (None, {'fields': ('id', 'query_type', 'endpoint')}),
        ('Context', {'fields': ('bigquery_table', 'user')}),
        ('Query', {'fields': ('sql_query', 'filters')}),
        ('Metrics', {'fields': ('execution_time_ms', 'bytes_processed', 'bytes_billed', 'row_count')}),
        ('Status', {'fields': ('is_success', 'error', 'cache_hit')}),
        ('Timestamps', {'fields': ('created_at',)}),
    )

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(CacheEntry)
class CacheEntryAdmin(admin.ModelAdmin):
    """Admin for CacheEntry model."""

    list_display = ('cache_key_short', 'query_type', 'bigquery_table', 'row_count', 'hit_count', 'expires_at', 'is_expired_display')
    list_filter = ('query_type', 'created_at')
    search_fields = ('cache_key', 'bigquery_table__name')
    autocomplete_fields = ['bigquery_table']
    readonly_fields = ('id', 'cache_key', 'created_at', 'last_accessed_at', 'hit_count')
    date_hierarchy = 'created_at'

    fieldsets = (
        (None, {'fields': ('id', 'cache_key', 'query_type')}),
        ('Context', {'fields': ('bigquery_table',)}),
        ('Query', {'fields': ('sql_query',)}),
        ('Data', {'fields': ('result_data', 'row_count')}),
        ('Usage', {'fields': ('hit_count', 'last_accessed_at')}),
        ('Expiration', {'fields': ('created_at', 'expires_at')}),
    )

    @admin.display(description='Cache Key')
    def cache_key_short(self, obj):
        return f"{obj.cache_key[:16]}..."

    @admin.display(boolean=True, description='Expired')
    def is_expired_display(self, obj):
        return obj.is_expired()
