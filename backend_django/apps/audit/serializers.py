"""
Serializers for audit API.
"""
from rest_framework import serializers
from .models import QueryLog, CacheEntry


class QueryLogSerializer(serializers.ModelSerializer):
    """Serializer for query log entries."""
    table_name = serializers.CharField(
        source='bigquery_table.name',
        read_only=True,
        allow_null=True
    )

    class Meta:
        model = QueryLog
        fields = [
            'id', 'query_type', 'endpoint', 'sql_query', 'filters',
            'execution_time_ms', 'bytes_processed', 'bytes_billed',
            'row_count', 'is_success', 'error', 'cache_hit',
            'created_at', 'table_name'
        ]
        read_only_fields = fields


class QueryLogResponseSerializer(serializers.Serializer):
    """Serializer for paginated query log response."""
    logs = QueryLogSerializer(many=True)
    total = serializers.IntegerField()
    limit = serializers.IntegerField()
    offset = serializers.IntegerField()


class UsageStatsSerializer(serializers.Serializer):
    """Serializer for usage statistics."""
    total_queries = serializers.IntegerField()
    total_bytes_processed = serializers.IntegerField()
    total_bytes_billed = serializers.IntegerField()
    avg_execution_time_ms = serializers.FloatField()
    cache_hit_rate = serializers.FloatField()
    queries_by_type = serializers.DictField(child=serializers.IntegerField())
    error_count = serializers.IntegerField()


class UsageTimeSeriesSerializer(serializers.Serializer):
    """Serializer for usage time series data."""
    period = serializers.CharField()
    query_count = serializers.IntegerField()
    bytes_processed = serializers.IntegerField()
    bytes_billed = serializers.IntegerField()
    avg_execution_time_ms = serializers.FloatField()


class CacheStatsSerializer(serializers.Serializer):
    """Serializer for cache statistics."""
    total_entries = serializers.IntegerField()
    total_hits = serializers.IntegerField()
    expired_entries = serializers.IntegerField()
    entries_by_type = serializers.DictField(child=serializers.IntegerField())


class CacheClearResponseSerializer(serializers.Serializer):
    """Serializer for cache clear response."""
    success = serializers.BooleanField()
    message = serializers.CharField()
    entries_deleted = serializers.IntegerField()


class ClearLogsResponseSerializer(serializers.Serializer):
    """Serializer for clear logs response."""
    success = serializers.BooleanField()
    message = serializers.CharField()
    logs_deleted = serializers.IntegerField()
