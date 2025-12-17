"""
Audit models for query logging and usage tracking.
"""
import uuid
from django.db import models
from django.utils import timezone


class QueryLog(models.Model):
    """Log of BigQuery queries executed."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Reference to the table
    bigquery_table = models.ForeignKey(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='query_logs',
        null=True,
        blank=True
    )

    # User who executed the query (for per-user OAuth tracking)
    user = models.ForeignKey(
        'users.User',
        on_delete=models.SET_NULL,
        related_name='query_logs',
        null=True,
        blank=True
    )

    # Query metadata
    query_type = models.CharField(max_length=50)  # pivot, count, dimension_values, etc.
    endpoint = models.CharField(max_length=255)
    sql_query = models.TextField()
    filters = models.JSONField(default=dict, blank=True)

    # Execution metrics
    execution_time_ms = models.IntegerField(default=0)
    bytes_processed = models.BigIntegerField(default=0)
    bytes_billed = models.BigIntegerField(default=0)
    row_count = models.IntegerField(default=0)

    # Status
    is_success = models.BooleanField(default=True)
    error = models.TextField(null=True, blank=True)

    # Cache info
    cache_hit = models.BooleanField(default=False)

    # Timestamps
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['-created_at']),
            models.Index(fields=['query_type']),
            models.Index(fields=['bigquery_table', '-created_at']),
            models.Index(fields=['user', '-created_at']),
        ]

    def __str__(self):
        return f"{self.query_type} at {self.created_at}"


class CacheEntry(models.Model):
    """Cache entry for BigQuery query results."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Cache key (hash of SQL query)
    cache_key = models.CharField(max_length=64, unique=True, db_index=True)

    # Metadata
    bigquery_table = models.ForeignKey(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='cache_entries',
        null=True,
        blank=True
    )
    query_type = models.CharField(max_length=50)
    sql_query = models.TextField()

    # Cached data
    result_data = models.JSONField()
    row_count = models.IntegerField(default=0)

    # Timestamps
    created_at = models.DateTimeField(default=timezone.now)
    expires_at = models.DateTimeField()
    last_accessed_at = models.DateTimeField(default=timezone.now)

    # Usage tracking
    hit_count = models.IntegerField(default=0)

    class Meta:
        ordering = ['-last_accessed_at']
        indexes = [
            models.Index(fields=['cache_key']),
            models.Index(fields=['expires_at']),
            models.Index(fields=['bigquery_table', 'query_type']),
        ]

    def __str__(self):
        return f"Cache: {self.cache_key[:16]}..."

    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return timezone.now() > self.expires_at
