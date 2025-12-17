"""
Rollup models for pre-aggregation tables.
"""
import uuid
from django.db import models
from django.utils import timezone


class RollupStatus(models.TextChoices):
    """Status of a rollup table."""
    PENDING = 'pending', 'Pending'
    CREATING = 'creating', 'Creating'
    READY = 'ready', 'Ready'
    REFRESHING = 'refreshing', 'Refreshing'
    ERROR = 'error', 'Error'
    STALE = 'stale', 'Stale'


class Rollup(models.Model):
    """Pre-aggregation rollup table definition."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Reference to source table
    bigquery_table = models.ForeignKey(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='rollups'
    )

    # Rollup identification
    name = models.CharField(max_length=255)
    rollup_id = models.CharField(max_length=100)  # Unique identifier for this rollup

    # BigQuery rollup table location
    rollup_project = models.CharField(max_length=255, blank=True)
    rollup_dataset = models.CharField(max_length=255, blank=True)
    rollup_table = models.CharField(max_length=255)

    # Rollup configuration
    dimensions = models.JSONField(default=list)  # List of dimension columns
    metrics = models.JSONField(default=list)  # List of metric columns (for reference)
    is_searchable = models.BooleanField(default=False)  # Includes search_term dimension

    # Status tracking
    status = models.CharField(
        max_length=20,
        choices=RollupStatus.choices,
        default=RollupStatus.PENDING
    )
    error_message = models.TextField(blank=True, default='')

    # Statistics
    row_count = models.BigIntegerField(default=0)
    size_bytes = models.BigIntegerField(default=0)
    last_refresh_at = models.DateTimeField(null=True, blank=True)
    refresh_duration_seconds = models.IntegerField(default=0)

    # Date range in rollup
    min_date = models.DateField(null=True, blank=True)
    max_date = models.DateField(null=True, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']
        unique_together = [['bigquery_table', 'rollup_id']]
        indexes = [
            models.Index(fields=['bigquery_table', 'status']),
            models.Index(fields=['rollup_id']),
        ]

    def __str__(self):
        return f"{self.name} ({self.status})"

    @property
    def full_rollup_path(self) -> str:
        """Get full BigQuery path for rollup table."""
        project = self.rollup_project or self.bigquery_table.project_id
        dataset = self.rollup_dataset or self.bigquery_table.dataset
        return f"{project}.{dataset}.{self.rollup_table}"

    @property
    def is_ready(self) -> bool:
        """Check if rollup is ready to use."""
        return self.status == RollupStatus.READY

    def mark_refreshing(self):
        """Mark rollup as refreshing."""
        self.status = RollupStatus.REFRESHING
        self.save(update_fields=['status', 'updated_at'])

    def mark_ready(self, row_count: int = 0, size_bytes: int = 0, duration_seconds: int = 0):
        """Mark rollup as ready after successful refresh."""
        self.status = RollupStatus.READY
        self.row_count = row_count
        self.size_bytes = size_bytes
        self.refresh_duration_seconds = duration_seconds
        self.last_refresh_at = timezone.now()
        self.error_message = ''
        self.save(update_fields=[
            'status', 'row_count', 'size_bytes',
            'refresh_duration_seconds', 'last_refresh_at',
            'error_message', 'updated_at'
        ])

    def mark_error(self, error_message: str):
        """Mark rollup as errored."""
        self.status = RollupStatus.ERROR
        self.error_message = error_message
        self.save(update_fields=['status', 'error_message', 'updated_at'])


class RollupConfig(models.Model):
    """Global rollup configuration for a table."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    bigquery_table = models.OneToOneField(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='rollup_config'
    )

    # Default project/dataset for rollup tables
    default_project = models.CharField(max_length=255, blank=True)
    default_dataset = models.CharField(max_length=255, blank=True)

    # Auto-refresh settings
    auto_refresh_enabled = models.BooleanField(default=False)
    refresh_schedule_cron = models.CharField(max_length=100, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Rollup config for {self.bigquery_table}"
