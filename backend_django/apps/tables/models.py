"""
BigQuery Table models.
"""
import uuid
from django.db import models
from django.conf import settings


class Visibility(models.TextChoices):
    PRIVATE = 'private', 'Private'
    ORGANIZATION = 'organization', 'Organization'
    PUBLIC = 'public', 'Public'


class BigQueryTable(models.Model):
    """BigQuery table configuration."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='bigquery_tables'
    )
    organization = models.ForeignKey(
        'organizations.Organization',
        on_delete=models.SET_NULL,
        related_name='bigquery_tables',
        null=True, blank=True
    )
    gcp_credential = models.ForeignKey(
        'credentials.GCPCredential',
        on_delete=models.SET_NULL,
        related_name='bigquery_tables',
        null=True, blank=True
    )

    # Table identification
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    project_id = models.CharField(max_length=255)
    dataset = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)
    billing_project = models.CharField(max_length=255, blank=True, null=True)

    # Access control
    visibility = models.CharField(
        max_length=20,
        choices=Visibility.choices,
        default=Visibility.PRIVATE
    )
    allowed_min_date = models.DateField(null=True, blank=True)
    allowed_max_date = models.DateField(null=True, blank=True)

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'bigquery_tables'
        indexes = [
            models.Index(fields=['owner']),
            models.Index(fields=['organization']),
        ]
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name} ({self.full_table_path})"

    @property
    def full_table_path(self):
        return f"{self.project_id}.{self.dataset}.{self.table_name}"


class AppSettings(models.Model):
    """Application-wide settings (singleton)."""

    id = models.AutoField(primary_key=True)
    default_billing_project = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'app_settings'

    @classmethod
    def get_instance(cls):
        """Get or create the singleton instance."""
        instance, _ = cls.objects.get_or_create(id=1)
        return instance

    def __str__(self):
        return f"AppSettings (billing: {self.default_billing_project})"
