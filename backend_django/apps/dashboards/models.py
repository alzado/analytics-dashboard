"""
Dashboard and widget models.
"""
import uuid
from django.db import models
from django.conf import settings


class Dashboard(models.Model):
    """Dashboard configuration."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Ownership
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='dashboards'
    )
    organization = models.ForeignKey(
        'organizations.Organization',
        on_delete=models.CASCADE,
        related_name='dashboards',
        null=True,
        blank=True
    )

    # Dashboard info
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, default='')

    # Default table (optional)
    default_table = models.ForeignKey(
        'tables.BigQueryTable',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='dashboards'
    )

    # Layout settings
    layout = models.JSONField(default=dict, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']

    def __str__(self):
        return self.name


class WidgetType(models.TextChoices):
    """Types of dashboard widgets."""
    PIVOT_TABLE = 'pivot_table', 'Pivot Table'
    KPI_CARD = 'kpi_card', 'KPI Card'
    LINE_CHART = 'line_chart', 'Line Chart'
    BAR_CHART = 'bar_chart', 'Bar Chart'
    PIE_CHART = 'pie_chart', 'Pie Chart'
    TEXT = 'text', 'Text'


class Widget(models.Model):
    """Dashboard widget configuration."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Dashboard - nullable for standalone widgets
    dashboard = models.ForeignKey(
        Dashboard,
        on_delete=models.CASCADE,
        related_name='widgets',
        null=True,
        blank=True
    )

    # Owner for standalone widgets
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='standalone_widgets',
        null=True,
        blank=True
    )

    # Draft status for autosaved widgets
    is_draft = models.BooleanField(default=False)

    # Widget info
    name = models.CharField(max_length=255)
    widget_type = models.CharField(
        max_length=50,
        choices=WidgetType.choices,
        default=WidgetType.PIVOT_TABLE
    )

    # Optional table override (uses dashboard default if not set)
    table = models.ForeignKey(
        'tables.BigQueryTable',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='widgets'
    )

    # Widget configuration
    config = models.JSONField(default=dict, blank=True)
    """
    Config schema varies by widget_type:

    pivot_table:
        - dimensions: List[str]
        - metrics: List[str]
        - filters: Dict[str, List[str]]
        - limit: int
        - sort_by: str
        - sort_order: str

    kpi_card:
        - metric: str
        - compare_to_previous: bool
        - filters: Dict[str, List[str]]

    line_chart:
        - x_dimension: str
        - y_metrics: List[str]
        - filters: Dict[str, List[str]]

    bar_chart:
        - dimension: str
        - metrics: List[str]
        - filters: Dict[str, List[str]]

    pie_chart:
        - dimension: str
        - metric: str
        - filters: Dict[str, List[str]]

    text:
        - content: str
        - markdown: bool
    """

    # Position in grid layout
    position = models.JSONField(default=dict, blank=True)
    """
    Position schema:
        - x: int (column)
        - y: int (row)
        - w: int (width in columns)
        - h: int (height in rows)
    """

    # Display order
    sort_order = models.IntegerField(default=0)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['sort_order', 'created_at']

    def __str__(self):
        return f"{self.name} ({self.widget_type})"

    def get_table(self):
        """Get the table for this widget (widget-specific or dashboard default)."""
        return self.table or self.dashboard.default_table
