"""
Schema models for metrics and dimensions.

Migrated from FastAPI Pydantic models to Django ORM.
The schema system supports:
- CalculatedMetric: Metrics with SQL formulas (e.g., CTR = queries_pdp / queries)
- Dimension: Columns used for grouping/filtering
- CalculatedDimension: Dimensions with SQL expressions
- SchemaConfig: Container linking all metrics/dimensions to a BigQuery table
"""
import uuid
from django.db import models
from django.conf import settings
from django.contrib.postgres.fields import ArrayField

from apps.core.models import BaseModel, TimestampedModel


class FormatType(models.TextChoices):
    """Display format types for metrics."""
    NUMBER = 'number', 'Number'
    CURRENCY = 'currency', 'Currency'
    PERCENT = 'percent', 'Percent'


class DataType(models.TextChoices):
    """Data types for dimensions."""
    STRING = 'STRING', 'String'
    INTEGER = 'INTEGER', 'Integer'
    FLOAT = 'FLOAT', 'Float'
    DATE = 'DATE', 'Date'
    BOOLEAN = 'BOOLEAN', 'Boolean'


class FilterType(models.TextChoices):
    """Filter UI types for dimensions."""
    SINGLE = 'single', 'Single Select'
    MULTI = 'multi', 'Multi Select'
    RANGE = 'range', 'Range'
    DATE_RANGE = 'date_range', 'Date Range'
    BOOLEAN = 'boolean', 'Boolean'


class SchemaConfig(BaseModel):
    """
    Container for schema configuration linked to a BigQuery table.
    Each BigQuery table has one schema configuration.
    """
    bigquery_table = models.OneToOneField(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='schema_config'
    )

    # Pivot table settings
    primary_sort_metric = models.CharField(
        max_length=100, null=True, blank=True,
        help_text='Default metric ID to sort pivot tables by'
    )
    avg_per_day_metric = models.CharField(
        max_length=100, null=True, blank=True,
        help_text='Metric ID to use for average per day calculation'
    )
    pagination_threshold = models.IntegerField(
        default=100,
        help_text='Paginate dimension values when count exceeds this'
    )

    version = models.IntegerField(default=1)

    class Meta:
        db_table = 'schema_configs'
        verbose_name = 'Schema Configuration'
        verbose_name_plural = 'Schema Configurations'

    def __str__(self):
        return f"Schema for {self.bigquery_table.name}"


class CalculatedMetric(BaseModel):
    """
    A calculated metric with a formula.

    Examples:
    - CTR: formula = '{queries_pdp} / {queries}'
           sql_expression = 'SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))'
    - Conversion Rate: formula = '{purchases} / {queries}'
    """
    schema_config = models.ForeignKey(
        SchemaConfig,
        on_delete=models.CASCADE,
        related_name='calculated_metrics'
    )

    # Identification
    metric_id = models.CharField(
        max_length=100,
        help_text='Unique identifier (e.g., "ctr", "conversion_rate")'
    )
    display_name = models.CharField(
        max_length=255,
        help_text='Human-readable name for UI'
    )

    # Formula and SQL
    formula = models.TextField(
        help_text='Formula expression (e.g., "{queries_pdp} / {queries}")'
    )
    sql_expression = models.TextField(
        help_text='Compiled SQL expression (e.g., "SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))")'
    )

    # Dependencies (stored as JSON arrays)
    depends_on = ArrayField(
        models.CharField(max_length=100),
        default=list, blank=True,
        help_text='All metric IDs used in formula'
    )
    depends_on_base = ArrayField(
        models.CharField(max_length=100),
        default=list, blank=True,
        help_text='Base metric IDs used (deprecated - kept for compatibility)'
    )
    depends_on_calculated = ArrayField(
        models.CharField(max_length=100),
        default=list, blank=True,
        help_text='Calculated metric IDs used in formula'
    )
    depends_on_dimensions = ArrayField(
        models.CharField(max_length=100),
        default=list, blank=True,
        help_text='Dimension IDs used in formula SQL expression'
    )

    # Display settings
    format_type = models.CharField(
        max_length=20,
        choices=FormatType.choices,
        default=FormatType.NUMBER
    )
    decimal_places = models.IntegerField(default=2)
    category = models.CharField(
        max_length=50, default='other',
        help_text='Metric category (volume, conversion, revenue, etc.)'
    )
    is_visible_by_default = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=999)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'calculated_metrics'
        unique_together = [['schema_config', 'metric_id']]
        ordering = ['sort_order', 'metric_id']
        indexes = [
            models.Index(fields=['schema_config', 'metric_id']),
            models.Index(fields=['category']),
        ]

    def __str__(self):
        return f"{self.display_name} ({self.metric_id})"


class Dimension(BaseModel):
    """
    A dimension representing a column used for grouping or filtering.

    Examples:
    - country: STRING dimension with multi-select filter
    - n_words_normalized: INTEGER dimension with range filter
    - date: DATE dimension with date_range filter
    """
    schema_config = models.ForeignKey(
        SchemaConfig,
        on_delete=models.CASCADE,
        related_name='dimensions'
    )

    # Identification
    dimension_id = models.CharField(
        max_length=100,
        help_text='Unique identifier (e.g., "country", "channel")'
    )
    column_name = models.CharField(
        max_length=255,
        help_text='Actual BigQuery column name'
    )
    display_name = models.CharField(
        max_length=255,
        help_text='Human-readable name for UI'
    )

    # Type and capabilities
    data_type = models.CharField(
        max_length=20,
        choices=DataType.choices,
        default=DataType.STRING
    )
    is_filterable = models.BooleanField(
        default=True,
        help_text='Can be used in filters'
    )
    is_groupable = models.BooleanField(
        default=True,
        help_text='Can be used for GROUP BY'
    )

    # Display settings
    sort_order = models.IntegerField(default=999)
    filter_type = models.CharField(
        max_length=20,
        choices=FilterType.choices,
        null=True, blank=True,
        help_text='Type of filter UI to show'
    )
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'dimensions'
        unique_together = [['schema_config', 'dimension_id']]
        ordering = ['sort_order', 'dimension_id']
        indexes = [
            models.Index(fields=['schema_config', 'dimension_id']),
            models.Index(fields=['is_filterable']),
            models.Index(fields=['is_groupable']),
        ]

    def __str__(self):
        return f"{self.display_name} ({self.dimension_id})"


class CalculatedDimension(BaseModel):
    """
    A calculated dimension with a SQL expression.

    Allows creating derived dimensions from existing columns.
    Example: rec_id = COALESCE(REGEXP_EXTRACT(query, r'pattern'), visit_id)
    """
    schema_config = models.ForeignKey(
        SchemaConfig,
        on_delete=models.CASCADE,
        related_name='calculated_dimensions'
    )

    # Identification
    dimension_id = models.CharField(
        max_length=100,
        help_text='Unique identifier (e.g., "rec_id", "search_category")'
    )
    display_name = models.CharField(
        max_length=255,
        help_text='Human-readable name for UI'
    )

    # SQL Expression
    sql_expression = models.TextField(
        help_text='SQL expression with {column} references'
    )

    # Dependencies
    depends_on = ArrayField(
        models.CharField(max_length=100),
        default=list, blank=True,
        help_text='Column names referenced in the expression'
    )

    # Type and capabilities
    data_type = models.CharField(
        max_length=20,
        choices=DataType.choices,
        default=DataType.STRING
    )
    is_filterable = models.BooleanField(default=True)
    is_groupable = models.BooleanField(default=True)

    # Display settings
    sort_order = models.IntegerField(default=999)
    filter_type = models.CharField(
        max_length=20,
        choices=FilterType.choices,
        default=FilterType.MULTI
    )
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'calculated_dimensions'
        unique_together = [['schema_config', 'dimension_id']]
        ordering = ['sort_order', 'dimension_id']
        indexes = [
            models.Index(fields=['schema_config', 'dimension_id']),
        ]

    def __str__(self):
        return f"{self.display_name} ({self.dimension_id})"


class CustomDimension(BaseModel):
    """
    A user-defined custom dimension (e.g., date ranges or metric conditions).

    Types:
    - date_range: Groups data by custom date periods (e.g., "Holiday Season", "Black Friday")
    - metric_condition: Groups data by metric thresholds (e.g., "High CVR", "Low CVR")
    """
    schema_config = models.ForeignKey(
        SchemaConfig,
        on_delete=models.CASCADE,
        related_name='custom_dimensions'
    )

    name = models.CharField(max_length=255)
    dimension_type = models.CharField(
        max_length=50,
        choices=[
            ('date_range', 'Date Range'),
            ('metric_condition', 'Metric Condition'),
        ],
        default='date_range'
    )

    # For metric_condition type
    metric = models.CharField(
        max_length=100, null=True, blank=True,
        help_text='Metric name to evaluate (for metric_condition type)'
    )

    # Values stored as JSON
    values_json = models.JSONField(
        default=list,
        help_text='List of value definitions (date ranges or metric conditions)'
    )

    class Meta:
        db_table = 'custom_dimensions'
        unique_together = [['schema_config', 'name']]
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.dimension_type})"


class OptimizedSourceStatus(models.TextChoices):
    """Status values for optimized source tables."""
    PENDING = 'pending', 'Pending'
    BUILDING = 'building', 'Building'
    READY = 'ready', 'Ready'
    ERROR = 'error', 'Error'


class OptimizedSourceConfig(BaseModel):
    """
    Configuration for an optimized source table with precomputed composite keys.

    This optimizes COUNT(DISTINCT CONCAT(...)) queries by precomputing
    the CONCAT columns once during table creation.
    """
    bigquery_table = models.OneToOneField(
        'tables.BigQueryTable',
        on_delete=models.CASCADE,
        related_name='optimized_source_config'
    )

    # Source and target table info
    source_table_path = models.CharField(
        max_length=500,
        help_text='Full BigQuery path to source table (project.dataset.table)'
    )
    optimized_table_name = models.CharField(
        max_length=255,
        help_text='Name of the optimized table (without project.dataset)'
    )
    target_project = models.CharField(
        max_length=255, null=True, blank=True,
        help_text='Target project for optimized table (defaults to source project)'
    )
    target_dataset = models.CharField(
        max_length=255, null=True, blank=True,
        help_text='Target dataset for optimized table (defaults to source dataset)'
    )

    # Composite key mappings (JSON array)
    composite_key_mappings = models.JSONField(
        default=list,
        help_text='List of composite key mappings: [{key_column_name, source_columns, metric_ids}]'
    )

    # Clustering configuration (JSON object)
    clustering = models.JSONField(
        null=True, blank=True,
        help_text='Clustering config: {columns: [], auto_detected: bool}'
    )

    # Partitioning
    partition_column = models.CharField(
        max_length=100,
        default='date',
        help_text='Column to partition by'
    )

    # Status
    status = models.CharField(
        max_length=20,
        choices=OptimizedSourceStatus.choices,
        default=OptimizedSourceStatus.PENDING
    )
    last_refresh_at = models.DateTimeField(null=True, blank=True)
    last_refresh_error = models.TextField(null=True, blank=True)

    # Statistics
    row_count = models.BigIntegerField(null=True, blank=True)
    size_bytes = models.BigIntegerField(null=True, blank=True)

    class Meta:
        db_table = 'optimized_source_configs'
        verbose_name = 'Optimized Source Configuration'
        verbose_name_plural = 'Optimized Source Configurations'

    def __str__(self):
        return f"Optimized source for {self.bigquery_table.name}"

    @property
    def optimized_table_path(self):
        """Get full BigQuery path for optimized table."""
        project = self.target_project or self.source_table_path.split('.')[0]
        dataset = self.target_dataset or self.source_table_path.split('.')[1]
        return f"{project}.{dataset}.{self.optimized_table_name}"
