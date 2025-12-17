from __future__ import annotations

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Union, Literal, Dict, Any
from datetime import date
from enum import Enum


class DateRangeType(str, Enum):
    """Type of date range selection"""
    ABSOLUTE = "absolute"
    RELATIVE = "relative"


class RelativeDatePreset(str, Enum):
    """Supported relative date presets"""
    TODAY = "today"
    YESTERDAY = "yesterday"
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"
    LAST_90_DAYS = "last_90_days"
    THIS_WEEK = "this_week"
    LAST_WEEK = "last_week"
    THIS_MONTH = "this_month"
    LAST_MONTH = "last_month"
    THIS_QUARTER = "this_quarter"
    LAST_QUARTER = "last_quarter"
    THIS_YEAR = "this_year"
    LAST_YEAR = "last_year"


class FilterParams(BaseModel):
    """Filter parameters for queries - fully dynamic dimension-based filtering"""
    # Date range filters (special handling for date dimension)
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    # Relative date support
    date_range_type: Optional[str] = "absolute"  # 'absolute' or 'relative'
    relative_date_preset: Optional[str] = None  # e.g., 'last_7_days', 'this_month'

    # Dynamic dimension filters
    # Format: { "dimension_id": ["value1", "value2"], ... }
    # Supports multi-select with OR logic within a dimension
    # Example: {"country": ["USA", "Canada"], "channel": ["Web", "App"]}
    dimension_filters: Optional[Dict[str, List[str]]] = Field(default_factory=dict, description="Dynamic dimension filters with multi-select support")

class OverviewMetrics(BaseModel):
    """Overview dashboard metrics - fully dynamic"""
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary

class TrendData(BaseModel):
    """Time series data point - fully dynamic"""
    date: Optional[str] = None  # Date string (YYYY-MM-DD)
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary

class DimensionBreakdown(BaseModel):
    """Breakdown by a specific dimension - fully dynamic"""
    dimension_value: str
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary
    percentage_of_total: float

class SearchTermData(BaseModel):
    """Individual search term metrics - fully dynamic"""
    search_term: str
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary

class FilterOptions(BaseModel):
    """Available filter options"""
    countries: List[str]
    channels: List[str]
    date_range: dict
    attributes: List[str]

class PivotRow(BaseModel):
    """Single row in pivot table with dynamic metrics - completely generic"""
    dimension_value: str
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary
    percentage_of_total: float  # Percentage relative to grand total (requires cross-row context)
    has_children: bool = False  # For hierarchical drill-down support

class PivotChildRow(BaseModel):
    """Child row in hierarchical pivot - completely generic"""
    dimension_value: str  # Generic name for the child dimension value
    metrics: Dict[str, Union[int, float]]  # All metrics as dynamic dictionary
    percentage_of_total: float  # Percentage relative to parent or grand total

class PivotResponse(BaseModel):
    """Pivot table response"""
    rows: List[PivotRow]
    total: Optional[PivotRow] = None  # Optional when error occurs
    available_dimensions: List[str] = []  # Default to empty when error occurs
    dimension_metadata: Optional[dict] = None  # Metadata about the dimension used (e.g., custom dimension name)
    total_count: Optional[int] = None  # Total number of dimension values (for pagination)
    baseline_totals: Optional[Dict[str, float]] = None  # Totals from simplest rollup (no dimensions) for comparison
    metric_warnings: Optional[Dict[str, bool]] = None  # Metrics that are potentially inflated (current > baseline)
    error: Optional[str] = None  # Error message when query cannot be executed
    error_type: Optional[str] = None  # Error type: "rollup_required", "query_error", etc.


class BigQueryInfo(BaseModel):
    """BigQuery connection and data information"""
    project_id: str
    dataset: str
    table: str
    table_full_path: str
    billing_project: Optional[str] = None  # Project for query computation/billing
    connection_status: str
    date_range: dict
    total_rows: int
    table_size_mb: float
    last_modified: str
    schema_columns: List[str]
    allowed_min_date: Optional[str] = None  # Configured minimum allowed date
    allowed_max_date: Optional[str] = None  # Configured maximum allowed date

class BigQueryConfig(BaseModel):
    """BigQuery configuration from UI"""
    project_id: str
    dataset: str
    table: str
    billing_project: Optional[str] = None  # Project for query computation/billing (defaults to project_id if not set)
    use_adc: bool = True  # Use Application Default Credentials (gcloud auth)
    credentials_json: str = ""  # Service account JSON (only if use_adc=False)
    allowed_min_date: Optional[str] = None  # Minimum allowed date for queries (YYYY-MM-DD)
    allowed_max_date: Optional[str] = None  # Maximum allowed date for queries (YYYY-MM-DD)

class BigQueryConfigResponse(BaseModel):
    """Response after configuring BigQuery"""
    success: bool
    message: str
    connection_status: str


# App Settings Models

class AppSettingsResponse(BaseModel):
    """Global application settings"""
    default_billing_project: Optional[str] = None


class AppSettingsUpdate(BaseModel):
    """Request to update app settings"""
    default_billing_project: Optional[str] = None


# Multi-Table Management Models

class TableInfoResponse(BaseModel):
    """Information about a BigQuery table configuration"""
    table_id: str
    name: str
    project_id: str
    dataset: str
    table: str
    created_at: str
    last_used_at: str
    is_active: bool = False

class TableListResponse(BaseModel):
    """List of all configured tables"""
    tables: List[TableInfoResponse]

class TableCreateRequest(BaseModel):
    """Request to create a new table configuration"""
    name: str = Field(..., min_length=1, max_length=100)
    project_id: str
    dataset: str
    table: str
    credentials_json: str = ""
    billing_project: Optional[str] = None  # Project for query billing (defaults to project_id if not set)
    allowed_min_date: Optional[str] = None
    allowed_max_date: Optional[str] = None

class TableUpdateRequest(BaseModel):
    """Request to update table metadata"""
    name: str = Field(..., min_length=1, max_length=100)

class TableConfigUpdateRequest(BaseModel):
    """Request to update table BigQuery configuration"""
    project_id: str
    dataset: str
    table: str
    credentials_json: str = ""
    billing_project: Optional[str] = None  # Project for query billing (defaults to project_id if not set)
    allowed_min_date: Optional[str] = None
    allowed_max_date: Optional[str] = None

class TableActivateRequest(BaseModel):
    """Request to activate a table"""
    table_id: str

class SchemaCopyRequest(BaseModel):
    """Request to copy schema from one table to another"""
    source_table_id: str
    target_table_id: str

class SchemaTemplateRequest(BaseModel):
    """Request to apply a schema template"""
    template_name: Literal["ecommerce", "saas", "marketing"]
    table_id: Optional[str] = None  # If None, applies to active table

class CustomDimensionValue(BaseModel):
    """A value within a custom dimension (e.g., a date range)"""
    label: str = Field(..., description="Display name for this value (e.g., 'Holiday Season')")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")
    # Relative date support
    date_range_type: Optional[str] = Field(default="absolute", description="'absolute' or 'relative'")
    relative_date_preset: Optional[str] = Field(default=None, description="Relative date preset (e.g., 'last_7_days')")

class MetricCondition(BaseModel):
    """A condition to evaluate a metric (e.g., conversion_rate > 0.05)"""
    operator: Literal[">", "<", ">=", "<=", "=", "between", "is_null", "is_not_null"] = Field(..., description="Comparison operator")
    value: Optional[float] = Field(None, description="Threshold value (not needed for is_null/is_not_null)")
    value_max: Optional[float] = Field(None, description="Maximum value for 'between' operator")

    @model_validator(mode='after')
    def validate_condition(self):
        """Validate metric condition based on operator"""
        if self.operator not in ['is_null', 'is_not_null'] and self.value is None:
            raise ValueError(f"value is required for operator '{self.operator}'")

        if self.operator == 'between':
            if self.value_max is None:
                raise ValueError("value_max is required for 'between' operator")
            if self.value is not None and self.value_max <= self.value:
                raise ValueError("value_max must be greater than value")

        return self

class MetricDimensionValue(BaseModel):
    """A value within a metric-based custom dimension (e.g., 'High CVR')"""
    label: str = Field(..., description="Display name for this value (e.g., 'High CVR')")
    conditions: List[MetricCondition] = Field(..., min_items=1, description="List of conditions (all must be met via AND logic)")

class CustomDimension(BaseModel):
    """A custom dimension defined by the user"""
    id: str = Field(..., description="Unique identifier (UUID)")
    name: str = Field(..., description="Dimension name (e.g., 'Seasonal Periods')")
    type: Literal["date_range", "metric_condition"] = Field(default="date_range", description="Dimension type")
    # For date_range type:
    values: Optional[List[CustomDimensionValue]] = Field(None, description="List of date range values (for date_range type)")
    # For metric_condition type:
    metric: Optional[str] = Field(None, description="Metric name to evaluate (for metric_condition type, e.g., 'conversion_rate')")
    metric_values: Optional[List[MetricDimensionValue]] = Field(None, description="List of metric condition values (for metric_condition type)")
    created_at: str = Field(..., description="ISO timestamp when created")
    updated_at: str = Field(..., description="ISO timestamp when last updated")

    @model_validator(mode='after')
    def validate_custom_dimension(self):
        """Validate custom dimension based on type"""
        if self.type == 'date_range':
            if not self.values:
                raise ValueError("values is required for date_range type")
        elif self.type == 'metric_condition':
            if not self.metric:
                raise ValueError("metric is required for metric_condition type")
            if not self.metric_values:
                raise ValueError("metric_values is required for metric_condition type")
        return self

class CustomDimensionCreate(BaseModel):
    """Request body for creating a custom dimension"""
    name: str = Field(..., min_length=1, max_length=100)
    type: Literal["date_range", "metric_condition"] = Field(default="date_range")
    # For date_range type:
    values: Optional[List[CustomDimensionValue]] = Field(None, min_items=1)
    # For metric_condition type:
    metric: Optional[str] = Field(None)
    metric_values: Optional[List[MetricDimensionValue]] = Field(None, min_items=1)

    @model_validator(mode='after')
    def validate_custom_dimension_create(self):
        """Validate custom dimension create based on type"""
        if self.type == 'date_range':
            if not self.values:
                raise ValueError("values is required for date_range type")
        elif self.type == 'metric_condition':
            if not self.metric:
                raise ValueError("metric is required for metric_condition type")
            if not self.metric_values:
                raise ValueError("metric_values is required for metric_condition type")
        return self

class CustomDimensionUpdate(BaseModel):
    """Request body for updating a custom dimension"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    # For date_range type:
    values: Optional[List[CustomDimensionValue]] = Field(None, min_items=1)
    # For metric_condition type:
    metric: Optional[str] = Field(None)
    metric_values: Optional[List[MetricDimensionValue]] = Field(None, min_items=1)

# Query Logging Models

class QueryLogEntry(BaseModel):
    """Single query log entry"""
    id: int
    timestamp: str
    endpoint: str
    query_type: str
    bytes_processed: int
    bytes_billed: int
    execution_time_ms: int
    filters: Optional[dict] = None
    row_count: Optional[int] = None
    error: Optional[str] = None

class QueryLogResponse(BaseModel):
    """Paginated query log response"""
    logs: List[QueryLogEntry]
    total: int
    limit: int
    offset: int

class QueryTypeStats(BaseModel):
    """Statistics for a specific query type"""
    query_type: str
    count: int
    bytes_processed: int

class UsageStats(BaseModel):
    """Aggregated usage statistics"""
    total_queries: int
    total_bytes_processed: int
    total_bytes_billed: int
    total_gb_processed: float
    total_gb_billed: float
    avg_execution_time_ms: float
    max_execution_time_ms: int
    min_execution_time_ms: int
    total_rows: int
    estimated_cost_usd: float
    by_query_type: List[QueryTypeStats]

class UsageTimeSeries(BaseModel):
    """Time-series usage data"""
    date: str
    queries: int
    bytes_processed: int
    bytes_billed: int
    gb_processed: float
    gb_billed: float
    avg_execution_time_ms: float
    estimated_cost_usd: float

class ClearLogsResponse(BaseModel):
    """Response after clearing logs"""
    success: bool
    message: str
    logs_deleted: int


# Dynamic Schema Models

# DEPRECATED: BaseMetric is no longer used. All metrics should be CalculatedMetric.
# This class is kept ONLY for backward compatibility with existing stored configs.
class BaseMetric(BaseModel):
    """DEPRECATED: Base metrics are no longer used.

    All metrics should be defined as CalculatedMetric with SQL expressions.
    This class is kept only for backward compatibility with existing stored schema configs.
    """
    id: str = Field(..., description="Unique identifier (e.g., 'queries', 'revenue')")
    column_name: str = Field(..., description="Actual BigQuery column name (or comma-separated columns for multi-column COUNT_DISTINCT)")
    display_name: str = Field(..., description="Human-readable name for UI")
    aggregation: Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT", "APPROX_COUNT_DISTINCT"] = Field(default="SUM", description="SQL aggregation function")
    data_type: Literal["INTEGER", "FLOAT", "NUMERIC"] = Field(default="INTEGER", description="Data type")
    format_type: Literal["number", "currency", "percent"] = Field(default="number", description="Display format")
    decimal_places: int = Field(default=0, description="Number of decimal places to show")
    category: str = Field(default="other", description="Metric category (volume, conversion, revenue, etc.)")
    is_visible_by_default: bool = Field(default=True, description="Whether to show in tables by default")
    sort_order: int = Field(default=999, description="Display order in UI (lower = first)")
    description: Optional[str] = Field(None, description="Metric description")
    is_system: bool = Field(default=False, description="Whether this is a system-generated virtual metric (e.g., days_in_range)")

    @model_validator(mode='after')
    def validate_multi_column(self):
        """Validate that multi-column syntax is only used with COUNT_DISTINCT"""
        if ',' in self.column_name and self.aggregation != 'COUNT_DISTINCT':
            raise ValueError("Multi-column syntax (comma-separated) can only be used with COUNT_DISTINCT aggregation")
        return self

class CalculatedMetric(BaseModel):
    """A calculated metric with a formula (e.g., CTR = queries_pdp / queries)"""
    id: str = Field(..., description="Unique identifier (e.g., 'ctr', 'conversion_rate')")
    display_name: str = Field(..., description="Human-readable name for UI")
    formula: str = Field(..., description="Formula expression (e.g., '{queries_pdp} / {queries}')")
    sql_expression: str = Field(..., description="Compiled SQL expression (e.g., 'SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))')")
    depends_on: List[str] = Field(default_factory=list, description="List of all metric IDs used in formula (both base and calculated)")
    depends_on_base: List[str] = Field(default_factory=list, description="List of base metric IDs used in formula")
    depends_on_calculated: List[str] = Field(default_factory=list, description="List of calculated metric IDs used in formula")
    depends_on_dimensions: List[str] = Field(default_factory=list, description="List of dimension IDs used in formula SQL expression")
    format_type: Literal["number", "currency", "percent"] = Field(default="number", description="Display format")
    decimal_places: int = Field(default=2, description="Number of decimal places to show")
    category: str = Field(default="other", description="Metric category")
    is_visible_by_default: bool = Field(default=True, description="Whether to show in tables by default")
    sort_order: int = Field(default=999, description="Display order in UI")
    description: Optional[str] = Field(None, description="Metric description")

class DimensionDef(BaseModel):
    """A dimension representing a column that can be used for grouping or filtering"""
    id: str = Field(..., description="Unique identifier (e.g., 'country', 'channel')")
    column_name: str = Field(..., description="Actual BigQuery column name")
    display_name: str = Field(..., description="Human-readable name for UI")
    data_type: Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"] = Field(default="STRING", description="Data type")
    is_filterable: bool = Field(default=True, description="Can be used in filters")
    is_groupable: bool = Field(default=True, description="Can be used for GROUP BY")
    sort_order: int = Field(default=999, description="Display order in UI")
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = Field(None, description="Type of filter UI to show")
    description: Optional[str] = Field(None, description="Dimension description")


class CalculatedDimensionDef(BaseModel):
    """A calculated dimension with a SQL expression that can reference table columns using {column_name} syntax"""
    id: str = Field(..., description="Unique identifier (e.g., 'rec_id', 'search_category')")
    display_name: str = Field(..., description="Human-readable name for UI")
    sql_expression: str = Field(..., description="SQL expression with {column} references. Example: COALESCE(REGEXP_EXTRACT({col1}, r'pattern'), {col2})")
    data_type: Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"] = Field(default="STRING", description="Output data type")
    is_filterable: bool = Field(default=True, description="Can be used in filters")
    is_groupable: bool = Field(default=True, description="Can be used for GROUP BY")
    sort_order: int = Field(default=999, description="Display order in UI")
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = Field(default="multi", description="Type of filter UI to show")
    depends_on: List[str] = Field(default_factory=list, description="Column names referenced in the expression")
    description: Optional[str] = Field(None, description="Dimension description")


class CalculatedDimensionCreate(BaseModel):
    """Request to create a calculated dimension"""
    id: Optional[str] = Field(None, description="Unique ID (auto-generated from display_name if not provided)")
    display_name: str = Field(..., min_length=1, max_length=100, description="Human-readable name")
    sql_expression: str = Field(..., min_length=1, description="SQL expression with {column} references")
    data_type: Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"] = Field(default="STRING", description="Output data type")
    is_filterable: bool = Field(default=True, description="Can be used in filters")
    is_groupable: bool = Field(default=True, description="Can be used for GROUP BY")
    sort_order: int = Field(default=999, description="Display order in UI")
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = Field(default="multi", description="Type of filter UI to show")
    description: Optional[str] = Field(None, description="Dimension description")


class CalculatedDimensionUpdate(BaseModel):
    """Request to update a calculated dimension (partial update)"""
    display_name: Optional[str] = Field(None, min_length=1, max_length=100, description="Human-readable name")
    sql_expression: Optional[str] = Field(None, min_length=1, description="SQL expression with {column} references")
    data_type: Optional[Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"]] = Field(None, description="Output data type")
    is_filterable: Optional[bool] = Field(None, description="Can be used in filters")
    is_groupable: Optional[bool] = Field(None, description="Can be used for GROUP BY")
    sort_order: Optional[int] = Field(None, description="Display order in UI")
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = Field(None, description="Type of filter UI to show")
    description: Optional[str] = Field(None, description="Dimension description")


class ExpressionValidationResult(BaseModel):
    """Result of validating a calculated dimension expression"""
    valid: bool = Field(..., description="Whether the expression is valid")
    errors: List[str] = Field(default_factory=list, description="List of validation errors")
    sql_expression: str = Field(default="", description="Compiled SQL expression (with {column} replaced)")
    depends_on: List[str] = Field(default_factory=list, description="Column names referenced in the expression")
    warnings: List[str] = Field(default_factory=list, description="Non-fatal warnings")


class SchemaConfig(BaseModel):
    """Complete schema configuration with all metrics and dimensions"""
    # DEPRECATED: base_metrics kept for backward compatibility but no longer used
    # All metrics should be defined as calculated_metrics with SQL expressions
    base_metrics: List[BaseMetric] = Field(default_factory=list, description="DEPRECATED: List of base metrics (kept for backward compatibility)")
    calculated_metrics: List[CalculatedMetric] = Field(default_factory=list, description="List of calculated metrics")
    dimensions: List[DimensionDef] = Field(default_factory=list, description="List of dimensions")
    calculated_dimensions: List[CalculatedDimensionDef] = Field(default_factory=list, description="List of calculated dimensions with SQL expressions")

    # Pivot table settings
    primary_sort_metric: Optional[str] = Field(None, description="Default metric ID to sort pivot tables by")
    avg_per_day_metric: Optional[str] = Field(None, description="Metric ID to use for average per day calculation in pivot tables")
    pagination_threshold: int = Field(default=100, description="Paginate dimension values when count exceeds this")

    # Rollup configuration (populated from separate file, not stored here)
    rollup_config: Optional["RollupConfig"] = Field(None, description="Pre-aggregation rollup configuration")

    version: int = Field(default=1, description="Schema version for migrations")
    created_at: str = Field(..., description="ISO timestamp when schema was created")
    updated_at: str = Field(..., description="ISO timestamp when schema was last updated")

# DEPRECATED: SchemaDetectionResult uses detected_base_metrics which are no longer used.
# Schema detection now creates calculated_metrics directly.
class SchemaDetectionResult(BaseModel):
    """Result of auto-detecting schema from BigQuery table.

    Note: detected_base_metrics is DEPRECATED and will always be empty.
    Detected metrics are now returned as calculated_metrics.
    """
    detected_base_metrics: List[BaseMetric] = Field(default_factory=list, description="DEPRECATED: Always empty")
    detected_dimensions: List[DimensionDef]
    column_count: int
    warnings: List[str] = Field(default_factory=list, description="Any warnings during detection")

# DEPRECATED: MetricCreate is no longer used. Use CalculatedMetricCreate instead.
class MetricCreate(BaseModel):
    """DEPRECATED: Request to create a base metric. Use CalculatedMetricCreate instead."""
    id: str
    column_name: str
    display_name: str
    aggregation: Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT", "APPROX_COUNT_DISTINCT"] = "SUM"
    data_type: Literal["INTEGER", "FLOAT", "NUMERIC"] = "INTEGER"
    format_type: Literal["number", "currency", "percent"] = "number"
    decimal_places: int = 0
    category: str = "other"
    is_visible_by_default: bool = True
    sort_order: int = 999
    description: Optional[str] = None

class CalculatedMetricCreate(BaseModel):
    """Request to create a calculated metric"""
    id: Optional[str] = None  # Auto-generated from display_name if not provided
    display_name: str
    formula: str
    format_type: Literal["number", "currency", "percent"] = "number"
    decimal_places: int = 2
    category: str = "other"
    is_visible_by_default: bool = True
    sort_order: int = 999
    description: Optional[str] = None

class DimensionCreate(BaseModel):
    """Request to create a dimension"""
    id: str
    column_name: str
    display_name: str
    data_type: Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"] = "STRING"
    is_filterable: bool = True
    is_groupable: bool = True
    sort_order: int = 999
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = None
    description: Optional[str] = None

# DEPRECATED: MetricUpdate is no longer used. Use CalculatedMetricUpdate instead.
class MetricUpdate(BaseModel):
    """DEPRECATED: Request to update a base metric. Use CalculatedMetricUpdate instead."""
    column_name: Optional[str] = None
    display_name: Optional[str] = None
    aggregation: Optional[Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT", "APPROX_COUNT_DISTINCT"]] = None
    data_type: Optional[Literal["INTEGER", "FLOAT", "STRING"]] = None
    format_type: Optional[Literal["number", "currency", "percent"]] = None
    decimal_places: Optional[int] = None
    category: Optional[str] = None
    is_visible_by_default: Optional[bool] = None
    sort_order: Optional[int] = None
    description: Optional[str] = None

class CalculatedMetricUpdate(BaseModel):
    """Request to update a calculated metric (partial update)"""
    display_name: Optional[str] = None
    formula: Optional[str] = None
    format_type: Optional[Literal["number", "currency", "percent"]] = None
    decimal_places: Optional[int] = None
    category: Optional[str] = None
    is_visible_by_default: Optional[bool] = None
    sort_order: Optional[int] = None
    description: Optional[str] = None

class DimensionUpdate(BaseModel):
    """Request to update a dimension (partial update)"""
    column_name: Optional[str] = None
    display_name: Optional[str] = None
    data_type: Optional[Literal["STRING", "INTEGER", "FLOAT", "DATE", "BOOLEAN"]] = None
    is_filterable: Optional[bool] = None
    is_groupable: Optional[bool] = None
    sort_order: Optional[int] = None
    filter_type: Optional[Literal["single", "multi", "range", "date_range", "boolean"]] = None
    description: Optional[str] = None

class PivotConfigUpdate(BaseModel):
    """Request to update pivot table configuration settings"""
    primary_sort_metric: Optional[str] = Field(None, description="Metric ID to use as default sort for pivot tables")
    avg_per_day_metric: Optional[str] = Field(None, description="Metric ID to use for average per day calculations")
    pagination_threshold: Optional[int] = Field(None, description="Paginate dimensions when unique values exceed this threshold", ge=1)


# Dashboard Models

class WidgetPosition(BaseModel):
    """Widget position in grid layout"""
    x: int = Field(..., description="X coordinate in grid (0-based)")
    y: int = Field(..., description="Y coordinate in grid (0-based)")
    w: int = Field(..., description="Width in grid units", ge=1, le=12)
    h: int = Field(..., description="Height in grid units", ge=1)

class RowSortConfig(BaseModel):
    """Row sorting configuration for widgets"""
    column: Union[str, int] = Field(..., description="Column to sort by (metric ID for single-table, column index for multi-table)")
    subColumn: Optional[Literal["value", "diff", "pctDiff"]] = Field(None, description="Sub-column type (value, diff, or pctDiff)")
    direction: Literal["asc", "desc"] = Field(..., description="Sort direction")
    metric: Optional[str] = Field(None, description="Metric ID (for multi-table mode)")

class WidgetConfig(BaseModel):
    """Configuration for a dashboard widget"""
    id: str = Field(..., description="Unique widget ID (UUID)")
    type: Literal["table", "chart"] = Field(..., description="Widget type")
    table_id: str = Field(..., description="BigQuery table ID this widget queries")
    title: str = Field(..., description="Widget title")

    # Pivot table configuration
    dimensions: List[str] = Field(default_factory=list, description="Selected dimension IDs")
    table_dimensions: List[str] = Field(default_factory=list, description="Selected table dimension IDs (for column-wise dimensions)")
    metrics: List[str] = Field(default_factory=list, description="Selected metric IDs")
    filters: Dict[str, List[str]] = Field(default_factory=dict, description="Dimension filters")

    # Date range
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")

    # Chart-specific config
    chart_type: Optional[Literal["bar", "line"]] = Field(None, description="Chart type (only for chart widgets)")

    # UI state (preserves editor state in widget)
    expanded_rows: List[str] = Field(default_factory=list, description="List of expanded row keys")
    column_order: Optional[List[int]] = Field(None, description="Order of columns (indices)")
    column_sort: Optional[Dict[str, str]] = Field(None, description="Column sorting config (metric and direction)")
    display_mode: Optional[Literal["pivot-table", "multi-table", "single-metric-chart"]] = Field(None, description="Explicit display mode")

    # Additional editor state for complete persistence
    date_range_type: Optional[Literal["absolute", "relative"]] = Field(None, description="Date range type (absolute dates or relative preset)")
    relative_date_preset: Optional[str] = Field(None, description="Relative date preset (e.g., 'last_7_days', 'last_30_days')")
    visible_metrics: Optional[List[str]] = Field(None, description="Visible metrics in multi-table display mode")
    merge_threshold: Optional[int] = Field(None, description="Grouping/merge threshold for rows")
    dimension_sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order for dimension column")
    children_sort_config: Optional[Dict[str, str]] = Field(None, description="Sort config for expanded child rows (column and direction)")
    row_sort_config: Optional[RowSortConfig] = Field(None, description="Row sorting config (column, subColumn, direction, metric)")

    # Position in grid
    position: WidgetPosition = Field(..., description="Widget position in grid layout")

    created_at: str = Field(..., description="ISO timestamp when widget was created")
    updated_at: str = Field(..., description="ISO timestamp when widget was last updated")

class DashboardConfig(BaseModel):
    """Complete dashboard configuration"""
    id: str = Field(..., description="Unique dashboard ID (UUID)")
    name: str = Field(..., description="Dashboard name")
    description: Optional[str] = Field(None, description="Dashboard description")
    widgets: List[WidgetConfig] = Field(default_factory=list, description="List of widgets in this dashboard")
    created_at: str = Field(..., description="ISO timestamp when dashboard was created")
    updated_at: str = Field(..., description="ISO timestamp when dashboard was last updated")

class DashboardListResponse(BaseModel):
    """List of all dashboards"""
    dashboards: List[DashboardConfig]

class DashboardCreateRequest(BaseModel):
    """Request to create a new dashboard"""
    name: str = Field(..., min_length=1, max_length=100, description="Dashboard name")
    description: Optional[str] = Field(None, max_length=500, description="Dashboard description")

class DashboardUpdateRequest(BaseModel):
    """Request to update a dashboard"""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Dashboard name")
    description: Optional[str] = Field(None, max_length=500, description="Dashboard description")
    widgets: Optional[List[WidgetConfig]] = Field(None, description="Complete list of widgets (replaces existing)")

class WidgetCreateRequest(BaseModel):
    """Request to add a widget to a dashboard"""
    type: Literal["table", "chart"] = Field(..., description="Widget type")
    table_id: str = Field(..., description="BigQuery table ID this widget queries")
    title: str = Field(..., min_length=1, max_length=100, description="Widget title")
    dimensions: List[str] = Field(default_factory=list, description="Selected dimension IDs")
    table_dimensions: List[str] = Field(default_factory=list, description="Selected table dimension IDs")
    metrics: List[str] = Field(default_factory=list, description="Selected metric IDs")
    filters: Dict[str, List[str]] = Field(default_factory=dict, description="Dimension filters")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    chart_type: Optional[Literal["bar", "line"]] = Field(None, description="Chart type (only for chart widgets)")
    position: WidgetPosition = Field(..., description="Widget position in grid layout")

    # UI state
    expanded_rows: Optional[List[str]] = Field(None, description="List of expanded row keys")
    column_order: Optional[List[int]] = Field(None, description="Order of columns (indices)")
    column_sort: Optional[Dict[str, str]] = Field(None, description="Column sorting config")
    display_mode: Optional[Literal["pivot-table", "multi-table", "single-metric-chart"]] = Field(None, description="Display mode")
    date_range_type: Optional[Literal["absolute", "relative"]] = Field(None, description="Date range type")
    relative_date_preset: Optional[str] = Field(None, description="Relative date preset")
    visible_metrics: Optional[List[str]] = Field(None, description="Visible metrics in multi-table mode")
    merge_threshold: Optional[int] = Field(None, description="Grouping/merge threshold")
    dimension_sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Dimension sort order")
    children_sort_config: Optional[Dict[str, str]] = Field(None, description="Child rows sort config")
    row_sort_config: Optional[RowSortConfig] = Field(None, description="Row sorting config (column, subColumn, direction, metric)")

class WidgetUpdateRequest(BaseModel):
    """Request to update a widget"""
    type: Optional[Literal["table", "chart"]] = Field(None, description="Widget type")
    title: Optional[str] = Field(None, min_length=1, max_length=100, description="Widget title")
    dimensions: Optional[List[str]] = Field(None, description="Selected dimension IDs")
    table_dimensions: Optional[List[str]] = Field(None, description="Selected table dimension IDs")
    metrics: Optional[List[str]] = Field(None, description="Selected metric IDs")
    filters: Optional[Dict[str, List[str]]] = Field(None, description="Dimension filters")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    chart_type: Optional[Literal["bar", "line"]] = Field(None, description="Chart type")
    position: Optional[WidgetPosition] = Field(None, description="Widget position")

    # UI state
    expanded_rows: Optional[List[str]] = Field(None, description="List of expanded row keys")
    column_order: Optional[List[int]] = Field(None, description="Order of columns (indices)")
    column_sort: Optional[Dict[str, str]] = Field(None, description="Column sorting config")
    display_mode: Optional[Literal["pivot-table", "multi-table", "single-metric-chart"]] = Field(None, description="Display mode")
    date_range_type: Optional[Literal["absolute", "relative"]] = Field(None, description="Date range type")
    relative_date_preset: Optional[str] = Field(None, description="Relative date preset")
    visible_metrics: Optional[List[str]] = Field(None, description="Visible metrics in multi-table mode")
    merge_threshold: Optional[int] = Field(None, description="Grouping/merge threshold")
    dimension_sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Dimension sort order")
    children_sort_config: Optional[Dict[str, str]] = Field(None, description="Child rows sort config")
    row_sort_config: Optional[RowSortConfig] = Field(None, description="Row sorting config (column, subColumn, direction, metric)")


# Significance Testing Models

class ColumnDefinition(BaseModel):
    """Definition of a column for significance testing."""
    column_index: int = Field(..., description="Index of the column in the pivot table")
    dimension_filters: Dict[str, List[str]] = Field(..., description="Dimension filters that define this column (e.g., {'country': ['USA']})")

class RowDefinition(BaseModel):
    """Definition of a row for per-row significance testing."""
    row_id: str = Field(..., description="Unique identifier for this row (e.g., dimension value)")
    dimension_filters: Dict[str, List[str]] = Field(default_factory=dict, description="Dimension filters that define this row (e.g., {'ops_score': ['5']})")

class SignificanceRequest(BaseModel):
    """Request for Bayesian significance analysis."""
    control_column: ColumnDefinition = Field(..., description="The reference/control column to compare against")
    treatment_columns: List[ColumnDefinition] = Field(..., description="Treatment columns to compare with control")
    metric_ids: List[str] = Field(..., description="Metric IDs to analyze")
    filters: FilterParams = Field(..., description="Base filters (date range, etc.)")
    rows: Optional[List[RowDefinition]] = Field(None, description="Optional list of rows to test. If provided, runs per-row significance tests.")

class SignificanceResultItem(BaseModel):
    """Result for one metric/column/row comparison using proportion-based testing."""
    metric_id: str = Field(..., description="Metric identifier")
    column_index: int = Field(..., description="Index of the treatment column")
    row_id: Optional[str] = Field(None, description="Row identifier if per-row testing, None for totals")
    prob_beat_control: float = Field(..., ge=0, le=1, description="Probability that treatment beats control (0-1)")
    credible_interval_lower: float = Field(..., description="Lower bound of 95% confidence interval for difference in proportions")
    credible_interval_upper: float = Field(..., description="Upper bound of 95% confidence interval for difference in proportions")
    mean_difference: float = Field(..., description="Difference in proportions (treatment - control)")
    relative_difference: float = Field(..., description="Relative difference as decimal (0.05 = 5% improvement)")
    is_significant: bool = Field(..., description="Whether the difference is statistically significant at 95% threshold")
    direction: Literal["better", "worse", "neutral"] = Field(..., description="Direction of the effect")
    control_mean: float = Field(..., description="Control proportion (successes/trials)")
    treatment_mean: float = Field(..., description="Treatment proportion (successes/trials)")
    # Event-based fields (proportion test)
    n_control_events: int = Field(..., description="Number of events (denominator/trials) for control group")
    n_treatment_events: int = Field(..., description="Number of events (denominator/trials) for treatment group")
    control_successes: int = Field(..., description="Number of successes (numerator) for control group")
    treatment_successes: int = Field(..., description="Number of successes (numerator) for treatment group")
    warning: Optional[str] = Field(None, description="Warning message if sample size is small")

class SignificanceResponse(BaseModel):
    """Response containing all significance results."""
    control_column_index: int = Field(..., description="Index of the control column")
    results: Dict[str, List[SignificanceResultItem]] = Field(..., description="Results per metric: {metric_id: [results per treatment column]}")


# ============================================================================
# Cache Management Models
# ============================================================================

class CacheTableStats(BaseModel):
    """Cache statistics per table."""
    table_id: str
    entries: int
    size_bytes: int
    hits: int

class CacheQueryTypeStats(BaseModel):
    """Cache statistics per query type."""
    query_type: str
    entries: int
    size_bytes: int
    hits: int

class CacheStats(BaseModel):
    """Cache statistics response."""
    total_entries: int
    total_size_bytes: int
    total_size_mb: float
    total_hits: int
    avg_hits_per_entry: float
    oldest_entry: Optional[str] = None
    newest_entry: Optional[str] = None
    by_table: List[Dict[str, Any]]
    by_query_type: List[Dict[str, Any]]

class CacheClearResponse(BaseModel):
    """Response after clearing cache."""
    success: bool
    message: str
    entries_deleted: int


# ============================================================================
# Rollup (Pre-Aggregation) Models
# ============================================================================

# Note: RollupMetricDef is kept for backward compatibility but no longer used
# in new rollups. All metrics are auto-included from schema.
class RollupMetricDef(BaseModel):
    """Definition of a metric within a rollup table (DEPRECATED - kept for backward compatibility)."""
    metric_id: str = Field(..., description="Reference to base metric ID in schema")
    include_conditional: bool = Field(default=False, description="Whether to include conditional variant (with flag=1)")
    flag_column: Optional[str] = Field(None, description="Column name for conditional flag (required if include_conditional=True)")

    @model_validator(mode='after')
    def validate_conditional_flag(self):
        if self.include_conditional and not self.flag_column:
            raise ValueError("flag_column is required when include_conditional is True")
        return self


class RollupDef(BaseModel):
    """Definition of a pre-aggregated rollup table.

    Note: Metrics are auto-included from schema (all base metrics + volume calculated metrics).
    The 'metrics' field is deprecated and kept only for backward compatibility.
    """
    id: str = Field(..., description="Unique rollup identifier (e.g., 'rollup_date_channel')")
    display_name: str = Field(..., description="Human-readable name for UI")
    description: Optional[str] = Field(None, description="Description of what this rollup is for")

    # Dimension configuration
    dimensions: List[str] = Field(default_factory=list, description="List of dimension IDs to group by (always includes 'date')")

    # Metric configuration (DEPRECATED - metrics are auto-included from schema)
    metrics: Optional[List[RollupMetricDef]] = Field(default=None, description="DEPRECATED: Metrics are auto-included from schema")

    # BigQuery table configuration
    target_project: Optional[str] = Field(None, description="Target project for rollup table (defaults to source project)")
    target_dataset: Optional[str] = Field(None, description="Target dataset for rollup table (defaults to source dataset)")
    target_table_name: Optional[str] = Field(None, description="Target table name (auto-generated if not provided)")

    # Status and metadata
    status: Literal["pending", "building", "ready", "error", "stale"] = Field(default="pending")
    last_refresh_at: Optional[str] = Field(None)
    last_refresh_error: Optional[str] = Field(None)
    row_count: Optional[int] = Field(None)
    size_bytes: Optional[int] = Field(None)

    # Timestamps
    created_at: str = Field(...)
    updated_at: str = Field(...)

class RollupConfig(BaseModel):
    """Configuration for all rollups for a table."""
    rollups: List[RollupDef] = Field(default_factory=list)
    default_target_project: Optional[str] = Field(None, description="Default project for rollup tables")
    default_target_dataset: Optional[str] = Field(None, description="Default dataset for rollup tables")
    version: int = Field(default=1)
    created_at: str = Field(...)
    updated_at: str = Field(...)


class RollupCreate(BaseModel):
    """Request to create a new rollup definition.

    Only dimensions need to be specified - all metrics are auto-included from schema.
    When creating a rollup, ALL dimension combinations are automatically generated
    (power set), including a baseline rollup with no dimensions.
    """
    id: Optional[str] = Field(None, description="Rollup ID (auto-generated from dimensions if not provided)")
    display_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    dimensions: List[str] = Field(default_factory=list, description="List of dimension IDs - all combinations will be created")
    # metrics field removed - auto-included from schema
    target_project: Optional[str] = Field(None)
    target_dataset: Optional[str] = Field(None)
    target_table_name: Optional[str] = Field(None)


class RollupUpdate(BaseModel):
    """Request to update a rollup definition (partial update)."""
    display_name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    dimensions: Optional[List[str]] = Field(None, description="List of dimension IDs (empty list for baseline rollup)")
    # metrics field removed - auto-included from schema
    target_project: Optional[str] = Field(None)
    target_dataset: Optional[str] = Field(None)
    target_table_name: Optional[str] = Field(None)


class RollupRefreshResponse(BaseModel):
    """Response after triggering a rollup refresh."""
    success: bool
    message: str
    rollup_id: str
    status: str
    table_path: Optional[str] = None
    bytes_processed: Optional[int] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[int] = None
    # Incremental refresh fields
    dates_added: Optional[int] = Field(None, description="Number of new dates added (incremental)")
    dates_processed: Optional[List[str]] = Field(None, description="List of dates that were added")
    metrics_added: Optional[int] = Field(None, description="Number of new metrics added (incremental)")
    metrics_processed: Optional[List[str]] = Field(None, description="List of metrics that were added")


class RollupStatusResponse(BaseModel):
    """Response showing rollup status including what's missing."""
    rollup_id: str
    table_exists: bool
    missing_dates: List[str] = Field(default_factory=list)
    missing_dates_count: int = 0
    missing_metrics: List[str] = Field(default_factory=list)
    missing_metrics_count: int = 0
    is_up_to_date: bool = True


class RollupListResponse(BaseModel):
    """Response listing all rollups."""
    rollups: List[RollupDef]
    default_target_project: Optional[str] = None
    default_target_dataset: Optional[str] = None


class RollupPreviewSqlResponse(BaseModel):
    """Response containing preview SQL for a rollup."""
    rollup_id: str
    sql: str
    target_table_path: str


# ============================================================================
# Optimized Source Table Models (Precomputed Composite Keys)
# ============================================================================

class CompositeKeyMapping(BaseModel):
    """Maps a precomputed composite key column to its source columns."""
    key_column_name: str = Field(..., description="Name of the precomputed key column (e.g., '_key_query_visit_id')")
    source_columns: List[str] = Field(..., description="Source columns used in the key (e.g., ['visit_id', 'query'])")
    metric_ids: List[str] = Field(default_factory=list, description="Metric IDs that use this composite key")


class ClusteringConfig(BaseModel):
    """Clustering configuration for optimized source table."""
    columns: List[str] = Field(default_factory=list, description="Up to 4 columns for BigQuery clustering")
    auto_detected: bool = Field(default=False, description="Whether columns were auto-detected based on cardinality")


class OptimizedSourceConfig(BaseModel):
    """Configuration for an optimized source table with precomputed composite keys."""
    id: str = Field(..., description="Unique ID (UUID)")
    source_table_path: str = Field(..., description="Original source table path (project.dataset.table)")
    optimized_table_name: str = Field(..., description="Name of the optimized table")
    target_project: Optional[str] = Field(None, description="Target project (defaults to source)")
    target_dataset: Optional[str] = Field(None, description="Target dataset (defaults to source)")

    # Composite key mappings
    composite_key_mappings: List[CompositeKeyMapping] = Field(default_factory=list)

    # Partitioning and clustering
    partition_column: str = Field(default="date", description="Column to partition by (always 'date')")
    clustering: Optional[ClusteringConfig] = Field(None, description="Clustering configuration")

    # Status tracking
    status: Literal["pending", "building", "ready", "error", "stale"] = Field(default="pending")
    last_refresh_at: Optional[str] = Field(None)
    last_refresh_error: Optional[str] = Field(None)
    row_count: Optional[int] = Field(None)
    size_bytes: Optional[int] = Field(None)

    # Timestamps
    created_at: str = Field(...)
    updated_at: str = Field(...)


class OptimizedSourceCreate(BaseModel):
    """Request to create an optimized source table."""
    clustering_columns: Optional[List[str]] = Field(None, description="Columns to cluster by (max 4). Auto-detected if not provided.")
    auto_detect_clustering: bool = Field(default=True, description="Auto-detect high-cardinality columns for clustering")
    target_project: Optional[str] = Field(None, description="Target project (defaults to source)")
    target_dataset: Optional[str] = Field(None, description="Target dataset (defaults to source)")


class OptimizedSourceResponse(BaseModel):
    """Response after creating/refreshing optimized source table."""
    success: bool
    message: str
    optimized_table_path: Optional[str] = Field(None, description="Full path to optimized table")
    composite_keys_created: List[str] = Field(default_factory=list, description="List of key columns created")
    clustering_columns: List[str] = Field(default_factory=list, description="Columns used for clustering")
    bytes_processed: Optional[int] = Field(None)
    row_count: Optional[int] = Field(None)
    execution_time_ms: Optional[int] = Field(None)


class OptimizedSourceStatusResponse(BaseModel):
    """Status of the optimized source table."""
    exists: bool = Field(..., description="Whether an optimized source config exists")
    config: Optional[OptimizedSourceConfig] = Field(None, description="Current configuration if exists")
    is_stale: bool = Field(default=False, description="Whether the optimized table needs refresh")
    stale_reasons: List[str] = Field(default_factory=list, description="Reasons why the table is stale")
    missing_keys: List[str] = Field(default_factory=list, description="Composite keys in schema not in optimized table")
    optimized_table_path: Optional[str] = Field(None, description="Full path to optimized table")


class OptimizedSourceAnalysis(BaseModel):
    """Analysis of what composite keys would be created from current schema."""
    composite_keys: List[CompositeKeyMapping] = Field(..., description="Keys that would be created")
    recommended_clustering: List[str] = Field(default_factory=list, description="Recommended clustering columns")
    estimated_key_count: int = Field(default=0, description="Number of composite key columns")
    metrics_with_composite_keys: List[str] = Field(default_factory=list, description="Metric IDs using composite keys")


class OptimizedSourcePreviewSql(BaseModel):
    """Preview SQL for creating optimized source table."""
    sql: str = Field(..., description="SQL that would be executed")
    target_table_path: str = Field(..., description="Target table path")
    composite_keys: List[str] = Field(default_factory=list, description="Key columns that would be created")
    clustering_columns: List[str] = Field(default_factory=list, description="Clustering columns")


# ============================================================================
# Authentication & User Management Models
# ============================================================================

class OrgRole(str, Enum):
    """Organization membership roles."""
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"


class DashboardRole(str, Enum):
    """Dashboard collaboration roles."""
    OWNER = "owner"
    EDITOR = "editor"
    VIEWER = "viewer"


class Visibility(str, Enum):
    """Resource visibility options."""
    PRIVATE = "private"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class CredentialType(str, Enum):
    """GCP credential types."""
    SERVICE_ACCOUNT = "service_account"
    OAUTH = "oauth"


class LibraryItemType(str, Enum):
    """Library item types."""
    DASHBOARD = "dashboard"
    SCHEMA = "schema"


# --- Auth Request/Response Models ---

class GoogleAuthRequest(BaseModel):
    """Request to authenticate with Google ID token."""
    id_token: str = Field(..., description="Google ID token from frontend")


class AuthTokenResponse(BaseModel):
    """Response with authentication tokens."""
    access_token: str = Field(..., description="JWT access token")
    refresh_token: str = Field(..., description="JWT refresh token")
    token_type: str = Field(default="bearer")
    expires_in: int = Field(default=86400, description="Token expiry in seconds")


class RefreshTokenRequest(BaseModel):
    """Request to refresh access token."""
    refresh_token: str = Field(..., description="JWT refresh token")


# --- User Models ---

class UserResponse(BaseModel):
    """User information response."""
    id: str = Field(..., description="User UUID")
    email: str
    name: str
    avatar_url: Optional[str] = None
    is_active: bool = True
    created_at: str
    last_login_at: Optional[str] = None

    class Config:
        from_attributes = True


class UserProfileUpdate(BaseModel):
    """Request to update user profile."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    avatar_url: Optional[str] = Field(None, max_length=500)


# --- Organization Models ---

class OrganizationCreate(BaseModel):
    """Request to create an organization."""
    name: str = Field(..., min_length=1, max_length=255)
    slug: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-z0-9-]+$")
    description: Optional[str] = None


class OrganizationUpdate(BaseModel):
    """Request to update an organization."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    avatar_url: Optional[str] = Field(None, max_length=500)


class OrganizationResponse(BaseModel):
    """Organization information response."""
    id: str
    name: str
    slug: str
    description: Optional[str] = None
    avatar_url: Optional[str] = None
    created_at: str
    member_count: int = 0
    user_role: Optional[str] = None  # Role of the requesting user

    class Config:
        from_attributes = True


class OrganizationListResponse(BaseModel):
    """List of organizations response."""
    organizations: List[OrganizationResponse]
    total: int


# --- Organization Membership Models ---

class MembershipResponse(BaseModel):
    """Organization membership information."""
    user_id: str
    organization_id: str
    role: OrgRole
    joined_at: str
    user: Optional[UserResponse] = None

    class Config:
        from_attributes = True


class InviteMemberRequest(BaseModel):
    """Request to invite a user to an organization."""
    email: str = Field(..., description="Email of user to invite")
    role: OrgRole = Field(default=OrgRole.MEMBER)


class UpdateMemberRoleRequest(BaseModel):
    """Request to update a member's role."""
    role: OrgRole


# --- GCP Credential Models ---

class GCPCredentialCreate(BaseModel):
    """Request to create GCP credentials."""
    name: str = Field(..., min_length=1, max_length=255)
    credential_type: CredentialType
    credentials_json: str = Field(..., description="Service account JSON or OAuth tokens")
    organization_id: Optional[str] = Field(None, description="Organization to associate with (optional)")
    is_default: bool = Field(default=False)


class GCPCredentialResponse(BaseModel):
    """GCP credential information (credentials excluded)."""
    id: str
    name: str
    credential_type: CredentialType
    project_id: str
    is_default: bool
    user_id: Optional[str] = None
    organization_id: Optional[str] = None
    created_at: str

    class Config:
        from_attributes = True


class GCPCredentialListResponse(BaseModel):
    """List of GCP credentials."""
    credentials: List[GCPCredentialResponse]
    total: int


# --- Dashboard Collaboration Models ---

class DashboardCollaboratorResponse(BaseModel):
    """Dashboard collaborator information."""
    user_id: str
    dashboard_id: str
    role: DashboardRole
    granted_at: str
    granted_by_id: Optional[str] = None
    user: Optional[UserResponse] = None

    class Config:
        from_attributes = True


class AddCollaboratorRequest(BaseModel):
    """Request to add a dashboard collaborator."""
    email: str = Field(..., description="Email of user to add")
    role: DashboardRole = Field(default=DashboardRole.VIEWER)


class UpdateCollaboratorRoleRequest(BaseModel):
    """Request to update a collaborator's role."""
    role: DashboardRole


class UpdateVisibilityRequest(BaseModel):
    """Request to update resource visibility."""
    visibility: Visibility


# --- Library Models ---

class LibraryItemResponse(BaseModel):
    """Library item information."""
    id: str
    item_type: str  # Using string for flexibility in API
    source_id: Optional[str] = None
    publisher_id: str
    organization_id: Optional[str] = None
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    visibility: str  # Using string for flexibility in API
    use_count: int = 0
    rating_count: int = 0
    average_rating: Optional[float] = None
    user_rating: Optional[int] = None  # Current user's rating
    published_at: str
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True


class LibraryItemListResponse(BaseModel):
    """List of library items with pagination."""
    items: List[LibraryItemResponse]
    total: int
    limit: int
    offset: int


class PublishToLibraryRequest(BaseModel):
    """Request to publish an item to the library."""
    source_id: str = Field(..., description="ID of dashboard/schema to publish")
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    visibility: Optional[str] = None  # Using string for flexibility
    organization_id: Optional[str] = None  # For schema publishing


class RateLibraryItemRequest(BaseModel):
    """Request to rate a library item."""
    rating: int = Field(..., ge=1, le=5, description="Rating from 1-5")


class UseLibraryItemRequest(BaseModel):
    """Request to use/copy a library item."""
    target_name: Optional[str] = Field(None, description="Name for the copy (optional)")
    organization_id: Optional[str] = Field(None, description="Organization to copy to (optional)")


# --- Audit Log Models ---

class AuditLogResponse(BaseModel):
    """Audit log entry."""
    id: str
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    action: str
    resource_type: str
    resource_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    ip_address: Optional[str] = None
    created_at: str

    class Config:
        from_attributes = True


class AuditLogListResponse(BaseModel):
    """List of audit logs."""
    logs: List[AuditLogResponse]
    total: int
