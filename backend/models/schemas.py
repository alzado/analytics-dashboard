from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Union, Literal, Dict
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
    total: PivotRow
    available_dimensions: List[str]
    dimension_metadata: Optional[dict] = None  # Metadata about the dimension used (e.g., custom dimension name)
    total_count: Optional[int] = None  # Total number of dimension values (for pagination)


class BigQueryInfo(BaseModel):
    """BigQuery connection and data information"""
    project_id: str
    dataset: str
    table: str
    table_full_path: str
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
    use_adc: bool = True  # Use Application Default Credentials (gcloud auth)
    credentials_json: str = ""  # Service account JSON (only if use_adc=False)
    allowed_min_date: Optional[str] = None  # Minimum allowed date for queries (YYYY-MM-DD)
    allowed_max_date: Optional[str] = None  # Maximum allowed date for queries (YYYY-MM-DD)

class BigQueryConfigResponse(BaseModel):
    """Response after configuring BigQuery"""
    success: bool
    message: str
    connection_status: str

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

class BaseMetric(BaseModel):
    """A base metric representing a raw BigQuery column that can be aggregated"""
    id: str = Field(..., description="Unique identifier (e.g., 'queries', 'revenue')")
    column_name: str = Field(..., description="Actual BigQuery column name (or comma-separated columns for multi-column COUNT_DISTINCT)")
    display_name: str = Field(..., description="Human-readable name for UI")
    aggregation: Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT"] = Field(default="SUM", description="SQL aggregation function")
    data_type: Literal["INTEGER", "FLOAT", "NUMERIC"] = Field(default="INTEGER", description="Data type")
    format_type: Literal["number", "currency", "percent"] = Field(default="number", description="Display format")
    decimal_places: int = Field(default=0, description="Number of decimal places to show")
    category: str = Field(default="other", description="Metric category (volume, conversion, revenue, etc.)")
    is_visible_by_default: bool = Field(default=True, description="Whether to show in tables by default")
    sort_order: int = Field(default=999, description="Display order in UI (lower = first)")
    description: Optional[str] = Field(None, description="Metric description")

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

class SchemaConfig(BaseModel):
    """Complete schema configuration with all metrics and dimensions"""
    base_metrics: List[BaseMetric] = Field(default_factory=list, description="List of base metrics")
    calculated_metrics: List[CalculatedMetric] = Field(default_factory=list, description="List of calculated metrics")
    dimensions: List[DimensionDef] = Field(default_factory=list, description="List of dimensions")

    # Pivot table settings
    primary_sort_metric: Optional[str] = Field(None, description="Default metric ID to sort pivot tables by")
    avg_per_day_metric: Optional[str] = Field(None, description="Metric ID to use for average per day calculation in pivot tables")
    pagination_threshold: int = Field(default=100, description="Paginate dimension values when count exceeds this")

    version: int = Field(default=1, description="Schema version for migrations")
    created_at: str = Field(..., description="ISO timestamp when schema was created")
    updated_at: str = Field(..., description="ISO timestamp when schema was last updated")

class SchemaDetectionResult(BaseModel):
    """Result of auto-detecting schema from BigQuery table"""
    detected_base_metrics: List[BaseMetric]
    detected_dimensions: List[DimensionDef]
    column_count: int
    warnings: List[str] = Field(default_factory=list, description="Any warnings during detection")

class MetricCreate(BaseModel):
    """Request to create a base metric"""
    id: str
    column_name: str
    display_name: str
    aggregation: Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT"] = "SUM"
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

class MetricUpdate(BaseModel):
    """Request to update a metric (partial update)"""
    display_name: Optional[str] = None
    aggregation: Optional[Literal["SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT"]] = None
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
    display_name: Optional[str] = None
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
