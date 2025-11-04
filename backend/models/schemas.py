from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Union, Literal
from datetime import date

class FilterParams(BaseModel):
    """Filter parameters for queries"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    country: Optional[str] = None
    channel: Optional[str] = None
    gcategory: Optional[str] = None
    query_intent_classification: Optional[str] = None
    n_words_normalized: Optional[int] = None
    n_attributes: Optional[int] = None
    n_attributes_min: Optional[int] = None
    n_attributes_max: Optional[int] = None
    attr_categoria: Optional[bool] = None
    attr_tipo: Optional[bool] = None
    attr_genero: Optional[bool] = None
    attr_marca: Optional[bool] = None
    attr_color: Optional[bool] = None
    attr_material: Optional[bool] = None
    attr_talla: Optional[bool] = None
    attr_modelo: Optional[bool] = None

class OverviewMetrics(BaseModel):
    """Overview dashboard metrics"""
    queries: int
    queries_pdp: int
    queries_a2c: int
    purchases: int
    revenue: float
    ctr: float
    a2c_rate: float
    conversion_rate: float
    pdp_conversion: float
    revenue_per_query: float
    aov: float
    avg_queries_per_day: float
    unique_search_terms: int

class TrendData(BaseModel):
    """Time series data point"""
    date: str
    queries: int
    queries_pdp: int
    queries_a2c: int
    purchases: int
    revenue: float
    ctr: float
    a2c_rate: float
    conversion_rate: float
    pdp_conversion: float
    revenue_per_query: float

class DimensionBreakdown(BaseModel):
    """Breakdown by a specific dimension"""
    dimension_value: str
    queries: int
    queries_pdp: int
    queries_a2c: int
    purchases: int
    revenue: float
    ctr: float
    a2c_rate: float
    conversion_rate: float
    pdp_conversion: float
    revenue_per_query: float
    avg_queries_per_day: float
    percentage_of_total: float

class SearchTermData(BaseModel):
    """Individual search term metrics"""
    search_term: str
    queries: int
    queries_pdp: int
    queries_a2c: int
    purchases: int
    revenue: float
    ctr: float
    conversion_rate: float
    pdp_conversion: float
    avg_queries_per_day: float
    n_words: int
    n_attributes: int

class FilterOptions(BaseModel):
    """Available filter options"""
    countries: List[str]
    channels: List[str]
    date_range: dict
    attributes: List[str]

class PivotRow(BaseModel):
    """Single row in pivot table"""
    dimension_value: str
    queries: int
    queries_pdp: int
    queries_a2c: int
    purchases: int
    revenue: float
    ctr: float
    a2c_rate: float
    conversion_rate: float
    pdp_conversion: float
    revenue_per_query: float
    aov: float
    avg_queries_per_day: float
    percentage_of_total: float
    search_term_count: int = 0
    has_children: bool = False

class PivotChildRow(BaseModel):
    """Child row (search term) in pivot table"""
    search_term: str
    queries: int
    queries_pdp: int
    purchases: int
    revenue: float
    ctr: float
    conversion_rate: float
    pdp_conversion: float
    avg_queries_per_day: float
    percentage_of_total: float
    aov: float

class PivotResponse(BaseModel):
    """Pivot table response"""
    rows: List[PivotRow]
    total: PivotRow
    available_dimensions: List[str]
    dimension_metadata: Optional[dict] = None  # Metadata about the dimension used (e.g., custom dimension name)


class BigQueryInfo(BaseModel):
    """BigQuery connection and data information"""
    project_id: str
    dataset: str
    table: str
    table_full_path: str
    connection_status: str
    date_range: dict
    total_rows: int
    total_searches: int
    total_revenue: float
    unique_search_terms: int
    available_countries: List[str]
    available_channels: List[str]
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

class CustomDimensionValue(BaseModel):
    """A value within a custom dimension (e.g., a date range)"""
    label: str = Field(..., description="Display name for this value (e.g., 'Holiday Season')")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")

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
