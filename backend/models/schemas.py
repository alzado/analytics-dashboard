from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

class FilterParams(BaseModel):
    """Filter parameters for queries"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    country: Optional[str] = None
    channel: Optional[str] = None
    gcategory: Optional[str] = None
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
