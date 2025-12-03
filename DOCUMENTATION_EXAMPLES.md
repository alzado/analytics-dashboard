# Documentation Examples

This file contains example documentation for the codebase. Use these as templates.

## Backend Python Example - bigquery_service.py

### Module Docstring
```python
"""
BigQuery service for querying search analytics data.

This module provides a service layer for interacting with Google BigQuery. It handles:
- Connection management with service account or ADC authentication
- Dynamic SQL query generation from schema configurations
- Query caching for performance optimization
- Date range filtering and clamping
- Multi-table support with independent configurations

The service dynamically builds SQL queries based on the schema configuration,
eliminating hardcoded metrics and enabling full customization.

Key Features:
    - Two authentication methods: ADC or Service Account JSON
    - Dynamic metric aggregation from schema definitions
    - Calculated metrics evaluated post-query
    - Date limit enforcement for access control
    - Query logging for usage statistics
    - Caching for date ranges and row counts

Architecture:
    The service follows a two-tier query pattern:
    1. Base metrics aggregated in inner query
    2. Calculated metrics computed in outer query referencing base metrics

    This avoids redundant aggregations and enables complex formulas.

Example:
    Basic usage with service account:

    >>> from services.bigquery_service import BigQueryService
    >>> service = BigQueryService(
    ...     project_id="my-project",
    ...     dataset="analytics",
    ...     table="search_data",
    ...     credentials_path="/path/to/service-account.json"
    ... )
    >>> service.set_date_limits(min_date="2024-01-01", max_date="2024-12-31")
    >>>
    >>> # Query KPI metrics
    >>> from models.schemas import FilterParams
    >>> filters = FilterParams(start_date="2024-01-01", end_date="2024-01-31")
    >>> metrics = service.query_kpi_metrics(filters)
"""
```

### Class Docstring
```python
class BigQueryService:
    """Service for querying BigQuery data with dynamic schema support.

    This class manages BigQuery connections and provides methods for querying
    search analytics data. All queries are built dynamically from the schema
    configuration, enabling full customization without code changes.

    Attributes:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        table_path: Full table path (project.dataset.table)
        table_id: Unique table ID for multi-table support
        client: BigQuery client instance
        schema_service: Schema service for loading configurations
        schema_config: Loaded schema configuration
        allowed_min_date: Optional minimum allowed date for queries
        allowed_max_date: Optional maximum allowed date for queries

    Note:
        The service automatically loads the schema configuration on initialization.
        If no schema exists, it attempts auto-detection from the BigQuery table.
    """
```

### Method Docstrings
```python
def __init__(
    self,
    project_id: str,
    dataset: str,
    table: str,
    credentials_path: Optional[str] = None,
    table_id: Optional[str] = None
):
    """Initialize BigQuery service with connection parameters.

    Creates a BigQuery client using either Application Default Credentials (ADC)
    or a service account JSON file. Also initializes the schema service and loads
    the schema configuration for dynamic query building.

    Args:
        project_id: GCP project ID containing the BigQuery dataset
        dataset: BigQuery dataset name
        table: BigQuery table name within the dataset
        credentials_path: Path to service account JSON file. If None, uses ADC.
        table_id: Unique identifier for this table in multi-table setups.
                  Required for proper schema and config isolation.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If ADC is not configured
            and no credentials_path is provided
        google.cloud.exceptions.NotFound: If the table doesn't exist

    Example:
        With service account:

        >>> service = BigQueryService(
        ...     project_id="my-project",
        ...     dataset="analytics",
        ...     table="search_data",
        ...     credentials_path="/secrets/service-account.json",
        ...     table_id="prod-table-1"
        ... )

        With ADC (run 'gcloud auth application-default login' first):

        >>> service = BigQueryService(
        ...     project_id="my-project",
        ...     dataset="analytics",
        ...     table="search_data",
        ...     table_id="prod-table-1"
        ... )
    """
```

```python
def build_filter_clause(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dimension_filters: Optional[Dict[str, List[str]]] = None,
    date_range_type: Optional[str] = "absolute",
    relative_date_preset: Optional[str] = None,
) -> str:
    """Build WHERE clause from filter parameters using dynamic dimensions.

    Constructs a SQL WHERE clause by combining date filters and dimension filters.
    All dimension filtering is done dynamically using the schema configuration,
    supporting STRING, INTEGER, FLOAT, BOOLEAN, and DATE types.

    Date handling:
        - Resolves relative dates (e.g., "last_7_days") to absolute dates
        - Applies date clamping based on allowed_min_date/allowed_max_date
        - Supports both single dates and date ranges

    Dimension filtering:
        - Multi-select: Multiple values with OR logic (IN clause)
        - Single-select: Single value equality
        - Proper SQL escaping for string values
        - Type-aware filtering (numeric, boolean, string, date)
        - Custom dimension support (date ranges, metric conditions)

    Args:
        start_date: Start date for filter in YYYY-MM-DD format
        end_date: End date for filter in YYYY-MM-DD format
        dimension_filters: Dictionary mapping dimension IDs to lists of values.
                          Example: {"country": ["USA", "Canada"], "channel": ["Web"]}
        date_range_type: 'absolute' for fixed dates or 'relative' for presets
        relative_date_preset: Relative date preset like 'last_7_days', 'this_month'

    Returns:
        SQL WHERE clause string including the "WHERE" keyword if non-empty,
        otherwise returns empty string. Multiple conditions are AND-ed together.

    Example:
        Simple date filter:

        >>> where = service.build_filter_clause(
        ...     start_date="2024-01-01",
        ...     end_date="2024-01-31"
        ... )
        "WHERE date BETWEEN '2024-01-01' AND '2024-01-31'"

        Multi-select dimension filter:

        >>> where = service.build_filter_clause(
        ...     start_date="2024-01-01",
        ...     end_date="2024-01-31",
        ...     dimension_filters={"country": ["USA", "Canada"], "channel": ["Web"]}
        ... )
        "WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
         AND country IN ('USA', 'Canada')
         AND channel = 'Web'"

        Relative date:

        >>> where = service.build_filter_clause(
        ...     date_range_type="relative",
        ...     relative_date_preset="last_7_days"
        ... )
        # Resolves to absolute dates for the last 7 days
    """
```

## Frontend TypeScript Example - api.ts

### File Header JSDoc
```typescript
/**
 * API client for search analytics backend.
 *
 * This module provides typed functions for all backend API endpoints. All functions
 * use fetch with proper error handling and type safety via TypeScript and Zod validation.
 *
 * Features:
 * - Type-safe API calls with Zod schema validation
 * - Automatic query string building for filters
 * - Multi-select filter support
 * - Relative date resolution
 * - Error handling with descriptive messages
 * - Support for multi-table operations
 *
 * Architecture:
 * - Each API endpoint has a dedicated function
 * - Filter parameters are converted to query strings
 * - Responses are validated against Zod schemas
 * - Errors are thrown with context for React Query
 *
 * @module lib/api
 */
```

### Function JSDoc Examples
```typescript
/**
 * Fetch pivot table data with dynamic dimension grouping.
 *
 * Retrieves aggregated metrics grouped by one or more dimensions. Supports pagination,
 * sorting, and complex filtering. Can fetch specific dimension values for multi-table
 * widgets or top-N results sorted by a metric.
 *
 * @param dimensions - Array of dimension IDs to group by (e.g., ["country", "channel"])
 * @param filters - Filter parameters including dates and dimension filters
 * @param limit - Maximum number of rows to return (default: 50)
 * @param offset - Number of rows to skip for pagination (default: 0)
 * @param dimensionValues - Optional array of specific dimension values to fetch (for multi-table matching)
 * @param tableId - Optional table ID for multi-table widget support
 * @param skipCount - Skip the total count query for faster initial loads (default: false)
 * @param metrics - Optional array of metric IDs to calculate (default: all metrics from schema)
 *
 * @returns Promise resolving to PivotResponse with rows, totals, and metadata
 *
 * @throws {Error} When the API request fails or returns invalid data
 *
 * @example
 * Basic pivot by single dimension:
 * ```tsx
 * const { data } = useQuery({
 *   queryKey: ['pivot', 'country', filters],
 *   queryFn: () => fetchPivotData(['country'], filters, 50, 0)
 * });
 * ```
 *
 * @example
 * Multi-dimensional pivot:
 * ```tsx
 * const data = await fetchPivotData(
 *   ['country', 'channel'],
 *   filters,
 *   100,
 *   0
 * );
 * ```
 *
 * @example
 * With specific metrics:
 * ```tsx
 * const data = await fetchPivotData(
 *   ['country'],
 *   filters,
 *   50,
 *   0,
 *   undefined,
 *   tableId,
 *   false,
 *   ['queries', 'revenue', 'ctr']  // Only calculate these metrics
 * );
 * ```
 */
export async function fetchPivotData(
  dimensions: string[],
  filters: FilterState,
  limit: number = 50,
  offset: number = 0,
  dimensionValues?: string[],
  tableId?: string,
  skipCount: boolean = false,
  metrics?: string[]
): Promise<PivotResponse> {
  // Implementation...
}
```

```typescript
/**
 * Create a new calculated metric with formula.
 *
 * Creates a calculated metric by parsing a user-friendly formula into SQL.
 * The formula can reference base metrics and other calculated metrics using
 * {metric_id} syntax. Division operations are automatically converted to SAFE_DIVIDE.
 *
 * @param metric - Calculated metric creation data with formula and display settings
 * @param tableId - Optional table ID for multi-table support
 *
 * @returns Promise resolving to the created CalculatedMetric with parsed SQL
 *
 * @throws {Error} When formula syntax is invalid or references unknown metrics
 * @throws {Error} When circular dependencies are detected
 *
 * @example
 * Simple ratio metric:
 * ```tsx
 * const ctr = await createCalculatedMetric({
 *   display_name: "Click-Through Rate",
 *   formula: "{clicks} / {impressions}",
 *   format_type: "percent",
 *   decimal_places: 2,
 *   category: "conversion"
 * });
 * // Generated SQL: SAFE_DIVIDE(SUM(clicks), SUM(impressions))
 * ```
 *
 * @example
 * Complex formula with multiple operations:
 * ```tsx
 * const metric = await createCalculatedMetric({
 *   display_name: "Adjusted Revenue",
 *   formula: "({revenue} - {refunds}) / {orders}",
 *   format_type: "currency",
 *   decimal_places: 2,
 *   category: "revenue"
 * });
 * ```
 */
export async function createCalculatedMetric(
  metric: CalculatedMetricCreate,
  tableId?: string
): Promise<CalculatedMetric> {
  // Implementation...
}
```

## React Component Example - PivotTableSection.tsx

```typescript
/**
 * Pivot table section component with hierarchical drill-down.
 *
 * Displays aggregated metrics grouped by selectable dimensions with expandable rows
 * that show search term details. Supports pagination, column visibility management,
 * sorting, and CSV export.
 *
 * Features:
 * - Dynamic dimension selection
 * - Hierarchical row expansion with search term drill-down
 * - Paginated children (100 search terms per page)
 * - Column visibility controls
 * - Cumulative percentage calculations
 * - CSV export of visible data
 * - Responsive loading and error states
 *
 * @component
 *
 * @param {Object} props - Component props
 * @param {FilterState} props.filters - Current filter state from FilterContext
 *
 * @returns {JSX.Element} Rendered pivot table section
 *
 * @example
 * ```tsx
 * <PivotTableSection filters={filters} />
 * ```
 */
export function PivotTableSection({ filters }: PivotTableSectionProps) {
  // Implementation...
}
```

## Custom Hook Example - use-pivot-config.ts

```typescript
/**
 * Hook for managing pivot table configuration state.
 *
 * Manages the selected dimension, pagination, and expanded row state for
 * the pivot table. Provides callbacks for changing dimensions and toggling
 * row expansion.
 *
 * @param {string} initialDimension - Initial dimension to group by (default: 'n_words_normalized')
 *
 * @returns {Object} Pivot configuration state and control functions
 * @returns {string} returns.dimension - Currently selected dimension ID
 * @returns {Function} returns.setDimension - Callback to change dimension
 * @returns {number} returns.limit - Number of rows per page
 * @returns {number} returns.offset - Current pagination offset
 * @returns {Function} returns.setOffset - Callback to change offset
 * @returns {Set<string>} returns.expandedRows - Set of expanded row dimension values
 * @returns {Function} returns.toggleExpanded - Callback to toggle row expansion
 *
 * @example
 * ```tsx
 * function MyPivotTable() {
 *   const {
 *     dimension,
 *     setDimension,
 *     expandedRows,
 *     toggleExpanded
 *   } = usePivotConfig('country');
 *
 *   return (
 *     <Select value={dimension} onChange={(e) => setDimension(e.target.value)}>
 *       <option value="country">Country</option>
 *       <option value="channel">Channel</option>
 *     </Select>
 *   );
 * }
 * ```
 */
export function usePivotConfig(initialDimension: string = 'n_words_normalized') {
  // Implementation...
}
```

