"""
BigQuery service for querying search analytics data.
"""
import os
import json
import logging
import tempfile
import time
from typing import Optional, Dict, List, Tuple, Any
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from .date_resolver import resolve_relative_date
from config import app_settings

logger = logging.getLogger(__name__)


class BigQueryService:
    """Service for querying BigQuery data."""

    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
        credentials_path: Optional[str] = None,
        table_id: Optional[str] = None,
        billing_project: Optional[str] = None
    ):
        """
        Initialize BigQuery service.

        Args:
            project_id: GCP project ID (where the data resides)
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON (optional)
            table_id: Table ID for multi-table support (optional)
            billing_project: GCP project ID for query billing (optional, defaults to project_id)
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_path = f"{project_id}.{dataset}.{table}"
        self.table_id = table_id
        # Billing project: use specified billing_project, then global default, then fall back to project_id
        self.billing_project = billing_project or app_settings.get_default_billing_project() or project_id

        # Date limits (optional)
        self.allowed_min_date: Optional[str] = None
        self.allowed_max_date: Optional[str] = None

        # Date range cache: {cache_key: (min_date, max_date, num_days)}
        self._date_range_cache: Dict[str, tuple] = {}

        # Count cache: {cache_key: total_count}
        self._count_cache: Dict[str, int] = {}

        # Create BigQuery client with billing project
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=self.billing_project)
        else:
            self.client = bigquery.Client(project=self.billing_project)

        # Initialize schema service and load schema
        self.schema_service = None
        self.schema_config = None
        self._load_schema()

    def set_date_limits(self, min_date: Optional[str] = None, max_date: Optional[str] = None) -> None:
        """
        Set allowed date range for queries.

        Args:
            min_date: Minimum allowed date (YYYY-MM-DD format)
            max_date: Maximum allowed date (YYYY-MM-DD format)
        """
        self.allowed_min_date = min_date
        self.allowed_max_date = max_date

    def _load_schema(self) -> None:
        """Load schema configuration from SchemaService."""
        try:
            from services.schema_service import SchemaService

            self.schema_service = SchemaService(self.client, table_id=self.table_id)

            # Try to load existing schema or create default
            self.schema_config = self.schema_service.load_schema()

            if not self.schema_config:
                # Auto-detect and create schema on first run
                self.schema_config = self.schema_service.get_or_create_schema(
                    self.project_id,
                    self.dataset,
                    self.table,
                    auto_detect=True
                )
        except Exception as e:
            print(f"Warning: Failed to load schema: {e}")
            print("BigQuery service will continue without dynamic schema")
            self.schema_service = None
            self.schema_config = None

    def _build_metric_select_clause(self, include_search_term: bool = False) -> str:
        """
        Build SELECT clause dynamically from schema.

        Args:
            include_search_term: Whether to include search_term column

        Returns:
            Comma-separated SELECT clause with aggregated metrics
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded. Cannot build metric SELECT clause without schema configuration.")

        select_parts = []

        # Add search_term if requested (not aggregated)
        if include_search_term:
            select_parts.append("search_term")

        # Add all base metrics with their aggregations
        for metric in self.schema_config.base_metrics:
            # Special handling for virtual metrics
            if metric.is_system and metric.id == 'days_in_range':
                # Virtual metric: compute days between min and max date in this group
                select_parts.append(f"DATE_DIFF(MAX(date), MIN(date), DAY) + 1 as {metric.id}")
            elif metric.aggregation == 'COUNT_DISTINCT':
                # Support multi-column COUNT_DISTINCT: column_name can be "col1, col2, col3"
                # BigQuery syntax: COUNT(DISTINCT col1, col2, col3)
                columns = metric.column_name.strip()
                select_parts.append(f"COUNT(DISTINCT {columns}) as {metric.id}")
            elif metric.aggregation == 'APPROX_COUNT_DISTINCT':
                # Approximate count distinct - faster and cheaper for large datasets (~2% error)
                # Support multi-column with FARM_FINGERPRINT for combining columns
                columns = metric.column_name.strip()
                if ',' in columns:
                    # Multiple columns: use FARM_FINGERPRINT(CONCAT(...)) for hashing
                    col_list = [c.strip() for c in columns.split(',')]
                    concat_cols = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in col_list])
                    select_parts.append(f"APPROX_COUNT_DISTINCT(FARM_FINGERPRINT(CONCAT({concat_cols}))) as {metric.id}")
                else:
                    select_parts.append(f"APPROX_COUNT_DISTINCT({columns}) as {metric.id}")
            else:
                select_parts.append(f"{metric.aggregation}({metric.column_name}) as {metric.id}")

        # Add "simple" calculated metrics that have no dependencies (raw SQL aggregations)
        # These must be computed at SQL level since they're not derived from other metrics
        if self.schema_config.calculated_metrics:
            for calc_metric in self.schema_config.calculated_metrics:
                # Check if this metric has no dependencies - it's a raw SQL aggregation
                if not calc_metric.depends_on or len(calc_metric.depends_on) == 0:
                    select_parts.append(f"{calc_metric.sql_expression} as {calc_metric.id}")

        return ",\n                ".join(select_parts)

    def _build_dimension_columns(self) -> List[str]:
        """
        Get list of dimension column names from schema.

        Returns:
            List of dimension column names
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded. Cannot get dimension columns without schema configuration.")

        return [dim.column_name for dim in self.schema_config.dimensions]

    def _get_calculated_dimension(self, dimension_id: str):
        """
        Get a calculated dimension definition by ID from schema.

        Args:
            dimension_id: The calculated dimension ID to look up

        Returns:
            CalculatedDimensionDef if found, None otherwise
        """
        if not self.schema_config:
            return None

        if not hasattr(self.schema_config, 'calculated_dimensions') or not self.schema_config.calculated_dimensions:
            return None

        return next(
            (d for d in self.schema_config.calculated_dimensions if d.id == dimension_id),
            None
        )

    def _is_calculated_dimension(self, dimension_id: str) -> bool:
        """
        Check if a dimension ID refers to a calculated dimension.

        Args:
            dimension_id: Dimension ID to check

        Returns:
            True if it's a calculated dimension
        """
        return self._get_calculated_dimension(dimension_id) is not None

    def _get_regular_dimension(self, dimension_id: str):
        """
        Get a regular dimension definition by ID from schema.

        Args:
            dimension_id: The dimension ID to look up

        Returns:
            DimensionDef if found, None otherwise
        """
        if not self.schema_config:
            return None

        return next(
            (d for d in self.schema_config.dimensions if d.id == dimension_id),
            None
        )

    def build_subquery_with_calculated_dimensions(
        self,
        calculated_dim_ids: List[str],
        base_where_clause: str = ""
    ) -> str:
        """
        Build a subquery that computes calculated dimension values.

        This wraps the base table in a subquery that adds calculated dimension columns.
        The outer query can then GROUP BY or filter on these calculated dimension aliases.

        Args:
            calculated_dim_ids: List of calculated dimension IDs to include
            base_where_clause: WHERE clause for base filters (dates, regular dimensions)

        Returns:
            Subquery SQL string ready to be used as FROM source
        """
        if not calculated_dim_ids:
            # No calculated dimensions - just return table reference
            return f"`{self.table_path}`"

        # Build calculated dimension expressions
        calc_dim_expressions = []
        for dim_id in calculated_dim_ids:
            calc_dim = self._get_calculated_dimension(dim_id)
            if calc_dim:
                # Wrap expression in parentheses and alias with dimension ID
                calc_dim_expressions.append(f"({calc_dim.sql_expression}) AS {dim_id}")

        if not calc_dim_expressions:
            # No valid calculated dimensions found
            return f"`{self.table_path}`"

        calc_dims_str = ",\n                ".join(calc_dim_expressions)

        return f"""(
            SELECT *,
                {calc_dims_str}
            FROM `{self.table_path}`
            {base_where_clause}
        )"""

    def build_filter_clause_for_calculated_dimension(
        self,
        dimension_id: str,
        values: List[str]
    ) -> str:
        """
        Build WHERE clause condition for filtering on a calculated dimension alias.

        This generates a filter condition that references the calculated dimension
        by its alias (since it's computed in an inner subquery).

        Args:
            dimension_id: Calculated dimension ID (used as alias in subquery)
            values: List of values to filter by

        Returns:
            WHERE condition string (e.g., "rec_id IN ('val1', 'val2')")
        """
        calc_dim = self._get_calculated_dimension(dimension_id)
        if not calc_dim:
            raise ValueError(f"Calculated dimension '{dimension_id}' not found")

        data_type = calc_dim.data_type

        # Handle special __NULL__ marker
        null_marker = "__NULL__"
        has_null = null_marker in values
        non_null_values = [v for v in values if v != null_marker]

        filter_parts = []

        if non_null_values:
            if len(non_null_values) == 1:
                # Single value - use equality
                value = non_null_values[0]
                if data_type in ["STRING", "DATE"]:
                    escaped_value = value.replace("'", "''")
                    filter_parts.append(f"{dimension_id} = '{escaped_value}'")
                elif data_type == "BOOLEAN":
                    bool_value = "TRUE" if value.lower() in ["true", "1", "yes"] else "FALSE"
                    filter_parts.append(f"{dimension_id} = {bool_value}")
                else:  # INTEGER, FLOAT
                    filter_parts.append(f"{dimension_id} = {value}")
            else:
                # Multiple values - use IN clause
                if data_type in ["STRING", "DATE"]:
                    escaped_values = [v.replace("'", "''") for v in non_null_values]
                    values_str = "', '".join(escaped_values)
                    filter_parts.append(f"{dimension_id} IN ('{values_str}')")
                elif data_type == "BOOLEAN":
                    bool_values = ", ".join([
                        "TRUE" if v.lower() in ["true", "1", "yes"] else "FALSE"
                        for v in non_null_values
                    ])
                    filter_parts.append(f"{dimension_id} IN ({bool_values})")
                else:  # INTEGER, FLOAT
                    values_str = ", ".join(non_null_values)
                    filter_parts.append(f"{dimension_id} IN ({values_str})")

        # Add IS NULL condition if __NULL__ marker was present
        if has_null:
            filter_parts.append(f"{dimension_id} IS NULL")

        # Combine with OR if multiple conditions
        if len(filter_parts) == 1:
            return filter_parts[0]
        elif len(filter_parts) > 1:
            return f"({' OR '.join(filter_parts)})"

        return ""

    def separate_dimension_filters(
        self,
        dimension_filters: Optional[Dict[str, List[str]]]
    ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Separate dimension filters into regular vs calculated dimension filters.

        Args:
            dimension_filters: Dictionary of dimension_id -> values

        Returns:
            Tuple of (regular_filters, calculated_filters)
        """
        if not dimension_filters:
            return {}, {}

        regular_filters = {}
        calculated_filters = {}

        for dim_id, values in dimension_filters.items():
            if self._is_calculated_dimension(dim_id):
                calculated_filters[dim_id] = values
            else:
                regular_filters[dim_id] = values

        return regular_filters, calculated_filters

    def query_calculated_dimension_values(
        self,
        dimension_id: str,
        filters: 'FilterParams',
        limit: int = 1000
    ) -> List[str]:
        """
        Get distinct values for a calculated dimension.

        Uses a subquery to compute the calculated dimension values first,
        then selects distinct values from the result.

        Args:
            dimension_id: Calculated dimension ID
            filters: FilterParams object with filter criteria
            limit: Maximum number of distinct values to return

        Returns:
            List of distinct values for the calculated dimension
        """
        calc_dim = self._get_calculated_dimension(dimension_id)
        if not calc_dim:
            raise ValueError(f"Calculated dimension '{dimension_id}' not found")

        # Build base where clause (excluding calculated dimension filters)
        regular_filters, _ = self.separate_dimension_filters(filters.dimension_filters)

        base_where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=regular_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Build subquery with calculated dimension
        subquery = self.build_subquery_with_calculated_dimensions(
            [dimension_id],
            base_where_clause
        )

        query = f"""
            SELECT DISTINCT {dimension_id} as value
            FROM {subquery}
            WHERE {dimension_id} IS NOT NULL
            ORDER BY value
            LIMIT {limit}
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'dimension': dimension_id
        }

        df = self._execute_and_log_query(
            query=query,
            query_type='calculated_dimension_values',
            endpoint=f'/api/pivot/dimension/{dimension_id}/values',
            filters=filters_dict
        )

        # Convert to list of strings
        return [str(val) for val in df['value'].tolist() if pd.notna(val)]

    def _execute_and_log_query(
        self,
        query: str,
        query_type: str,
        endpoint: str = "unknown",
        filters: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query and log its metrics.
        Includes caching layer - checks cache before BigQuery, stores result after.

        Args:
            query: SQL query to execute
            query_type: Type of query (kpi, trends, pivot, etc.)
            endpoint: API endpoint that triggered the query
            filters: Applied filters as dictionary

        Returns:
            DataFrame with query results
        """
        # Import here to avoid circular dependency
        from services.query_logger import get_query_logger
        from services.query_cache_service import get_query_cache, QueryCacheService

        # Check cache first
        cache = get_query_cache()
        cache_key = QueryCacheService.sql_to_cache_key(query) if cache else None

        if cache and cache_key:
            cached_rows = cache.get(cache_key)
            if cached_rows is not None:
                # Cache hit - return DataFrame from cached rows
                return pd.DataFrame(cached_rows)

        # Cache miss - execute BigQuery query
        start_time = time.time()
        error_msg = None
        bytes_processed = 0
        bytes_billed = 0
        row_count = 0

        try:
            # Execute query
            query_job = self.client.query(query)
            df = query_job.to_dataframe()

            # Get query statistics
            if query_job.total_bytes_processed:
                bytes_processed = query_job.total_bytes_processed
            if query_job.total_bytes_billed:
                bytes_billed = query_job.total_bytes_billed

            row_count = len(df)

            # Store in cache (raw DataFrame as list of dicts)
            if cache and cache_key:
                try:
                    cache.set(
                        cache_key=cache_key,
                        query_type=query_type,
                        table_id=self.table_id or 'default',
                        sql_query=query,
                        result=df.to_dict('records'),
                        row_count=row_count
                    )
                except Exception as cache_error:
                    # Don't fail the query if caching fails
                    pass

            return df

        except Exception as e:
            error_msg = str(e)
            raise

        finally:
            # Log query execution (only for actual BigQuery calls, not cache hits)
            execution_time_ms = int((time.time() - start_time) * 1000)

            logger = get_query_logger()
            if logger:
                try:
                    logger.log_query(
                        endpoint=endpoint,
                        query_type=query_type,
                        bytes_processed=bytes_processed,
                        bytes_billed=bytes_billed,
                        execution_time_ms=execution_time_ms,
                        filters=filters,
                        row_count=row_count,
                        error=error_msg
                    )
                except Exception as log_error:
                    # Don't fail the query if logging fails
                    pass

    def get_date_range_cached(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        dimension_filters: Optional[Dict] = None,
        dimensions: Optional[List[str]] = None,
        date_range_type: Optional[str] = "absolute",
        relative_date_preset: Optional[str] = None
    ) -> tuple:
        """
        Get date range with caching to avoid redundant queries.
        Cache key includes table_id, table_path, dates, filters, and dimensions
        to support multiple widgets with different configurations on the same table.

        Args:
            start_date: Filter start date
            end_date: Filter end date
            dimension_filters: Dimension filters to include in cache key
            dimensions: Dimensions being selected/grouped (affects query results)
            date_range_type: 'absolute' or 'relative'
            relative_date_preset: Relative date preset

        Returns:
            Tuple of (min_date, max_date, num_days)
        """
        import hashlib

        # Create cache key from all parameters that affect the date range query
        # Include dimensions because different groupings can affect available date ranges
        filter_str = (
            f"{self.table_id or 'default'}|"
            f"{self.table_path}|"
            f"{start_date or 'none'}|"
            f"{end_date or 'none'}|"
            f"{str(sorted(dimension_filters.items()) if dimension_filters else 'none')}|"
            f"{str(sorted(dimensions) if dimensions else 'none')}"
        )
        cache_key = hashlib.md5(filter_str.encode()).hexdigest()

        # Check cache - DISABLED FOR DEBUGGING
        # if cache_key in self._date_range_cache:
        #     min_date, max_date, num_days = self._date_range_cache[cache_key]
        #     return min_date, max_date, num_days

        # Cache miss - execute query
        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters,
            date_range_type=date_range_type if 'date_range_type' in locals() else "absolute",
            relative_date_preset=relative_date_preset if 'relative_date_preset' in locals() else None
        )

        date_range_query = f"""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM `{self.table_path}`
            {where_clause}
        """

        date_range_df = self._execute_and_log_query(
            date_range_query,
            query_type="pivot_date_range",
            endpoint="date_range_cache"
        )

        # Calculate number of days
        num_days = 1
        min_date = None
        max_date = None

        if not date_range_df.empty and date_range_df['min_date'].iloc[0] is not None and date_range_df['max_date'].iloc[0] is not None:
            min_date = date_range_df['min_date'].iloc[0]
            max_date = date_range_df['max_date'].iloc[0]
            num_days = (max_date - min_date).days + 1

        # Store in cache - DISABLED FOR DEBUGGING
        # self._date_range_cache[cache_key] = (min_date, max_date, num_days)

        return min_date, max_date, num_days

    def get_count_cached(
        self,
        group_cols: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        dimension_filters: Optional[Dict] = None,
        use_approx: bool = False
    ) -> int:
        """
        Get count of distinct dimension values with caching to avoid redundant queries.
        Cache key includes table_id, table_path, dates, filters, and dimensions.

        Args:
            group_cols: List of columns to group by (dimension columns)
            start_date: Filter start date
            end_date: Filter end date
            dimension_filters: Dimension filters to include in cache key
            use_approx: Use APPROX_COUNT_DISTINCT for very large datasets (faster but approximate)

        Returns:
            Total count of dimension value combinations
        """
        import hashlib

        # Create cache key from all parameters that affect the count query
        filter_str = (
            f"{self.table_id or 'default'}|"
            f"{self.table_path}|"
            f"count|"
            f"{','.join(sorted(group_cols))}|"
            f"{start_date or 'none'}|"
            f"{end_date or 'none'}|"
            f"{str(sorted(dimension_filters.items()) if dimension_filters else 'none')}|"
            f"approx={use_approx}"
        )
        cache_key = hashlib.md5(filter_str.encode()).hexdigest()

        # Check cache - DISABLED FOR DEBUGGING
        # if cache_key in self._count_cache:
        #     total_count = self._count_cache[cache_key]
        #     return total_count

        # Cache miss - execute query
        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters,
            date_range_type=date_range_type if 'date_range_type' in locals() else "absolute",
            relative_date_preset=relative_date_preset if 'relative_date_preset' in locals() else None
        )

        # Quote column names if they contain special characters
        quoted_group_cols = [self._quote_column_name(col) for col in group_cols]
        group_by_clause = ", ".join(quoted_group_cols)

        # Build count query - use approximate or exact based on parameter
        if len(quoted_group_cols) > 1:
            # For multiple dimensions, count distinct combinations
            if use_approx:
                # Cast all columns to STRING for CONCAT to handle mixed types (STRING, INT64, DATE, etc.)
                cast_cols = [f"CAST({col} AS STRING)" for col in quoted_group_cols]
                concat_expr = ', "-", '.join(cast_cols)
                count_query = f"""
                    SELECT APPROX_COUNT_DISTINCT(CONCAT({concat_expr})) as total_count
                    FROM `{self.table_path}`
                    {where_clause}
                """
            else:
                count_query = f"""
                    SELECT COUNT(*) as total_count
                    FROM (
                        SELECT {group_by_clause}
                        FROM `{self.table_path}`
                        {where_clause}
                        GROUP BY {group_by_clause}
                    )
                """
        else:
            # For single dimension, use COUNT DISTINCT or APPROX_COUNT_DISTINCT
            if use_approx:
                count_query = f"""
                    SELECT APPROX_COUNT_DISTINCT({group_by_clause}) as total_count
                    FROM `{self.table_path}`
                    {where_clause}
                """
            else:
                count_query = f"""
                    SELECT COUNT(DISTINCT {group_by_clause}) as total_count
                    FROM `{self.table_path}`
                    {where_clause}
                """

        count_df = self._execute_and_log_query(
            count_query,
            query_type="pivot_count",
            endpoint="count_cache"
        )

        total_count = int(count_df['total_count'].iloc[0]) if not count_df.empty else 0

        # Store in cache - DISABLED FOR DEBUGGING
        # self._count_cache[cache_key] = total_count

        return total_count

    def _resolve_dates(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        date_range_type: Optional[str] = "absolute",
        relative_date_preset: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Resolve dates from either absolute or relative date range.

        Args:
            start_date: Absolute start date (YYYY-MM-DD)
            end_date: Absolute end date (YYYY-MM-DD)
            date_range_type: 'absolute' or 'relative'
            relative_date_preset: Relative preset (e.g., 'last_7_days')

        Returns:
            Tuple of (resolved_start_date, resolved_end_date)
        """
        # If relative date type, resolve the preset
        if date_range_type == "relative" and relative_date_preset:
            try:
                return resolve_relative_date(relative_date_preset)
            except ValueError as e:
                # If invalid preset, fall back to provided absolute dates
                print(f"Warning: Invalid relative date preset '{relative_date_preset}': {e}")
                return start_date, end_date

        # Otherwise use absolute dates
        return start_date, end_date

    def _quote_column_name(self, column_name: str) -> str:
        """
        Quote column name with backticks if needed for BigQuery.

        BigQuery requires backticks for column names containing special characters like hyphens.

        Args:
            column_name: The column name to potentially quote

        Returns:
            Column name wrapped in backticks if it contains special characters
        """
        # Check if column name needs quoting (contains special chars except underscore)
        needs_quoting = any(char in column_name for char in ['-', ' ', '.', ':', '/', '\\', '(', ')', '[', ']'])

        if needs_quoting:
            # Remove existing backticks if any and re-add them
            clean_name = column_name.strip('`')
            return f"`{clean_name}`"

        return column_name

    def _clamp_dates(
        self,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Clamp dates to allowed range.

        Args:
            start_date: Requested start date
            end_date: Requested end date

        Returns:
            Tuple of (clamped_start_date, clamped_end_date)
        """
        clamped_start = start_date
        clamped_end = end_date

        # Clamp start_date to allowed_min_date
        if self.allowed_min_date and clamped_start:
            if clamped_start < self.allowed_min_date:
                clamped_start = self.allowed_min_date
        elif self.allowed_min_date and not clamped_start:
            # If no start_date provided but min limit exists, use min limit
            clamped_start = self.allowed_min_date

        # Clamp end_date to allowed_max_date
        if self.allowed_max_date and clamped_end:
            if clamped_end > self.allowed_max_date:
                clamped_end = self.allowed_max_date
        elif self.allowed_max_date and not clamped_end:
            # If no end_date provided but max limit exists, use max limit
            clamped_end = self.allowed_max_date

        return clamped_start, clamped_end

    def build_filter_clause(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dimension_filters: Optional[Dict[str, List[str]]] = None,
        date_range_type: Optional[str] = "absolute",
        relative_date_preset: Optional[str] = None,
        use_dimension_ids: bool = False,
    ) -> str:
        """
        Build WHERE clause from filter parameters - fully dynamic.

        Args:
            start_date: Start date for date range filter (YYYY-MM-DD)
            end_date: End date for date range filter (YYYY-MM-DD)
            dimension_filters: Dictionary mapping dimension IDs to lists of values for multi-select filtering
                              Example: {"country": ["USA", "Canada"], "channel": ["Web"]}
            date_range_type: 'absolute' or 'relative'
            relative_date_preset: Relative date preset (e.g., 'last_7_days')
            use_dimension_ids: If True, use dimension IDs as column names (for rollup tables)
                              If False, use column_name from schema (for source tables)

        Returns:
            WHERE clause string (includes "WHERE" keyword if non-empty)
        """
        conditions = []

        # Resolve relative dates to absolute dates
        resolved_start, resolved_end = self._resolve_dates(
            start_date, end_date, date_range_type, relative_date_preset
        )

        # Apply date clamping
        clamped_start, clamped_end = self._clamp_dates(resolved_start, resolved_end)

        # Date range filter (special handling for date dimension)
        if clamped_start and clamped_end:
            conditions.append(f"date BETWEEN '{clamped_start}' AND '{clamped_end}'")
        elif clamped_start:
            conditions.append(f"date >= '{clamped_start}'")
        elif clamped_end:
            conditions.append(f"date <= '{clamped_end}'")

        # Dynamic dimension filters
        if dimension_filters:
            from services.custom_dimension_service import get_custom_dimension_service

            for dimension_id, values in dimension_filters.items():
                if not values:  # Skip empty filter arrays
                    continue

                # Handle custom dimensions - they are virtual dimensions that need to be resolved
                if dimension_id.startswith("custom_"):
                    # Extract the UUID part after "custom_"
                    custom_dim_id = dimension_id.replace("custom_", "")
                    custom_dim_service = get_custom_dimension_service()
                    custom_dim = custom_dim_service.get_by_id(custom_dim_id)

                    if not custom_dim:
                        continue  # Skip if custom dimension not found

                    if custom_dim.type == "date_range":
                        # For date_range custom dimensions: Resolve to date conditions
                        from services.date_resolver import resolve_relative_date
                        date_conditions = []
                        for value_label in values:
                            matching_value = next((v for v in custom_dim.values if v.label == value_label), None)
                            if matching_value:
                                # Resolve relative dates to absolute dates if needed
                                if matching_value.date_range_type == 'relative' and matching_value.relative_date_preset:
                                    start_date, end_date = resolve_relative_date(matching_value.relative_date_preset)
                                else:
                                    start_date = matching_value.start_date
                                    end_date = matching_value.end_date

                                # Only add condition if we have valid dates
                                if start_date and end_date:
                                    date_conditions.append(
                                        f"date BETWEEN '{start_date}' AND '{end_date}'"
                                    )

                        if date_conditions:
                            if len(date_conditions) == 1:
                                conditions.append(date_conditions[0])
                            else:
                                # Multiple date ranges with OR
                                conditions.append(f"({' OR '.join(date_conditions)})")

                    elif custom_dim.type == "metric_condition":
                        # For metric_condition custom dimensions: Resolve to metric conditions
                        metric_conditions = []
                        for value_label in values:
                            matching_value = next((v for v in custom_dim.metric_values if v.label == value_label), None)
                            if matching_value:
                                # Build condition SQL from the metric conditions
                                condition_sql = self.build_metric_condition_sql(
                                    custom_dim.metric,
                                    [c.dict() for c in matching_value.conditions]
                                )
                                metric_conditions.append(condition_sql)

                        if metric_conditions:
                            if len(metric_conditions) == 1:
                                conditions.append(metric_conditions[0])
                            else:
                                # Multiple metric conditions with OR
                                conditions.append(f"({' OR '.join(metric_conditions)})")

                    continue  # Skip to next filter

                # Find dimension in schema to get column name and data type
                dimension_def = None
                if self.schema_config:
                    dimension_def = next((d for d in self.schema_config.dimensions if d.id == dimension_id), None)

                # For rollup tables, use dimension ID as column name (rollups use dimension IDs as columns)
                # For source tables, use column_name from schema (actual column name in BigQuery)
                if use_dimension_ids:
                    column_name = dimension_id
                else:
                    column_name = dimension_def.column_name if dimension_def else dimension_id
                data_type = dimension_def.data_type if dimension_def else "STRING"

                # Quote column name if it contains special characters
                quoted_column_name = self._quote_column_name(column_name)

                # Handle special __NULL__ marker - convert to IS NULL condition
                null_marker = "__NULL__"
                has_null = null_marker in values
                non_null_values = [v for v in values if v != null_marker]

                # Build filter condition based on data type
                filter_parts = []

                if non_null_values:
                    if len(non_null_values) == 1:
                        # Single value - use equality
                        value = non_null_values[0]
                        if data_type in ["STRING", "DATE"]:
                            # Escape single quotes in string values
                            escaped_value = value.replace("'", "''")
                            filter_parts.append(f"{quoted_column_name} = '{escaped_value}'")
                        elif data_type == "BOOLEAN":
                            bool_value = "TRUE" if value.lower() in ["true", "1", "yes"] else "FALSE"
                            filter_parts.append(f"{quoted_column_name} = {bool_value}")
                        else:  # INTEGER, FLOAT
                            filter_parts.append(f"{quoted_column_name} = {value}")
                    else:
                        # Multiple values - use IN clause with OR logic
                        if data_type in ["STRING", "DATE"]:
                            # Escape single quotes in string values
                            escaped_values = [v.replace("'", "''") for v in non_null_values]
                            values_str = "', '".join(escaped_values)
                            filter_parts.append(f"{quoted_column_name} IN ('{values_str}')")
                        elif data_type == "BOOLEAN":
                            bool_values = ", ".join([
                                "TRUE" if v.lower() in ["true", "1", "yes"] else "FALSE"
                                for v in non_null_values
                            ])
                            filter_parts.append(f"{quoted_column_name} IN ({bool_values})")
                        else:  # INTEGER, FLOAT
                            values_str = ", ".join(non_null_values)
                            filter_parts.append(f"{quoted_column_name} IN ({values_str})")

                # Add IS NULL condition if __NULL__ marker was present
                if has_null:
                    filter_parts.append(f"{quoted_column_name} IS NULL")

                # Combine with OR if we have both NULL and non-NULL conditions
                if len(filter_parts) == 1:
                    conditions.append(filter_parts[0])
                elif len(filter_parts) > 1:
                    conditions.append(f"({' OR '.join(filter_parts)})")

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        return where_clause

    def build_metric_condition_sql(
        self,
        metric: str,
        conditions: List[Dict]
    ) -> str:
        """
        Build SQL expression from metric conditions.

        Args:
            metric: The metric column name (e.g., 'conversion_rate')
            conditions: List of condition dicts with 'operator', 'value', and optionally 'value_max'

        Returns:
            SQL WHERE clause expression (e.g., "conversion_rate > 0.05 AND conversion_rate < 0.1")
        """
        condition_parts = []

        for cond in conditions:
            operator = cond['operator']
            value = cond.get('value')
            value_max = cond.get('value_max')

            if operator == '>':
                condition_parts.append(f"{metric} > {value}")
            elif operator == '<':
                condition_parts.append(f"{metric} < {value}")
            elif operator == '>=':
                condition_parts.append(f"{metric} >= {value}")
            elif operator == '<=':
                condition_parts.append(f"{metric} <= {value}")
            elif operator == '=':
                condition_parts.append(f"{metric} = {value}")
            elif operator == 'between':
                condition_parts.append(f"{metric} BETWEEN {value} AND {value_max}")
            elif operator == 'is_null':
                condition_parts.append(f"{metric} IS NULL")
            elif operator == 'is_not_null':
                condition_parts.append(f"{metric} IS NOT NULL")

        return " AND ".join(condition_parts) if condition_parts else "TRUE"

    def query_all_data(
        self,
        filters: 'FilterParams'
    ) -> pd.DataFrame:
        """
        Query all data from BigQuery with filters.

        Args:
            filters: FilterParams object with filter criteria

        Returns:
            DataFrame with all columns
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        query = f"""
            SELECT *
            FROM `{self.table_path}`
            {where_clause}
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters
        }

        return self._execute_and_log_query(
            query=query,
            query_type='query_all',
            endpoint='/api/query_all',
            filters=filters_dict
        )

    def query_kpi_metrics(
        self,
        filters: 'FilterParams'
    ) -> Dict:
        """
        Query aggregated KPI metrics dynamically from schema.

        Args:
            filters: FilterParams object with filter criteria

        Returns:
            Dictionary with aggregated metrics (all base metrics from schema)
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Build dynamic SELECT clause from schema
        select_clause = self._build_metric_select_clause()

        query = f"""
            SELECT
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters
        }

        df = self._execute_and_log_query(
            query=query,
            query_type='kpi',
            endpoint='/api/overview',
            filters=filters_dict
        )

        if df.empty:
            return {}

        # Convert DataFrame row to dict dynamically
        row = df.iloc[0]
        result = {}

        # Add all metrics from the query result
        for col in df.columns:
            value = row[col]
            # Convert pandas types to Python native types
            if pd.isna(value):
                result[col] = 0
            elif df[col].dtype in ['int64', 'int32', 'int16', 'int8']:
                result[col] = int(value)
            elif df[col].dtype in ['float64', 'float32']:
                result[col] = float(value)
            else:
                result[col] = value

        return result

    def query_timeseries(
        self,
        filters: 'FilterParams',
        granularity: str = 'daily'
    ) -> pd.DataFrame:
        """
        Query time-series data dynamically from schema.

        Args:
            filters: FilterParams object with filter criteria
            granularity: 'daily', 'weekly', or 'monthly'

        Returns:
            DataFrame with time-series data (all base metrics from schema)
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Map granularity to BigQuery date truncation
        date_trunc_map = {
            'daily': 'DAY',
            'weekly': 'WEEK',
            'monthly': 'MONTH',
        }
        date_trunc = date_trunc_map.get(granularity, 'DAY')

        # Build dynamic SELECT clause from schema
        select_clause = self._build_metric_select_clause()

        query = f"""
            SELECT
                DATE_TRUNC(date, {date_trunc}) as date,
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY date
            ORDER BY date
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'granularity': granularity
        }

        return self._execute_and_log_query(
            query=query,
            query_type='trends',
            endpoint='/api/trends',
            filters=filters_dict
        )

    def query_dimension_breakdown(
        self,
        dimension: str,
        filters: 'FilterParams',
        limit: int = 20
    ) -> pd.DataFrame:
        """
        Query breakdown by dimension dynamically from schema.

        Args:
            dimension: Column to group by (e.g., 'channel', 'country', 'n_words_normalized')
            filters: FilterParams object with filter criteria
            limit: Maximum number of rows to return

        Returns:
            DataFrame with dimension breakdown (all base metrics from schema)
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Find dimension column name and data type from schema
        group_col = dimension
        data_type = "STRING"
        if self.schema_config:
            # Look up dimension by ID to get column name
            dim = next((d for d in self.schema_config.dimensions if d.id == dimension), None)
            if dim:
                group_col = dim.column_name
                data_type = dim.data_type
            else:
                # Fallback: use dimension as is (might be a column name)
                group_col = dimension

        # Build dynamic SELECT clause from schema
        select_clause = self._build_metric_select_clause()

        # Use COALESCE to handle NULL values - convert to '__NULL__' marker for strings, or keep numeric NULLs visible
        # This ensures NULL values are grouped together and can be filtered later
        if data_type in ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC"]:
            # For numeric columns, cast to string and use COALESCE to mark NULLs
            dimension_expr = f"COALESCE(CAST({group_col} AS STRING), '__NULL__')"
        else:
            dimension_expr = f"COALESCE(CAST({group_col} AS STRING), '__NULL__')"

        query = f"""
            SELECT
                {dimension_expr} as dimension_value,
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY dimension_value
            ORDER BY dimension_value
            LIMIT {limit}
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'dimension': dimension,
            'limit': limit
        }

        return self._execute_and_log_query(
            query=query,
            query_type='breakdown',
            endpoint=f'/api/breakdown/{dimension}',
            filters=filters_dict
        )

    def query_search_terms(
        self,
        filters: 'FilterParams',
        limit: int = 100,
        sort_by: str = 'queries'
    ) -> pd.DataFrame:
        """
        Query search terms data dynamically from schema.

        Args:
            filters: FilterParams object with filter criteria
            limit: Maximum number of rows to return
            sort_by: Metric ID to sort by

        Returns:
            DataFrame with search terms and all base metrics from schema
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Build dynamic SELECT clause from schema (including search_term)
        select_clause = self._build_metric_select_clause(include_search_term=True)

        # Validate sort column against available metrics
        # If schema not loaded, use default; otherwise check if metric exists
        if self.schema_config:
            valid_sorts = [m.id for m in self.schema_config.base_metrics]
        else:
            valid_sorts = ['queries', 'purchases', 'revenue', 'queries_pdp', 'queries_a2c']

        if sort_by not in valid_sorts and len(valid_sorts) > 0:
            sort_by = valid_sorts[0]  # Default to first metric

        query = f"""
            SELECT
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY search_term
            ORDER BY {sort_by} DESC
            LIMIT {limit}
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'limit': limit,
            'sort_by': sort_by
        }

        return self._execute_and_log_query(
            query=query,
            query_type='search_terms',
            endpoint='/api/search-terms',
            filters=filters_dict
        )

    def query_dimension_values(
        self,
        dimension: str,
        filters: 'FilterParams'
    ) -> List[str]:
        """
        Get distinct values for a given dimension from schema.

        Args:
            dimension: Dimension ID or column name to get distinct values for
            filters: FilterParams object with filter criteria

        Returns:
            List of distinct values for the dimension
        """
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Find dimension column name from schema
        group_col = dimension
        if self.schema_config:
            # Look up dimension by ID to get column name
            dim = next((d for d in self.schema_config.dimensions if d.id == dimension), None)
            if dim:
                group_col = dim.column_name
            else:
                # Fallback: use dimension as is (might be a column name)
                group_col = dimension

        query = f"""
            SELECT DISTINCT {group_col} as value
            FROM `{self.table_path}`
            {where_clause}
            ORDER BY value
        """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'dimension': dimension
        }

        df = self._execute_and_log_query(
            query=query,
            query_type='dimension_values',
            endpoint=f'/api/pivot/dimension/{dimension}/values',
            filters=filters_dict
        )

        # Convert to list of strings, filtering out any null values
        return [str(val) for val in df['value'].tolist() if pd.notna(val)]

    # =========================================================================
    # Rollup-Aware Query Methods
    # =========================================================================

    def _get_query_router(self):
        """Get query router service if rollups are configured."""
        from services.query_router_service import QueryRouterService
        from services.rollup_service import RollupService

        if not self.schema_config:
            return None

        # Load rollup config
        rollup_service = RollupService(self.client, self.table_id)
        rollup_config = rollup_service.load_config()

        if not rollup_config or not rollup_config.rollups:
            return None

        return QueryRouterService(
            rollup_config=rollup_config,
            schema_config=self.schema_config,
            source_project_id=self.project_id,
            source_dataset=self.dataset
        )

    def _has_distinct_metrics(self, metric_ids: List[str]) -> bool:
        """Check if any of the requested metrics use COUNT_DISTINCT."""
        if not self.schema_config:
            return False

        for metric_id in metric_ids:
            metric = next(
                (m for m in self.schema_config.base_metrics if m.id == metric_id),
                None
            )
            if metric and metric.aggregation in ("COUNT_DISTINCT", "APPROX_COUNT_DISTINCT"):
                return True
        return False

    def query_rollup_table(
        self,
        rollup_table_path: str,
        dimensions: List[str],
        metrics: List[str],
        filters: 'FilterParams',
        needs_reaggregation: bool = False,
        limit: int = 100,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: str = "DESC"
    ) -> pd.DataFrame:
        """
        Query a rollup table directly.

        Args:
            rollup_table_path: Full path to rollup table
            dimensions: Dimension IDs to include
            metrics: Metric IDs to include
            filters: Filter parameters
            needs_reaggregation: Whether to re-aggregate (for SUM metrics)
            limit: Max rows to return
            offset: Row offset for pagination
            sort_by: Metric to sort by
            sort_order: ASC or DESC

        Returns:
            DataFrame with query results
        """
        # For rollup tables, use dimension IDs as column names
        where_clause = self.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset,
            use_dimension_ids=True  # Rollup tables use dimension IDs as column names
        )

        # Build SELECT parts
        select_parts = []
        group_parts = []

        for dim_id in dimensions:
            select_parts.append(dim_id)
            group_parts.append(dim_id)

        if needs_reaggregation:
            # Re-aggregate metrics when collapsing dimensions
            for metric_id in metrics:
                # Check if it's a base metric
                base_metric = next(
                    (m for m in self.schema_config.base_metrics if m.id == metric_id),
                    None
                )
                if base_metric:
                    # Base metric - use its defined aggregation
                    if base_metric.aggregation in ("SUM", "COUNT"):
                        select_parts.append(f"SUM({metric_id}) AS {metric_id}")
                    else:
                        # Non-summable base metric (AVG, etc.)
                        select_parts.append(metric_id)
                else:
                    # Check if it's a calculated metric
                    calc_metric = next(
                        (m for m in self.schema_config.calculated_metrics if m.id == metric_id),
                        None
                    )
                    if calc_metric:
                        # Volume metrics (additive) -> SUM
                        # Conversion/rate metrics -> cannot be summed, need recalculation
                        if calc_metric.category == "volume":
                            select_parts.append(f"SUM({metric_id}) AS {metric_id}")
                        else:
                            # For conversion metrics, we can't just sum them
                            # They need to be recalculated from the summed volume metrics
                            # Skip them here - they'll be recalculated in Python
                            pass
                    else:
                        # Unknown metric - try to sum it
                        select_parts.append(f"SUM({metric_id}) AS {metric_id}")
        else:
            # Direct select (exact match)
            select_parts.extend(metrics)

        # Safeguard: ensure we have something to select
        if not select_parts:
            raise ValueError(
                f"Cannot query rollup: no columns to select. "
                f"Dimensions: {dimensions}, Metrics: {metrics}. "
                f"This may indicate a rollup configuration mismatch."
            )

        # Determine sort metric - must be a column that exists in the rollup table
        # Volume calculated metrics ARE stored in rollups, conversion metrics are NOT
        sort_metric = None
        if sort_by:
            # Check if sort_by is a calculated metric
            calc_metric = next(
                (m for m in (self.schema_config.calculated_metrics if self.schema_config else []) if m.id == sort_by),
                None
            )
            if calc_metric:
                # Volume calculated metrics ARE stored in rollups - can sort in SQL
                # Conversion metrics are computed in Python after query - sort in Python
                if calc_metric.category == "volume":
                    sort_metric = sort_by
                else:
                    sort_metric = None
            else:
                # Base metric - can sort in SQL
                sort_metric = sort_by
        elif metrics:
            # Default to first metric if it's a base metric or volume calculated metric
            first_metric = metrics[0]
            calc_metric = next(
                (m for m in (self.schema_config.calculated_metrics if self.schema_config else []) if m.id == first_metric),
                None
            )
            if calc_metric:
                if calc_metric.category == "volume":
                    sort_metric = first_metric
            else:
                sort_metric = first_metric

        order_clause = f"ORDER BY {sort_metric} {sort_order}" if sort_metric else ""

        if needs_reaggregation:
            # Only include GROUP BY if there are dimensions to group by
            group_clause = f"GROUP BY {', '.join(group_parts)}" if group_parts else ""
            query = f"""
                SELECT {', '.join(select_parts)}
                FROM `{rollup_table_path}`
                {where_clause}
                {group_clause}
                {order_clause}
                LIMIT {limit} OFFSET {offset}
            """
        else:
            query = f"""
                SELECT {', '.join(select_parts)}
                FROM `{rollup_table_path}`
                {where_clause}
                {order_clause}
                LIMIT {limit} OFFSET {offset}
            """

        filters_dict = {
            'start_date': filters.start_date,
            'end_date': filters.end_date,
            'dimension_filters': filters.dimension_filters,
            'dimensions': dimensions,
            'rollup_table': rollup_table_path
        }

        return self._execute_and_log_query(
            query=query,
            query_type='rollup_query',
            endpoint='/api/pivot',
            filters=filters_dict
        )

    def get_route_decision(
        self,
        dimensions: List[str],
        metrics: List[str],
        filters: Optional['FilterParams'] = None,
        require_rollup: bool = False
    ):
        """
        Get routing decision for a query without executing.

        Args:
            dimensions: Dimensions to group by
            metrics: Metrics to aggregate
            filters: Filter parameters
            require_rollup: Error if no suitable rollup

        Returns:
            RouteDecision object
        """
        from services.query_router_service import RouteDecision

        router = self._get_query_router()
        if not router:
            return RouteDecision(
                use_rollup=False,
                reason="No rollups configured"
            )

        filter_dims = filters.dimension_filters if filters else None
        return router.route_query(
            query_dimensions=dimensions,
            query_metrics=metrics,
            query_filters=filter_dims,
            require_rollup=require_rollup
        )

    def query_rollup_aggregates(
        self,
        rollup_table_path: str,
        metric_ids: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dimension_filters: Optional[Dict[str, List[str]]] = None,
        date_range_type: Optional[str] = "absolute",
        relative_date_preset: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query aggregated totals from a rollup table by summing metrics.

        Re-aggregates by summing all rows (collapsing all dimensions)
        while respecting dimension filters. Used for significance testing.

        Args:
            rollup_table_path: Full BigQuery path to rollup table
            metric_ids: List of metric column names to sum
            start_date: Start date for date range filter
            end_date: End date for date range filter
            dimension_filters: Dimension filters to apply
            date_range_type: 'absolute' or 'relative'
            relative_date_preset: Relative date preset

        Returns:
            Dict mapping metric_id to summed value
        """
        # Build SELECT: SUM(metric) for each metric
        select_parts = []
        for metric_id in metric_ids:
            select_parts.append(f"SUM({metric_id}) AS {metric_id}")

        if not select_parts:
            return {}

        # Build WHERE clause from filters (reuse existing method)
        # For rollup tables, use dimension IDs as column names
        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters,
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset,
            use_dimension_ids=True  # Rollup tables use dimension IDs as column names
        )

        query = f"""
        SELECT {', '.join(select_parts)}
        FROM `{rollup_table_path}`
        {where_clause}
        """

        logger.info(f"Querying rollup aggregates from {rollup_table_path}")
        logger.debug(f"Query: {query}")

        job = self.client.query(query)
        result = job.result()

        # Get the single aggregated row
        row = next(iter(result), None)
        if not row:
            # No data matching filters - return zeros
            return {metric_id: 0 for metric_id in metric_ids}

        return {metric_id: row[metric_id] or 0 for metric_id in metric_ids}

    def list_tables_in_dataset(self) -> List[Dict]:
        """
        List all tables in the configured dataset.

        Returns:
            List of dictionaries with table metadata
        """
        dataset_ref = f"{self.project_id}.{self.dataset}"
        tables = self.client.list_tables(dataset_ref)

        table_list = []
        for table in tables:
            # Get detailed table information
            table_ref = self.client.get_table(f"{dataset_ref}.{table.table_id}")
            table_list.append({
                "table_name": table.table_id,
                "description": table_ref.description or "",
                "row_count": table_ref.num_rows,
                "size_mb": round(table_ref.num_bytes / (1024 * 1024), 2),
                "created": table_ref.created.isoformat() if table_ref.created else "",
                "modified": table_ref.modified.isoformat() if table_ref.modified else ""
            })

        return table_list

    def get_table_date_range(self) -> Dict:
        """
        Get the date range for this service's configured table.

        Returns:
            Dictionary with min_date, max_date, and total_rows
        """
        table_path = self.table_path

        # Check if table has a 'date' column
        table_ref = self.client.get_table(table_path)
        has_date_column = any(field.name == 'date' for field in table_ref.schema)

        if not has_date_column:
            return {
                "min_date": None,
                "max_date": None,
                "total_rows": table_ref.num_rows,
                "has_date_column": False
            }

        # Query date range
        query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as total_rows
            FROM `{table_path}`
        """

        df = self.client.query(query).to_dataframe()
        row = df.iloc[0]

        return {
            "min_date": row['min_date'].strftime('%Y-%m-%d') if pd.notna(row['min_date']) else None,
            "max_date": row['max_date'].strftime('%Y-%m-%d') if pd.notna(row['max_date']) else None,
            "total_rows": int(row['total_rows']),
            "has_date_column": True
        }


# Global instances (multi-table support)
_bq_services: Dict[str, BigQueryService] = {}


def get_bigquery_service(table_id: Optional[str] = None, reload_schema: bool = True) -> Optional[BigQueryService]:
    """
    Get BigQuery service instance for a specific table.

    Args:
        table_id: Table ID to get service for. If None, returns None.
        reload_schema: If True, always reload schema from disk to ensure freshness.

    Returns:
        BigQuery service instance or None
    """
    if table_id is None:
        return None

    service = _bq_services.get(table_id)

    # Always reload schema from disk to ensure we have the latest configuration
    if service and reload_schema:
        service._load_schema()

    return service


def clear_bigquery_service(table_id: Optional[str] = None) -> None:
    """
    Clear BigQuery service instance(s).

    Args:
        table_id: Specific table ID to clear. If None, clears all.
    """
    global _bq_services

    if table_id:
        if table_id in _bq_services:
            del _bq_services[table_id]
    else:
        _bq_services = {}


def initialize_bigquery_service(
    project_id: str,
    dataset: str,
    table: str,
    credentials_path: Optional[str] = None,
    table_id: Optional[str] = None,
    billing_project: Optional[str] = None
) -> BigQueryService:
    """
    Initialize BigQuery service for a specific table.

    Args:
        project_id: GCP project ID (where the data resides)
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_path: Path to service account JSON (optional)
        table_id: Table ID to associate with this service
        billing_project: GCP project ID for query billing (optional, defaults to project_id)

    Returns:
        BigQuery service instance
    """
    global _bq_services

    if table_id is None:
        raise ValueError("No table_id specified")

    # Use global default billing project as fallback if not specified
    effective_billing_project = billing_project or app_settings.get_default_billing_project()

    service = BigQueryService(project_id, dataset, table, credentials_path, table_id, effective_billing_project)
    _bq_services[table_id] = service
    return service


def initialize_bigquery_with_json(
    project_id: str,
    dataset: str,
    table: str,
    credentials_json: str,
    table_id: Optional[str] = None,
    billing_project: Optional[str] = None
) -> BigQueryService:
    """
    Initialize BigQuery service with credentials JSON string.

    Args:
        project_id: GCP project ID (where the data resides)
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_json: Service account JSON as string
        table_id: Table ID to associate with this service
        billing_project: GCP project ID for query billing (optional, defaults to project_id)

    Returns:
        BigQuery service instance
    """
    global _bq_services

    if table_id is None:
        raise ValueError("No table_id specified")

    # Billing project: use specified billing_project, then global default, then fall back to project_id
    effective_billing_project = billing_project or app_settings.get_default_billing_project() or project_id

    # Parse JSON and create credentials
    try:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        # Create BigQuery client directly with billing project
        client = bigquery.Client(credentials=credentials, project=effective_billing_project)

        # Create service instance manually
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = project_id
        service.dataset = dataset
        service.table = table
        service.table_path = f"{project_id}.{dataset}.{table}"
        service.client = client
        service.table_id = table_id
        service.billing_project = effective_billing_project
        # Initialize date limit attributes
        service.allowed_min_date = None
        service.allowed_max_date = None
        # Initialize caches
        service._date_range_cache = {}
        service._count_cache = {}

        # Initialize schema attributes and load schema
        service.schema_service = None
        service.schema_config = None
        service._load_schema()

        _bq_services[table_id] = service
        return service
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid credentials JSON: {e}")
    except Exception as e:
        raise ValueError(f"Failed to initialize BigQuery: {e}")


def get_bigquery_info(table_id: Optional[str] = None) -> dict:
    """
    Get comprehensive BigQuery connection and data information.

    Args:
        table_id: Optional table ID to get info for. If None, uses first available table.

    Returns:
        Dictionary with BigQuery metadata
    """
    bq_service = get_bigquery_service(table_id)
    if bq_service is None:
        # Return not-configured status instead of raising error
        from config import table_registry
        # If no table_id provided, try first available table
        if not table_id:
            tables = table_registry.list_tables()
            if tables:
                table_info = tables[0]
                return {
                    "project_id": table_info.project_id,
                    "dataset": table_info.dataset,
                    "table": table_info.table,
                    "table_full_path": f"{table_info.project_id}.{table_info.dataset}.{table_info.table}",
                    "connection_status": "configured but not connected",
                    "date_range": {"min": "", "max": ""},
                    "total_rows": 0,
                    "table_size_mb": 0.0,
                    "last_modified": "",
                    "schema_columns": [],
                    "allowed_min_date": table_info.allowed_min_date,
                    "allowed_max_date": table_info.allowed_max_date
                }
        # No tables configured at all
        return {
            "project_id": "",
            "dataset": "",
            "table": "",
            "table_full_path": "",
            "connection_status": "not configured",
            "date_range": {"min": "", "max": ""},
            "total_rows": 0,
            "table_size_mb": 0.0,
            "last_modified": "",
            "schema_columns": [],
            "allowed_min_date": None,
            "allowed_max_date": None
        }

    try:
        # Get table metadata
        table_ref = bq_service.client.get_table(bq_service.table_path)

        # Date access limits removed - return full date range from table
        # Get date range
        date_query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_service.table_path}`
        """
        date_df = bq_service.client.query(date_query).to_dataframe()

        # Get summary statistics - just row count, no hardcoded columns
        stats_query = f"""
            SELECT
                COUNT(*) as total_rows
            FROM `{bq_service.table_path}`
        """
        stats_df = bq_service.client.query(stats_query).to_dataframe()

        # Extract schema column names
        schema_columns = [field.name for field in table_ref.schema]

        min_date = date_df['min_date'].iloc[0]
        max_date = date_df['max_date'].iloc[0]

        return {
            "project_id": bq_service.project_id,
            "dataset": bq_service.dataset,
            "table": bq_service.table,
            "table_full_path": bq_service.table_path,
            "connection_status": "connected",
            "date_range": {
                "min": min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
                "max": max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None
            },
            "total_rows": int(stats_df['total_rows'].iloc[0]),
            "table_size_mb": round(table_ref.num_bytes / (1024 * 1024), 2),
            "last_modified": table_ref.modified.isoformat(),
            "schema_columns": schema_columns,
            "allowed_min_date": bq_service.allowed_min_date,
            "allowed_max_date": bq_service.allowed_max_date
        }

    except Exception as e:
        return {
            "project_id": bq_service.project_id,
            "dataset": bq_service.dataset,
            "table": bq_service.table,
            "table_full_path": bq_service.table_path,
            "connection_status": f"error: {str(e)}",
            "date_range": {},
            "total_rows": 0,
            "table_size_mb": 0.0,
            "last_modified": "",
            "schema_columns": [],
            "allowed_min_date": bq_service.allowed_min_date,
            "allowed_max_date": bq_service.allowed_max_date
        }
