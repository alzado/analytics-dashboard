"""
BigQuery service for querying search analytics data.
Django implementation of the FastAPI BigQuery service.

This service uses per-user OAuth credentials for BigQuery access.
Each user must authorize BigQuery access before querying data.
"""
import logging
import time
from typing import Optional, Dict, List, Tuple, Any, TYPE_CHECKING
from datetime import datetime, timedelta

from google.cloud import bigquery
from rest_framework.exceptions import AuthenticationFailed
import pandas as pd

from apps.tables.models import BigQueryTable

if TYPE_CHECKING:
    from apps.users.models import User

logger = logging.getLogger(__name__)


class BigQueryService:
    """Service for querying BigQuery data using per-user OAuth credentials."""

    def __init__(self, bigquery_table: BigQueryTable, user: 'User'):
        """
        Initialize BigQuery service with user's OAuth credentials.

        Args:
            bigquery_table: BigQueryTable model instance with connection details
            user: User model instance with GCP OAuth tokens

        Raises:
            AuthenticationFailed: If user hasn't authorized BigQuery access
        """
        self.bigquery_table = bigquery_table
        self.user = user
        self.project_id = bigquery_table.project_id
        self.dataset = bigquery_table.dataset
        self.table = bigquery_table.table_name
        self.table_path = bigquery_table.full_table_path
        self.table_id = str(bigquery_table.id)

        # Billing project: use specified billing_project or fall back to project_id
        self.billing_project = bigquery_table.billing_project or self.project_id

        # Date limits (optional)
        self.allowed_min_date: Optional[str] = bigquery_table.allowed_min_date
        self.allowed_max_date: Optional[str] = bigquery_table.allowed_max_date

        # Initialize client (lazy-loaded)
        self._client = None

        # Schema configuration (loaded lazily)
        self._schema_config = None

    @property
    def client(self) -> bigquery.Client:
        """
        Lazy-load BigQuery client.

        Tries user's OAuth credentials first, falls back to Application Default
        Credentials (ADC) from gcloud CLI if OAuth not configured.
        """
        if self._client is None:
            credentials = None

            # Try user's OAuth credentials first
            if self.user:
                from apps.users.gcp_oauth_service import GCPOAuthService
                credentials = GCPOAuthService.get_valid_credentials(self.user)

            if credentials:
                # Use user's OAuth credentials
                self._client = bigquery.Client(
                    project=self.billing_project,
                    credentials=credentials
                )
                logger.info(f"Using OAuth credentials for user {self.user.email}")
            else:
                # Fall back to Application Default Credentials (gcloud CLI)
                try:
                    self._client = bigquery.Client(project=self.billing_project)
                    logger.info("Using Application Default Credentials (gcloud CLI)")
                except Exception as e:
                    logger.error(f"Failed to create BigQuery client with ADC: {e}")
                    raise AuthenticationFailed(
                        "BigQuery access not available. Please run 'gcloud auth application-default login' "
                        "or authorize BigQuery access in Settings."
                    )

        return self._client

    def refresh_client(self) -> None:
        """Force refresh the BigQuery client (e.g., after token refresh)."""
        self._client = None

    @property
    def schema_config(self):
        """Lazy-load schema configuration."""
        if self._schema_config is None:
            self._load_schema()
        return self._schema_config

    def _load_schema(self) -> None:
        """Load schema configuration from SchemaService."""
        try:
            from apps.schemas.models import SchemaConfig
            self._schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()
        except Exception as e:
            logger.warning(f"Failed to load schema: {e}")
            self._schema_config = None

    def set_date_limits(
        self,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> None:
        """Set allowed date range for queries."""
        self.allowed_min_date = min_date
        self.allowed_max_date = max_date

    def _clamp_dates(
        self,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Clamp requested date range to allowed limits.

        Returns:
            Tuple of (clamped_start_date, clamped_end_date)
        """
        result_start = start_date
        result_end = end_date

        if self.allowed_min_date and start_date:
            if start_date < self.allowed_min_date:
                result_start = self.allowed_min_date

        if self.allowed_max_date and end_date:
            if end_date > self.allowed_max_date:
                result_end = self.allowed_max_date

        return result_start, result_end

    def get_table_info(self) -> dict:
        """Get BigQuery table info and date range."""
        try:
            table_ref = self.table_path
            table_obj = self.client.get_table(table_ref)

            # Get date range
            date_query = f"""
                SELECT
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(*) as total_rows
                FROM `{self.table_path}`
            """
            result = self.client.query(date_query).result()
            row = list(result)[0]

            # Get table size in MB
            table_size_mb = (table_obj.num_bytes or 0) / (1024 * 1024)

            # Get schema column names
            schema_columns = [field.name for field in table_obj.schema]

            return {
                'date_range': {
                    'min': str(row.min_date) if row.min_date else None,
                    'max': str(row.max_date) if row.max_date else None
                },
                'total_rows': row.total_rows,
                'schema_fields': len(table_obj.schema),
                'table_size_mb': round(table_size_mb, 2),
                'last_modified': table_obj.modified.isoformat() if table_obj.modified else None,
                'schema_columns': schema_columns
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            raise

    def build_filter_clause(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dimension_filters: Optional[Dict[str, List[str]]] = None,
        date_range_type: str = "absolute",
        relative_date_preset: Optional[str] = None
    ) -> str:
        """
        Build WHERE clause for BigQuery queries.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimension_filters: Dict of dimension_id -> list of values
            date_range_type: "absolute" or "relative"
            relative_date_preset: Preset like "last_7_days", "last_30_days", etc.

        Returns:
            WHERE clause string (including "WHERE" keyword)
        """
        conditions = []

        # Handle relative dates
        if date_range_type == "relative" and relative_date_preset:
            start_date, end_date = self._resolve_relative_dates(relative_date_preset)

        # Clamp dates to allowed limits
        start_date, end_date = self._clamp_dates(start_date, end_date)

        # Add date conditions
        if start_date:
            conditions.append(f"date >= '{start_date}'")
        if end_date:
            conditions.append(f"date <= '{end_date}'")

        # Add dimension filters
        if dimension_filters:
            for dim_id, values in dimension_filters.items():
                if not values:
                    continue

                # Handle special __NULL__ marker
                null_marker = "__NULL__"
                has_null = null_marker in values
                non_null_values = [v for v in values if v != null_marker]

                filter_parts = []

                # Determine data type from schema (default to STRING)
                data_type = "STRING"
                if self.schema_config:
                    try:
                        dim = self.schema_config.dimensions.get(dimension_id=dim_id)
                        data_type = dim.data_type
                    except Exception:
                        pass  # Use default STRING type

                # Numeric types don't need quotes
                is_numeric = data_type in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOLEAN", "BOOL")

                if non_null_values:
                    if is_numeric:
                        # For numeric types, don't quote values
                        if len(non_null_values) == 1:
                            filter_parts.append(f"{dim_id} = {non_null_values[0]}")
                        else:
                            values_str = ", ".join(non_null_values)
                            filter_parts.append(f"{dim_id} IN ({values_str})")
                    else:
                        # For string types, quote values
                        if len(non_null_values) == 1:
                            escaped_value = non_null_values[0].replace("'", "''")
                            filter_parts.append(f"{dim_id} = '{escaped_value}'")
                        else:
                            escaped_values = [v.replace("'", "''") for v in non_null_values]
                            values_str = "', '".join(escaped_values)
                            filter_parts.append(f"{dim_id} IN ('{values_str}')")

                if has_null:
                    filter_parts.append(f"{dim_id} IS NULL")

                if filter_parts:
                    if len(filter_parts) == 1:
                        conditions.append(filter_parts[0])
                    else:
                        conditions.append(f"({' OR '.join(filter_parts)})")

        if conditions:
            return "WHERE " + " AND ".join(conditions)
        return ""

    def _resolve_relative_dates(
        self,
        preset: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolve relative date preset to absolute dates."""
        today = datetime.now().date()

        presets = {
            'today': (today, today),
            'yesterday': (today - timedelta(days=1), today - timedelta(days=1)),
            'last_7_days': (today - timedelta(days=6), today),
            'last_14_days': (today - timedelta(days=13), today),
            'last_30_days': (today - timedelta(days=29), today),
            'last_90_days': (today - timedelta(days=89), today),
            'this_week': (today - timedelta(days=today.weekday()), today),
            'this_month': (today.replace(day=1), today),
            'last_month': (
                (today.replace(day=1) - timedelta(days=1)).replace(day=1),
                today.replace(day=1) - timedelta(days=1)
            ),
        }

        if preset in presets:
            start, end = presets[preset]
            return str(start), str(end)

        return None, None

    def _build_metric_select_clause(
        self,
        include_search_term: bool = False
    ) -> str:
        """
        Build SELECT clause dynamically from schema.

        Returns:
            Comma-separated SELECT clause with aggregated metrics
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded")

        select_parts = []

        if include_search_term:
            select_parts.append("search_term")

        # Add calculated metrics with their SQL expressions
        for metric in self.schema_config.calculated_metrics.all():
            select_parts.append(f"{metric.sql_expression} as {metric.metric_id}")

        return ",\n                ".join(select_parts)

    def execute_query(
        self,
        query: str,
        query_type: str = "unknown",
        endpoint: str = "unknown",
        filters: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query and return results as DataFrame.

        Args:
            query: SQL query to execute
            query_type: Type of query (for logging)
            endpoint: API endpoint that triggered the query (for logging)
            filters: Applied filters (for logging)

        Returns:
            DataFrame with query results
        """
        start_time = time.time()

        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()

            execution_time = time.time() - start_time
            bytes_processed = query_job.total_bytes_processed or 0
            bytes_billed = query_job.total_bytes_billed or 0

            # Log the query (if audit app is available)
            self._log_query(
                query=query,
                query_type=query_type,
                endpoint=endpoint,
                filters=filters,
                execution_time=execution_time,
                bytes_processed=bytes_processed,
                bytes_billed=bytes_billed,
                row_count=len(df)
            )

            return df

        except Exception as e:
            execution_time = time.time() - start_time
            self._log_query(
                query=query,
                query_type=query_type,
                endpoint=endpoint,
                filters=filters,
                execution_time=execution_time,
                error=str(e)
            )
            raise

    def _log_query(
        self,
        query: str,
        query_type: str,
        endpoint: str,
        filters: Optional[Dict],
        execution_time: float,
        bytes_processed: int = 0,
        bytes_billed: int = 0,
        row_count: int = 0,
        error: Optional[str] = None
    ) -> None:
        """Log query execution to audit system with user attribution."""
        try:
            from apps.audit.models import QueryLog

            QueryLog.objects.create(
                bigquery_table=self.bigquery_table,
                user=self.user,  # Track which user executed the query
                query_type=query_type,
                endpoint=endpoint,
                sql_query=query,
                filters=filters or {},
                execution_time_ms=int(execution_time * 1000),
                bytes_processed=bytes_processed,
                bytes_billed=bytes_billed,
                row_count=row_count,
                error=error,
                is_success=error is None
            )
        except Exception as e:
            logger.warning(f"Failed to log query: {e}")

    def query_pivot_data(
        self,
        dimensions: List[str],
        filters: Dict,
        limit: int = 50,
        offset: int = 0,
        metrics: Optional[List[str]] = None,
        table_path: Optional[str] = None,
        dimension_values: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Query pivot table data grouped by dimensions.

        Args:
            dimensions: List of dimensions to group by
            filters: Filter parameters
            limit: Max rows to return
            offset: Rows to skip
            metrics: Specific metrics to calculate (default: all)
            table_path: Override table path (for rollup queries). Defaults to base table.
            dimension_values: Specific dimension values to fetch (for multi-table matching).
                             When provided, returns only rows matching these values without LIMIT/OFFSET.

        Returns:
            DataFrame with aggregated data
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path
        is_rollup_query = table_path is not None

        # Build WHERE clause
        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        # Build metric SELECT clause
        # For rollup queries, use SUM(metric_id) since metrics are pre-computed
        # For base table queries, use the full SQL expression
        if is_rollup_query:
            metric_select = self._build_rollup_metric_select_clause()
        else:
            metric_select = self._build_metric_select_clause()

        # Build GROUP BY
        if dimensions:
            dim_columns = ", ".join(dimensions)
            group_by = f"GROUP BY {dim_columns}"
            select_dims = f"{dim_columns},"
        else:
            dim_columns = ""
            group_by = ""
            select_dims = ""

        # Build ORDER BY (by first metric descending)
        # For rollup queries, only use volume metrics since conversion metrics
        # don't exist as columns in the rollup table
        order_by = ""
        if self.schema_config:
            if is_rollup_query:
                first_metric = self.schema_config.calculated_metrics.filter(category='volume').first()
            else:
                first_metric = self.schema_config.calculated_metrics.first()
            if first_metric:
                order_by = f"ORDER BY {first_metric.metric_id} DESC"

        # Build dimension values filter (for multi-table matching)
        dimension_values_filter = ""
        if dimension_values and len(dimension_values) > 0 and dimensions:
            connector = "AND" if where_clause else "WHERE"

            if len(dimensions) > 1:
                # Multiple dimensions: filter on CONCAT expression
                # The dimension_values are already in "value1 - value2" format
                cast_cols = [f"COALESCE(CAST({dim} AS STRING), '__NULL__')" for dim in dimensions]
                separator = ', " - ", '
                concat_args = separator.join(cast_cols)
                concat_expr = f"CONCAT({concat_args})"

                # Escape single quotes in values
                escaped_values = [v.replace("'", "''") for v in dimension_values]
                values_list = "', '".join(escaped_values)
                dimension_values_filter = f"{connector} {concat_expr} IN ('{values_list}')"
            else:
                # Single dimension: direct IN clause with NULL handling
                dim_col = dimensions[0]

                # Handle special __NULL__ marker - separate NULL values from regular values
                null_marker = "__NULL__"
                has_null = null_marker in dimension_values
                non_null_values = [v for v in dimension_values if v != null_marker]

                filter_parts = []

                # Determine data type from schema (default to STRING)
                data_type = "STRING"
                if self.schema_config:
                    try:
                        dim = self.schema_config.dimensions.get(dimension_id=dim_col)
                        data_type = dim.data_type
                    except Exception:
                        pass  # Use default STRING type

                # Numeric types don't need quotes
                is_numeric = data_type in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOLEAN", "BOOL")

                # Format non-null values
                if non_null_values:
                    if is_numeric:
                        # For numeric types, don't quote values
                        values_list = ", ".join(non_null_values)
                        filter_parts.append(f"{dim_col} IN ({values_list})")
                    else:
                        # For string types, quote values
                        escaped_values = [v.replace("'", "''") for v in non_null_values]
                        values_list = "', '".join(escaped_values)
                        filter_parts.append(f"{dim_col} IN ('{values_list}')")

                # Add IS NULL condition if __NULL__ marker was present
                if has_null:
                    filter_parts.append(f"{dim_col} IS NULL")

                # Combine filter parts
                if filter_parts:
                    if len(filter_parts) == 1:
                        dimension_values_filter = f"{connector} {filter_parts[0]}"
                    else:
                        dimension_values_filter = f"{connector} ({' OR '.join(filter_parts)})"

        # Build query - different structure depending on whether dimension_values is provided
        if dimension_values and len(dimension_values) > 0:
            # When filtering by specific values, don't use LIMIT/OFFSET
            # Sort by dimension value for consistent ordering across columns
            # Only add ORDER BY if we have dimensions to sort by
            order_by_dims = f"ORDER BY {dim_columns}" if dim_columns else ""
            query = f"""
                SELECT
                    {select_dims}
                    {metric_select}
                FROM `{query_table}`
                {where_clause}
                {dimension_values_filter}
                {group_by}
                {order_by_dims}
            """
        else:
            # Normal query with LIMIT/OFFSET
            query = f"""
                SELECT
                    {select_dims}
                    {metric_select}
                FROM `{query_table}`
                {where_clause}
                {group_by}
                {order_by}
                LIMIT {limit}
                OFFSET {offset}
            """

        return self.execute_query(
            query=query,
            query_type='pivot',
            endpoint='/api/pivot',
            filters=filters
        )

    def _build_rollup_metric_select_clause(self) -> str:
        """
        Build SELECT clause for rollup table queries.

        Rollup tables store pre-computed metrics as columns.
        We SUM these columns to re-aggregate across dimensions.

        Only volume metrics are stored in rollup tables - conversion/rate
        metrics are calculated in Python from the volume metrics.

        Returns:
            Comma-separated SELECT clause with SUM(metric_id) as metric_id
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded")

        select_parts = []

        # Only SUM volume metrics - conversion metrics are calculated in Python
        for metric in self.schema_config.calculated_metrics.filter(category='volume'):
            select_parts.append(f"SUM({metric.metric_id}) as {metric.metric_id}")

        return ",\n                ".join(select_parts)

    def query_dimension_values(
        self,
        dimension: str,
        filters: Dict,
        limit: int = 1000,
        table_path: Optional[str] = None,
        sort_by_metric: Optional[str] = None
    ) -> List[str]:
        """
        Get distinct values for a dimension.

        Args:
            dimension: Dimension column name
            filters: Filter parameters
            limit: Max values to return
            table_path: Optional table path override (for rollup tables)
            sort_by_metric: Optional metric to sort by (descending). If not provided,
                           uses schema's primary_sort_metric if available.

        Returns:
            List of distinct values, sorted by metric if available
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path

        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        # Build NOT NULL condition - use WHERE if no other filters, AND otherwise
        not_null_clause = f"WHERE {dimension} IS NOT NULL" if not where_clause else f"AND {dimension} IS NOT NULL"

        # Determine sort metric: use provided, or fall back to schema's primary_sort_metric
        # Note: Metric sorting only works on rollup tables which have pre-aggregated metrics
        is_rollup_query = table_path is not None
        effective_sort_metric = None
        if is_rollup_query:
            effective_sort_metric = sort_by_metric
            if not effective_sort_metric and self.schema_config:
                effective_sort_metric = self.schema_config.primary_sort_metric

        # Build query - with or without metric sorting
        if effective_sort_metric and is_rollup_query:
            # Sort by metric (descending) - shows highest-volume values first
            query = f"""
                SELECT {dimension} as value, SUM({effective_sort_metric}) as sort_metric
                FROM `{query_table}`
                {where_clause}
                {not_null_clause}
                GROUP BY {dimension}
                ORDER BY sort_metric DESC
                LIMIT {limit}
            """
        else:
            # Fall back to alphabetical sort
            query = f"""
                SELECT DISTINCT {dimension} as value
                FROM `{query_table}`
                {where_clause}
                {not_null_clause}
                ORDER BY value
                LIMIT {limit}
            """

        df = self.execute_query(
            query=query,
            query_type='dimension_values',
            endpoint=f'/api/pivot/dimension/{dimension}/values',
            filters=filters
        )

        return [str(val) for val in df['value'].tolist() if pd.notna(val)]

    def _build_dimension_columns(self) -> List[str]:
        """
        Get list of dimension column names from schema.

        Returns:
            List of dimension column names
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded. Cannot get dimension columns without schema configuration.")

        return [dim.column_name for dim in self.schema_config.dimensions.all()]

    def _get_calculated_dimension(self, dimension_id: str):
        """
        Get a calculated dimension definition by ID from schema.

        Args:
            dimension_id: The calculated dimension ID to look up

        Returns:
            CalculatedDimension if found, None otherwise
        """
        if not self.schema_config:
            return None

        try:
            return self.schema_config.calculated_dimensions.get(dimension_id=dimension_id)
        except Exception:
            return None

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
            Dimension if found, None otherwise
        """
        if not self.schema_config:
            return None

        try:
            return self.schema_config.dimensions.get(dimension_id=dimension_id)
        except Exception:
            return None

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
            return f"`{self.table_path}`"

        calc_dim_expressions = []
        for dim_id in calculated_dim_ids:
            calc_dim = self._get_calculated_dimension(dim_id)
            if calc_dim:
                calc_dim_expressions.append(f"({calc_dim.sql_expression}) AS {dim_id}")

        if not calc_dim_expressions:
            return f"`{self.table_path}`"

        calc_dims_str = ",\n                ".join(calc_dim_expressions)

        return f"""(
            SELECT *,
                {calc_dims_str}
            FROM `{self.table_path}`
            {base_where_clause}
        )"""

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

    def _quote_column_name(self, column_name: str) -> str:
        """
        Quote column name with backticks if needed for BigQuery.

        BigQuery requires backticks for column names containing special characters like hyphens.

        Args:
            column_name: The column name to potentially quote

        Returns:
            Column name wrapped in backticks if it contains special characters
        """
        needs_quoting = any(char in column_name for char in ['-', ' ', '.', ':', '/', '\\', '(', ')', '[', ']'])

        if needs_quoting:
            clean_name = column_name.strip('`')
            return f"`{clean_name}`"

        return column_name

    def get_date_range_cached(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        dimension_filters: Optional[Dict] = None,
        dimensions: Optional[List[str]] = None,
        date_range_type: Optional[str] = "absolute",
        relative_date_preset: Optional[str] = None
    ) -> Tuple[Any, Any, int]:
        """
        Get date range for pivot calculations.

        Args:
            start_date: Filter start date
            end_date: Filter end date
            dimension_filters: Dimension filters
            dimensions: Dimensions being grouped
            date_range_type: 'absolute' or 'relative'
            relative_date_preset: Relative date preset

        Returns:
            Tuple of (min_date, max_date, num_days)
        """
        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters,
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset
        )

        date_range_query = f"""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM `{self.table_path}`
            {where_clause}
        """

        date_range_df = self.execute_query(
            date_range_query,
            query_type="pivot_date_range",
            endpoint="date_range_cache"
        )

        num_days = 1
        min_date = None
        max_date = None

        if not date_range_df.empty and pd.notna(date_range_df['min_date'].iloc[0]) and pd.notna(date_range_df['max_date'].iloc[0]):
            min_date = date_range_df['min_date'].iloc[0]
            max_date = date_range_df['max_date'].iloc[0]
            num_days = (max_date - min_date).days + 1

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
        Get count of distinct dimension values.

        Args:
            group_cols: List of columns to group by
            start_date: Filter start date
            end_date: Filter end date
            dimension_filters: Dimension filters
            use_approx: Use APPROX_COUNT_DISTINCT for large datasets

        Returns:
            Total count of dimension value combinations
        """
        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters
        )

        quoted_group_cols = [self._quote_column_name(col) for col in group_cols]
        group_by_clause = ", ".join(quoted_group_cols)

        if len(quoted_group_cols) > 1:
            if use_approx:
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

        count_df = self.execute_query(
            count_query,
            query_type="pivot_count",
            endpoint="count_cache"
        )

        return int(count_df['total_count'].iloc[0]) if not count_df.empty else 0

    def query_kpi_metrics(
        self,
        filters: Dict,
        table_path: Optional[str] = None
    ) -> Dict:
        """
        Query aggregated KPI metrics dynamically from schema.

        Args:
            filters: Filter parameters dict
            table_path: Override table path (for rollup queries). Defaults to base table.

        Returns:
            Dictionary with aggregated metrics
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path
        is_rollup_query = table_path is not None

        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        # For rollup queries, use SUM(metric) since metrics are pre-computed
        if is_rollup_query:
            select_clause = self._build_rollup_metric_select_clause()
        else:
            select_clause = self._build_metric_select_clause()

        query = f"""
            SELECT
                {select_clause}
            FROM `{query_table}`
            {where_clause}
        """

        df = self.execute_query(
            query=query,
            query_type='kpi',
            endpoint='/api/overview',
            filters=filters
        )

        if df.empty:
            return {}

        row = df.iloc[0]
        result = {}

        for col in df.columns:
            value = row[col]
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
        filters: Dict,
        granularity: str = 'daily',
        table_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query time-series data dynamically from schema.

        Args:
            filters: Filter parameters dict
            granularity: 'daily', 'weekly', or 'monthly'
            table_path: Override table path (for rollup queries). Defaults to base table.

        Returns:
            DataFrame with time-series data
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path
        is_rollup_query = table_path is not None

        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        date_trunc_map = {
            'daily': 'DAY',
            'weekly': 'WEEK',
            'monthly': 'MONTH',
        }
        date_trunc = date_trunc_map.get(granularity, 'DAY')

        # For rollup queries, use SUM(metric) since metrics are pre-computed
        if is_rollup_query:
            select_clause = self._build_rollup_metric_select_clause()
        else:
            select_clause = self._build_metric_select_clause()

        query = f"""
            SELECT
                DATE_TRUNC(date, {date_trunc}) as date,
                {select_clause}
            FROM `{query_table}`
            {where_clause}
            GROUP BY date
            ORDER BY date
        """

        return self.execute_query(
            query=query,
            query_type='trends',
            endpoint='/api/trends',
            filters={**filters, 'granularity': granularity}
        )

    def query_dimension_breakdown(
        self,
        dimension: str,
        filters: Dict,
        limit: int = 20,
        table_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query breakdown by dimension dynamically from schema.

        Args:
            dimension: Column to group by
            filters: Filter parameters dict
            limit: Maximum number of rows to return
            table_path: Override table path (for rollup queries). Defaults to base table.

        Returns:
            DataFrame with dimension breakdown
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path
        is_rollup_query = table_path is not None

        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        group_col = dimension
        data_type = "STRING"
        if self.schema_config:
            try:
                dim = self.schema_config.dimensions.get(dimension_id=dimension)
                group_col = dim.column_name
                data_type = dim.data_type
            except Exception:
                group_col = dimension

        # For rollup queries, use SUM(metric) since metrics are pre-computed
        if is_rollup_query:
            select_clause = self._build_rollup_metric_select_clause()
        else:
            select_clause = self._build_metric_select_clause()
        dimension_expr = f"COALESCE(CAST({group_col} AS STRING), '__NULL__')"

        query = f"""
            SELECT
                {dimension_expr} as dimension_value,
                {select_clause}
            FROM `{query_table}`
            {where_clause}
            GROUP BY dimension_value
            ORDER BY dimension_value
            LIMIT {limit}
        """

        return self.execute_query(
            query=query,
            query_type='breakdown',
            endpoint=f'/api/breakdown/{dimension}',
            filters={**filters, 'dimension': dimension, 'limit': limit}
        )

    def query_search_terms(
        self,
        filters: Dict,
        limit: int = 100,
        sort_by: str = 'queries',
        table_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query search terms data dynamically from schema.

        Args:
            filters: Filter parameters dict
            limit: Maximum number of rows to return
            sort_by: Metric ID to sort by
            table_path: Override table path (for rollup queries). Defaults to base table.

        Returns:
            DataFrame with search terms and all base metrics
        """
        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.table_path
        is_rollup_query = table_path is not None

        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        # For rollup queries, use SUM(metric) since metrics are pre-computed
        if is_rollup_query:
            select_clause = "search_term,\n                " + self._build_rollup_metric_select_clause()
        else:
            select_clause = self._build_metric_select_clause(include_search_term=True)

        query = f"""
            SELECT
                {select_clause}
            FROM `{query_table}`
            {where_clause}
            GROUP BY search_term
            ORDER BY {sort_by} DESC
            LIMIT {limit}
        """

        return self.execute_query(
            query=query,
            query_type='search_terms',
            endpoint='/api/search-terms',
            filters={**filters, 'limit': limit, 'sort_by': sort_by}
        )

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
        table_ref = self.client.get_table(self.table_path)
        has_date_column = any(field.name == 'date' for field in table_ref.schema)

        if not has_date_column:
            return {
                "min_date": None,
                "max_date": None,
                "total_rows": table_ref.num_rows,
                "has_date_column": False
            }

        query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as total_rows
            FROM `{self.table_path}`
        """

        df = self.client.query(query).to_dataframe()
        row = df.iloc[0]

        return {
            "min_date": row['min_date'].strftime('%Y-%m-%d') if pd.notna(row['min_date']) else None,
            "max_date": row['max_date'].strftime('%Y-%m-%d') if pd.notna(row['max_date']) else None,
            "total_rows": int(row['total_rows']),
            "has_date_column": True
        }

    def query_rollup_table(
        self,
        rollup_table_path: str,
        dimensions: List[str],
        metrics: List[str],
        filters: Dict,
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
        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        select_parts = []
        group_parts = []

        for dim_id in dimensions:
            select_parts.append(dim_id)
            group_parts.append(dim_id)

        if needs_reaggregation:
            for metric_id in metrics:
                select_parts.append(f"SUM({metric_id}) AS {metric_id}")
        else:
            select_parts.extend(metrics)

        if not select_parts:
            raise ValueError(
                f"Cannot query rollup: no columns to select. "
                f"Dimensions: {dimensions}, Metrics: {metrics}."
            )

        sort_metric = sort_by if sort_by else (metrics[0] if metrics else None)
        order_clause = f"ORDER BY {sort_metric} {sort_order}" if sort_metric else ""

        if needs_reaggregation and group_parts:
            group_clause = f"GROUP BY {', '.join(group_parts)}"
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

        return self.execute_query(
            query=query,
            query_type='rollup_query',
            endpoint='/api/pivot',
            filters={**filters, 'dimensions': dimensions, 'rollup_table': rollup_table_path}
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
        select_parts = [f"SUM({metric_id}) AS {metric_id}" for metric_id in metric_ids]

        if not select_parts:
            return {}

        where_clause = self.build_filter_clause(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters,
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset
        )

        query = f"""
        SELECT {', '.join(select_parts)}
        FROM `{rollup_table_path}`
        {where_clause}
        """

        logger.info(f"Querying rollup aggregates from {rollup_table_path}")

        job = self.client.query(query)
        result = job.result()

        row = next(iter(result), None)
        if not row:
            return {metric_id: 0 for metric_id in metric_ids}

        return {metric_id: row[metric_id] or 0 for metric_id in metric_ids}

    def query_aggregated_totals(
        self,
        metric_ids: List[str],
        filters: Dict,
        dimension_filters: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, float]:
        """
        Query aggregated totals for specific metrics.

        Args:
            metric_ids: List of metric IDs to aggregate
            filters: Base filter parameters (date range, etc.)
            dimension_filters: Additional dimension filters to apply

        Returns:
            Dict of metric_id -> aggregated value
        """
        if not self.schema_config:
            raise ValueError("Schema not loaded")

        # Build combined dimension filters
        combined_filters = dict(filters.get('dimension_filters', {}))
        if dimension_filters:
            combined_filters.update(dimension_filters)

        # Build WHERE clause
        where_clause = self.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=combined_filters,
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        # Build SELECT clause for requested metrics
        select_parts = []
        for metric_id in metric_ids:
            try:
                metric = self.schema_config.calculated_metrics.get(metric_id=metric_id)
                select_parts.append(f"{metric.sql_expression} as {metric.metric_id}")
            except Exception:
                # Metric not found, skip
                logger.warning(f"Metric {metric_id} not found in schema")
                continue

        if not select_parts:
            return {}

        select_clause = ",\n                ".join(select_parts)

        query = f"""
            SELECT
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
        """

        df = self.execute_query(
            query=query,
            query_type='aggregated_totals',
            endpoint='/api/significance',
            filters=filters
        )

        if df.empty:
            return {}

        # Convert to dict
        result = {}
        for metric_id in metric_ids:
            if metric_id in df.columns:
                value = df[metric_id].iloc[0]
                result[metric_id] = float(value) if pd.notna(value) else 0.0

        return result
