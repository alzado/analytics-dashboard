"""
BigQuery service for querying search analytics data.
"""
import os
import json
import tempfile
import time
from typing import Optional, Dict, List
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


class BigQueryService:
    """Service for querying BigQuery data."""

    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
        credentials_path: Optional[str] = None,
        table_id: Optional[str] = None
    ):
        """
        Initialize BigQuery service.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON (optional)
            table_id: Table ID for multi-table support (optional)
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_path = f"{project_id}.{dataset}.{table}"
        self.table_id = table_id

        # Date limits (optional)
        self.allowed_min_date: Optional[str] = None
        self.allowed_max_date: Optional[str] = None

        # Create BigQuery client
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.client = bigquery.Client(project=project_id)

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
            if metric.aggregation == 'COUNT_DISTINCT':
                # Support multi-column COUNT_DISTINCT: column_name can be "col1, col2, col3"
                # BigQuery syntax: COUNT(DISTINCT col1, col2, col3)
                columns = metric.column_name.strip()
                select_parts.append(f"COUNT(DISTINCT {columns}) as {metric.id}")
            else:
                select_parts.append(f"{metric.aggregation}({metric.column_name}) as {metric.id}")

        # Note: Calculated metrics are computed post-query using their sql_expression
        # They cannot be included in the SELECT directly as they depend on base metrics

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

    def _execute_and_log_query(
        self,
        query: str,
        query_type: str,
        endpoint: str = "unknown",
        filters: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query and log its metrics.

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

        print(f"[QUERY LOGGER DEBUG] _execute_and_log_query called: endpoint={endpoint}, type={query_type}")

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

            print(f"[QUERY LOGGER DEBUG] Query executed: bytes_processed={bytes_processed}, bytes_billed={bytes_billed}, rows={row_count}")

            return df

        except Exception as e:
            error_msg = str(e)
            print(f"[QUERY LOGGER DEBUG] Query error: {error_msg}")
            raise

        finally:
            # Log query execution
            execution_time_ms = int((time.time() - start_time) * 1000)

            logger = get_query_logger()
            print(f"[QUERY LOGGER DEBUG] Logger instance: {logger}")
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
                    print(f"[QUERY LOGGER DEBUG] Log written successfully")
                except Exception as log_error:
                    # Don't fail the query if logging fails
                    print(f"[QUERY LOGGER DEBUG] Failed to log query: {log_error}")

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
    ) -> str:
        """
        Build WHERE clause from filter parameters - fully dynamic.

        Args:
            start_date: Start date for date range filter (YYYY-MM-DD)
            end_date: End date for date range filter (YYYY-MM-DD)
            dimension_filters: Dictionary mapping dimension IDs to lists of values for multi-select filtering
                              Example: {"country": ["USA", "Canada"], "channel": ["Web"]}

        Returns:
            WHERE clause string (includes "WHERE" keyword if non-empty)
        """
        conditions = []

        # Date range filter (special handling for date dimension)
        if start_date and end_date:
            conditions.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            conditions.append(f"date >= '{start_date}'")
        elif end_date:
            conditions.append(f"date <= '{end_date}'")

        # Dynamic dimension filters
        if dimension_filters:
            for dimension_id, values in dimension_filters.items():
                if not values:  # Skip empty filter arrays
                    continue

                # Find dimension in schema to get column name and data type
                dimension_def = None
                if self.schema_config:
                    dimension_def = next((d for d in self.schema_config.dimensions if d.id == dimension_id), None)

                # Default to using dimension_id as column_name if not found in schema
                column_name = dimension_def.column_name if dimension_def else dimension_id
                data_type = dimension_def.data_type if dimension_def else "STRING"

                # Build filter condition based on data type
                if len(values) == 1:
                    # Single value - use equality
                    value = values[0]
                    if data_type in ["STRING", "DATE"]:
                        # Escape single quotes in string values
                        escaped_value = value.replace("'", "''")
                        conditions.append(f"{column_name} = '{escaped_value}'")
                    elif data_type == "BOOLEAN":
                        bool_value = "TRUE" if value.lower() in ["true", "1", "yes"] else "FALSE"
                        conditions.append(f"{column_name} = {bool_value}")
                    else:  # INTEGER, FLOAT
                        conditions.append(f"{column_name} = {value}")
                else:
                    # Multiple values - use IN clause with OR logic
                    if data_type in ["STRING", "DATE"]:
                        # Escape single quotes in string values
                        escaped_values = [v.replace("'", "''") for v in values]
                        values_str = "', '".join(escaped_values)
                        conditions.append(f"{column_name} IN ('{values_str}')")
                    elif data_type == "BOOLEAN":
                        bool_values = ", ".join([
                            "TRUE" if v.lower() in ["true", "1", "yes"] else "FALSE"
                            for v in values
                        ])
                        conditions.append(f"{column_name} IN ({bool_values})")
                    else:  # INTEGER, FLOAT
                        values_str = ", ".join(values)
                        conditions.append(f"{column_name} IN ({values_str})")

        if conditions:
            return "WHERE " + " AND ".join(conditions)
        return ""

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
            dimension_filters=filters.dimension_filters
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
            dimension_filters=filters.dimension_filters
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
            dimension_filters=filters.dimension_filters
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
            dimension_filters=filters.dimension_filters
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

        # Build dynamic SELECT clause from schema
        select_clause = self._build_metric_select_clause()

        query = f"""
            SELECT
                {group_col} as dimension_value,
                {select_clause}
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY {group_col}
            ORDER BY {group_col}
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
            dimension_filters=filters.dimension_filters
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
            dimension_filters=filters.dimension_filters
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

    def get_table_date_range(self, table_name: str) -> Dict:
        """
        Get the date range for a specific table.

        Args:
            table_name: Name of the table to query

        Returns:
            Dictionary with min_date, max_date, and total_rows
        """
        table_path = f"{self.project_id}.{self.dataset}.{table_name}"

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


def get_bigquery_service(table_id: Optional[str] = None) -> Optional[BigQueryService]:
    """
    Get BigQuery service instance for a specific table.

    Args:
        table_id: Table ID to get service for. If None, uses active table.

    Returns:
        BigQuery service instance or None
    """
    from config import table_registry

    if table_id is None:
        table_id = table_registry.get_active_table_id()

    if table_id is None:
        return None

    return _bq_services.get(table_id)


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
    table_id: Optional[str] = None
) -> BigQueryService:
    """
    Initialize BigQuery service for a specific table.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_path: Path to service account JSON (optional)
        table_id: Table ID to associate with this service

    Returns:
        BigQuery service instance
    """
    global _bq_services
    from config import table_registry

    # Use active table if not specified
    if table_id is None:
        table_id = table_registry.get_active_table_id()

    if table_id is None:
        raise ValueError("No table_id specified and no active table set")

    service = BigQueryService(project_id, dataset, table, credentials_path, table_id)
    _bq_services[table_id] = service
    return service


def initialize_bigquery_with_json(
    project_id: str,
    dataset: str,
    table: str,
    credentials_json: str,
    table_id: Optional[str] = None
) -> BigQueryService:
    """
    Initialize BigQuery service with credentials JSON string.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_json: Service account JSON as string
        table_id: Table ID to associate with this service

    Returns:
        BigQuery service instance
    """
    global _bq_services
    from config import table_registry

    # Use active table if not specified
    if table_id is None:
        table_id = table_registry.get_active_table_id()

    if table_id is None:
        raise ValueError("No table_id specified and no active table set")

    # Parse JSON and create credentials
    try:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        # Create BigQuery client directly
        client = bigquery.Client(credentials=credentials, project=project_id)

        # Create service instance manually
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = project_id
        service.dataset = dataset
        service.table = table
        service.table_path = f"{project_id}.{dataset}.{table}"
        service.client = client
        service.table_id = table_id
        # Initialize date limit attributes
        service.allowed_min_date = None
        service.allowed_max_date = None

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


def get_bigquery_info() -> dict:
    """
    Get comprehensive BigQuery connection and data information.

    Returns:
        Dictionary with BigQuery metadata
    """
    bq_service = get_bigquery_service()
    if bq_service is None:
        # Return not-configured status instead of raising error
        from config import table_registry
        active_id = table_registry.get_active_table_id()
        if active_id:
            table_info = table_registry.get_table(active_id)
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
        else:
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
