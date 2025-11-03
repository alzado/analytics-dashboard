"""
BigQuery service for querying search analytics data.
"""
import os
import json
import tempfile
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
        credentials_path: Optional[str] = None
    ):
        """
        Initialize BigQuery service.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON (optional)
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_path = f"{project_id}.{dataset}.{table}"

        # Date limits (optional)
        self.allowed_min_date: Optional[str] = None
        self.allowed_max_date: Optional[str] = None

        # Create BigQuery client
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.client = bigquery.Client(project=project_id)

    def set_date_limits(self, min_date: Optional[str] = None, max_date: Optional[str] = None) -> None:
        """
        Set allowed date range for queries.

        Args:
            min_date: Minimum allowed date (YYYY-MM-DD format)
            max_date: Maximum allowed date (YYYY-MM-DD format)
        """
        self.allowed_min_date = min_date
        self.allowed_max_date = max_date

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
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> str:
        """
        Build WHERE clause from filter parameters.

        Returns:
            WHERE clause string (includes "WHERE" keyword if non-empty)
        """
        # Clamp dates to allowed range
        start_date, end_date = self._clamp_dates(start_date, end_date)

        conditions = []

        # Date filters
        if start_date and end_date:
            conditions.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            conditions.append(f"date >= '{start_date}'")
        elif end_date:
            conditions.append(f"date <= '{end_date}'")

        # Country filter
        if country:
            conditions.append(f"country = '{country}'")

        # Channel filter
        if channel:
            conditions.append(f"channel = '{channel}'")

        # GCategory filter
        if gcategory:
            conditions.append(f"gcategory_name = '{gcategory}'")

        # Attribute count filters
        if n_attributes_min is not None:
            conditions.append(f"n_attributes >= {n_attributes_min}")
        if n_attributes_max is not None:
            conditions.append(f"n_attributes <= {n_attributes_max}")

        if conditions:
            return "WHERE " + " AND ".join(conditions)
        return ""

    def query_all_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query all data from BigQuery with filters.

        Returns:
            DataFrame with all columns
        """
        where_clause = self.build_filter_clause(
            start_date, end_date, country, channel, gcategory,
            n_attributes_min, n_attributes_max
        )

        query = f"""
            SELECT *
            FROM `{self.table_path}`
            {where_clause}
        """

        return self.client.query(query).to_dataframe()

    def query_kpi_metrics(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> Dict:
        """
        Query aggregated KPI metrics.

        Returns:
            Dictionary with aggregated metrics
        """
        where_clause = self.build_filter_clause(
            start_date, end_date, country, channel, gcategory,
            n_attributes_min, n_attributes_max
        )

        query = f"""
            SELECT
                SUM(queries) as total_queries,
                SUM(queries_pdp) as total_queries_pdp,
                SUM(queries_a2c) as total_queries_a2c,
                SUM(purchases) as total_purchases,
                SUM(gross_purchase) as total_revenue,
                SUM(products_pdp_1p) as total_products_pdp_1p,
                SUM(products_pdp_3p) as total_products_pdp_3p,
                SUM(products_a2c_1p) as total_products_a2c_1p,
                SUM(products_a2c_3p) as total_products_a2c_3p,
                SUM(purchases_1p) as total_purchases_1p,
                SUM(purchases_3p) as total_purchases_3p,
                SUM(gross_purchase_1p) as total_revenue_1p,
                SUM(gross_purchase_3p) as total_revenue_3p,
                COUNT(DISTINCT search_term) as unique_search_terms
            FROM `{self.table_path}`
            {where_clause}
        """

        df = self.client.query(query).to_dataframe()
        if df.empty:
            return {}

        row = df.iloc[0]
        return {
            'queries': int(row['total_queries'] or 0),
            'queries_pdp': int(row['total_queries_pdp'] or 0),
            'queries_a2c': int(row['total_queries_a2c'] or 0),
            'purchases': int(row['total_purchases'] or 0),
            'revenue': float(row['total_revenue'] or 0),
            'products_pdp_1p': int(row['total_products_pdp_1p'] or 0),
            'products_pdp_3p': int(row['total_products_pdp_3p'] or 0),
            'products_a2c_1p': int(row['total_products_a2c_1p'] or 0),
            'products_a2c_3p': int(row['total_products_a2c_3p'] or 0),
            'purchases_1p': int(row['total_purchases_1p'] or 0),
            'purchases_3p': int(row['total_purchases_3p'] or 0),
            'revenue_1p': float(row['total_revenue_1p'] or 0),
            'revenue_3p': float(row['total_revenue_3p'] or 0),
            'unique_search_terms': int(row['unique_search_terms'] or 0),
        }

    def query_timeseries(
        self,
        granularity: str = 'daily',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query time-series data.

        Args:
            granularity: 'daily', 'weekly', or 'monthly'

        Returns:
            DataFrame with time-series data
        """
        where_clause = self.build_filter_clause(
            start_date, end_date, country, channel, gcategory,
            n_attributes_min, n_attributes_max
        )

        # Map granularity to BigQuery date truncation
        date_trunc_map = {
            'daily': 'DAY',
            'weekly': 'WEEK',
            'monthly': 'MONTH',
        }
        date_trunc = date_trunc_map.get(granularity, 'DAY')

        query = f"""
            SELECT
                DATE_TRUNC(date, {date_trunc}) as date,
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY date
            ORDER BY date
        """

        return self.client.query(query).to_dataframe()

    def query_dimension_breakdown(
        self,
        dimension: str,
        limit: int = 20,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query breakdown by dimension.

        Args:
            dimension: Column to group by (e.g., 'channel', 'country', 'n_words')

        Returns:
            DataFrame with dimension breakdown
        """
        where_clause = self.build_filter_clause(
            start_date, end_date, country, channel, gcategory,
            n_attributes_min, n_attributes_max
        )

        # Map dimension names to column names
        dimension_map = {
            'n_words': 'n_words',
            'n_attributes': 'n_attributes',
            'channel': 'channel',
            'country': 'country',
            'gcategory_name': 'gcategory_name',
        }
        group_col = dimension_map.get(dimension, dimension)

        query = f"""
            SELECT
                {group_col} as dimension_value,
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY {group_col}
            ORDER BY queries DESC
            LIMIT {limit}
        """

        return self.client.query(query).to_dataframe()

    def query_search_terms(
        self,
        limit: int = 100,
        sort_by: str = 'queries',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
        channel: Optional[str] = None,
        gcategory: Optional[str] = None,
        n_attributes_min: Optional[int] = None,
        n_attributes_max: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query search terms data.

        Returns:
            DataFrame with search terms
        """
        where_clause = self.build_filter_clause(
            start_date, end_date, country, channel, gcategory,
            n_attributes_min, n_attributes_max
        )

        # Validate sort column
        valid_sorts = ['queries', 'purchases', 'revenue', 'queries_pdp', 'queries_a2c']
        if sort_by not in valid_sorts:
            sort_by = 'queries'

        query = f"""
            SELECT
                search_term,
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                MAX(n_words) as n_words,
                MAX(n_attributes) as n_attributes
            FROM `{self.table_path}`
            {where_clause}
            GROUP BY search_term
            ORDER BY {sort_by} DESC
            LIMIT {limit}
        """

        return self.client.query(query).to_dataframe()

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


# Global instance (will be initialized on startup)
_bq_service: Optional[BigQueryService] = None


def get_bigquery_service() -> Optional[BigQueryService]:
    """Get the global BigQuery service instance."""
    return _bq_service


def clear_bigquery_service() -> None:
    """Clear the global BigQuery service instance."""
    global _bq_service
    _bq_service = None


def initialize_bigquery_service(
    project_id: str,
    dataset: str,
    table: str,
    credentials_path: Optional[str] = None
) -> BigQueryService:
    """
    Initialize the global BigQuery service.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_path: Path to service account JSON (optional)

    Returns:
        BigQuery service instance
    """
    global _bq_service
    _bq_service = BigQueryService(project_id, dataset, table, credentials_path)
    return _bq_service


def initialize_bigquery_with_json(
    project_id: str,
    dataset: str,
    table: str,
    credentials_json: str
) -> BigQueryService:
    """
    Initialize the global BigQuery service with credentials JSON string.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        credentials_json: Service account JSON as string

    Returns:
        BigQuery service instance
    """
    global _bq_service

    # Parse JSON and create credentials
    try:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        # Create BigQuery client directly
        client = bigquery.Client(credentials=credentials, project=project_id)

        # Create service instance manually
        _bq_service = BigQueryService.__new__(BigQueryService)
        _bq_service.project_id = project_id
        _bq_service.dataset = dataset
        _bq_service.table = table
        _bq_service.table_path = f"{project_id}.{dataset}.{table}"
        _bq_service.client = client
        # Initialize date limit attributes
        _bq_service.allowed_min_date = None
        _bq_service.allowed_max_date = None

        return _bq_service
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
        from config import config
        return {
            "project_id": config.BIGQUERY_PROJECT_ID or "",
            "dataset": config.BIGQUERY_DATASET or "",
            "table": config.BIGQUERY_TABLE or "",
            "table_full_path": "",
            "connection_status": "not configured",
            "date_range": {"min": "", "max": ""},
            "total_rows": 0,
            "total_searches": 0,
            "total_revenue": 0.0,
            "unique_search_terms": 0,
            "available_countries": [],
            "available_channels": [],
            "table_size_mb": 0.0,
            "last_modified": "",
            "schema_columns": [],
            "allowed_min_date": config.ALLOWED_MIN_DATE,
            "allowed_max_date": config.ALLOWED_MAX_DATE
        }

    try:
        # Get table metadata
        table_ref = bq_service.client.get_table(bq_service.table_path)

        # Build date filter clause if date limits are set
        date_filter_conditions = []
        if bq_service.allowed_min_date:
            date_filter_conditions.append(f"date >= '{bq_service.allowed_min_date}'")
        if bq_service.allowed_max_date:
            date_filter_conditions.append(f"date <= '{bq_service.allowed_max_date}'")

        date_filter_clause = ""
        if date_filter_conditions:
            date_filter_clause = "WHERE " + " AND ".join(date_filter_conditions)

        # Get date range
        date_query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_service.table_path}`
            {date_filter_clause}
        """
        date_df = bq_service.client.query(date_query).to_dataframe()

        # Get summary statistics
        stats_query = f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(queries) as total_searches,
                SUM(gross_purchase) as total_revenue,
                COUNT(DISTINCT search_term) as unique_search_terms
            FROM `{bq_service.table_path}`
            {date_filter_clause}
        """
        stats_df = bq_service.client.query(stats_query).to_dataframe()

        # Get available countries
        # Combine date filter with country NOT NULL condition
        countries_where = date_filter_clause
        if countries_where:
            countries_where += " AND country IS NOT NULL"
        else:
            countries_where = "WHERE country IS NOT NULL"

        countries_query = f"""
            SELECT DISTINCT country
            FROM `{bq_service.table_path}`
            {countries_where}
            ORDER BY country
        """
        countries_df = bq_service.client.query(countries_query).to_dataframe()

        # Get available channels
        # Combine date filter with channel NOT NULL condition
        channels_where = date_filter_clause
        if channels_where:
            channels_where += " AND channel IS NOT NULL"
        else:
            channels_where = "WHERE channel IS NOT NULL"

        channels_query = f"""
            SELECT DISTINCT channel
            FROM `{bq_service.table_path}`
            {channels_where}
            ORDER BY channel
        """
        channels_df = bq_service.client.query(channels_query).to_dataframe()

        # Extract schema column names
        schema_columns = [field.name for field in table_ref.schema]

        return {
            "project_id": bq_service.project_id,
            "dataset": bq_service.dataset,
            "table": bq_service.table,
            "table_full_path": bq_service.table_path,
            "connection_status": "connected",
            "date_range": {
                "min": date_df['min_date'].iloc[0].strftime('%Y-%m-%d'),
                "max": date_df['max_date'].iloc[0].strftime('%Y-%m-%d')
            },
            "total_rows": int(stats_df['total_rows'].iloc[0]),
            "total_searches": int(stats_df['total_searches'].iloc[0]),
            "total_revenue": float(stats_df['total_revenue'].iloc[0]),
            "unique_search_terms": int(stats_df['unique_search_terms'].iloc[0]),
            "available_countries": countries_df['country'].tolist(),
            "available_channels": channels_df['channel'].tolist(),
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
            "total_searches": 0,
            "total_revenue": 0.0,
            "unique_search_terms": 0,
            "available_countries": [],
            "available_channels": [],
            "table_size_mb": 0.0,
            "last_modified": "",
            "schema_columns": [],
            "allowed_min_date": bq_service.allowed_min_date,
            "allowed_max_date": bq_service.allowed_max_date
        }
