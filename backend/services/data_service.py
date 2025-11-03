"""
Data service - BigQuery implementation only.
All data is fetched from BigQuery on-demand.
"""
from typing import List, Dict, Optional
import numpy as np
import math
from models.schemas import (
    FilterParams,
    OverviewMetrics,
    TrendData,
    DimensionBreakdown,
    SearchTermData,
    FilterOptions,
    PivotRow,
    PivotChildRow,
    PivotResponse
)
from services.bigquery_service import get_bigquery_service


def safe_float(value: float) -> float:
    """Convert a value to float, replacing NaN and infinity with 0."""
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return float(value)


def get_overview_metrics(filters: FilterParams) -> OverviewMetrics:
    """Get overview metrics from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    metrics_dict = bq_service.query_kpi_metrics(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    # Calculate derived metrics
    queries = metrics_dict.get('queries', 0)
    queries_pdp = metrics_dict.get('queries_pdp', 0)
    queries_a2c = metrics_dict.get('queries_a2c', 0)
    purchases = metrics_dict.get('purchases', 0)
    revenue = metrics_dict.get('revenue', 0)

    # Calculate number of days for avg_queries_per_day
    num_days = 1
    if filters.start_date and filters.end_date:
        from datetime import datetime
        start = datetime.strptime(filters.start_date, '%Y-%m-%d')
        end = datetime.strptime(filters.end_date, '%Y-%m-%d')
        num_days = (end - start).days + 1

    return OverviewMetrics(
        queries=queries,
        queries_pdp=queries_pdp,
        queries_a2c=queries_a2c,
        purchases=purchases,
        revenue=revenue,
        ctr=queries_pdp / queries if queries > 0 else 0,
        a2c_rate=queries_a2c / queries if queries > 0 else 0,
        conversion_rate=purchases / queries if queries > 0 else 0,
        pdp_conversion=purchases / queries_pdp if queries_pdp > 0 else 0,
        revenue_per_query=revenue / queries if queries > 0 else 0,
        aov=revenue / purchases if purchases > 0 else 0,
        avg_queries_per_day=queries / num_days if num_days > 0 else 0,
        unique_search_terms=metrics_dict.get('unique_search_terms', 0)
    )


def get_trend_data(filters: FilterParams, granularity: str = "daily") -> List[TrendData]:
    """Get time series trend data from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_timeseries(
        granularity=granularity,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(TrendData(
            date=row['date'].strftime('%Y-%m-%d'),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            a2c_rate=float(row['a2c_rate']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            revenue_per_query=float(row['revenue_per_query'])
        ))

    return result


def get_dimension_breakdown(dimension: str, filters: FilterParams, limit: int = 20) -> List[DimensionBreakdown]:
    """Get breakdown by dimension from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_dimension_breakdown(
        dimension=dimension,
        limit=limit,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    total_queries = df['queries'].sum()

    # Calculate number of days for avg_queries_per_day
    num_days = 1
    if filters.start_date and filters.end_date:
        from datetime import datetime
        start = datetime.strptime(filters.start_date, '%Y-%m-%d')
        end = datetime.strptime(filters.end_date, '%Y-%m-%d')
        num_days = (end - start).days + 1

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0
    df['percentage_of_total'] = (df['queries'] / total_queries * 100) if total_queries > 0 else 0

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(DimensionBreakdown(
            dimension_value=str(row['dimension_value']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            a2c_rate=float(row['a2c_rate']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            revenue_per_query=float(row['revenue_per_query']),
            avg_queries_per_day=float(row['avg_queries_per_day']),
            percentage_of_total=float(row['percentage_of_total'])
        ))

    return result


def get_search_terms(filters: FilterParams, limit: int = 100, sort_by: str = "queries") -> List[SearchTermData]:
    """Get top search terms from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_search_terms(
        limit=limit,
        sort_by=sort_by,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(SearchTermData(
            search_term=str(row['search_term']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            avg_queries_per_day=float(row['avg_queries_per_day']),
            n_words=int(row['n_words']),
            n_attributes=int(row['n_attributes'])
        ))

    return result


def get_filter_options() -> FilterOptions:
    """Get available filter options from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Query for distinct countries
    countries_query = f"SELECT DISTINCT country FROM `{bq_service.table_path}` WHERE country IS NOT NULL ORDER BY country"
    countries_df = bq_service.client.query(countries_query).to_dataframe()
    countries = countries_df['country'].tolist()

    # Query for distinct channels
    channels_query = f"SELECT DISTINCT channel FROM `{bq_service.table_path}` WHERE channel IS NOT NULL ORDER BY channel"
    channels_df = bq_service.client.query(channels_query).to_dataframe()
    channels = channels_df['channel'].tolist()

    # Query for date range
    date_query = f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM `{bq_service.table_path}`"
    date_df = bq_service.client.query(date_query).to_dataframe()

    date_range = {
        'min': date_df['min_date'].iloc[0].strftime('%Y-%m-%d'),
        'max': date_df['max_date'].iloc[0].strftime('%Y-%m-%d')
    }

    attributes = ['categoria', 'tipo', 'genero', 'marca', 'color', 'material', 'talla', 'modelo']

    return FilterOptions(
        countries=countries,
        channels=channels,
        date_range=date_range,
        attributes=attributes
    )


def get_pivot_data(dimensions: List[str], filters: FilterParams, limit: int = 50) -> PivotResponse:
    """Get hierarchical pivot table data by dimensions from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Dimension map for available dimensions
    dimension_map = {
        'n_words': 'n_words_normalized',
        'n_attributes': 'n_attributes',
        'channel': 'channel',
        'country': 'country',
        'gcategory_name': 'gcategory_name'
    }

    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    where_clause_for_dates = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )
    date_range_query = f"""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM `{bq_service.table_path}`
        {where_clause_for_dates}
    """
    date_range_df = bq_service.client.query(date_range_query).to_dataframe()

    # Calculate number of days from actual data
    num_days = 1
    if not date_range_df.empty and date_range_df['min_date'].iloc[0] is not None and date_range_df['max_date'].iloc[0] is not None:
        min_date = date_range_df['min_date'].iloc[0]
        max_date = date_range_df['max_date'].iloc[0]
        num_days = (max_date - min_date).days + 1

    # If no dimensions provided, return aggregated totals as a single row
    if not dimensions:
        # Build filter clause
        where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            country=filters.country,
            channel=filters.channel,
            gcategory=filters.gcategory,
            n_attributes_min=filters.n_attributes_min,
            n_attributes_max=filters.n_attributes_max
        )

        # Query for aggregated totals
        query = f"""
            SELECT
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count
            FROM `{bq_service.table_path}`
            {where_clause}
        """

        df = bq_service.client.query(query).to_dataframe()

        if df.empty or df['queries'].iloc[0] == 0:
            # Return empty response with zero totals
            total_row = PivotRow(
                dimension_value="Total",
                queries=0,
                queries_pdp=0,
                queries_a2c=0,
                purchases=0,
                revenue=0.0,
                ctr=0.0,
                a2c_rate=0.0,
                conversion_rate=0.0,
                pdp_conversion=0.0,
                revenue_per_query=0.0,
                aov=0.0,
                avg_queries_per_day=0.0,
                percentage_of_total=100.0,
                search_term_count=0,
                has_children=False
            )
            return PivotResponse(
                rows=[total_row],
                total=total_row,
                available_dimensions=list(dimension_map.keys())
            )

        # Calculate metrics
        row_data = df.iloc[0]
        queries = int(row_data['queries'])
        queries_pdp = int(row_data['queries_pdp'])
        queries_a2c = int(row_data['queries_a2c'])
        purchases = int(row_data['purchases'])
        revenue = safe_float(row_data['revenue'])
        search_term_count = int(row_data['search_term_count'])

        ctr = safe_float(queries_pdp / queries) if queries > 0 else 0.0
        a2c_rate = safe_float(queries_a2c / queries) if queries > 0 else 0.0
        conversion_rate = safe_float(purchases / queries) if queries > 0 else 0.0
        pdp_conversion = safe_float(purchases / queries_pdp) if queries_pdp > 0 else 0.0
        revenue_per_query = safe_float(revenue / queries) if queries > 0 else 0.0
        aov = safe_float(revenue / purchases) if purchases > 0 else 0.0
        avg_queries_per_day = safe_float(queries / num_days) if num_days > 0 else 0.0

        total_row = PivotRow(
            dimension_value="All Data",
            queries=queries,
            queries_pdp=queries_pdp,
            queries_a2c=queries_a2c,
            purchases=purchases,
            revenue=revenue,
            ctr=ctr,
            a2c_rate=a2c_rate,
            conversion_rate=conversion_rate,
            pdp_conversion=pdp_conversion,
            revenue_per_query=revenue_per_query,
            aov=aov,
            avg_queries_per_day=avg_queries_per_day,
            percentage_of_total=100.0,
            search_term_count=search_term_count,
            has_children=False
        )
        return PivotResponse(
            rows=[total_row],
            total=total_row,
            available_dimensions=list(dimension_map.keys())
        )

    # Build filter clause
    where_clause = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    # Map all dimensions to their column names
    group_cols = [dimension_map.get(dim, dim) for dim in dimensions]
    group_by_clause = ", ".join(group_cols)

    # Build SELECT clause for dimension values
    # For multiple dimensions, concat them with " - " separator
    if len(group_cols) > 1:
        # Convert each column to string and join with separator
        cast_cols = [f"CAST({col} AS STRING)" for col in group_cols]
        separator = ', " - ", '
        concat_args = separator.join(cast_cols)
        dim_value_clause = f"CONCAT({concat_args}) as dimension_value"
    else:
        dim_value_clause = f"{group_cols[0]} as dimension_value"

    # Query for pivot data
    query = f"""
        WITH grouped_data AS (
            SELECT
                {dim_value_clause},
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count
            FROM `{bq_service.table_path}`
            {where_clause}
            GROUP BY {group_by_clause}
            ORDER BY queries DESC
            LIMIT {limit}
        ),
        total_data AS (
            SELECT SUM(queries) as total_queries
            FROM `{bq_service.table_path}`
            {where_clause}
        )
        SELECT
            grouped_data.*,
            total_data.total_queries
        FROM grouped_data
        CROSS JOIN total_data
    """

    df = bq_service.client.query(query).to_dataframe()

    if df.empty:
        # Return empty response with zero totals
        total_row = PivotRow(
            dimension_value="Total",
            queries=0,
            queries_pdp=0,
            queries_a2c=0,
            purchases=0,
            revenue=0.0,
            ctr=0.0,
            a2c_rate=0.0,
            conversion_rate=0.0,
            pdp_conversion=0.0,
            revenue_per_query=0.0,
            aov=0.0,
            avg_queries_per_day=0.0,
            percentage_of_total=100.0,
            search_term_count=0,
            has_children=False
        )
        return PivotResponse(
            rows=[],
            total=total_row,
            available_dimensions=list(dimension_map.keys())
        )

    total_queries = df['total_queries'].iloc[0]

    # Calculate metrics for each row
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']
    df['aov'] = df['revenue'] / df['purchases']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0
    df['percentage_of_total'] = (df['queries'] / total_queries * 100) if total_queries > 0 else 0
    df['has_children'] = True  # All dimension rows have search terms as children

    # Fill NaN and infinity with 0
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # Convert to PivotRow objects
    rows = []
    for _, row in df.iterrows():
        rows.append(PivotRow(
            dimension_value=str(row['dimension_value']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=safe_float(row['revenue']),
            ctr=safe_float(row['ctr']),
            a2c_rate=safe_float(row['a2c_rate']),
            conversion_rate=safe_float(row['conversion_rate']),
            pdp_conversion=safe_float(row['pdp_conversion']),
            revenue_per_query=safe_float(row['revenue_per_query']),
            aov=safe_float(row['aov']),
            avg_queries_per_day=safe_float(row['avg_queries_per_day']),
            percentage_of_total=safe_float(row['percentage_of_total']),
            search_term_count=int(row['search_term_count']),
            has_children=True
        ))

    # Calculate totals
    total_row = PivotRow(
        dimension_value="Total",
        queries=int(df['queries'].sum()),
        queries_pdp=int(df['queries_pdp'].sum()),
        queries_a2c=int(df['queries_a2c'].sum()),
        purchases=int(df['purchases'].sum()),
        revenue=safe_float(df['revenue'].sum()),
        ctr=safe_float(df['queries_pdp'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        a2c_rate=safe_float(df['queries_a2c'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        conversion_rate=safe_float(df['purchases'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        pdp_conversion=safe_float(df['purchases'].sum() / df['queries_pdp'].sum()) if df['queries_pdp'].sum() > 0 else 0.0,
        revenue_per_query=safe_float(df['revenue'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        aov=safe_float(df['revenue'].sum() / df['purchases'].sum()) if df['purchases'].sum() > 0 else 0.0,
        avg_queries_per_day=safe_float(df['queries'].sum() / num_days) if num_days > 0 else 0.0,
        percentage_of_total=100.0,
        search_term_count=int(df['search_term_count'].sum()),
        has_children=False
    )

    return PivotResponse(
        rows=rows,
        total=total_row,
        available_dimensions=list(dimension_map.keys())
    )


def get_pivot_children(
    dimension: str,
    value: str,
    filters: FilterParams,
    limit: int = 100,
    offset: int = 0
) -> List[PivotChildRow]:
    """Get child rows (search terms) for a specific dimension value from BigQuery

    If dimension is empty string, fetches all search terms without dimension filtering
    """
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    where_clause_for_dates = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )
    date_range_query = f"""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM `{bq_service.table_path}`
        {where_clause_for_dates}
    """
    date_range_df = bq_service.client.query(date_range_query).to_dataframe()

    # Calculate number of days from actual data
    num_days = 1
    if not date_range_df.empty and date_range_df['min_date'].iloc[0] is not None and date_range_df['max_date'].iloc[0] is not None:
        min_date = date_range_df['min_date'].iloc[0]
        max_date = date_range_df['max_date'].iloc[0]
        num_days = (max_date - min_date).days + 1

    # Build base filter clause
    where_conditions = []

    # Apply same date clamping as parent query to ensure consistency
    clamped_start, clamped_end = bq_service._clamp_dates(filters.start_date, filters.end_date)

    if clamped_start and clamped_end:
        where_conditions.append(f"date BETWEEN '{clamped_start}' AND '{clamped_end}'")
    elif clamped_start:
        where_conditions.append(f"date >= '{clamped_start}'")
    elif clamped_end:
        where_conditions.append(f"date <= '{clamped_end}'")
    if filters.country:
        where_conditions.append(f"country = '{filters.country}'")
    if filters.channel:
        where_conditions.append(f"channel = '{filters.channel}'")
    if filters.n_attributes_min is not None:
        where_conditions.append(f"n_attributes >= {filters.n_attributes_min}")
    if filters.n_attributes_max is not None:
        where_conditions.append(f"n_attributes <= {filters.n_attributes_max}")

    # Add dimension filter only if dimension is specified
    if dimension:  # If dimension is provided (not empty string)
        # Built-in dimensions - map to actual column names
        dimension_map = {
            'n_words': 'n_words_normalized',  # Actual column name in BigQuery
            'n_attributes': 'n_attributes',
            'channel': 'channel',
            'country': 'country',
            'gcategory_name': 'gcategory_name'
        }
        group_col = dimension_map.get(dimension, dimension)

        # Numeric dimensions don't need quotes, string dimensions do
        numeric_dimensions = {'n_words', 'n_attributes', 'n_words_normalized'}
        if dimension in numeric_dimensions or group_col in numeric_dimensions:
            where_conditions.append(f"{group_col} = {value}")
        else:
            where_conditions.append(f"{group_col} = '{value}'")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # First, get the grand total queries for percentage calculation (same filters, no dimension restriction)
    base_where_conditions = []
    if clamped_start and clamped_end:
        base_where_conditions.append(f"date BETWEEN '{clamped_start}' AND '{clamped_end}'")
    elif clamped_start:
        base_where_conditions.append(f"date >= '{clamped_start}'")
    elif clamped_end:
        base_where_conditions.append(f"date <= '{clamped_end}'")
    if filters.country:
        base_where_conditions.append(f"country = '{filters.country}'")
    if filters.channel:
        base_where_conditions.append(f"channel = '{filters.channel}'")
    if filters.n_attributes_min is not None:
        base_where_conditions.append(f"n_attributes >= {filters.n_attributes_min}")
    if filters.n_attributes_max is not None:
        base_where_conditions.append(f"n_attributes <= {filters.n_attributes_max}")

    base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""

    total_query = f"""
        SELECT SUM(queries) as total_queries
        FROM `{bq_service.table_path}`
        {base_where_clause}
    """

    total_df = bq_service.client.query(total_query).to_dataframe()
    grand_total_queries = float(total_df['total_queries'].iloc[0]) if not total_df.empty and total_df['total_queries'].iloc[0] is not None else 0

    # Query for search terms within this dimension value
    query = f"""
        SELECT
            search_term,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as revenue
        FROM `{bq_service.table_path}`
        {where_clause}
        GROUP BY search_term
        ORDER BY queries DESC
        LIMIT {limit}
        OFFSET {offset}
    """

    df = bq_service.client.query(query).to_dataframe()

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0

    # Calculate percentage of total (relative to grand total)
    df['percentage_of_total'] = df['queries'] / grand_total_queries if grand_total_queries > 0 else 0

    # Calculate AOV (Average Order Value)
    df['aov'] = df['revenue'] / df['purchases']

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to PivotChildRow objects
    children = []
    for _, row in df.iterrows():
        children.append(PivotChildRow(
            search_term=str(row['search_term']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            avg_queries_per_day=float(row['avg_queries_per_day']),
            percentage_of_total=float(row['percentage_of_total']),
            aov=float(row['aov'])
        ))

    return children
