import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import requests
import json
import os
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


# Exchange rate cache
EXCHANGE_RATE_CACHE_FILE = '.exchange_rates_cache.json'


def load_exchange_rate_cache() -> Dict:
    """Load cached exchange rates from file."""
    if os.path.exists(EXCHANGE_RATE_CACHE_FILE):
        try:
            with open(EXCHANGE_RATE_CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_exchange_rate_cache(cache: Dict):
    """Save exchange rates to cache file."""
    with open(EXCHANGE_RATE_CACHE_FILE, 'w') as f:
        json.dump(cache, f)


def fetch_exchange_rates(start_date: datetime, end_date: datetime) -> Dict[str, float]:
    """
    Fetch USD/CLP exchange rates for a date range.
    Uses frankfurter.app API (free, no auth required).
    Falls back to a manual rate if API fails.
    """
    cache = load_exchange_rate_cache()
    rates = {}

    # Generate all dates in range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Default fallback rate (approximate average for 2025)
    DEFAULT_CLP_RATE = 950.0

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')

        # Check cache first
        if date_str in cache:
            rates[date_str] = cache[date_str]
            continue

        # Try to fetch from API
        try:
            # Frankfurter API - supports CLP to USD conversion
            # Note: For 2025 dates (future), we'll use a default rate
            url = f"https://api.frankfurter.app/{date_str}?from=USD&to=CLP"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                if 'rates' in data and 'CLP' in data['rates']:
                    # We get USD to CLP, so 1 USD = X CLP
                    clp_per_usd = data['rates']['CLP']
                    rates[date_str] = clp_per_usd
                    cache[date_str] = clp_per_usd
                else:
                    rates[date_str] = DEFAULT_CLP_RATE
                    cache[date_str] = DEFAULT_CLP_RATE
            else:
                # Use default rate
                rates[date_str] = DEFAULT_CLP_RATE
                cache[date_str] = DEFAULT_CLP_RATE

        except Exception as e:
            # If API fails, use default rate
            print(f"Failed to fetch rate for {date_str}, using default: {e}")
            rates[date_str] = DEFAULT_CLP_RATE
            cache[date_str] = DEFAULT_CLP_RATE

    # Save updated cache
    save_exchange_rate_cache(cache)

    return rates


def apply_currency_conversion(df: pd.DataFrame, exchange_rates: Dict[str, float]) -> pd.DataFrame:
    """
    Apply currency conversion from CLP to USD using daily exchange rates.
    """
    df = df.copy()

    # Create a date string column for mapping
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')

    # Map exchange rates to dataframe
    df['exchange_rate'] = df['date_str'].map(exchange_rates)

    # Fill any missing rates with a default
    df['exchange_rate'] = df['exchange_rate'].fillna(950.0)

    # Convert gross_purchase from CLP to USD
    df['gross_purchase_usd'] = df['gross_purchase'] / df['exchange_rate']

    # Replace gross_purchase with USD version
    df['gross_purchase'] = df['gross_purchase_usd']
    df = df.drop(columns=['gross_purchase_usd', 'date_str'])

    return df


@st.cache_data(show_spinner=False)
def load_data(file_path: str) -> pd.DataFrame:
    """Load CSV data and perform initial processing."""
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    return df


@st.cache_data(show_spinner=False, ttl=3600)
def load_data_from_bigquery(
    project_id: str,
    dataset_table: str,
    credentials_path: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    countries: Optional[List[str]] = None,
    channels: Optional[List[str]] = None,
    sample_percent: Optional[float] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Load data from BigQuery with optimized filtering and aggregation.

    Args:
        project_id: GCP project ID
        dataset_table: Full table path (e.g., 'dataset.table' or 'project.dataset.table')
        credentials_path: Path to service account JSON file (optional if using default credentials)
        date_start: Start date filter (YYYY-MM-DD)
        date_end: End date filter (YYYY-MM-DD)
        countries: List of countries to filter
        channels: List of channels to filter
        sample_percent: Percentage of data to sample (1-100), None for all data
        limit: Optional row limit for testing

    Returns:
        DataFrame with the query results (aggregated by date, country, channel, search_term)
    """
    import time

    try:
        # Set up credentials
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            # Use default credentials
            client = bigquery.Client(project=project_id)

        # Build WHERE clause
        where_conditions = []

        if date_start and date_end:
            where_conditions.append(f"date BETWEEN '{date_start}' AND '{date_end}'")
        elif date_start:
            where_conditions.append(f"date >= '{date_start}'")
        elif date_end:
            where_conditions.append(f"date <= '{date_end}'")

        if countries and len(countries) > 0:
            country_list = "', '".join(countries)
            where_conditions.append(f"country IN ('{country_list}')")

        if channels and len(channels) > 0:
            channel_list = "', '".join(channels)
            where_conditions.append(f"channel IN ('{channel_list}')")

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Sampling clause (for quick exploration of large datasets)
        sample_clause = f"TABLESAMPLE SYSTEM ({sample_percent} PERCENT)" if sample_percent else ""

        # Build optimized query with aggregation
        # This pre-aggregates data in BigQuery to reduce data transfer
        query = f"""
            SELECT
                country,
                channel,
                date,
                search_term,
                n_words_normalized,
                n_attributes,
                attr_categoria,
                attr_tipo,
                attr_genero,
                attr_marca,
                attr_color,
                attr_material,
                attr_talla,
                attr_modelo,
                SUM(visits) as visits,
                SUM(queries) as queries,
                SUM(visits_pdp) as visits_pdp,
                SUM(queries_pdp) as queries_pdp,
                SUM(visits_a2c) as visits_a2c,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as gross_purchase
            FROM `{dataset_table}` {sample_clause}
            {where_clause}
            GROUP BY
                country, channel, date, search_term,
                n_words_normalized, n_attributes,
                attr_categoria, attr_tipo, attr_genero, attr_marca,
                attr_color, attr_material, attr_talla, attr_modelo
            ORDER BY date DESC, queries DESC
            {"LIMIT " + str(limit) if limit else ""}
        """

        # Create placeholders for progress tracking
        info_placeholder = st.empty()
        progress_placeholder = st.empty()
        metrics_placeholder = st.empty()

        info_placeholder.info(f"🔍 Submitting query to BigQuery... (Filters: {len(where_conditions)} applied)")

        # Start query job
        start_time = time.time()
        query_job = client.query(query)

        info_placeholder.info(f"⏳ Query submitted. Job ID: {query_job.job_id}")

        # Poll job status with progress updates
        last_update = 0
        while not query_job.done():
            elapsed = time.time() - start_time

            # Update every 0.5 seconds
            if elapsed - last_update >= 0.5:
                # Get job details
                query_job.reload()

                # Format elapsed time
                if elapsed < 60:
                    elapsed_str = f"{elapsed:.1f}s"
                else:
                    mins = int(elapsed // 60)
                    secs = elapsed % 60
                    elapsed_str = f"{mins}m {secs:.1f}s"

                # Build status message
                status_parts = [f"⏱️ Elapsed: {elapsed_str}"]

                # Add job state
                if query_job.state:
                    status_parts.append(f"Status: {query_job.state}")

                # Show progress info
                progress_placeholder.info(" | ".join(status_parts))

                # Show metrics if available
                if hasattr(query_job, 'total_bytes_processed') and query_job.total_bytes_processed:
                    bytes_processed = query_job.total_bytes_processed
                    if bytes_processed > 0:
                        # Format bytes
                        if bytes_processed < 1024:
                            size_str = f"{bytes_processed} B"
                        elif bytes_processed < 1024**2:
                            size_str = f"{bytes_processed/1024:.2f} KB"
                        elif bytes_processed < 1024**3:
                            size_str = f"{bytes_processed/(1024**2):.2f} MB"
                        else:
                            size_str = f"{bytes_processed/(1024**3):.2f} GB"

                        col1, col2 = metrics_placeholder.columns(2)
                        with col1:
                            st.metric("Bytes Processed", size_str)
                        with col2:
                            st.metric("Elapsed Time", elapsed_str)

                last_update = elapsed

            time.sleep(0.5)

        # Query completed, now download results
        total_elapsed = time.time() - start_time
        if total_elapsed < 60:
            elapsed_str = f"{total_elapsed:.1f}s"
        else:
            mins = int(total_elapsed // 60)
            secs = total_elapsed % 60
            elapsed_str = f"{mins}m {secs:.1f}s"

        info_placeholder.info(f"📥 Query completed in {elapsed_str}. Downloading results...")

        # Get job statistics
        query_job.reload()
        bytes_processed = query_job.total_bytes_processed or 0
        bytes_billed = query_job.total_bytes_billed or 0

        # Format bytes
        def format_bytes(b):
            if b < 1024:
                return f"{b} B"
            elif b < 1024**2:
                return f"{b/1024:.2f} KB"
            elif b < 1024**3:
                return f"{b/(1024**2):.2f} MB"
            else:
                return f"{b/(1024**3):.2f} GB"

        # Download results with progress
        download_start = time.time()
        df = query_job.to_dataframe()
        download_time = time.time() - download_start

        # Convert date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # Clear progress placeholders
        progress_placeholder.empty()
        metrics_placeholder.empty()

        # Show final success message with statistics
        total_time = time.time() - start_time
        if total_time < 60:
            total_time_str = f"{total_time:.1f}s"
        else:
            mins = int(total_time // 60)
            secs = total_time % 60
            total_time_str = f"{mins}m {secs:.1f}s"

        success_msg = f"✅ Loaded {len(df):,} rows from BigQuery in {total_time_str}"
        if bytes_processed > 0:
            success_msg += f" | Processed: {format_bytes(bytes_processed)}"
        if bytes_billed > 0 and bytes_billed != bytes_processed:
            success_msg += f" | Billed: {format_bytes(bytes_billed)}"

        info_placeholder.success(success_msg)

        return df

    except Exception as e:
        st.error(f"❌ Error loading data from BigQuery: {str(e)}")
        raise e


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived metrics for the dataset."""
    df = df.copy()

    # Avoid division by zero
    df['ctr'] = np.where(df['queries'] > 0, df['queries_pdp'] / df['queries'], 0)
    df['a2c_rate'] = np.where(df['queries'] > 0, df['queries_a2c'] / df['queries'], 0)
    df['conversion_rate'] = np.where(df['queries'] > 0, df['purchases'] / df['queries'], 0)
    df['revenue_per_query'] = np.where(df['queries'] > 0, df['gross_purchase'] / df['queries'], 0)
    df['avg_order_value'] = np.where(df['purchases'] > 0, df['gross_purchase'] / df['purchases'], 0)

    return df


def filter_data(
    df: pd.DataFrame,
    date_range: Tuple[datetime, datetime] = None,
    countries: List[str] = None,
    channels: List[str] = None,
    attributes: Dict[str, bool] = None,
    n_attributes_range: Tuple[int, int] = None,
    word_counts: List[str] = None,
    attribute_combinations: List[str] = None
) -> pd.DataFrame:
    """Filter dataframe based on user selections."""
    filtered_df = df.copy()

    # Date range filter
    if date_range:
        filtered_df = filtered_df[
            (filtered_df['date'] >= date_range[0]) &
            (filtered_df['date'] <= date_range[1])
        ]

    # Country filter
    if countries and len(countries) > 0:
        filtered_df = filtered_df[filtered_df['country'].isin(countries)]

    # Channel filter
    if channels and len(channels) > 0:
        filtered_df = filtered_df[filtered_df['channel'].isin(channels)]

    # Individual attribute filters (must have attribute = 1)
    if attributes:
        for attr, value in attributes.items():
            if value:  # Only filter if attribute is selected
                filtered_df = filtered_df[filtered_df[attr] == 1]

    # Number of attributes filter
    if n_attributes_range:
        min_attrs, max_attrs = n_attributes_range
        filtered_df = filtered_df[
            (filtered_df['n_attributes'] >= min_attrs) &
            (filtered_df['n_attributes'] <= max_attrs)
        ]

    # Number of words filter
    if word_counts and len(word_counts) > 0:
        filtered_df = group_word_counts(filtered_df)
        filtered_df = filtered_df[filtered_df['n_words_grouped'].isin(word_counts)]

    # Attribute combination filter
    if attribute_combinations and len(attribute_combinations) > 0:
        filtered_df = add_attribute_combination_column(filtered_df)
        filtered_df = filtered_df[filtered_df['attribute_combination'].isin(attribute_combinations)]

    return filtered_df


def aggregate_by_dimension(
    df: pd.DataFrame,
    dimension: str,
    metric_cols: List[str] = None
) -> pd.DataFrame:
    """Aggregate data by a specific dimension."""
    if metric_cols is None:
        metric_cols = [
            'visits', 'queries', 'visits_pdp', 'queries_pdp',
            'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
        ]

    agg_df = df.groupby(dimension)[metric_cols].sum().reset_index()

    # Recalculate rates after aggregation
    agg_df['ctr'] = np.where(agg_df['queries'] > 0, agg_df['queries_pdp'] / agg_df['queries'], 0)
    agg_df['a2c_rate'] = np.where(agg_df['queries'] > 0, agg_df['queries_a2c'] / agg_df['queries'], 0)
    agg_df['conversion_rate'] = np.where(agg_df['queries'] > 0, agg_df['purchases'] / agg_df['queries'], 0)
    agg_df['revenue_per_query'] = np.where(agg_df['queries'] > 0, agg_df['gross_purchase'] / agg_df['queries'], 0)
    agg_df['avg_order_value'] = np.where(agg_df['purchases'] > 0, agg_df['gross_purchase'] / agg_df['purchases'], 0)

    return agg_df


def aggregate_by_search_term(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data by search term."""
    metric_cols = [
        'visits', 'queries', 'visits_pdp', 'queries_pdp',
        'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    agg_df = df.groupby('search_term')[metric_cols].sum().reset_index()

    # Recalculate rates
    agg_df['ctr'] = np.where(agg_df['queries'] > 0, agg_df['queries_pdp'] / agg_df['queries'], 0)
    agg_df['a2c_rate'] = np.where(agg_df['queries'] > 0, agg_df['queries_a2c'] / agg_df['queries'], 0)
    agg_df['conversion_rate'] = np.where(agg_df['queries'] > 0, agg_df['purchases'] / agg_df['queries'], 0)
    agg_df['revenue_per_query'] = np.where(agg_df['queries'] > 0, agg_df['gross_purchase'] / agg_df['queries'], 0)
    agg_df['avg_order_value'] = np.where(agg_df['purchases'] > 0, agg_df['gross_purchase'] / agg_df['purchases'], 0)

    return agg_df


def aggregate_time_series(
    df: pd.DataFrame,
    freq: str = 'D'
) -> pd.DataFrame:
    """Aggregate data by time period (daily, weekly, monthly)."""
    metric_cols = [
        'visits', 'queries', 'visits_pdp', 'queries_pdp',
        'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    df_ts = df.copy()
    df_ts = df_ts.groupby(pd.Grouper(key='date', freq=freq))[metric_cols].sum().reset_index()

    # Recalculate rates
    df_ts['ctr'] = np.where(df_ts['queries'] > 0, df_ts['queries_pdp'] / df_ts['queries'], 0)
    df_ts['a2c_rate'] = np.where(df_ts['queries'] > 0, df_ts['queries_a2c'] / df_ts['queries'], 0)
    df_ts['conversion_rate'] = np.where(df_ts['queries'] > 0, df_ts['purchases'] / df_ts['queries'], 0)
    df_ts['revenue_per_query'] = np.where(df_ts['queries'] > 0, df_ts['gross_purchase'] / df_ts['queries'], 0)
    df_ts['avg_order_value'] = np.where(df_ts['purchases'] > 0, df_ts['gross_purchase'] / df_ts['purchases'], 0)

    return df_ts


def group_word_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a grouped word count column to the dataframe.
    Groups: 1, 2, 3, 4+
    """
    df = df.copy()
    df['n_words_grouped'] = df['n_words_normalized'].apply(
        lambda x: '4+' if x >= 4 else str(int(x))
    )
    return df


def add_query_volume_segment(df: pd.DataFrame, threshold: int, daily_threshold: int = None) -> pd.DataFrame:
    """
    Add a query volume segment column to the dataframe based on threshold.

    Args:
        df: Input dataframe
        threshold: Minimum total queries for high volume segment
        daily_threshold: Daily average threshold for labels (optional)

    Returns:
        DataFrame with 'query_volume_segment' column added
    """
    df = df.copy()
    # Use daily threshold in labels if provided, otherwise show total
    if daily_threshold:
        df['query_volume_segment'] = df['queries'].apply(
            lambda x: f'High Volume (≥{daily_threshold}/day)' if x >= threshold else f'Low Volume (<{daily_threshold}/day)'
        )
    else:
        df['query_volume_segment'] = df['queries'].apply(
            lambda x: f'High Volume (≥{threshold})' if x >= threshold else f'Low Volume (<{threshold})'
        )
    return df


def aggregate_by_word_count_grouped(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data by grouped word count (1, 2, 3, 4+)."""
    # Add grouped column
    df = group_word_counts(df)

    metric_cols = [
        'visits', 'queries', 'visits_pdp', 'queries_pdp',
        'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    agg_df = df.groupby('n_words_grouped')[metric_cols].sum().reset_index()

    # Recalculate rates
    agg_df['ctr'] = np.where(agg_df['queries'] > 0, agg_df['queries_pdp'] / agg_df['queries'], 0)
    agg_df['a2c_rate'] = np.where(agg_df['queries'] > 0, agg_df['queries_a2c'] / agg_df['queries'], 0)
    agg_df['conversion_rate'] = np.where(agg_df['queries'] > 0, agg_df['purchases'] / agg_df['queries'], 0)
    agg_df['revenue_per_query'] = np.where(agg_df['queries'] > 0, agg_df['gross_purchase'] / agg_df['queries'], 0)
    agg_df['avg_order_value'] = np.where(agg_df['purchases'] > 0, agg_df['gross_purchase'] / agg_df['purchases'], 0)

    # Sort by word count (custom order: 1, 2, 3, 4+)
    order = {'1': 0, '2': 1, '3': 2, '4+': 3}
    agg_df['sort_order'] = agg_df['n_words_grouped'].map(order)
    agg_df = agg_df.sort_values('sort_order').drop('sort_order', axis=1)

    return agg_df


def get_attribute_columns() -> List[str]:
    """Return list of attribute column names."""
    return [
        'attr_categoria', 'attr_tipo', 'attr_genero', 'attr_marca',
        'attr_color', 'attr_material', 'attr_talla', 'attr_modelo'
    ]


def create_attribute_combination_label(row: pd.Series) -> str:
    """
    Create a label representing the combination of attributes in a search.

    Args:
        row: A row from the dataframe with attribute columns

    Returns:
        String label like "marca + color" or "No attributes"
    """
    attr_cols = get_attribute_columns()
    active_attrs = []

    for attr in attr_cols:
        if attr in row and row[attr] == 1:
            # Remove 'attr_' prefix and capitalize
            attr_name = attr.replace('attr_', '').capitalize()
            active_attrs.append(attr_name)

    if len(active_attrs) == 0:
        return "No attributes"
    else:
        # Sort alphabetically for consistency
        active_attrs.sort()
        return " + ".join(active_attrs)


def add_attribute_combination_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a column with attribute combination labels."""
    df = df.copy()
    df['attribute_combination'] = df.apply(create_attribute_combination_label, axis=1)
    return df


def aggregate_by_attribute_combination(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """
    Aggregate data by attribute combinations and return top N by query volume.

    Args:
        df: Input dataframe
        top_n: Number of top combinations to return

    Returns:
        Aggregated dataframe with top attribute combinations
    """
    # Add combination labels
    df = add_attribute_combination_column(df)

    metric_cols = [
        'visits', 'queries', 'visits_pdp', 'queries_pdp',
        'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    agg_df = df.groupby('attribute_combination')[metric_cols].sum().reset_index()

    # Recalculate rates
    agg_df['ctr'] = np.where(agg_df['queries'] > 0, agg_df['queries_pdp'] / agg_df['queries'], 0)
    agg_df['a2c_rate'] = np.where(agg_df['queries'] > 0, agg_df['queries_a2c'] / agg_df['queries'], 0)
    agg_df['conversion_rate'] = np.where(agg_df['queries'] > 0, agg_df['purchases'] / agg_df['queries'], 0)
    agg_df['revenue_per_query'] = np.where(agg_df['queries'] > 0, agg_df['gross_purchase'] / agg_df['queries'], 0)
    agg_df['avg_order_value'] = np.where(agg_df['purchases'] > 0, agg_df['gross_purchase'] / agg_df['purchases'], 0)

    # Sort by queries and get top N
    agg_df = agg_df.sort_values('queries', ascending=False).head(top_n)

    return agg_df


def get_kpis(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate overall KPIs for the filtered dataset."""
    total_queries = df['queries'].sum()
    total_revenue = df['gross_purchase'].sum()
    total_purchases = df['purchases'].sum()
    total_pdp_queries = df['queries_pdp'].sum()

    return {
        'total_queries': int(total_queries),
        'total_revenue': float(total_revenue),
        'avg_ctr': float(total_pdp_queries / total_queries) if total_queries > 0 else 0,
        'avg_conversion': float(total_purchases / total_queries) if total_queries > 0 else 0,
        'revenue_per_query': float(total_revenue / total_queries) if total_queries > 0 else 0,
        'total_purchases': int(total_purchases),
        'avg_order_value': float(total_revenue / total_purchases) if total_purchases > 0 else 0
    }


def analyze_by_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze performance by individual attributes."""
    attr_cols = get_attribute_columns()
    results = []

    for attr in attr_cols:
        attr_data = df[df[attr] == 1]
        if len(attr_data) > 0:
            total_queries = attr_data['queries'].sum()
            results.append({
                'attribute': attr.replace('attr_', '').title(),
                'queries': int(total_queries),
                'queries_pdp': int(attr_data['queries_pdp'].sum()),
                'queries_a2c': int(attr_data['queries_a2c'].sum()),
                'purchases': int(attr_data['purchases'].sum()),
                'revenue': float(attr_data['gross_purchase'].sum()),
                'ctr': float(attr_data['queries_pdp'].sum() / total_queries) if total_queries > 0 else 0,
                'a2c_rate': float(attr_data['queries_a2c'].sum() / total_queries) if total_queries > 0 else 0,
                'conversion_rate': float(attr_data['purchases'].sum() / total_queries) if total_queries > 0 else 0,
                'revenue_per_query': float(attr_data['gross_purchase'].sum() / total_queries) if total_queries > 0 else 0
            })

    return pd.DataFrame(results)


def get_top_search_terms(
    df: pd.DataFrame,
    metric: str = 'queries',
    top_n: int = 20
) -> pd.DataFrame:
    """Get top N search terms by specified metric."""
    agg_df = aggregate_by_search_term(df)
    return agg_df.nlargest(top_n, metric)


def compare_periods(
    df: pd.DataFrame,
    period1: Tuple[datetime, datetime],
    period2: Tuple[datetime, datetime]
) -> Dict[str, pd.DataFrame]:
    """Compare metrics between two time periods."""
    df1 = filter_data(df, date_range=period1)
    df2 = filter_data(df, date_range=period2)

    kpis1 = get_kpis(df1)
    kpis2 = get_kpis(df2)

    # Calculate percentage changes
    comparison = {}
    for key in kpis1.keys():
        val1 = kpis1[key]
        val2 = kpis2[key]
        pct_change = ((val2 - val1) / val1 * 100) if val1 > 0 else 0
        comparison[key] = {
            'period1': val1,
            'period2': val2,
            'change': val2 - val1,
            'pct_change': pct_change
        }

    return {
        'comparison': comparison,
        'period1_data': df1,
        'period2_data': df2
    }


def get_available_dimensions() -> Dict[str, str]:
    """
    Return available dimensions for cross-tab analysis.

    Returns:
        Dictionary mapping display names to internal column names/keys
    """
    return {
        'Attribute Combination': 'attribute_combination',
        'Number of Words': 'n_words_grouped',
        'Query Volume Segment': 'query_volume_segment',
        'Channel': 'channel',
        'Country': 'country',
        'Category': 'gcategory_name'
    }


def get_available_metrics() -> Dict[str, Dict[str, str]]:
    """
    Return available metrics for cross-tab analysis.

    Returns:
        Dictionary mapping display names to metric info (column name and format)
    """
    return {
        'Conversion Rate (%)': {'column': 'conversion_rate', 'format': 'percent'},
        'Click-Through Rate (%)': {'column': 'ctr', 'format': 'percent'},
        'Add-to-Cart Rate (%)': {'column': 'a2c_rate', 'format': 'percent'},
        'Revenue per Query ($)': {'column': 'revenue_per_query', 'format': 'currency'},
        'Avg Order Value ($)': {'column': 'avg_order_value', 'format': 'currency'},
        'Queries': {'column': 'queries', 'format': 'number'},
        'Purchases': {'column': 'purchases', 'format': 'number'}
    }


def prepare_dimension_data(df: pd.DataFrame, dimension: str, volume_threshold: int = 100, daily_volume_threshold: int = None) -> pd.DataFrame:
    """
    Prepare dataframe by adding computed dimension columns if needed.

    Args:
        df: Input dataframe
        dimension: Dimension key from get_available_dimensions()
        volume_threshold: Total query threshold for query volume segment (default: 100)
        daily_volume_threshold: Daily average threshold for labels (optional)

    Returns:
        DataFrame with dimension column added if needed
    """
    df = df.copy()

    # Skip if dimension column already exists (e.g., from BigQuery aggregation)
    if dimension in df.columns:
        return df

    if dimension == 'attribute_combination':
        df = add_attribute_combination_column(df)
    elif dimension == 'n_words_grouped':
        df = group_word_counts(df)
    elif dimension == 'query_volume_segment':
        df = add_query_volume_segment(df, volume_threshold, daily_volume_threshold)
    elif dimension.startswith('attr_'):
        # For individual attributes, create a readable label
        df[f'{dimension}_label'] = df[dimension].apply(
            lambda x: dimension.replace('attr_', '').capitalize() if x == 1 else 'Other'
        )
        # Use the label column instead
        dimension = f'{dimension}_label'

    return df


def create_crosstab_data(
    df: pd.DataFrame,
    row_dimension: str,
    col_dimension: str,
    metric: str
) -> pd.DataFrame:
    """
    Create cross-tab data for two dimensions and a metric.

    Args:
        df: Input dataframe (already filtered)
        row_dimension: Dimension key for rows
        col_dimension: Dimension key for columns
        metric: Metric column name to aggregate

    Returns:
        Pivot table with row dimension as index, column dimension as columns
    """
    # Prepare data with computed dimensions
    df = prepare_dimension_data(df, row_dimension)
    df = prepare_dimension_data(df, col_dimension)

    # Handle attribute label columns
    if row_dimension.startswith('attr_') and f'{row_dimension}_label' in df.columns:
        row_dim_col = f'{row_dimension}_label'
    else:
        row_dim_col = row_dimension

    if col_dimension.startswith('attr_') and f'{col_dimension}_label' in df.columns:
        col_dim_col = f'{col_dimension}_label'
    else:
        col_dim_col = col_dimension

    # Aggregate metrics by both dimensions
    metric_cols = [
        'visits', 'queries', 'visits_pdp', 'queries_pdp',
        'visits_a2c', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    grouped = df.groupby([row_dim_col, col_dim_col])[metric_cols].sum().reset_index()

    # Recalculate rates
    grouped['ctr'] = np.where(grouped['queries'] > 0, grouped['queries_pdp'] / grouped['queries'], 0)
    grouped['a2c_rate'] = np.where(grouped['queries'] > 0, grouped['queries_a2c'] / grouped['queries'], 0)
    grouped['conversion_rate'] = np.where(grouped['queries'] > 0, grouped['purchases'] / grouped['queries'], 0)
    grouped['revenue_per_query'] = np.where(grouped['queries'] > 0, grouped['gross_purchase'] / grouped['queries'], 0)
    grouped['avg_order_value'] = np.where(grouped['purchases'] > 0, grouped['gross_purchase'] / grouped['purchases'], 0)

    # Create pivot table
    pivot = grouped.pivot(index=row_dim_col, columns=col_dim_col, values=metric)

    # Sort by total queries in each dimension
    row_totals = grouped.groupby(row_dim_col)['queries'].sum().sort_values(ascending=False)
    col_totals = grouped.groupby(col_dim_col)['queries'].sum().sort_values(ascending=False)

    # Reindex to sort
    pivot = pivot.reindex(index=row_totals.index, columns=col_totals.index)

    return pivot


def get_drill_down_search_terms(
    df: pd.DataFrame,
    row_dimension: str,
    row_value: str,
    col_dimension: str,
    col_value: str,
    metric: str,
    top_n: int = 20
) -> pd.DataFrame:
    """
    Get search terms for a specific cell in the cross-tab.

    Args:
        df: Input dataframe (already filtered)
        row_dimension: Row dimension key
        row_value: Specific value in row dimension
        col_dimension: Column dimension key
        col_value: Specific value in column dimension
        metric: Metric to display
        top_n: Number of top search terms to return

    Returns:
        DataFrame with search terms, queries, and metric
    """
    # Prepare data with computed dimensions
    df = prepare_dimension_data(df, row_dimension)
    df = prepare_dimension_data(df, col_dimension)

    # Handle attribute label columns
    if row_dimension.startswith('attr_') and f'{row_dimension}_label' in df.columns:
        row_dim_col = f'{row_dimension}_label'
    else:
        row_dim_col = row_dimension

    if col_dimension.startswith('attr_') and f'{col_dimension}_label' in df.columns:
        col_dim_col = f'{col_dimension}_label'
    else:
        col_dim_col = col_dimension

    # Filter to specific cell
    cell_data = df[
        (df[row_dim_col] == row_value) &
        (df[col_dim_col] == col_value)
    ]

    if len(cell_data) == 0:
        return pd.DataFrame()

    # Aggregate by search term
    search_terms = aggregate_by_search_term(cell_data)

    # Get top N by queries
    search_terms = search_terms.nlargest(top_n, 'queries')

    # Return only relevant columns
    return search_terms[['search_term', 'queries', metric]].copy()


@st.cache_data(ttl=600, show_spinner=False)
def build_hierarchical_table(
    df: pd.DataFrame,
    dimensions: List[str],
    max_rows_per_level: int = 20,
    sort_configs: List[dict] = None,
    total_queries: int = None,
    cumulative_threshold: float = 0.80,
    volume_threshold: int = 100,
    daily_volume_threshold: int = None
) -> pd.DataFrame:
    """
    Build hierarchical table data for multiple dimensions.

    Args:
        df: Input dataframe (already filtered)
        dimensions: List of dimension keys in hierarchy order
        max_rows_per_level: DEPRECATED - kept for backward compatibility
        sort_configs: List of sort configurations, one per dimension level
                     Each config is {'sort_by': 'queries', 'ascending': False}
        total_queries: Total queries for overall cumulative % calculation
        cumulative_threshold: Show rows until this cumulative % is reached (default 0.80 = 80%)
        volume_threshold: Total query threshold for query volume segment (default: 100)
        daily_volume_threshold: Daily average threshold for labels (optional)

    Returns:
        DataFrame with hierarchical structure including:
        - level: hierarchy level (0, 1, 2, ...)
        - path: unique path identifier for expand/collapse
        - dimension_name: name of current dimension
        - dimension_value: value for this row
        - parent_path: path to parent row
        - queries, ctr, conversion_rate, etc.: metrics
    """
    # Default sort configs if not provided
    if sort_configs is None:
        sort_configs = [{'sort_by': 'queries', 'ascending': False}] * len(dimensions)
    if not dimensions or len(dimensions) == 0:
        return pd.DataFrame()

    # Prepare data with all needed dimension columns
    df_prep = df.copy()
    for dim in dimensions:
        df_prep = prepare_dimension_data(df_prep, dim, volume_threshold, daily_volume_threshold)

    # Convert dimension keys to actual column names
    dim_cols = []
    for dim in dimensions:
        if dim.startswith('attr_') and f'{dim}_label' in df_prep.columns:
            dim_cols.append(f'{dim}_label')
        else:
            dim_cols.append(dim)

    # Metric columns to aggregate
    metric_cols = [
        'queries', 'queries_pdp', 'queries_a2c', 'purchases', 'gross_purchase'
    ]

    # Build hierarchical rows
    rows = []

    def add_level_rows(filtered_df, level, parent_path="", dim_index=0):
        """Recursively build rows for each level."""
        if dim_index >= len(dim_cols):
            return

        current_dim = dim_cols[dim_index]

        # Aggregate by current dimension
        grouped = filtered_df.groupby(current_dim)[metric_cols].sum().reset_index()

        # Calculate rates
        grouped['ctr'] = np.where(grouped['queries'] > 0, grouped['queries_pdp'] / grouped['queries'], 0)
        grouped['a2c_rate'] = np.where(grouped['queries'] > 0, grouped['queries_a2c'] / grouped['queries'], 0)
        grouped['conversion_rate'] = np.where(grouped['queries'] > 0, grouped['purchases'] / grouped['queries'], 0)
        grouped['revenue_per_query'] = np.where(grouped['queries'] > 0, grouped['gross_purchase'] / grouped['queries'], 0)
        grouped['avg_order_value'] = np.where(grouped['purchases'] > 0, grouped['gross_purchase'] / grouped['purchases'], 0)

        # Calculate total for accurate cumulative %
        total_queries_in_parent = grouped['queries'].sum()

        # Get sort config for this level
        sort_config = sort_configs[dim_index] if dim_index < len(sort_configs) else {'sort_by': 'queries', 'ascending': False}

        # Sort by specified metric for this level
        grouped = grouped.sort_values(sort_config['sort_by'], ascending=sort_config['ascending'])

        # Calculate cumulative % BEFORE each row (to determine what to keep vs aggregate)
        grouped['cumulative_before'] = grouped['queries'].cumsum().shift(1, fill_value=0)
        grouped['cumulative_pct_before'] = (grouped['cumulative_before'] / total_queries_in_parent * 100) if total_queries_in_parent > 0 else 0

        # Calculate actual cumulative % (for display)
        grouped['cumulative_queries_in_group'] = grouped['queries'].cumsum()
        grouped['cumulative_pct_in_group'] = (grouped['cumulative_queries_in_group'] / total_queries_in_parent * 100) if total_queries_in_parent > 0 else 0

        # Split rows: keep rows that START before threshold, aggregate the rest
        threshold_pct = cumulative_threshold * 100
        rows_to_keep = grouped[grouped['cumulative_pct_before'] < threshold_pct]
        rows_to_aggregate = grouped[grouped['cumulative_pct_before'] >= threshold_pct]

        # If first row already exceeds threshold, keep at least one row
        if len(rows_to_keep) == 0 and len(grouped) > 0:
            rows_to_keep = grouped.head(1)
            rows_to_aggregate = grouped.iloc[1:]

        # Add rows up to threshold
        for idx, row in rows_to_keep.iterrows():
            value = row[current_dim]
            path = f"{parent_path}/{current_dim}={value}" if parent_path else f"{current_dim}={value}"

            # Calculate % of parent
            pct_of_parent = (row['queries'] / total_queries_in_parent * 100) if total_queries_in_parent > 0 else 0

            row_data = {
                'level': level,
                'path': path,
                'parent_path': parent_path,
                'dimension_name': dimensions[dim_index],
                'dimension_value': str(value),
                'queries': int(row['queries']),
                'pct_of_parent': float(pct_of_parent),
                'cumulative_pct_in_parent': float(row['cumulative_pct_in_group']),
                'ctr': float(row['ctr']),
                'a2c_rate': float(row['a2c_rate']),
                'conversion_rate': float(row['conversion_rate']),
                'revenue_per_query': float(row['revenue_per_query']),
                'purchases': int(row['purchases']),
                'gross_purchase': float(row['gross_purchase']),
                'avg_order_value': float(row['avg_order_value']),
                'has_children': dim_index < len(dim_cols) - 1,
                'is_other': False
            }
            rows.append(row_data)

            # Recursively add child levels (if not last dimension)
            if dim_index < len(dim_cols) - 1:
                child_df = filtered_df[filtered_df[current_dim] == value]
                add_level_rows(child_df, level + 1, path, dim_index + 1)

        # Aggregate remaining rows into "Other"
        if len(rows_to_aggregate) > 0:
            other_metrics = rows_to_aggregate[metric_cols].sum()

            # Store the values that are aggregated into "Other"
            other_values = rows_to_aggregate[current_dim].tolist()

            # Calculate rates for "Other"
            other_ctr = other_metrics['queries_pdp'] / other_metrics['queries'] if other_metrics['queries'] > 0 else 0
            other_a2c = other_metrics['queries_a2c'] / other_metrics['queries'] if other_metrics['queries'] > 0 else 0
            other_conv = other_metrics['purchases'] / other_metrics['queries'] if other_metrics['queries'] > 0 else 0
            other_rpq = other_metrics['gross_purchase'] / other_metrics['queries'] if other_metrics['queries'] > 0 else 0
            other_aov = other_metrics['gross_purchase'] / other_metrics['purchases'] if other_metrics['purchases'] > 0 else 0

            # Calculate cumulative for "Other" row
            last_cumulative = rows_to_keep['cumulative_queries_in_group'].iloc[-1] if len(rows_to_keep) > 0 else 0
            other_cumulative = last_cumulative + other_metrics['queries']
            other_cumulative_pct = (other_cumulative / total_queries_in_parent * 100) if total_queries_in_parent > 0 else 0

            # Calculate % of parent for "Other"
            other_pct_of_parent = (other_metrics['queries'] / total_queries_in_parent * 100) if total_queries_in_parent > 0 else 0

            path = f"{parent_path}/{current_dim}=Other" if parent_path else f"{current_dim}=Other"

            other_row = {
                'level': level,
                'path': path,
                'parent_path': parent_path,
                'dimension_name': dimensions[dim_index],
                'dimension_value': 'Other',
                'queries': int(other_metrics['queries']),
                'pct_of_parent': float(other_pct_of_parent),
                'cumulative_pct_in_parent': float(other_cumulative_pct),
                'ctr': float(other_ctr),
                'a2c_rate': float(other_a2c),
                'conversion_rate': float(other_conv),
                'revenue_per_query': float(other_rpq),
                'purchases': int(other_metrics['purchases']),
                'gross_purchase': float(other_metrics['gross_purchase']),
                'avg_order_value': float(other_aov),
                'has_children': dim_index < len(dim_cols) - 1,  # "Other" can have children if not last dimension
                'is_other': True,
                'other_values': other_values  # Store the values aggregated into "Other"
            }
            rows.append(other_row)

            # Recursively add child levels for "Other" (if not last dimension)
            if dim_index < len(dim_cols) - 1:
                # Filter to only the values that are in "Other"
                other_df = filtered_df[filtered_df[current_dim].isin(other_values)]
                add_level_rows(other_df, level + 1, path, dim_index + 1)

    # Start building from level 0
    add_level_rows(df_prep, 0, "", 0)

    result_df = pd.DataFrame(rows)

    # Calculate overall cumulative % (only for level 0 rows to avoid double-counting)
    if not result_df.empty:
        # Use provided total_queries or calculate from original df
        if total_queries is None:
            total_queries = int(df['queries'].sum())

        # Calculate cumulative only for level 0 rows
        level_0_mask = result_df['level'] == 0
        level_0_df = result_df[level_0_mask].copy()

        if not level_0_df.empty:
            # Reset index to preserve the original order from traversal (which is already sorted)
            level_0_df = level_0_df.reset_index(drop=True)

            # Calculate cumulative in the order rows appear (which is sorted order)
            level_0_df['cumulative_queries_overall'] = level_0_df['queries'].cumsum()
            level_0_df['cumulative_pct_overall'] = (level_0_df['cumulative_queries_overall'] / total_queries * 100) if total_queries > 0 else 0

            # Create a mapping from path to cumulative values
            path_to_cumulative = level_0_df.set_index('path')[['cumulative_queries_overall', 'cumulative_pct_overall']].to_dict('index')

            # Add cumulative values to result_df
            result_df['cumulative_queries_overall'] = result_df.apply(
                lambda row: path_to_cumulative.get(row['path'], {}).get('cumulative_queries_overall', 0) if row['level'] == 0 else 0,
                axis=1
            )
            result_df['cumulative_pct_overall'] = result_df.apply(
                lambda row: path_to_cumulative.get(row['path'], {}).get('cumulative_pct_overall', 0) if row['level'] == 0 else 0,
                axis=1
            )

    return result_df


# ============================================================================
# ON-DEMAND BIGQUERY QUERY MANAGER
# ============================================================================

import hashlib
import time

def get_query_hash(query: str) -> str:
    """
    Generate a hash for a query string to use as cache key.

    Args:
        query: SQL query string

    Returns:
        SHA256 hash of the query
    """
    return hashlib.sha256(query.encode()).hexdigest()[:16]


def is_cache_valid(cache_entry: Dict, ttl: int = 300) -> bool:
    """
    Check if a cache entry is still valid based on TTL.

    Args:
        cache_entry: Dictionary with 'timestamp' and 'data' keys
        ttl: Time-to-live in seconds (default 5 minutes)

    Returns:
        True if cache is still valid, False otherwise
    """
    if cache_entry is None:
        return False
    current_time = time.time()
    return (current_time - cache_entry.get('timestamp', 0)) < ttl


def cleanup_cache(cache_dict: Dict, current_time: float = None) -> None:
    """
    Remove expired entries from cache dictionary (in-place).

    Args:
        cache_dict: Cache dictionary to clean up
        current_time: Current timestamp (defaults to time.time())
    """
    if current_time is None:
        current_time = time.time()

    # Find expired keys
    expired_keys = [
        key for key, entry in cache_dict.items()
        if not is_cache_valid(entry)
    ]

    # Remove expired entries
    for key in expired_keys:
        del cache_dict[key]


def get_bigquery_client(
    project_id: str,
    credentials_path: Optional[str] = None
) -> bigquery.Client:
    """
    Get or create a BigQuery client (reusable).

    Args:
        project_id: GCP project ID
        credentials_path: Path to service account JSON (optional)

    Returns:
        BigQuery client instance
    """
    # Store client in session state for reuse
    if 'bq_client' not in st.session_state:
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            st.session_state.bq_client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            st.session_state.bq_client = bigquery.Client(project=project_id)

    return st.session_state.bq_client


def build_filter_clause(
    base_filters: Dict,
    parent_filters: Optional[Dict] = None,
    dataset_table: str = None
) -> str:
    """
    Build WHERE clause from filter dictionaries.

    Args:
        base_filters: Dictionary with base filters (date_start, date_end, countries, channels, etc.)
        parent_filters: Optional parent dimension filters for drill-down
        dataset_table: Table name for sampling clause

    Returns:
        WHERE clause string (includes "WHERE" keyword if non-empty)
    """
    conditions = []

    # Date filters
    if base_filters.get('date_start') and base_filters.get('date_end'):
        conditions.append(f"date BETWEEN '{base_filters['date_start']}' AND '{base_filters['date_end']}'")
    elif base_filters.get('date_start'):
        conditions.append(f"date >= '{base_filters['date_start']}'")
    elif base_filters.get('date_end'):
        conditions.append(f"date <= '{base_filters['date_end']}'")

    # Country filter
    if base_filters.get('countries'):
        country_list = "', '".join(base_filters['countries'])
        conditions.append(f"country IN ('{country_list}')")

    # Channel filter
    if base_filters.get('channels'):
        channel_list = "', '".join(base_filters['channels'])
        conditions.append(f"channel IN ('{channel_list}')")

    # Category filter
    if base_filters.get('categories'):
        category_list = "', '".join(base_filters['categories'])
        conditions.append(f"gcategory_name IN ('{category_list}')")

    # Number of attributes filter
    if base_filters.get('n_attributes_range'):
        min_attrs, max_attrs = base_filters['n_attributes_range']
        conditions.append(f"n_attributes >= {min_attrs} AND n_attributes <= {max_attrs}")

    # Word count filter
    if base_filters.get('word_counts'):
        # Convert word count groups to SQL conditions
        word_conditions = []
        for wc in base_filters['word_counts']:
            if wc == '4+':
                word_conditions.append("n_words_normalized >= 4")
            else:
                word_conditions.append(f"n_words_normalized = {wc}")
        if word_conditions:
            conditions.append(f"({' OR '.join(word_conditions)})")

    # Parent dimension filters (for drill-down)
    if parent_filters:
        for dim, value in parent_filters.items():
            # Escape single quotes in values
            safe_value = value.replace("'", "\\'")
            conditions.append(f"{dim} = '{safe_value}'")

    return "WHERE " + " AND ".join(conditions) if conditions else ""


def execute_cached_query(
    client: bigquery.Client,
    query: str,
    ttl: int = 300,
    show_progress: bool = False
) -> pd.DataFrame:
    """
    Execute a BigQuery query with caching support.

    Args:
        client: BigQuery client
        query: SQL query string
        ttl: Cache time-to-live in seconds (default 5 minutes)
        show_progress: Show progress tracking UI

    Returns:
        DataFrame with query results
    """
    # Initialize cache in session state
    if 'query_cache' not in st.session_state:
        st.session_state.query_cache = {}

    # Clean up expired cache entries (max every 60 seconds)
    if 'last_cache_cleanup' not in st.session_state or \
       (time.time() - st.session_state.get('last_cache_cleanup', 0)) > 60:
        cleanup_cache(st.session_state.query_cache)
        st.session_state.last_cache_cleanup = time.time()

    # Check cache
    query_hash = get_query_hash(query)
    cache_entry = st.session_state.query_cache.get(query_hash)

    if is_cache_valid(cache_entry, ttl):
        # Return cached result
        return cache_entry['data']

    # Execute query
    if show_progress:
        import time as time_module
        info_placeholder = st.empty()
        start_time = time_module.time()

        info_placeholder.info("⏳ Executing BigQuery query...")
        query_job = client.query(query)

        # Wait for completion
        query_job.result()

        elapsed = time_module.time() - start_time
        info_placeholder.empty()
    else:
        query_job = client.query(query)

    # Get results
    df = query_job.to_dataframe()

    # Convert date column if present
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # Cache result
    st.session_state.query_cache[query_hash] = {
        'data': df,
        'timestamp': time.time(),
        'bytes_processed': query_job.total_bytes_processed or 0,
        'bytes_billed': query_job.total_bytes_billed or 0
    }

    # Track total bytes processed in session
    if 'total_bytes_processed' not in st.session_state:
        st.session_state.total_bytes_processed = 0
    st.session_state.total_bytes_processed += (query_job.total_bytes_processed or 0)

    return df


# ============================================================================
# QUERY BUILDERS FOR ON-DEMAND SECTIONS
# ============================================================================

def build_kpi_query(dataset_table: str, base_filters: Dict) -> str:
    """
    Build query for Overview KPIs (single aggregated query).

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    query = f"""
        SELECT
            SUM(queries) as total_queries,
            SUM(queries_pdp) as total_queries_pdp,
            SUM(queries_a2c) as total_queries_a2c,
            SUM(purchases) as total_purchases,
            SUM(gross_purchase) as total_revenue
        FROM `{dataset_table}`
        {where_clause}
    """
    return query


def build_timeseries_query(dataset_table: str, base_filters: Dict, freq: str = 'D') -> str:
    """
    Build query for time-series trend analysis.

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary
        freq: Frequency ('D' for daily, 'W' for weekly, 'M' for monthly)

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    # Map frequency to BigQuery date functions
    if freq == 'D':
        date_expr = "date"
    elif freq == 'W':
        date_expr = "DATE_TRUNC(date, WEEK)"
    elif freq == 'M':
        date_expr = "DATE_TRUNC(date, MONTH)"
    else:
        date_expr = "date"

    query = f"""
        SELECT
            {date_expr} as date,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY date
        ORDER BY date
    """
    return query


def build_channel_query(dataset_table: str, base_filters: Dict) -> str:
    """
    Build query for channel performance analysis.

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    query = f"""
        SELECT
            channel,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY channel
        ORDER BY queries DESC
    """
    return query


def build_word_count_query(dataset_table: str, base_filters: Dict) -> str:
    """
    Build query for word count analysis (grouped 1, 2, 3, 4+).

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    query = f"""
        SELECT
            CASE
                WHEN n_words_normalized >= 4 THEN '4+'
                ELSE CAST(CAST(n_words_normalized AS INT64) AS STRING)
            END as n_words_grouped,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY n_words_grouped
        ORDER BY
            CASE n_words_grouped
                WHEN '1' THEN 1
                WHEN '2' THEN 2
                WHEN '3' THEN 3
                WHEN '4+' THEN 4
            END
    """
    return query


def build_attribute_combination_query(dataset_table: str, base_filters: Dict, top_n: int = 15) -> str:
    """
    Build query for attribute combination analysis.

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary
        top_n: Number of top combinations to return

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    query = f"""
        WITH attr_combos AS (
            SELECT
                CASE
                    WHEN attr_categoria = 0 AND attr_tipo = 0 AND attr_genero = 0 AND
                         attr_marca = 0 AND attr_color = 0 AND attr_material = 0 AND
                         attr_talla = 0 AND attr_modelo = 0 THEN 'No attributes'
                    ELSE ARRAY_TO_STRING([
                        IF(attr_categoria = 1, 'Categoria', NULL),
                        IF(attr_tipo = 1, 'Tipo', NULL),
                        IF(attr_genero = 1, 'Genero', NULL),
                        IF(attr_marca = 1, 'Marca', NULL),
                        IF(attr_color = 1, 'Color', NULL),
                        IF(attr_material = 1, 'Material', NULL),
                        IF(attr_talla = 1, 'Talla', NULL),
                        IF(attr_modelo = 1, 'Modelo', NULL)
                    ], ' + ')
                END as attribute_combination,
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as gross_purchase
            FROM `{dataset_table}`
            {where_clause}
            GROUP BY attribute_combination
        )
        SELECT *
        FROM attr_combos
        ORDER BY queries DESC
        LIMIT {top_n}
    """
    return query


def build_search_terms_query(
    dataset_table: str,
    base_filters: Dict,
    search_filter: Optional[str] = None,
    sort_by: str = 'queries',
    ascending: bool = False,
    limit: Optional[int] = None
) -> str:
    """
    Build query for search term explorer.

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary
        search_filter: Optional text filter for search term
        sort_by: Column to sort by
        ascending: Sort direction
        limit: Max rows to return

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters)

    # Add search term filter if provided
    if search_filter:
        if where_clause:
            where_clause += f" AND search_term LIKE '%{search_filter}%'"
        else:
            where_clause = f"WHERE search_term LIKE '%{search_filter}%'"

    sort_direction = "ASC" if ascending else "DESC"
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT
            search_term,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY search_term
        ORDER BY {sort_by} {sort_direction}
        {limit_clause}
    """
    return query


def build_dimension_level_query(
    dataset_table: str,
    base_filters: Dict,
    dimension: str,
    parent_filters: Optional[Dict] = None,
    cumulative_threshold: float = 0.80,
    sort_by: str = 'queries',
    ascending: bool = False,
    volume_threshold: int = 100,
    daily_volume_threshold: int = None
) -> str:
    """
    Build query for one level of hierarchical pivot table.

    Args:
        dataset_table: Full BigQuery table path
        base_filters: Base filter dictionary
        dimension: Dimension column name to group by
        parent_filters: Optional filters from parent dimensions
        cumulative_threshold: Show rows until this cumulative % is reached
        sort_by: Column to sort by
        ascending: Sort direction
        volume_threshold: Total query threshold for query_volume_segment dimension
        daily_volume_threshold: Daily average threshold for labels (optional)

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters, parent_filters)

    # Get number of days for queries_per_day calculation
    num_days = base_filters.get('num_days', 1)

    # Map derived metrics to their SQL expressions for sorting
    # Note: average_queries_per_day is calculated in the CTE, so we just reference the column
    derived_metric_expressions = {
        'ctr': 'SAFE_DIVIDE(queries_pdp, queries)',
        'a2c_rate': 'SAFE_DIVIDE(queries_a2c, queries)',
        'conversion_rate': 'SAFE_DIVIDE(purchases, queries)',
        'revenue_per_query': 'SAFE_DIVIDE(gross_purchase, queries)',
        'avg_order_value': 'SAFE_DIVIDE(gross_purchase, purchases)',
        'average_queries_per_day': 'average_queries_per_day'
    }

    # Use derived expression for sorting if applicable
    sort_expression = derived_metric_expressions.get(sort_by, sort_by)

    # For word count grouping
    if dimension == 'n_words_grouped':
        dimension_expr = """
            CASE
                WHEN n_words_normalized >= 4 THEN '4+'
                ELSE CAST(CAST(n_words_normalized AS INT64) AS STRING)
            END
        """
    # For attribute combination
    elif dimension == 'attribute_combination':
        dimension_expr = """
            CASE
                WHEN attr_categoria = 0 AND attr_tipo = 0 AND attr_genero = 0 AND
                     attr_marca = 0 AND attr_color = 0 AND attr_material = 0 AND
                     attr_talla = 0 AND attr_modelo = 0 THEN 'No attributes'
                ELSE ARRAY_TO_STRING([
                    IF(attr_categoria = 1, 'Categoria', NULL),
                    IF(attr_tipo = 1, 'Tipo', NULL),
                    IF(attr_genero = 1, 'Genero', NULL),
                    IF(attr_marca = 1, 'Marca', NULL),
                    IF(attr_color = 1, 'Color', NULL),
                    IF(attr_material = 1, 'Material', NULL),
                    IF(attr_talla = 1, 'Talla', NULL),
                    IF(attr_modelo = 1, 'Modelo', NULL)
                ], ' + ')
            END
        """
    # For query volume segment
    elif dimension == 'query_volume_segment':
        # Note: This is computed AFTER aggregation by search_term, so we need a subquery
        # Use daily threshold in label if provided, otherwise use total
        if daily_volume_threshold:
            dimension_expr = f"""
            CASE
                WHEN queries >= {volume_threshold} THEN 'High Volume (≥{daily_volume_threshold}/day)'
                ELSE 'Low Volume (<{daily_volume_threshold}/day)'
            END
        """
        else:
            dimension_expr = f"""
            CASE
                WHEN queries >= {volume_threshold} THEN 'High Volume (≥{volume_threshold})'
                ELSE 'Low Volume (<{volume_threshold})'
            END
        """
    else:
        # For regular dimensions, handle NULL values by converting them to '-'
        dimension_expr = f"COALESCE({dimension}, '-')"

    sort_direction = "ASC" if ascending else "DESC"

    # Special handling for query_volume_segment - needs two-step aggregation
    if dimension == 'query_volume_segment':
        query = f"""
            WITH search_term_aggregation AS (
                SELECT
                    search_term,
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(gross_purchase) as gross_purchase
                FROM `{dataset_table}`
                {where_clause}
                GROUP BY search_term
            ),
            dimension_totals AS (
                SELECT
                    {dimension_expr} as dimension_value,
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(gross_purchase) as gross_purchase,
                    COUNT(DISTINCT search_term) as search_term_count,
                    SAFE_DIVIDE(SUM(queries), {num_days}) as average_queries_per_day,
                    SAFE_DIVIDE(SAFE_DIVIDE(SUM(queries), {num_days}), COUNT(DISTINCT search_term)) as average_queries_per_search_term_per_day
                FROM search_term_aggregation
                GROUP BY dimension_value
                ORDER BY {sort_expression} {sort_direction}
            ),
            grand_total AS (
                SELECT SUM(queries) as total_queries_all
                FROM dimension_totals
            ),
            cumulative_by_queries AS (
                SELECT
                    *,
                    SUM(queries) OVER (ORDER BY queries DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_queries,
                    SUM(queries) OVER () as total_queries,
                    ROW_NUMBER() OVER (ORDER BY queries DESC) as row_num
                FROM dimension_totals
            ),
            with_cumulative AS (
                SELECT
                    *,
                    COALESCE(LAG(cumulative_queries) OVER (ORDER BY row_num), 0) as prev_cumulative_queries
                FROM cumulative_by_queries
            ),
            within_threshold_ids AS (
                SELECT dimension_value
                FROM with_cumulative
                WHERE prev_cumulative_queries / total_queries < {cumulative_threshold}
            ),
            within_threshold AS (
                SELECT
                    dt.dimension_value,
                    dt.queries,
                    dt.queries_pdp,
                    dt.queries_a2c,
                    dt.purchases,
                    dt.gross_purchase,
                    dt.search_term_count,
                    dt.average_queries_per_day,
                    dt.average_queries_per_search_term_per_day
                FROM dimension_totals dt
                WHERE dt.dimension_value IN (SELECT dimension_value FROM within_threshold_ids)
            ),
            beyond_threshold AS (
                SELECT
                    'Other' as dimension_value,
                    SUM(dt.queries) as queries,
                    SUM(dt.queries_pdp) as queries_pdp,
                    SUM(dt.queries_a2c) as queries_a2c,
                    SUM(dt.purchases) as purchases,
                    SUM(dt.gross_purchase) as gross_purchase,
                    SUM(dt.search_term_count) as search_term_count,
                    SAFE_DIVIDE(SUM(dt.queries), {num_days}) as average_queries_per_day,
                    SAFE_DIVIDE(SAFE_DIVIDE(SUM(dt.queries), {num_days}), SUM(dt.search_term_count)) as average_queries_per_search_term_per_day
                FROM dimension_totals dt
                WHERE dt.dimension_value NOT IN (SELECT dimension_value FROM within_threshold_ids)
            )
            SELECT * FROM within_threshold
            UNION ALL
            (SELECT * FROM beyond_threshold WHERE queries > 0)
            ORDER BY
                CASE WHEN dimension_value = 'Other' THEN 1 ELSE 0 END,
                {sort_expression} {sort_direction}
        """
    else:
        # Normal query for other dimensions
        query = f"""
            WITH search_term_aggregation AS (
                SELECT
                    {dimension_expr} as dimension_value,
                    search_term,
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(gross_purchase) as gross_purchase
                FROM `{dataset_table}`
                {where_clause}
                GROUP BY dimension_value, search_term
            ),
            dimension_totals AS (
                SELECT
                    dimension_value,
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(gross_purchase) as gross_purchase,
                    COUNT(DISTINCT search_term) as search_term_count,
                    SAFE_DIVIDE(SUM(queries), {num_days}) as average_queries_per_day,
                    SAFE_DIVIDE(SAFE_DIVIDE(SUM(queries), {num_days}), COUNT(DISTINCT search_term)) as average_queries_per_search_term_per_day
                FROM search_term_aggregation
                GROUP BY dimension_value
                ORDER BY {sort_expression} {sort_direction}
            ),
            grand_total AS (
                SELECT SUM(queries) as total_queries_all
                FROM dimension_totals
            ),
            cumulative_by_queries AS (
                SELECT
                    *,
                    SUM(queries) OVER (ORDER BY queries DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_queries,
                    SUM(queries) OVER () as total_queries,
                    ROW_NUMBER() OVER (ORDER BY queries DESC) as row_num
                FROM dimension_totals
            ),
            with_cumulative AS (
                SELECT
                    *,
                    COALESCE(LAG(cumulative_queries) OVER (ORDER BY row_num), 0) as prev_cumulative_queries
                FROM cumulative_by_queries
            ),
            within_threshold_ids AS (
                SELECT dimension_value
                FROM with_cumulative
                WHERE prev_cumulative_queries / total_queries < {cumulative_threshold}
            ),
            within_threshold AS (
                SELECT
                    dt.dimension_value,
                    dt.queries,
                    dt.queries_pdp,
                    dt.queries_a2c,
                    dt.purchases,
                    dt.gross_purchase,
                    dt.search_term_count,
                    dt.average_queries_per_day,
                    dt.average_queries_per_search_term_per_day
                FROM dimension_totals dt
                WHERE dt.dimension_value IN (SELECT dimension_value FROM within_threshold_ids)
            ),
            beyond_threshold AS (
                SELECT
                    'Other' as dimension_value,
                    SUM(dt.queries) as queries,
                    SUM(dt.queries_pdp) as queries_pdp,
                    SUM(dt.queries_a2c) as queries_a2c,
                    SUM(dt.purchases) as purchases,
                    SUM(dt.gross_purchase) as gross_purchase,
                    SUM(dt.search_term_count) as search_term_count,
                    SAFE_DIVIDE(SUM(dt.queries), {num_days}) as average_queries_per_day,
                    SAFE_DIVIDE(SAFE_DIVIDE(SUM(dt.queries), {num_days}), SUM(dt.search_term_count)) as average_queries_per_search_term_per_day
                FROM dimension_totals dt
                WHERE dt.dimension_value NOT IN (SELECT dimension_value FROM within_threshold_ids)
            )
            SELECT * FROM within_threshold
            UNION ALL
            (SELECT * FROM beyond_threshold WHERE queries > 0)
            ORDER BY
                CASE WHEN dimension_value = 'Other' THEN 1 ELSE 0 END,
                {sort_expression} {sort_direction}
        """
    return query


def build_hierarchical_query(dataset_table: str, base_filters: Dict, dimension_keys: list, volume_threshold: int = 100, daily_volume_threshold: int = None) -> str:
    """
    Build query to fetch aggregated data by multiple dimensions for hierarchical table.

    Args:
        dataset_table: Full BigQuery table name (project.dataset.table)
        base_filters: Dictionary of filter conditions
        dimension_keys: List of dimension column names to group by
        volume_threshold: Total query threshold for query volume segment (default: 100)
        daily_volume_threshold: Daily average threshold for labels (optional)

    Returns:
        SQL query string
    """
    where_clause = build_filter_clause(base_filters, dataset_table=dataset_table)

    # Build SELECT and GROUP BY clauses, handling derived dimensions
    select_cols = []
    group_by_cols = []
    has_volume_segment = 'query_volume_segment' in dimension_keys

    for dim_key in dimension_keys:
        if dim_key == 'attribute_combination':
            # Build attribute combination expression in SQL
            attr_expr = """
                CASE
                    WHEN attr_categoria = 0 AND attr_tipo = 0 AND attr_genero = 0 AND
                         attr_color = 0 AND attr_talla = 0 AND attr_marca = 0 THEN 'No attributes'
                    ELSE ARRAY_TO_STRING([
                        IF(attr_categoria = 1, 'Categoria', NULL),
                        IF(attr_tipo = 1, 'Tipo', NULL),
                        IF(attr_genero = 1, 'Genero', NULL),
                        IF(attr_color = 1, 'Color', NULL),
                        IF(attr_talla = 1, 'Talla', NULL),
                        IF(attr_marca = 1, 'Marca', NULL)
                    ], ' + ')
                END as attribute_combination
            """
            select_cols.append(attr_expr)
            group_by_cols.append('attribute_combination')
        elif dim_key == 'n_words_grouped':
            # Build word count grouping in SQL
            word_expr = """
                CASE
                    WHEN n_words_normalized >= 4 THEN '4+'
                    ELSE CAST(CAST(n_words_normalized AS INT64) AS STRING)
                END as n_words_grouped
            """
            select_cols.append(word_expr)
            group_by_cols.append('n_words_grouped')
        elif dim_key == 'query_volume_segment':
            # For volume segment, we skip it here and handle after aggregation
            pass
        else:
            # Regular column
            select_cols.append(dim_key)
            group_by_cols.append(dim_key)

    # Build query with or without volume segment
    if has_volume_segment:
        # Use a subquery to first aggregate, then classify by volume
        # For the inner query, use full expressions
        inner_select_clause = ',\n                '.join(select_cols) if select_cols else ''
        # For the outer query, just reference the column names
        outer_select_list = [dim for dim in group_by_cols] if group_by_cols else []
        outer_select_clause = ',\n            '.join(outer_select_list) if outer_select_list else ''

        group_by_clause = ', '.join(group_by_cols) if group_by_cols else ''

        if inner_select_clause:
            inner_select_with_comma = f"{inner_select_clause},\n                "
        else:
            inner_select_with_comma = ""

        if outer_select_clause:
            outer_select_with_comma = f"{outer_select_clause},\n            "
        else:
            outer_select_with_comma = ""

        if group_by_clause:
            inner_group_by = f"GROUP BY {group_by_clause}"
        else:
            inner_group_by = ""

        # Use daily threshold in label if provided, otherwise use total
        if daily_volume_threshold:
            volume_label_high = f'High Volume (≥{daily_volume_threshold}/day)'
            volume_label_low = f'Low Volume (<{daily_volume_threshold}/day)'
        else:
            volume_label_high = f'High Volume (≥{volume_threshold})'
            volume_label_low = f'Low Volume (<{volume_threshold})'

        query = f"""
        SELECT
            {outer_select_with_comma}CASE
                WHEN total_queries >= {volume_threshold} THEN '{volume_label_high}'
                ELSE '{volume_label_low}'
            END as query_volume_segment,
            total_queries as queries,
            total_queries_pdp as queries_pdp,
            total_queries_a2c as queries_a2c,
            total_purchases as purchases,
            total_gross_purchase as gross_purchase
        FROM (
            SELECT
                {inner_select_with_comma}SUM(queries) as total_queries,
                SUM(queries_pdp) as total_queries_pdp,
                SUM(queries_a2c) as total_queries_a2c,
                SUM(purchases) as total_purchases,
                SUM(gross_purchase) as total_gross_purchase
            FROM `{dataset_table}`
            {where_clause}
            {inner_group_by}
        )
        ORDER BY queries DESC
        """
    else:
        select_clause = ',\n            '.join(select_cols)
        group_by_clause = ', '.join(group_by_cols)

        query = f"""
        SELECT
            {select_clause},
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY {group_by_clause}
        ORDER BY queries DESC
        """
    return query


def convert_attribute_combination_to_condition(combination_value: str) -> str:
    """
    Convert an attribute combination value to SQL conditions on underlying attr_* columns.

    Args:
        combination_value: String like "Categoria + Tipo" or "No attributes"

    Returns:
        SQL condition string
    """
    # Mapping of display names to column names
    attr_map = {
        'Categoria': 'attr_categoria',
        'Tipo': 'attr_tipo',
        'Genero': 'attr_genero',
        'Color': 'attr_color',
        'Talla': 'attr_talla',
        'Marca': 'attr_marca'
    }

    if combination_value == 'No attributes':
        # All attributes are 0
        conditions = [f"{col} = 0" for col in attr_map.values()]
        return '(' + ' AND '.join(conditions) + ')'

    # Parse the combination value to get active attributes
    # Split by ' + ' to get individual attribute names
    parts = [p.strip() for p in combination_value.split('+')]
    active_attrs = set()
    for part in parts:
        if part in attr_map:
            active_attrs.add(attr_map[part])

    # Build conditions: active attrs = 1, others = 0
    conditions = []
    for display_name, col_name in attr_map.items():
        if col_name in active_attrs:
            conditions.append(f"{col_name} = 1")
        else:
            conditions.append(f"{col_name} = 0")

    return '(' + ' AND '.join(conditions) + ')'


def build_drill_down_query(dataset_table: str, base_filters: Dict, drill_filters: Dict, path_to_other_values: Dict = None) -> str:
    """
    Build query for search term drill-down with dimension filters.

    Args:
        dataset_table: Full BigQuery table name (project.dataset.table)
        base_filters: Dictionary of base filter conditions
        drill_filters: Dictionary of dimension filters from the drill path
        path_to_other_values: Dictionary mapping dimensions to lists of "Other" values

    Returns:
        SQL query string
    """
    # Build base WHERE clause
    where_clause = build_filter_clause(base_filters, dataset_table=dataset_table)

    # Add dimension filters
    drill_conditions = []
    for dim_key, dim_value in drill_filters.items():
        if path_to_other_values and dim_key in path_to_other_values:
            # This is an "Other" row - filter by list of values
            if dim_key == 'attribute_combination':
                # For attribute_combination, convert each value to attribute conditions
                attr_conditions = []
                for val in path_to_other_values[dim_key]:
                    attr_conditions.append(convert_attribute_combination_to_condition(val))
                drill_conditions.append(f"({' OR '.join(attr_conditions)})")
            elif dim_key == 'n_words_grouped':
                # For n_words_grouped, convert to n_words_normalized conditions
                word_conditions = []
                for val in path_to_other_values[dim_key]:
                    if val == '4+':
                        word_conditions.append('n_words_normalized >= 4')
                    else:
                        word_conditions.append(f'n_words_normalized = {val}')
                drill_conditions.append(f"({' OR '.join(word_conditions)})")
            else:
                # Regular dimension
                values_str = ', '.join([f"'{v}'" for v in path_to_other_values[dim_key]])
                drill_conditions.append(f"{dim_key} IN ({values_str})")
        else:
            # Regular filter - handle derived dimensions
            if dim_key == 'attribute_combination':
                # Convert attribute combination to underlying attr_* conditions
                drill_conditions.append(convert_attribute_combination_to_condition(dim_value))
            elif dim_key == 'n_words_grouped':
                # Convert word count group to n_words_normalized condition
                if dim_value == '4+':
                    drill_conditions.append('n_words_normalized >= 4')
                else:
                    drill_conditions.append(f'n_words_normalized = {dim_value}')
            else:
                # Regular column filter
                # Special handling for '-' to match NULL, empty, and '-' values
                if dim_value == '-':
                    drill_conditions.append(f"({dim_key} IS NULL OR {dim_key} = '' OR {dim_key} = '-')")
                else:
                    drill_conditions.append(f"{dim_key} = '{dim_value}'")

    # Combine WHERE clauses
    if where_clause and drill_conditions:
        full_where = where_clause + " AND " + " AND ".join(drill_conditions)
    elif drill_conditions:
        full_where = "WHERE " + " AND ".join(drill_conditions)
    else:
        full_where = where_clause

    query = f"""
        -- Query version: v2_derived_dims_fixed
        SELECT
            search_term,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(queries_a2c) as queries_a2c,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as gross_purchase
        FROM `{dataset_table}`
        {full_where}
        GROUP BY search_term
        ORDER BY queries DESC
    """
    return query
