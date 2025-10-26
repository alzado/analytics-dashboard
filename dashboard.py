import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_processing as dp
import visualizations as viz

# Helper function for number formatting
def format_number(value, decimals=2):
    """Format number with commas as thousand separator. No decimals for numbers > 10."""
    if value > 10:
        return f"{int(value):,}"
    else:
        return f"{value:,.{decimals}f}"

# Helper function to calculate cumulative percentage
def add_cumulative_percentage(df, total_value, value_column='queries'):
    """
    Add cumulative percentage column to dataframe based on current row order.

    Args:
        df: DataFrame with data in desired display order
        total_value: Total value to use as denominator (100%)
        value_column: Column name to use for cumulative calculation (default: 'queries')

    Returns:
        DataFrame with cumulative_pct column added
    """
    if total_value > 0 and value_column in df.columns:
        df['cumulative_pct'] = (df[value_column].cumsum() / total_value * 100)
    else:
        df['cumulative_pct'] = 0
    return df

# Helper function to calculate search terms for top X%
def calculate_terms_for_top_pct(client, dataset_table, base_filters, dimension_column, dimension_value, concentration_pct):
    """
    Calculate how many search terms account for top X% of queries within a dimension value.

    Args:
        client: BigQuery client
        dataset_table: Table name
        base_filters: Base filter dict
        dimension_column: Name of the dimension column
        dimension_value: Value of the dimension to filter
        concentration_pct: Percentage threshold (e.g., 80 for 80%)

    Returns:
        Number of search terms accounting for top X%
    """
    from data_processing import build_filter_clause, convert_attribute_combination_to_condition

    # Build WHERE clause from base filters
    where_clause = build_filter_clause(base_filters)

    # Add dimension filter - handle derived dimensions (skip if None for grand total)
    if dimension_value is not None and dimension_value != 'Other':
        safe_value = str(dimension_value).replace("'", "\\'")

        # Special handling for derived dimensions
        if dimension_column == 'attribute_combination':
            # Convert attribute combination to underlying attr_* conditions
            attr_condition = convert_attribute_combination_to_condition(dimension_value)
            if where_clause:
                where_clause += f" AND {attr_condition}"
            else:
                where_clause = f"WHERE {attr_condition}"
        elif dimension_column == 'n_words_grouped':
            # Convert word count group to n_words_normalized condition
            if dimension_value == '4+':
                word_condition = 'n_words_normalized >= 4'
            else:
                word_condition = f'n_words_normalized = {dimension_value}'
            if where_clause:
                where_clause += f" AND {word_condition}"
            else:
                where_clause = f"WHERE {word_condition}"
        else:
            # Regular column filter
            # Special handling for '-' to match NULL, empty, and '-' values
            if safe_value == '-':
                condition = f"({dimension_column} IS NULL OR {dimension_column} = '' OR {dimension_column} = '-')"
            else:
                condition = f"{dimension_column} = '{safe_value}'"

            if where_clause:
                where_clause += f" AND {condition}"
            else:
                where_clause = f"WHERE {condition}"

    threshold_decimal = concentration_pct / 100.0

    query = f"""
    WITH search_terms AS (
        SELECT
            search_term,
            SUM(queries) as queries
        FROM `{dataset_table}`
        {where_clause}
        GROUP BY search_term
    ),
    ranked_terms AS (
        SELECT
            search_term,
            queries,
            SUM(queries) OVER (ORDER BY queries DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_queries,
            SUM(queries) OVER () as total_queries
        FROM search_terms
    )
    SELECT
        COUNT(*) as terms_count
    FROM ranked_terms
    WHERE (cumulative_queries - queries) / total_queries < {threshold_decimal}
    """

    try:
        result = client.query(query).to_dataframe()
        if not result.empty:
            return int(result.iloc[0]['terms_count'])
    except Exception as e:
        # Log error for debugging
        import streamlit as st
        if 'calc_terms_errors' not in st.session_state:
            st.session_state.calc_terms_errors = []
        st.session_state.calc_terms_errors.append({
            'dimension': dimension_column,
            'value': dimension_value,
            'error': str(e)
        })
    return 0

# Page configuration
st.set_page_config(
    page_title="Search Analytics Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("🔍 Search Analytics Dashboard")
st.caption("💵 All monetary values displayed in CLP (Chilean Pesos)")

# BigQuery Configuration
st.sidebar.header("📊 BigQuery Settings")


def initialize_bigquery_connection(bq_params):
    """
    Initialize BigQuery connection and store parameters in session state.
    For on-demand query mode.
    """
    # Store connection parameters
    st.session_state.bq_params = bq_params

    # Initialize BigQuery client
    st.session_state.bq_client = dp.get_bigquery_client(
        project_id=bq_params['project_id'],
        credentials_path=bq_params.get('credentials_path')
    )

    # Query min/max dates from the table for date range widget
    try:
        date_query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_params['table']}`
        """
        date_df = dp.execute_cached_query(st.session_state.bq_client, date_query, ttl=3600)

        st.session_state.bq_min_date = pd.to_datetime(date_df['min_date'].iloc[0])
        st.session_state.bq_max_date = pd.to_datetime(date_df['max_date'].iloc[0])
    except Exception as e:
        st.error(f"Error querying date range: {str(e)}")
        # Fallback to defaults
        from datetime import datetime, timedelta
        st.session_state.bq_min_date = datetime.now() - timedelta(days=365)
        st.session_state.bq_max_date = datetime.now()

    st.session_state.bq_initialized = True

# Default to the provided table
default_table = "tc-sc-bi-bigdata-fcom-dev.sandbox_lmellado.query_intent_classification"

bq_project = st.sidebar.text_input(
    "Project ID",
    value="tc-sc-bi-bigdata-fcom-dev",
    help="GCP project ID"
)

bq_table = st.sidebar.text_input(
    "Table",
    value=default_table,
    help="Full table path (project.dataset.table or dataset.table)"
)

bq_creds = st.sidebar.text_input(
    "Credentials Path (optional)",
    value="",
    help="Path to service account JSON file. Leave empty to use default credentials."
)

st.sidebar.markdown("---")
st.sidebar.subheader("⚡ Performance Filters")
st.sidebar.caption("Apply filters BEFORE loading data (recommended for large tables)")

# Date range filter (CRITICAL for 22M records!)
from datetime import date, timedelta

col1, col2 = st.sidebar.columns(2)
with col1:
    bq_date_start = st.date_input(
        "Start Date",
        value=date.today() - timedelta(days=90),
        help="Filter data from this date (pushes filter to BigQuery)"
    )
with col2:
    bq_date_end = st.date_input(
        "End Date",
        value=date.today(),
        help="Filter data until this date"
    )

# Pre-filter options
bq_countries = st.sidebar.multiselect(
    "Pre-filter Countries",
    options=["CL", "AR", "BR", "MX", "CO", "PE"],
    default=[],
    help="Filter countries in BigQuery (leave empty for all)"
)

bq_channels = st.sidebar.multiselect(
    "Pre-filter Channels",
    options=["App", "Web"],
    default=[],
    help="Filter channels in BigQuery (leave empty for all)"
)

# Sampling option for quick exploration
bq_sample = st.sidebar.slider(
    "Sample % (for testing)",
    min_value=0,
    max_value=100,
    value=0,
    step=5,
    help="Sample a percentage of data for quick exploration. 0 = use all filtered data"
)

bq_limit = st.sidebar.number_input(
    "Row Limit (for testing)",
    min_value=0,
    max_value=1000000,
    value=0,
    step=10000,
    help="Limit rows after filtering. 0 = no limit (use with caution on large datasets!)"
)

# Show estimated query info
if bq_date_start and bq_date_end:
    days_diff = (bq_date_end - bq_date_start).days
    st.sidebar.info(f"📅 Date range: {days_diff} days")

# Estimate data volume
if bq_sample > 0:
    st.sidebar.info(f"📊 Using {bq_sample}% sample of data")
elif bq_limit > 0:
    st.sidebar.info(f"📊 Limited to {bq_limit:,} rows")
else:
    st.sidebar.warning("⚠️ No sampling or limit - may load large dataset!")

st.sidebar.markdown("---")

# Connect Button (for on-demand mode)
load_button = st.sidebar.button("🚀 Connect to BigQuery", type="primary", use_container_width=True)

# Clear Cache Button
if st.sidebar.button("🗑️ Clear Query Cache", use_container_width=True):
    if 'query_cache' in st.session_state:
        st.session_state.query_cache = {}
        st.success("✅ Cache cleared successfully!")
        st.rerun()

bq_params = {
    'project_id': bq_project,
    'table': bq_table,
    'credentials_path': bq_creds if bq_creds else None,
    'date_start': str(bq_date_start) if bq_date_start else None,
    'date_end': str(bq_date_end) if bq_date_end else None,
    'countries': bq_countries if bq_countries else None,
    'channels': bq_channels if bq_channels else None,
    'sample_percent': bq_sample if bq_sample > 0 else None,
    'limit': bq_limit if bq_limit > 0 else None
}

# Initialize connection when button is clicked
if load_button or not st.session_state.get('bq_initialized', False):
    with st.spinner('🔄 Connecting to BigQuery...'):
        initialize_bigquery_connection(bq_params)
        st.success(f"✅ Connected to BigQuery: {bq_params['table']}")

# Check if connection is initialized
if not st.session_state.get('bq_initialized', False):
    # Show instructions if not connected yet
    st.info("👆 Configure your BigQuery settings in the sidebar and click **'Connect to BigQuery'**.")
    st.markdown("""
    ### On-Demand Query Mode 🚀

    **This dashboard uses on-demand BigQuery queries** - data is fetched as needed for each section,
    rather than loading everything upfront. This means:

    ✅ **Faster initial load** - No waiting for huge datasets
    ✅ **Lower memory usage** - Only load what you're viewing
    ✅ **Reduced costs** - Only query what you need
    ✅ **Real-time filtering** - Filters apply directly in BigQuery

    **Quick Start:**
    1. Set your **Date Range** (filters data in BigQuery)
    2. Optionally add **Country** or **Channel** filters
    3. Click **'Connect to BigQuery'**
    4. Dashboard sections will query data on-demand!

    **Note:** Remove any sampling/limit settings for production use.
    """)
    st.stop()  # Stop execution until connected

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date = st.session_state.bq_min_date
max_date = st.session_state.bq_max_date

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key='date_range'
)

# Convert to datetime
if len(date_range) == 2:
    start_date, end_date = date_range
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
else:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[0])

# Period comparison disabled in BigQuery mode
enable_comparison = False
comparison_date_range = None

# Country filter
st.sidebar.subheader("Geographic")
# Use predefined list or query from BigQuery
country_options = ['CL', 'AR', 'BR', 'MX', 'CO', 'PE']

countries = st.sidebar.multiselect(
    "Countries",
    options=country_options,
    default=None
)

# Channel filter
st.sidebar.subheader("Channel")
# Use predefined list
channel_options = ['App', 'Web']

channels = st.sidebar.multiselect(
    "Channels",
    options=channel_options,
    default=None
)

# Category filter
st.sidebar.subheader("Category")
# You may want to query this from BigQuery or provide a predefined list
category_options = []  # Will be populated from data

categories = st.sidebar.multiselect(
    "Categories",
    options=category_options,
    default=None
)

# Number of words filter
st.sidebar.subheader("Query Length")
word_options = ['1', '2', '3', '4+']
selected_words = st.sidebar.multiselect(
    "Number of Words",
    options=word_options,
    default=None
)

# Attribute combination filter disabled in BigQuery mode
selected_combos = None

# Attribute filters
st.sidebar.subheader("Attributes")

# Individual attribute toggles (removed - only using attribute combinations)
attr_cols = dp.get_attribute_columns()
selected_attributes = {}  # Empty dict - no individual attribute filters

# Number of attributes filter
with st.sidebar.expander("Number of Attributes", expanded=False):
    max_attrs = 8  # Known max from schema

    n_attrs_range = st.slider(
        "Attribute Count Range",
        min_value=0,
        max_value=max_attrs,
        value=(0, max_attrs),
        key='n_attrs_range'
    )

# Build base filters dictionary (used for BigQuery queries)
base_filters = {
    'date_start': str(start_date.date()),
    'date_end': str(end_date.date()),
    'countries': countries if countries else None,
    'channels': channels if channels else None,
    'categories': categories if categories else None,
    'n_attributes_range': n_attrs_range,
    'word_counts': selected_words if selected_words else None,
    'attribute_combinations': selected_combos if selected_combos else None,
    'num_days': (end_date - start_date).days + 1  # Add number of days for queries/day calculation
}

# Main content area
# Section 1: Overview KPIs
st.header("Overview KPIs")

# Query BigQuery for KPIs
query = dp.build_kpi_query(st.session_state.bq_params['table'], base_filters)
result_df = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate KPIs from BigQuery results
total_queries = int(result_df['total_queries'].iloc[0]) if result_df['total_queries'].iloc[0] else 0
total_revenue = float(result_df['total_revenue'].iloc[0]) if result_df['total_revenue'].iloc[0] else 0.0
total_purchases = int(result_df['total_purchases'].iloc[0]) if result_df['total_purchases'].iloc[0] else 0
total_queries_pdp = int(result_df['total_queries_pdp'].iloc[0]) if result_df['total_queries_pdp'].iloc[0] else 0

# Query for search term count (fast COUNT DISTINCT)
where_clause = dp.build_filter_clause(base_filters)
count_query = f"""
SELECT COUNT(DISTINCT search_term) as search_term_count
FROM `{st.session_state.bq_params['table']}`
{where_clause}
"""
count_result = dp.execute_cached_query(st.session_state.bq_client, count_query, ttl=300)
search_term_count = int(count_result.iloc[0]['search_term_count']) if not count_result.empty else 0

# Calculate search terms for top 50% (fixed concentration for KPI dashboard)
search_terms_top_50_pct = calculate_terms_for_top_pct(
    st.session_state.bq_client,
    st.session_state.bq_params['table'],
    base_filters,
    None,  # No dimension column
    None,  # No dimension value
    50  # 50% concentration
)

# Calculate number of days
num_days = base_filters.get('num_days', 1)

kpis = {
    'total_queries': total_queries,
    'total_revenue': total_revenue,
    'avg_ctr': float(total_queries_pdp / total_queries) if total_queries > 0 else 0.0,
    'avg_conversion': float(total_purchases / total_queries) if total_queries > 0 else 0.0,
    'revenue_per_query': float(total_revenue / total_queries) if total_queries > 0 else 0.0,
    'total_purchases': total_purchases,
    'avg_order_value': float(total_revenue / total_purchases) if total_purchases > 0 else 0.0,
    'search_term_count': search_term_count,
    'average_queries_per_day': (total_queries / num_days) if num_days > 0 else 0.0,
    'average_queries_per_search_term_per_day': (total_queries / num_days / search_term_count) if (num_days > 0 and search_term_count > 0) else 0.0,
    'search_terms_for_top_50_pct': search_terms_top_50_pct
}

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric(
        "Total Search Terms",
        format_number(kpis['search_term_count'])
    )

with col2:
    st.metric(
        "Avg Queries per Day",
        f"{format_number(kpis['average_queries_per_day'])}"
    )

with col3:
    st.metric(
        "Avg Queries per ST per Day",
        f"{format_number(kpis['average_queries_per_search_term_per_day'])}"
    )

with col4:
    st.metric(
        "Search Terms for Top 50%",
        format_number(kpis['search_terms_for_top_50_pct'])
    )

with col5:
    st.metric(
        "Conversion Rate",
        f"{kpis['avg_conversion']*100:.2f}%"
    )

with col6:
    st.metric(
        "Avg Order Value",
        f"${format_number(kpis['avg_order_value'])}"
    )

st.divider()

# Section 2: Trend Analysis
st.header("Trend Analysis")

# Frequency selector
freq_options = {
    'Daily': 'D',
    'Weekly': 'W',
    'Monthly': 'M'
}
freq_label = st.selectbox("Time Granularity", options=list(freq_options.keys()), index=0)
freq = freq_options[freq_label]

# Query BigQuery for time series
query = dp.build_timeseries_query(st.session_state.bq_params['table'], base_filters, freq=freq)
ts_data = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate derived metrics
ts_data['ctr'] = np.where(ts_data['queries'] > 0, ts_data['queries_pdp'] / ts_data['queries'], 0)
ts_data['a2c_rate'] = np.where(ts_data['queries'] > 0, ts_data['queries_a2c'] / ts_data['queries'], 0)
ts_data['conversion_rate'] = np.where(ts_data['queries'] > 0, ts_data['purchases'] / ts_data['queries'], 0)
ts_data['revenue_per_query'] = np.where(ts_data['queries'] > 0, ts_data['gross_purchase'] / ts_data['queries'], 0)
ts_data['avg_order_value'] = np.where(ts_data['purchases'] > 0, ts_data['gross_purchase'] / ts_data['purchases'], 0)

ts_comparison = None  # Comparison not supported in BigQuery mode

# Metric selector for trends
trend_metrics = st.multiselect(
    "Select Metrics to Display",
    options=['queries', 'revenue_per_query', 'ctr', 'conversion_rate', 'a2c_rate'],
    default=['queries', 'revenue_per_query']
)

if trend_metrics:
    for metric in trend_metrics:
        metric_names = {
            'queries': 'Total Queries',
            'revenue_per_query': 'Revenue per Query',
            'ctr': 'Click-Through Rate',
            'conversion_rate': 'Conversion Rate',
            'a2c_rate': 'Add-to-Cart Rate'
        }

        fig = viz.create_trend_chart(
            ts_data,
            metric,
            title=f"{metric_names.get(metric, metric)} Over Time",
            yaxis_title=metric_names.get(metric, metric),
            comparison_df=ts_comparison
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# Section 3: Channel Performance
st.header("Channel Performance")

# Query BigQuery for channel performance
query = dp.build_channel_query(st.session_state.bq_params['table'], base_filters)
channel_data = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate derived metrics
channel_data['ctr'] = np.where(channel_data['queries'] > 0, channel_data['queries_pdp'] / channel_data['queries'], 0)
channel_data['a2c_rate'] = np.where(channel_data['queries'] > 0, channel_data['queries_a2c'] / channel_data['queries'], 0)
channel_data['conversion_rate'] = np.where(channel_data['queries'] > 0, channel_data['purchases'] / channel_data['queries'], 0)
channel_data['revenue_per_query'] = np.where(channel_data['queries'] > 0, channel_data['gross_purchase'] / channel_data['queries'], 0)
channel_data['avg_order_value'] = np.where(channel_data['purchases'] > 0, channel_data['gross_purchase'] / channel_data['purchases'], 0)

has_channel_data = len(channel_data) > 0

if has_channel_data:
    # Use BigQuery data for chart
    fig_channel = viz.create_channel_comparison(channel_data)

    st.plotly_chart(fig_channel, use_container_width=True)

    # Detailed channel table
    st.subheader("Channel Details")

    channel_details = channel_data.copy()

    # Sort by queries descending
    channel_details = channel_details.sort_values('queries', ascending=False).reset_index(drop=True)

    # Calculate % of total queries and cumulative %
    total_queries = channel_details['queries'].sum()
    channel_details['query_pct'] = (channel_details['queries'] / total_queries * 100)
    channel_details['cumulative_queries'] = channel_details['queries'].cumsum()
    channel_details['cumulative_pct'] = (channel_details['cumulative_queries'] / total_queries * 100)

    # Select and reorder columns
    channel_details = channel_details[[
        'channel', 'queries', 'query_pct', 'cumulative_pct', 'ctr', 'a2c_rate', 'conversion_rate',
        'revenue_per_query', 'purchases', 'gross_purchase'
    ]]

    # Format columns
    channel_details['query_pct'] = channel_details['query_pct'].apply(lambda x: f"{x:.2f}%")
    channel_details['cumulative_pct'] = channel_details['cumulative_pct'].apply(lambda x: f"{x:.2f}%")
    channel_details['ctr'] = channel_details['ctr'].apply(lambda x: f"{x*100:.2f}%")
    channel_details['a2c_rate'] = channel_details['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
    channel_details['conversion_rate'] = channel_details['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
    channel_details['revenue_per_query'] = channel_details['revenue_per_query'].apply(lambda x: f"${x:.2f}")
    channel_details['gross_purchase'] = channel_details['gross_purchase'].apply(lambda x: f"${x:,.2f}")

    # Rename columns for display
    channel_details = channel_details.rename(columns={
        'query_pct': '% of queries',
        'cumulative_pct': 'cumulative %'
    })

    st.dataframe(channel_details, use_container_width=True, hide_index=True)
else:
    st.info("No channel data available with current filters.")

st.divider()

# Section 4: Search Query Length Analysis
st.header("Search Query Length Analysis")

# Query BigQuery for word count analysis
query = dp.build_word_count_query(st.session_state.bq_params['table'], base_filters)
n_words_grouped = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate derived metrics
n_words_grouped['ctr'] = np.where(n_words_grouped['queries'] > 0, n_words_grouped['queries_pdp'] / n_words_grouped['queries'], 0)
n_words_grouped['a2c_rate'] = np.where(n_words_grouped['queries'] > 0, n_words_grouped['queries_a2c'] / n_words_grouped['queries'], 0)
n_words_grouped['conversion_rate'] = np.where(n_words_grouped['queries'] > 0, n_words_grouped['purchases'] / n_words_grouped['queries'], 0)
n_words_grouped['revenue_per_query'] = np.where(n_words_grouped['queries'] > 0, n_words_grouped['gross_purchase'] / n_words_grouped['queries'], 0)
n_words_grouped['avg_order_value'] = np.where(n_words_grouped['purchases'] > 0, n_words_grouped['gross_purchase'] / n_words_grouped['purchases'], 0)

has_word_count_data = len(n_words_grouped) > 0

if has_word_count_data:
    st.subheader("Performance by Number of Words (1, 2, 3, 4+)")

    # Create visualization
    fig_n_words = viz.create_n_words_chart(n_words_grouped, word_col='n_words_grouped')
    st.plotly_chart(fig_n_words, use_container_width=True)

    # Detailed table by grouped n_words
    st.subheader("Details by Number of Words")

    # Calculate % of total queries and cumulative %
    total_queries = n_words_grouped['queries'].sum()
    n_words_grouped['query_pct'] = (n_words_grouped['queries'] / total_queries * 100)
    n_words_grouped['cumulative_queries'] = n_words_grouped['queries'].cumsum()
    n_words_grouped['cumulative_pct'] = (n_words_grouped['cumulative_queries'] / total_queries * 100)

    n_words_details = n_words_grouped[[
        'n_words_grouped', 'queries', 'query_pct', 'cumulative_pct', 'ctr', 'a2c_rate', 'conversion_rate',
        'revenue_per_query', 'purchases', 'gross_purchase'
    ]].copy()

    # Format columns
    n_words_details['query_pct'] = n_words_details['query_pct'].apply(lambda x: f"{x:.2f}%")
    n_words_details['cumulative_pct'] = n_words_details['cumulative_pct'].apply(lambda x: f"{x:.2f}%")
    n_words_details['ctr'] = n_words_details['ctr'].apply(lambda x: f"{x*100:.2f}%")
    n_words_details['a2c_rate'] = n_words_details['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
    n_words_details['conversion_rate'] = n_words_details['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
    n_words_details['revenue_per_query'] = n_words_details['revenue_per_query'].apply(lambda x: f"${x:.2f}")
    n_words_details['gross_purchase'] = n_words_details['gross_purchase'].apply(lambda x: f"${x:,.2f}")

    # Rename columns for clarity
    n_words_details = n_words_details.rename(columns={
        'n_words_grouped': 'num_words',
        'query_pct': '% of queries',
        'cumulative_pct': 'cumulative %'
    })

    st.dataframe(n_words_details, use_container_width=True, hide_index=True)

    # Add info box about grouping
    st.info("📊 Queries with 4 or more words are grouped together as '4+' for clearer visualization due to lower individual volumes.")
else:
    st.info("No word count data available with current filters.")

st.divider()

# Section 5: Attribute Analysis
st.header("Attribute Analysis")

st.subheader("Performance by Attribute Combinations")

# Control for number of combinations to show
top_n = st.slider("Number of top combinations to display", min_value=10, max_value=30, value=15, step=5)

# Query BigQuery for attribute combinations
query = dp.build_attribute_combination_query(st.session_state.bq_params['table'], base_filters, top_n=top_n)
attr_combos = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate derived metrics
attr_combos['ctr'] = np.where(attr_combos['queries'] > 0, attr_combos['queries_pdp'] / attr_combos['queries'], 0)
attr_combos['a2c_rate'] = np.where(attr_combos['queries'] > 0, attr_combos['queries_a2c'] / attr_combos['queries'], 0)
attr_combos['conversion_rate'] = np.where(attr_combos['queries'] > 0, attr_combos['purchases'] / attr_combos['queries'], 0)
attr_combos['revenue_per_query'] = np.where(attr_combos['queries'] > 0, attr_combos['gross_purchase'] / attr_combos['queries'], 0)
attr_combos['avg_order_value'] = np.where(attr_combos['purchases'] > 0, attr_combos['gross_purchase'] / attr_combos['purchases'], 0)

if not attr_combos.empty:
    fig_attr_combos = viz.create_attribute_combination_chart(attr_combos)
    st.plotly_chart(fig_attr_combos, use_container_width=True)

    # Detailed table
    st.subheader(f"Top {top_n} Attribute Combination Details")

    # Calculate % of total queries and cumulative %
    # Use the KPI total we already calculated for this filtered period
    total_queries_all = kpis['total_queries']
    attr_combos['query_pct'] = (attr_combos['queries'] / total_queries_all * 100)
    attr_combos['cumulative_queries'] = attr_combos['queries'].cumsum()
    attr_combos['cumulative_pct'] = (attr_combos['cumulative_queries'] / total_queries_all * 100)

    attr_combo_display = attr_combos[[
        'attribute_combination', 'queries', 'query_pct', 'cumulative_pct', 'ctr', 'a2c_rate', 'conversion_rate',
        'revenue_per_query', 'purchases', 'gross_purchase'
    ]].copy()

    # Format columns
    attr_combo_display['query_pct'] = attr_combo_display['query_pct'].apply(lambda x: f"{x:.2f}%")
    attr_combo_display['cumulative_pct'] = attr_combo_display['cumulative_pct'].apply(lambda x: f"{x:.2f}%")
    attr_combo_display['ctr'] = attr_combo_display['ctr'].apply(lambda x: f"{x*100:.2f}%")
    attr_combo_display['a2c_rate'] = attr_combo_display['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
    attr_combo_display['conversion_rate'] = attr_combo_display['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
    attr_combo_display['revenue_per_query'] = attr_combo_display['revenue_per_query'].apply(lambda x: f"${x:.2f}")
    attr_combo_display['gross_purchase'] = attr_combo_display['gross_purchase'].apply(lambda x: f"${x:,.2f}")

    # Rename columns for clarity
    attr_combo_display = attr_combo_display.rename(columns={
        'attribute_combination': 'combination',
        'query_pct': '% of queries',
        'cumulative_pct': 'cumulative %'
    })

    st.dataframe(attr_combo_display, use_container_width=True, hide_index=True)

    # Add insight box
    st.info(f"💡 Showing top {top_n} attribute combinations by query volume. " +
           "Combinations like 'Marca + Color' show how customers combine different attributes in their searches.")
else:
    st.info("No attribute combination data available with current filters.")

st.divider()

# Section 6: Search Term Explorer
st.header("Search Term Explorer")

# Top search terms chart
st.subheader("Top Search Terms")

col_metric, col_top_n = st.columns([3, 1])
with col_metric:
    top_metric = st.selectbox(
        "Rank by",
        options=['queries', 'gross_purchase', 'purchases', 'queries_pdp'],
        index=0,
        key='top_metric'
    )
with col_top_n:
    top_n_chart = st.number_input("Show top", min_value=5, max_value=100, value=20, step=5)

# Query BigQuery for search terms (get more than needed for local filtering/sorting)
query = dp.build_search_terms_query(
    st.session_state.bq_params['table'],
    base_filters,
    search_filter=None,  # Don't filter in query - do it in pandas for flexibility
    sort_by='queries',
    ascending=False,
    limit=1000  # Get top 1000 for local manipulation
)
search_term_agg = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

# Calculate derived metrics
search_term_agg['ctr'] = np.where(search_term_agg['queries'] > 0, search_term_agg['queries_pdp'] / search_term_agg['queries'], 0)
search_term_agg['a2c_rate'] = np.where(search_term_agg['queries'] > 0, search_term_agg['queries_a2c'] / search_term_agg['queries'], 0)
search_term_agg['conversion_rate'] = np.where(search_term_agg['queries'] > 0, search_term_agg['purchases'] / search_term_agg['queries'], 0)
search_term_agg['revenue_per_query'] = np.where(search_term_agg['queries'] > 0, search_term_agg['gross_purchase'] / search_term_agg['queries'], 0)
search_term_agg['avg_order_value'] = np.where(search_term_agg['purchases'] > 0, search_term_agg['gross_purchase'] / search_term_agg['purchases'], 0)

if not search_term_agg.empty:
    fig_top_searches = viz.create_top_searches_chart(search_term_agg, metric=top_metric, top_n=top_n_chart)
    st.plotly_chart(fig_top_searches, use_container_width=True)

    # Detailed search term table with multi-level drill-down
    st.subheader("Search Term Details")

    # Search box
    search_filter = st.text_input("Filter search terms (contains)", value="")

    # Apply search filter
    if search_filter:
        display_df = search_term_agg[search_term_agg['search_term'].str.contains(search_filter, case=False, na=False)]
    else:
        display_df = search_term_agg

    # Sort options
    sort_col, sort_order = st.columns([3, 1])
    with sort_col:
        sort_by = st.selectbox(
            "Sort by",
            options=['queries', 'ctr', 'a2c_rate', 'conversion_rate', 'revenue_per_query', 'gross_purchase', 'purchases'],
            index=0,
            key='sort_by'
        )
    with sort_order:
        ascending = st.checkbox("Ascending", value=False, key='ascending')

    display_df = display_df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)

    # Calculate cumulative % based on current sort (by queries)
    total_queries_st = display_df['queries'].sum()
    display_df['query_pct'] = (display_df['queries'] / total_queries_st * 100)
    display_df['cumulative_queries'] = display_df['queries'].cumsum()
    display_df['cumulative_pct'] = (display_df['cumulative_queries'] / total_queries_st * 100)

    # Format for display
    display_formatted = display_df.copy()
    display_formatted['query_pct'] = display_formatted['query_pct'].apply(lambda x: f"{x:.2f}%")
    display_formatted['cumulative_pct'] = display_formatted['cumulative_pct'].apply(lambda x: f"{x:.2f}%")
    display_formatted['ctr'] = display_formatted['ctr'].apply(lambda x: f"{x*100:.2f}%")
    display_formatted['a2c_rate'] = display_formatted['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
    display_formatted['conversion_rate'] = display_formatted['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
    display_formatted['revenue_per_query'] = display_formatted['revenue_per_query'].apply(lambda x: f"${x:.2f}")
    display_formatted['gross_purchase'] = display_formatted['gross_purchase'].apply(lambda x: f"${format_number(x)}")
    display_formatted['avg_order_value'] = display_formatted['avg_order_value'].apply(lambda x: f"${x:.2f}")

    # Rename columns for display
    display_formatted = display_formatted.rename(columns={
        'query_pct': '% of queries',
        'cumulative_pct': 'cumulative %'
    })

    # Select columns to display
    display_cols = [
        'search_term', 'queries', '% of queries', 'cumulative %', 'ctr', 'a2c_rate', 'conversion_rate',
        'revenue_per_query', 'purchases', 'gross_purchase', 'avg_order_value'
    ]

    st.dataframe(
        display_formatted[display_cols],
        use_container_width=True,
        hide_index=True,
        height=600
    )

    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="Download Search Term Data (CSV)",
        data=csv,
        file_name=f"search_terms_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
else:
    st.info("No search term data available with current filters.")

st.divider()

# Section 7: Hierarchical Pivot Table
st.header("📊 Hierarchical Pivot Table")
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem 1.5rem; border-radius: 8px; margin-bottom: 1rem; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
    <p style="color: white; margin: 0; font-size: 0.95rem; font-weight: 500;">
        <strong>Interactive Multi-Dimensional Analysis</strong><br/>
        <span style="font-size: 0.85rem; opacity: 0.95;">Drill down through multiple dimensions with expandable hierarchies • Professional pivot table interface • Export-ready for presentations</span>
    </p>
</div>
""", unsafe_allow_html=True)

# Initialize session state for expanded rows
if 'expanded_paths' not in st.session_state:
    st.session_state.expanded_paths = set()

# Calculate number of days in selected period
num_days = (end_date - start_date).days + 1

# Volume threshold input (daily rate)
daily_volume_threshold = st.number_input(
    "Daily Query Volume Threshold",
    min_value=1,
    value=10,
    step=5,
    help=f"Daily average queries to separate High Volume from Low Volume search terms. Current period: {num_days} days.",
    key='volume_threshold'
)

# Convert daily threshold to total threshold for the period
volume_threshold = daily_volume_threshold * num_days

# Display the calculated threshold
st.caption(f"💡 Daily threshold of {daily_volume_threshold} = {volume_threshold:,} total queries over {num_days} days")

# Get available dimensions
dimensions = dp.get_available_dimensions()

# Initialize session state for dimension order
if 'dimension_order' not in st.session_state:
    st.session_state.dimension_order = ['Attribute Combination', 'Number of Words']

# Dimension selector
st.markdown("""
    <div style="background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); padding: 0.75rem 1rem; border-radius: 6px; margin-bottom: 1rem;">
        <p style="margin: 0; color: white; font-weight: 600; font-size: 0.9rem;">🎯 Configure Dimensions</p>
        <p style="margin: 0.25rem 0 0 0; color: rgba(255, 255, 255, 0.9); font-size: 0.8rem;">Select and order dimensions to create your hierarchical breakdown</p>
    </div>
""", unsafe_allow_html=True)

# Available dimensions selector
available_dims = [d for d in dimensions.keys() if d not in st.session_state.dimension_order]
add_dimension = st.selectbox(
    "Add dimension",
    options=['-- Select to add --'] + available_dims,
    key='add_dimension_select'
)

if add_dimension != '-- Select to add --' and len(st.session_state.dimension_order) < 4:
    st.session_state.dimension_order.append(add_dimension)
    st.rerun()

# Display current dimensions with reorder controls
if len(st.session_state.dimension_order) > 0:
    st.write("**Current hierarchy order:**")

    for idx, dim_name in enumerate(st.session_state.dimension_order):
        cols = st.columns([3, 0.5, 0.5, 0.5])

        with cols[0]:
            st.write(f"{idx + 1}. {dim_name}")

        with cols[1]:
            if idx > 0:
                if st.button("⬆", key=f"up_{idx}"):
                    st.session_state.dimension_order[idx], st.session_state.dimension_order[idx-1] = \
                        st.session_state.dimension_order[idx-1], st.session_state.dimension_order[idx]
                    st.rerun()

        with cols[2]:
            if idx < len(st.session_state.dimension_order) - 1:
                if st.button("⬇", key=f"down_{idx}"):
                    st.session_state.dimension_order[idx], st.session_state.dimension_order[idx+1] = \
                        st.session_state.dimension_order[idx+1], st.session_state.dimension_order[idx]
                    st.rerun()

        with cols[3]:
            if st.button("✕", key=f"remove_{idx}"):
                st.session_state.dimension_order.pop(idx)
                st.rerun()

selected_dimension_names = st.session_state.dimension_order

# Column visibility configuration
all_available_columns = ['queries', 'search_term_count', 'average_queries_per_day', 'average_queries_per_search_term_per_day', 'search_terms_for_top_x_pct', '% of queries', 'cumulative %', 'ctr', 'a2c_rate', 'conversion_rate', 'revenue_per_query', 'purchases', 'gross_purchase', 'avg_order_value']

# Initialize or migrate visible_columns
if 'visible_columns' not in st.session_state:
    st.session_state.visible_columns = ['queries', 'search_term_count', 'average_queries_per_search_term_per_day', 'search_terms_for_top_x_pct', '% of queries', 'cumulative %', 'ctr', 'conversion_rate', 'revenue_per_query']
else:
    # Migrate old column names to new ones and filter out any invalid values
    migrated = []
    for col in st.session_state.visible_columns:
        if col == 'queries_per_day':
            migrated.append('average_queries_per_search_term_per_day')
        elif col == 'average_queries_per_day':
            migrated.append('average_queries_per_search_term_per_day')
        elif col == 'terms_for_top_x_pct':
            migrated.append('search_terms_for_top_x_pct')
        elif col == 'search_terms_for_top_80_pct':
            migrated.append('search_terms_for_top_x_pct')
        else:
            migrated.append(col)
    # Filter to only include valid columns
    st.session_state.visible_columns = [col for col in migrated if col in all_available_columns]

# Ensure default only contains valid options
valid_defaults = [col for col in st.session_state.visible_columns if col in all_available_columns]

visible_columns = st.multiselect(
    "Select columns to display",
    options=all_available_columns,
    default=valid_defaults,
    key='column_selector'
)

# Update session state
if visible_columns:
    st.session_state.visible_columns = visible_columns

# Global settings
col_thresh, col_search, col_concentration = st.columns([2, 2, 2])
with col_thresh:
    cumulative_threshold = st.slider(
        "Cumulative threshold %",
        min_value=50,
        max_value=100,
        value=80,
        step=5,
        key='cumulative_threshold_hierarchy',
        help="Show rows until this cumulative % is reached, aggregate the rest as 'Other'"
    )
with col_concentration:
    search_term_concentration_pct = st.slider(
        "Search term concentration %",
        min_value=50,
        max_value=100,
        value=80,
        step=5,
        key='search_term_concentration',
        help="Calculate how many search terms account for top X% of queries within each dimension value"
    )

# Search term aggregation threshold
col_search1, col_search2 = st.columns([2, 3])
with col_search1:
    min_queries_for_search_terms = st.number_input(
        "Min queries for search terms",
        min_value=1,
        max_value=100,
        value=5,
        step=1,
        key='min_queries_search_terms',
        help="Search terms with fewer queries will be aggregated into 'Other'"
    )

# Default sort configuration: sort by queries descending
sort_configs = [{'sort_by': 'queries', 'ascending': False} for _ in range(len(selected_dimension_names))]

if len(selected_dimension_names) == 0:
    # Show Grand Total when no dimensions selected
    st.info("Select dimensions above to see detailed breakdowns by Attribute Combination, Number of Words, Channel, etc.")
else:
    # Convert dimension names to keys
    selected_dimension_keys = [dimensions[name] for name in selected_dimension_names]

    # Initialize session state for inline expansion
    if 'pivot_expanded' not in st.session_state:
        st.session_state.pivot_expanded = {}  # {row_id: {'type': 'dimension'/'search', 'page': 0}}
    if 'pivot_selected_row' not in st.session_state:
        st.session_state.pivot_selected_row = None

    # Current level is always 0 for main table (no navigation)
    current_level = 0

    # Query data for current level
    try:
        with st.spinner('Loading pivot table data...'):
            if current_level < len(selected_dimension_keys):
                # Query for first dimension level (main table)
                current_dimension_key = selected_dimension_keys[current_level]
                current_dimension_name = selected_dimension_names[current_level]

                # Query BigQuery for main table
                sort_config = sort_configs[current_level] if current_level < len(sort_configs) else {'sort_by': 'queries', 'ascending': False}

                query = dp.build_dimension_level_query(
                    st.session_state.bq_params['table'],
                    base_filters,
                    current_dimension_key,
                    parent_filters={},  # No filters for main table
                    cumulative_threshold=cumulative_threshold / 100.0,
                    sort_by=sort_config['sort_by'],
                    ascending=sort_config['ascending'],
                    volume_threshold=volume_threshold,
                    daily_volume_threshold=daily_volume_threshold
                )
                pivot_data = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

                # Calculate derived metrics
                pivot_data['ctr'] = np.where(pivot_data['queries'] > 0,
                                             pivot_data['queries_pdp'] / pivot_data['queries'], 0)
                pivot_data['a2c_rate'] = np.where(pivot_data['queries'] > 0,
                                                  pivot_data['queries_a2c'] / pivot_data['queries'], 0)
                pivot_data['conversion_rate'] = np.where(pivot_data['queries'] > 0,
                                                         pivot_data['purchases'] / pivot_data['queries'], 0)
                pivot_data['revenue_per_query'] = np.where(pivot_data['queries'] > 0,
                                                           pivot_data['gross_purchase'] / pivot_data['queries'], 0)
                pivot_data['avg_order_value'] = np.where(pivot_data['purchases'] > 0,
                                                         pivot_data['gross_purchase'] / pivot_data['purchases'], 0)
                # Calculate average_queries_per_day (queries / num_days) if not already present
                if 'average_queries_per_day' not in pivot_data.columns:
                    pivot_data['average_queries_per_day'] = pivot_data['queries'] / num_days
                # Calculate average_queries_per_search_term_per_day (queries / num_days / search_term_count) if not already present
                if 'average_queries_per_search_term_per_day' not in pivot_data.columns:
                    if 'search_term_count' in pivot_data.columns:
                        pivot_data['average_queries_per_search_term_per_day'] = pivot_data['queries'] / num_days / pivot_data['search_term_count']
                    else:
                        pivot_data['average_queries_per_search_term_per_day'] = 0

                # Calculate percentages
                total_queries_all = kpis['total_queries']
                pivot_data['pct_of_total'] = (pivot_data['queries'] / total_queries_all * 100) if total_queries_all > 0 else 0

                # Calculate cumulative % based on current row order
                pivot_data = add_cumulative_percentage(pivot_data, total_queries_all, 'queries')

                # Calculate search terms for top X% for each dimension value
                pivot_data['search_terms_for_top_x_pct'] = pivot_data['dimension_value'].apply(
                    lambda val: calculate_terms_for_top_pct(
                        st.session_state.bq_client,
                        st.session_state.bq_params['table'],
                        base_filters,
                        current_dimension_key,
                        val,
                        search_term_concentration_pct
                    )
                )

                # Display level header
                st.subheader(f"📊 Level {current_level + 1}: {current_dimension_name}")

                if not pivot_data.empty:
                    # Format data for display
                    display_data = pivot_data.copy()
                    display_data = display_data.rename(columns={
                        'dimension_value': current_dimension_name,
                        'queries': 'queries',
                        'search_term_count': 'search_term_count',
                        'average_queries_per_day': 'average_queries_per_day',
                        'average_queries_per_search_term_per_day': 'average_queries_per_search_term_per_day',
                        'search_terms_for_top_x_pct': 'search_terms_for_top_x_pct',
                        'pct_of_total': '% of queries',
                        'cumulative_pct': 'cumulative %',
                        'ctr': 'ctr',
                        'a2c_rate': 'a2c_rate',
                        'conversion_rate': 'conversion_rate',
                        'revenue_per_query': 'revenue_per_query',
                        'purchases': 'purchases',
                        'gross_purchase': 'gross_purchase',
                        'avg_order_value': 'avg_order_value'
                    })

                    # Format columns for display
                    display_formatted = display_data.copy()
                    display_formatted['queries'] = display_formatted['queries'].apply(lambda x: f"{int(x):,}")
                    if 'search_term_count' in display_formatted.columns:
                        display_formatted['search_term_count'] = display_formatted['search_term_count'].apply(lambda x: f"{int(x):,}")
                    if 'average_queries_per_day' in display_formatted.columns:
                        display_formatted['average_queries_per_day'] = display_formatted['average_queries_per_day'].apply(lambda x: f"{int(x):,}")
                    if 'average_queries_per_search_term_per_day' in display_formatted.columns:
                        display_formatted['average_queries_per_search_term_per_day'] = display_formatted['average_queries_per_search_term_per_day'].apply(lambda x: f"{x:.1f}")
                    if 'search_terms_for_top_x_pct' in display_formatted.columns:
                        display_formatted['search_terms_for_top_x_pct'] = display_formatted['search_terms_for_top_x_pct'].apply(lambda x: f"{int(x):,}")
                    display_formatted['% of queries'] = display_formatted['% of queries'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                    display_formatted['cumulative %'] = display_formatted['cumulative %'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                    display_formatted['ctr'] = display_formatted['ctr'].apply(lambda x: f"{x*100:.2f}%")
                    display_formatted['a2c_rate'] = display_formatted['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
                    display_formatted['conversion_rate'] = display_formatted['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
                    display_formatted['purchases'] = display_formatted['purchases'].apply(lambda x: f"{int(x):,}")
                    display_formatted['revenue_per_query'] = display_formatted['revenue_per_query'].apply(lambda x: f"${x:,.2f}")
                    display_formatted['gross_purchase'] = display_formatted['gross_purchase'].apply(lambda x: f"${format_number(x)}")
                    display_formatted['avg_order_value'] = display_formatted['avg_order_value'].apply(lambda x: f"${format_number(x)}")

                    # Filter to only show selected columns + dimension column
                    columns_to_show = [current_dimension_name] + [col for col in st.session_state.visible_columns if col in display_formatted.columns]
                    # Remove duplicates while preserving order
                    seen = set()
                    columns_to_show = [col for col in columns_to_show if not (col in seen or seen.add(col))]
                    display_formatted = display_formatted[columns_to_show]

                    # Check if there's an expanded row and merge data
                    expanded_row_idx = None
                    expanded_value = None
                    if st.session_state.pivot_selected_row:
                        # Extract the selected value from row_id
                        row_id = st.session_state.pivot_selected_row
                        if row_id in st.session_state.pivot_expanded:
                            # Extract value from row_id format: "dimension_key_value"
                            expanded_value = row_id.replace(f"{current_dimension_key}_", "", 1)
                            # Find the row in formatted display data by matching the dimension value
                            for idx in range(len(display_formatted)):
                                if pivot_data.iloc[idx]['dimension_value'] == expanded_value:
                                    expanded_row_idx = idx
                                    break

                    # Build combined table with expanded rows
                    if expanded_row_idx is not None:
                        selected_value = pivot_data.iloc[expanded_row_idx]['dimension_value']
                        row_id = st.session_state.pivot_selected_row
                        expansion_info = st.session_state.pivot_expanded[row_id]

                        # Expansion controls
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            if current_level < len(selected_dimension_keys) - 1:
                                if st.button("🔽 Next Dimension", key='expand_dimension', use_container_width=True):
                                    st.session_state.pivot_expanded[row_id] = {'type': 'dimension', 'page': 0}
                                    st.rerun()
                        with col2:
                            if st.button("🔍 Search Terms", key='expand_search', use_container_width=True):
                                st.session_state.pivot_expanded[row_id] = {'type': 'search', 'page': 0}
                                st.rerun()
                        with col3:
                            if st.button("⬆ Collapse", key='collapse', use_container_width=True):
                                del st.session_state.pivot_expanded[row_id]
                                st.session_state.pivot_selected_row = None
                                st.rerun()

                        # Build combined dataframe with search terms inline
                        if expansion_info['type'] == 'search':
                            # Query for search terms
                            drill_query = dp.build_drill_down_query(
                                st.session_state.bq_params['table'],
                                base_filters,
                                {current_dimension_key: selected_value},
                                None
                            )
                            search_terms = dp.execute_cached_query(st.session_state.bq_client, drill_query, ttl=300)

                            if not search_terms.empty:
                                # Calculate metrics
                                search_terms['ctr'] = np.where(search_terms['queries'] > 0,
                                                               search_terms['queries_pdp'] / search_terms['queries'], 0)
                                search_terms['a2c_rate'] = np.where(search_terms['queries'] > 0,
                                                                    search_terms['queries_a2c'] / search_terms['queries'], 0)
                                search_terms['conversion_rate'] = np.where(search_terms['queries'] > 0,
                                                                           search_terms['purchases'] / search_terms['queries'], 0)
                                search_terms['revenue_per_query'] = np.where(search_terms['queries'] > 0,
                                                                             search_terms['gross_purchase'] / search_terms['queries'], 0)
                                search_terms['avg_order_value'] = np.where(search_terms['purchases'] > 0,
                                                                           search_terms['gross_purchase'] / search_terms['purchases'], 0)
                                search_terms['average_queries_per_day'] = search_terms['queries'] / num_days

                                # Calculate percentages and cumulative values
                                total_queries_search = search_terms['queries'].sum()
                                search_terms['pct_of_total'] = (search_terms['queries'] / total_queries_all * 100) if total_queries_all > 0 else 0
                                search_terms['cumulative_queries'] = search_terms['queries'].cumsum()
                                search_terms['cumulative_pct'] = (search_terms['cumulative_queries'] / total_queries_search * 100) if total_queries_search > 0 else 0

                                # Apply same sorting as main table (sort by the computed column if it exists)
                                sort_config = sort_configs[current_level] if current_level < len(sort_configs) else {'sort_by': 'queries', 'ascending': False}
                                sort_by_col = sort_config['sort_by']

                                # Ensure the sort column exists in search_terms
                                if sort_by_col in search_terms.columns:
                                    search_terms = search_terms.sort_values(sort_by_col, ascending=sort_config['ascending'])
                                else:
                                    # Fallback to queries if the column doesn't exist
                                    search_terms = search_terms.sort_values('queries', ascending=False)

                                # Paginate
                                page_size = 5
                                current_page = expansion_info['page']
                                total_pages = (len(search_terms) + page_size - 1) // page_size
                                start_idx = current_page * page_size
                                end_idx = min(start_idx + page_size, len(search_terms))
                                search_page = search_terms.iloc[start_idx:end_idx].copy()

                                # Format search terms for merging - rename search_term to match main table column
                                search_display = search_page.copy()
                                search_display = search_display.rename(columns={'search_term': current_dimension_name})

                                # Format numbers exactly like main table
                                search_display['% of queries'] = search_display['pct_of_total'].apply(lambda x: f"{x:.2f}%")
                                search_display['cumulative %'] = search_display['cumulative_pct'].apply(lambda x: f"{x:.2f}%")
                                if 'average_queries_per_day' in search_display.columns:
                                    search_display['average_queries_per_day'] = search_display['average_queries_per_day'].apply(lambda x: f"{int(x):,}")
                                if 'average_queries_per_search_term_per_day' in search_display.columns:
                                    search_display['average_queries_per_search_term_per_day'] = search_display['average_queries_per_search_term_per_day'].apply(lambda x: f"{x:.1f}")
                                search_display['ctr'] = search_display['ctr'].apply(lambda x: f"{x*100:.2f}%")
                                search_display['a2c_rate'] = search_display['a2c_rate'].apply(lambda x: f"{x*100:.2f}%")
                                search_display['conversion_rate'] = search_display['conversion_rate'].apply(lambda x: f"{x*100:.2f}%")
                                search_display['revenue_per_query'] = search_display['revenue_per_query'].apply(lambda x: f"${x:.2f}")
                                search_display['gross_purchase'] = search_display['gross_purchase'].apply(lambda x: f"${format_number(x)}")
                                search_display['avg_order_value'] = search_display['avg_order_value'].apply(lambda x: f"${format_number(x)}")

                                # Match exact columns from main table in same order
                                main_cols = display_formatted.columns.tolist()
                                # Ensure search_display has all columns (add missing ones with empty strings)
                                for col in main_cols:
                                    if col not in search_display.columns:
                                        search_display[col] = ''
                                # Reorder to match main table exactly
                                search_display = search_display[main_cols]

                                # Prepend indent to search terms
                                search_display[current_dimension_name] = '    ↳ ' + search_display[current_dimension_name].astype(str)

                                # Build combined table: rows before + selected row + search terms + rows after
                                combined_table = pd.concat([
                                    display_formatted.iloc[:expanded_row_idx+1],
                                    search_display,
                                    display_formatted.iloc[expanded_row_idx+1:]
                                ], ignore_index=True)

                                # Display combined table
                                st.dataframe(
                                    combined_table,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=600
                                )

                                # Pagination controls
                                st.markdown(f"**Page {current_page + 1} of {total_pages}** (showing {start_idx + 1}-{end_idx} of {len(search_terms)} search terms)")
                                col_prev, col_next = st.columns(2)
                                with col_prev:
                                    if current_page > 0:
                                        if st.button("← Previous", key='prev_page', use_container_width=True):
                                            st.session_state.pivot_expanded[row_id]['page'] = current_page - 1
                                            st.rerun()
                                with col_next:
                                    if current_page < total_pages - 1:
                                        if st.button("Next →", key='next_page', use_container_width=True):
                                            st.session_state.pivot_expanded[row_id]['page'] = current_page + 1
                                            st.rerun()
                            else:
                                # No search terms - just show the regular table
                                st.dataframe(
                                    display_formatted,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=600
                                )
                                st.info("No search terms found")
                        else:
                            # Dimension expansion - keep original behavior for now
                            st.dataframe(
                                display_formatted,
                                use_container_width=True,
                                hide_index=True,
                                height=600
                            )

                    else:
                        # No expansion - display full table with selection enabled
                        selection = st.dataframe(
                            display_formatted,
                            use_container_width=True,
                            hide_index=True,
                            height=600,
                            on_select="rerun",
                            selection_mode="single-row"
                        )

                        # Handle row selection with contextual expansion buttons
                        if selection and len(selection.selection.rows) > 0:
                            selected_idx = selection.selection.rows[0]

                            selected_value = pivot_data.iloc[selected_idx]['dimension_value']
                            row_id = f"{current_dimension_key}_{selected_value}"

                            # Update selected row
                            st.session_state.pivot_selected_row = row_id

                            # Show expansion controls with column-aligned buttons
                            st.markdown("---")
                            st.markdown(f"**Selected:** {selected_value}")
                            st.caption(f"Click **{current_dimension_name}** to expand dimensions, or **% of queries** to view search terms")

                            col1, col2 = st.columns(2)
                            with col1:
                                if current_level < len(selected_dimension_keys) - 1:
                                    if st.button(f"📊 {current_dimension_name}", key='expand_dimension_init', use_container_width=True, help="Expand to next dimension level"):
                                        st.session_state.pivot_expanded[row_id] = {'type': 'dimension', 'page': 0}
                                        st.rerun()
                                else:
                                    st.button(f"📊 {current_dimension_name}", key='expand_dimension_disabled', use_container_width=True, disabled=True, help="No more dimensions to expand")
                            with col2:
                                if st.button("🔍 % of queries", key='expand_search_init', use_container_width=True, help="View search terms for this value"):
                                    st.session_state.pivot_expanded[row_id] = {'type': 'search', 'page': 0}
                                    st.rerun()

                else:
                    st.info("No data available for this dimension with current filters.")

            else:
                st.info("You've reached the end of the dimension hierarchy. Use the 'Show Search Terms' option to explore search terms.")

    except Exception as e:
        st.error(f"Error loading pivot table: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# Footer
st.divider()
st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
