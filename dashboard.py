import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_processing as dp
import visualizations as viz

# Helper function for number formatting
def format_number(value, decimals=2, use_abbreviated=None):
    """Format number with commas as thousand separator or K/M/B abbreviation."""
    # Get setting from session state if not provided
    if use_abbreviated is None:
        use_abbreviated = st.session_state.get('use_abbreviated_numbers', True)

    if use_abbreviated:
        # Use K/M/B notation for large numbers
        abs_value = abs(value)
        if abs_value >= 1_000_000_000:
            return f"{value/1_000_000_000:.1f}B"
        elif abs_value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif abs_value >= 1_000:
            return f"{value/1_000:.1f}K"
        elif value > 10:
            return f"{int(value):,}"
        else:
            return f"{value:,.{decimals}f}"
    else:
        # Traditional comma separator
        if value > 10:
            return f"{int(value):,}"
        else:
            return f"{value:,.{decimals}f}"

# Dynamic formatting helper functions
def fmt_percentage(value):
    """Format percentage based on user settings."""
    decimals = st.session_state.get('percentage_decimals', 2)
    return f"{value*100:.{decimals}f}%"

def fmt_currency(value):
    """Format currency based on user settings."""
    decimals = st.session_state.get('currency_decimals', 2)
    show_symbol = st.session_state.get('show_currency_symbol', True)
    use_abbreviated = st.session_state.get('use_abbreviated_numbers', True)

    symbol = "$" if show_symbol else ""

    if use_abbreviated:
        abs_value = abs(value)
        if abs_value >= 1_000_000_000:
            return f"{symbol}{value/1_000_000_000:.{decimals}f}B"
        elif abs_value >= 1_000_000:
            return f"{symbol}{value/1_000_000:.{decimals}f}M"
        elif abs_value >= 1_000:
            return f"{symbol}{value/1_000:.{decimals}f}K"
        else:
            return f"{symbol}{value:.{decimals}f}"
    else:
        return f"{symbol}{format_number(value, decimals, use_abbreviated=False)}"

def fmt_decimal(value):
    """Format decimal number based on user settings."""
    decimals = st.session_state.get('decimal_number_places', 1)
    return f"{value:.{decimals}f}"

def fmt_whole_number(value):
    """Format whole number based on user settings."""
    use_abbreviated = st.session_state.get('use_abbreviated_numbers', True)
    return format_number(value, decimals=0, use_abbreviated=use_abbreviated)

# Helper function to calculate cumulative percentage
def add_cumulative_percentage(df, total_value, value_column='queries'):
    """
    Add cumulative percentage column to dataframe based on current row order.
    Excludes "Total" rows from cumulative calculation.

    Args:
        df: DataFrame with data in desired display order
        total_value: Total value to use as denominator (100%)
        value_column: Column name to use for cumulative calculation (default: 'queries')

    Returns:
        DataFrame with cumulative_pct column added
    """
    if total_value > 0 and value_column in df.columns:
        # Exclude "Total" rows from cumulative calculation
        if 'dimension_value' in df.columns:
            # Create mask for non-Total rows
            non_total_mask = df['dimension_value'] != 'Total'
            # Calculate cumulative sum only for non-Total rows
            df['cumulative_pct'] = None
            df.loc[non_total_mask, 'cumulative_pct'] = (
                df.loc[non_total_mask, value_column].cumsum() / total_value * 100
            )
        else:
            # No dimension_value column, calculate normally
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

# Helper function to extract dimension filters from pivot row
def extract_dimension_filters_from_row(row_data, dimension_keys, dimension_names):
    """
    Extract dimension filters from a selected pivot table row.

    Args:
        row_data: Series containing the selected row data
        dimension_keys: List of dimension column keys (e.g., ['channel', 'n_words_grouped'])
        dimension_names: List of dimension display names (e.g., ['Channel', 'Number of Words'])

    Returns:
        tuple: (dimension_filters dict, display_name string)
    """
    dimension_filters = {}
    display_parts = []

    # Check if this is a search term row
    if 'Search Term' in row_data.index and pd.notna(row_data.get('Search Term')):
        search_term = row_data['Search Term']
        if search_term and search_term != '-':
            # Clean the search term - remove streamlit display characters
            if isinstance(search_term, str):
                search_term = search_term.replace('▶', '').replace('▼', '').replace('►', '').strip()
            dimension_filters['search_term'] = search_term
            display_parts.append(f"'{search_term}'")

    # Extract dimension values from the row
    for dim_name in dimension_names:
        if dim_name in row_data.index:
            dim_value = row_data[dim_name]
            if pd.notna(dim_value) and dim_value != '-' and dim_value != '':
                # Clean the value - remove streamlit display characters (▶, ▼, etc.)
                if isinstance(dim_value, str):
                    # Remove common unicode display characters used by streamlit
                    dim_value = dim_value.replace('▶', '').replace('▼', '').replace('►', '').strip()

                # Find corresponding key
                dim_key = None
                for i, name in enumerate(dimension_names):
                    if name == dim_name and i < len(dimension_keys):
                        dim_key = dimension_keys[i]
                        break

                if dim_key:
                    dimension_filters[dim_key] = dim_value
                    display_parts.append(f"{dim_value}")

    display_name = " > ".join(display_parts) if display_parts else "All Data"

    return dimension_filters, display_name

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
        # Build WHERE clause to respect date filters
        where_conditions = []
        if bq_params.get('date_start') and bq_params.get('date_end'):
            where_conditions.append(f"date BETWEEN '{bq_params['date_start']}' AND '{bq_params['date_end']}'")
        elif bq_params.get('date_start'):
            where_conditions.append(f"date >= '{bq_params['date_start']}'")
        elif bq_params.get('date_end'):
            where_conditions.append(f"date <= '{bq_params['date_end']}'")

        if bq_params.get('countries'):
            country_list = "', '".join(bq_params['countries'])
            where_conditions.append(f"country IN ('{country_list}')")

        if bq_params.get('channels'):
            channel_list = "', '".join(bq_params['channels'])
            where_conditions.append(f"channel IN ('{channel_list}')")

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        date_query = f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_params['table']}`
            {where_clause}
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
        value=None,
        help="Filter data from this date (pushes filter to BigQuery)"
    )
with col2:
    bq_date_end = st.date_input(
        "End Date",
        value=None,
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
if load_button:
    # Validate that dates are selected
    if not bq_date_start or not bq_date_end:
        st.sidebar.error("⚠️ Please select both Start Date and End Date before connecting.")
    else:
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

# Initialize advanced date filtering toggle
if 'use_advanced_dates' not in st.session_state:
    st.session_state.use_advanced_dates = False

# Date range filter
min_date = st.session_state.bq_min_date
max_date = st.session_state.bq_max_date

# Toggle for advanced date filtering
use_advanced = st.sidebar.checkbox(
    "Use Advanced Date Filtering",
    value=st.session_state.use_advanced_dates,
    key='use_advanced_dates',
    help="Enable to use multiple date ranges (includes/excludes). When enabled, the normal date range will be hidden."
)

# Only show normal date range if advanced mode is NOT active
if not use_advanced:
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
else:
    # When advanced mode is on, use full date range from BigQuery
    start_date = pd.to_datetime(min_date)
    end_date = pd.to_datetime(max_date)

# Advanced Date Filtering
with st.sidebar.expander("📅 Advanced Date Filtering", expanded=use_advanced):
    st.caption("Add multiple date ranges to include or exclude specific periods")

    # Initialize session state for date ranges
    if 'additional_date_ranges' not in st.session_state:
        st.session_state.additional_date_ranges = []
    if 'excluded_date_ranges' not in st.session_state:
        st.session_state.excluded_date_ranges = []

    # Additional Include Ranges
    st.subheader("➕ Additional Ranges to Include")
    st.caption("Add more date ranges beyond the main range above")

    # Display existing additional ranges
    ranges_to_remove = []
    for i, (r_start, r_end) in enumerate(st.session_state.additional_date_ranges):
        col1, col2, col3 = st.columns([3, 3, 1])
        with col1:
            st.text(f"{r_start}")
        with col2:
            st.text(f"to {r_end}")
        with col3:
            if st.button("❌", key=f"remove_include_{i}"):
                ranges_to_remove.append(i)

    # Remove marked ranges
    for idx in sorted(ranges_to_remove, reverse=True):
        st.session_state.additional_date_ranges.pop(idx)
        st.rerun()

    # Add new include range
    col1, col2 = st.columns(2)
    with col1:
        new_include_start = st.date_input(
            "Start",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key='new_include_start'
        )
    with col2:
        new_include_end = st.date_input(
            "End",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key='new_include_end'
        )

    if st.button("➕ Add Include Range"):
        if new_include_start <= new_include_end:
            st.session_state.additional_date_ranges.append((
                str(new_include_start),
                str(new_include_end)
            ))
            st.rerun()
        else:
            st.error("Start date must be before end date")

    st.markdown("---")

    # Excluded Ranges
    st.subheader("🚫 Ranges to Exclude")
    st.caption("Exclude specific periods (e.g., holidays, promotions)")

    # Display existing excluded ranges
    excluded_to_remove = []
    for i, (r_start, r_end) in enumerate(st.session_state.excluded_date_ranges):
        col1, col2, col3 = st.columns([3, 3, 1])
        with col1:
            st.text(f"{r_start}")
        with col2:
            st.text(f"to {r_end}")
        with col3:
            if st.button("❌", key=f"remove_exclude_{i}"):
                excluded_to_remove.append(i)

    # Remove marked ranges
    for idx in sorted(excluded_to_remove, reverse=True):
        st.session_state.excluded_date_ranges.pop(idx)
        st.rerun()

    # Add new exclude range
    col1, col2 = st.columns(2)
    with col1:
        new_exclude_start = st.date_input(
            "Start",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key='new_exclude_start'
        )
    with col2:
        new_exclude_end = st.date_input(
            "End",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key='new_exclude_end'
        )

    if st.button("➕ Add Exclude Range"):
        if new_exclude_start <= new_exclude_end:
            st.session_state.excluded_date_ranges.append((
                str(new_exclude_start),
                str(new_exclude_end)
            ))
            st.rerun()
        else:
            st.error("Start date must be before end date")

    # Summary
    if st.session_state.additional_date_ranges or st.session_state.excluded_date_ranges:
        st.markdown("---")
        st.caption(f"**Active filters:** {len(st.session_state.additional_date_ranges) + 1} ranges included, {len(st.session_state.excluded_date_ranges)} ranges excluded")

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
# When advanced mode is active, don't include normal date_start/date_end
base_filters = {
    'date_start': None if use_advanced else str(start_date.date()),
    'date_end': None if use_advanced else str(end_date.date()),
    'additional_date_ranges': st.session_state.additional_date_ranges if st.session_state.additional_date_ranges else None,
    'excluded_date_ranges': st.session_state.excluded_date_ranges if st.session_state.excluded_date_ranges else None,
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

# Extract 1p and 3p values
total_products_pdp_1p = int(result_df['total_products_pdp_1p'].iloc[0]) if result_df['total_products_pdp_1p'].iloc[0] else 0
total_products_pdp_3p = int(result_df['total_products_pdp_3p'].iloc[0]) if result_df['total_products_pdp_3p'].iloc[0] else 0
total_products_a2c_1p = int(result_df['total_products_a2c_1p'].iloc[0]) if result_df['total_products_a2c_1p'].iloc[0] else 0
total_products_a2c_3p = int(result_df['total_products_a2c_3p'].iloc[0]) if result_df['total_products_a2c_3p'].iloc[0] else 0
total_purchases_1p = int(result_df['total_purchases_1p'].iloc[0]) if result_df['total_purchases_1p'].iloc[0] else 0
total_purchases_3p = int(result_df['total_purchases_3p'].iloc[0]) if result_df['total_purchases_3p'].iloc[0] else 0
total_revenue_1p = float(result_df['total_revenue_1p'].iloc[0]) if result_df['total_revenue_1p'].iloc[0] else 0.0
total_revenue_3p = float(result_df['total_revenue_3p'].iloc[0]) if result_df['total_revenue_3p'].iloc[0] else 0.0

# Calculate percentages for 1p vs 3p
total_clicks_1p_3p = total_products_pdp_1p + total_products_pdp_3p
pct_clicks_1p = (total_products_pdp_1p / total_clicks_1p_3p * 100) if total_clicks_1p_3p > 0 else 0.0
pct_clicks_3p = (total_products_pdp_3p / total_clicks_1p_3p * 100) if total_clicks_1p_3p > 0 else 0.0

total_a2c_1p_3p = total_products_a2c_1p + total_products_a2c_3p
pct_a2c_1p = (total_products_a2c_1p / total_a2c_1p_3p * 100) if total_a2c_1p_3p > 0 else 0.0
pct_a2c_3p = (total_products_a2c_3p / total_a2c_1p_3p * 100) if total_a2c_1p_3p > 0 else 0.0

total_purchases_1p_3p = total_purchases_1p + total_purchases_3p
pct_purchases_1p = (total_purchases_1p / total_purchases_1p_3p * 100) if total_purchases_1p_3p > 0 else 0.0
pct_purchases_3p = (total_purchases_3p / total_purchases_1p_3p * 100) if total_purchases_1p_3p > 0 else 0.0

aov_1p = (total_revenue_1p / total_purchases_1p) if total_purchases_1p > 0 else 0.0
aov_3p = (total_revenue_3p / total_purchases_3p) if total_purchases_3p > 0 else 0.0
total_aov = aov_1p + aov_3p
pct_aov_1p = (aov_1p / total_aov * 100) if total_aov > 0 else 0.0
pct_aov_3p = (aov_3p / total_aov * 100) if total_aov > 0 else 0.0

# Query for search term count (fast COUNT DISTINCT)
where_clause = dp.build_filter_clause(base_filters)
count_query = f"""
SELECT COUNT(DISTINCT search_term) as search_term_count
FROM `{st.session_state.bq_params['table']}`
{where_clause}
"""
count_result = dp.execute_cached_query(st.session_state.bq_client, count_query, ttl=300)
search_term_count = int(count_result.iloc[0]['search_term_count']) if not count_result.empty else 0

# Calculate search terms for top 80% (fixed concentration for KPI dashboard)
search_terms_top_80_pct = calculate_terms_for_top_pct(
    st.session_state.bq_client,
    st.session_state.bq_params['table'],
    base_filters,
    None,  # No dimension column
    None,  # No dimension value
    80  # 80% concentration
)

# Calculate number of days
num_days = base_filters.get('num_days', 1)

kpis = {
    'total_queries': total_queries,
    'total_revenue': total_revenue,
    'avg_ctr': float(total_queries_pdp / total_queries) if total_queries > 0 else 0.0,
    'avg_conversion': float(total_purchases / total_queries) if total_queries > 0 else 0.0,
    'avg_pdp_conversion': float(total_purchases / total_queries_pdp) if total_queries_pdp > 0 else 0.0,
    'revenue_per_query': float(total_revenue / total_queries) if total_queries > 0 else 0.0,
    'total_purchases': total_purchases,
    'avg_order_value': float(total_revenue / total_purchases) if total_purchases > 0 else 0.0,
    'search_term_count': search_term_count,
    'average_queries_per_day': (total_queries / num_days) if num_days > 0 else 0.0,
    'average_queries_per_search_term_per_day': (total_queries / num_days / search_term_count) if (num_days > 0 and search_term_count > 0) else 0.0,
    'search_terms_for_top_80_pct': search_terms_top_80_pct,
    'pct_clicks_1p': pct_clicks_1p,
    'pct_clicks_3p': pct_clicks_3p,
    'pct_a2c_1p': pct_a2c_1p,
    'pct_a2c_3p': pct_a2c_3p,
    'pct_purchases_1p': pct_purchases_1p,
    'pct_purchases_3p': pct_purchases_3p,
    'pct_aov_1p': pct_aov_1p,
    'pct_aov_3p': pct_aov_3p,
    'aov_1p': aov_1p,
    'aov_3p': aov_3p
}

col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

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
        f"{kpis['average_queries_per_search_term_per_day']:.1f}"
    )

with col4:
    st.metric(
        "Search Terms for Top 80%",
        format_number(kpis['search_terms_for_top_80_pct'])
    )

with col5:
    st.metric(
        "CTR",
        f"{kpis['avg_ctr']*100:.2f}%"
    )

with col6:
    st.metric(
        "PDP Conversion",
        f"{kpis['avg_pdp_conversion']*100:.2f}%"
    )

with col7:
    st.metric(
        "Avg Order Value",
        f"${format_number(kpis['avg_order_value'])}"
    )

# Second row: 1p vs 3p metrics
st.markdown("### 1p vs 3p Breakdown")
col1_1p, col2_1p, col3_1p, col4_1p = st.columns(4)

with col1_1p:
    st.metric(
        "Clicks 1p / 3p",
        f"{kpis['pct_clicks_1p']:,.0f}% / {kpis['pct_clicks_3p']:,.0f}%"
    )

with col2_1p:
    st.metric(
        "Add to Cart 1p / 3p",
        f"{kpis['pct_a2c_1p']:,.0f}% / {kpis['pct_a2c_3p']:,.0f}%"
    )

with col3_1p:
    st.metric(
        "Purchases 1p / 3p",
        f"{kpis['pct_purchases_1p']:,.0f}% / {kpis['pct_purchases_3p']:,.0f}%"
    )

with col4_1p:
    st.metric(
        "AOV 1p / 3p",
        f"${kpis['aov_1p']:,.0f} / ${kpis['aov_3p']:,.0f}"
    )

st.divider()

# Hierarchical Pivot Table
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

# Initialize session state for pivot row trend viewing (multi-row comparison)
if 'pivot_rows_for_trend' not in st.session_state:
    st.session_state.pivot_rows_for_trend = []  # List of rows to compare

# Initialize session state for checkbox selection tracking
if 'selected_rows_for_comparison' not in st.session_state:
    st.session_state.selected_rows_for_comparison = {}  # {row_id: {'dimension_filters': {...}, 'display_name': str}}

# Initialize session state for chart grouping
if 'metric_chart_groups' not in st.session_state:
    st.session_state.metric_chart_groups = []  # List of lists of metrics to combine in charts

# Row Selection for Trend Comparison
num_selected = len(st.session_state.selected_rows_for_comparison)
if num_selected > 0:
    st.markdown(f"""
    <div style="background: #f0f9ff; padding: 0.75rem 1rem; border-radius: 6px; border-left: 4px solid #3b82f6; margin-bottom: 1rem;">
        <p style="margin: 0; color: #1e40af; font-size: 0.9rem;">
            <strong>{num_selected} row(s) selected for comparison</strong>
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Display selected rows as chips
    col_chips, col_buttons = st.columns([3, 1])
    with col_chips:
        chips_html = '<div style="display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.5rem;">'
        for row_id, row_data in list(st.session_state.selected_rows_for_comparison.items())[:5]:  # Show first 5
            chips_html += f'<span style="background: #dbeafe; color: #1e40af; padding: 0.25rem 0.75rem; border-radius: 12px; font-size: 0.85rem;">{row_data["display_name"]}</span>'
        if num_selected > 5:
            chips_html += f'<span style="background: #e5e7eb; color: #374151; padding: 0.25rem 0.75rem; border-radius: 12px; font-size: 0.85rem;">+{num_selected - 5} more</span>'
        chips_html += '</div>'
        st.markdown(chips_html, unsafe_allow_html=True)

    with col_buttons:
        col_compare, col_clear = st.columns(2)
        with col_compare:
            if st.button("📊 Compare", key='compare_trends_btn', use_container_width=True):
                # Copy selected rows to pivot_rows_for_trend for analysis
                st.session_state.pivot_rows_for_trend = list(st.session_state.selected_rows_for_comparison.values())
                st.rerun()
        with col_clear:
            if st.button("✕ Clear", key='clear_selection_btn', use_container_width=True):
                st.session_state.selected_rows_for_comparison = {}
                st.session_state.pivot_rows_for_trend = []
                st.rerun()

# Calculate number of days in selected period
num_days = (end_date - start_date).days + 1

# Column Format Configuration
with st.expander("🎨 Column Format Settings", expanded=False):
    st.markdown("**Customize how numbers are displayed in the pivot table**")

    col_fmt1, col_fmt2, col_fmt3 = st.columns(3)

    with col_fmt1:
        percentage_decimals = st.selectbox(
            "Percentage Decimals",
            options=[0, 1, 2, 3, 4],
            index=2,  # Default to 2 decimal places
            help="Number of decimal places for percentages (CTR, conversion rates, etc.)",
            key='percentage_decimals'
        )

    with col_fmt2:
        currency_decimals = st.selectbox(
            "Currency Decimals",
            options=[0, 1, 2],
            index=2,  # Default to 2 decimal places
            help="Number of decimal places for currency values",
            key='currency_decimals'
        )

    with col_fmt3:
        use_abbreviated_numbers = st.checkbox(
            "Use Abbreviated Numbers (K/M/B)",
            value=True,
            help="Use K for thousands, M for millions, B for billions in large numbers",
            key='use_abbreviated_numbers'
        )

    col_fmt4, col_fmt5 = st.columns(2)

    with col_fmt4:
        decimal_number_places = st.selectbox(
            "Decimal Number Places",
            options=[0, 1, 2, 3, 4],
            index=1,  # Default to 1 decimal place
            help="Decimal places for numbers like average queries per day",
            key='decimal_number_places'
        )

    with col_fmt5:
        show_currency_symbol = st.checkbox(
            "Show Currency Symbol ($)",
            value=True,
            help="Show $ symbol for currency values",
            key='show_currency_symbol'
        )

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
    st.session_state.dimension_order = []

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
all_available_columns = ['queries', 'search_term_count', 'average_queries_per_day', 'average_queries_per_search_term_per_day', 'search_terms_for_top_x_pct', '% of queries', 'cumulative %', 'ctr', 'a2c_rate', 'conversion_rate', 'pdp_conversion', 'revenue_per_query', 'purchases', 'gross_purchase', 'avg_order_value', 'pct_clicks_1p', 'pct_clicks_3p', 'pct_a2c_1p', 'pct_a2c_3p', 'pct_purchases_1p', 'pct_purchases_3p', 'aov_1p', 'aov_3p']

# Metrics that make sense for time-series trending (exclude counts, percentages, cumulative values)
TRENDABLE_METRICS = ['queries', 'ctr', 'a2c_rate', 'conversion_rate', 'pdp_conversion', 'revenue_per_query', 'purchases', 'gross_purchase', 'avg_order_value', 'pct_clicks_1p', 'pct_clicks_3p', 'pct_a2c_1p', 'pct_a2c_3p', 'pct_purchases_1p', 'pct_purchases_3p', 'aov_1p', 'aov_3p']

# Initialize or migrate visible_columns
if 'visible_columns' not in st.session_state:
    st.session_state.visible_columns = []  # Empty list = show no columns by default
elif st.session_state.visible_columns is not None:
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
if st.session_state.visible_columns is None:
    valid_defaults = all_available_columns  # None = all columns
else:
    valid_defaults = [col for col in st.session_state.visible_columns if col in all_available_columns]
    # Force update session state if migration resulted in changes
    if set(valid_defaults) != set(st.session_state.visible_columns):
        st.session_state.visible_columns = valid_defaults

st.multiselect(
    "Select columns to display",
    options=all_available_columns,
    default=valid_defaults,
    key='visible_columns',
    help="Select specific columns to display. Leave empty to show only the dimension column."
)

# Global settings
# Check if search_terms_for_top_x_pct is visible
show_concentration = ('search_terms_for_top_x_pct' in st.session_state.visible_columns)
if show_concentration:
    col_thresh, col_concentration = st.columns([2, 2])
    with col_thresh:
        cumulative_threshold = st.slider(
            "Cumulative threshold %",
            min_value=50,
            max_value=100,
            value=100,
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
else:
    cumulative_threshold = st.slider(
        "Cumulative threshold %",
        min_value=50,
        max_value=100,
        value=100,
        step=5,
        key='cumulative_threshold_hierarchy',
        help="Show rows until this cumulative % is reached, aggregate the rest as 'Other'"
    )
    search_term_concentration_pct = 100  # Default value when not visible

# Search term aggregation threshold
col_search1, col_search2 = st.columns([2, 3])
with col_search1:
    min_queries_for_search_terms = st.number_input(
        "Min queries for search terms",
        min_value=1,
        max_value=100,
        value=1,
        step=1,
        key='min_queries_search_terms',
        help="Search terms with fewer queries will be aggregated into 'Other'"
    )

# Default sort configuration: sort by queries descending
sort_configs = [{'sort_by': 'queries', 'ascending': False} for _ in range(len(selected_dimension_names))]

if len(selected_dimension_names) == 0:
    # Show Grand Total when no dimensions selected
    st.subheader("📊 Total")

    # Initialize session state for total row expansion
    if 'pivot_expanded' not in st.session_state:
        st.session_state.pivot_expanded = {}
    if 'pivot_selected_row' not in st.session_state:
        st.session_state.pivot_selected_row = None

    # Create a single Total row using the KPI data already calculated
    total_row = pd.DataFrame([{
        'dimension_value': 'Total',
        'queries': kpis['total_queries'],
        'search_term_count': kpis['search_term_count'],
        'average_queries_per_day': kpis['average_queries_per_day'],
        'average_queries_per_search_term_per_day': kpis['average_queries_per_search_term_per_day'],
        'search_terms_for_top_x_pct': kpis['search_terms_for_top_80_pct'],
        'pct_of_total': 100.0,
        'cumulative_pct': None,  # Total row shouldn't show cumulative %
        'ctr': kpis['avg_ctr'],
        'a2c_rate': 0.0,  # Not in KPIs, set to 0
        'conversion_rate': kpis['avg_conversion'],
        'pdp_conversion': kpis['avg_pdp_conversion'],
        'revenue_per_query': kpis['revenue_per_query'],
        'purchases': kpis['total_purchases'],
        'gross_purchase': kpis['total_revenue'],
        'avg_order_value': kpis['avg_order_value'],
        'pct_clicks_1p': kpis.get('pct_clicks_1p', 0.0),
        'pct_clicks_3p': kpis.get('pct_clicks_3p', 0.0),
        'pct_a2c_1p': kpis.get('pct_a2c_1p', 0.0),
        'pct_a2c_3p': kpis.get('pct_a2c_3p', 0.0),
        'pct_purchases_1p': kpis.get('pct_purchases_1p', 0.0),
        'pct_purchases_3p': kpis.get('pct_purchases_3p', 0.0),
        'aov_1p': kpis.get('aov_1p', 0.0),
        'aov_3p': kpis.get('aov_3p', 0.0)
    }])

    # Add tracking columns
    total_row['_row_id'] = 'total_row'
    total_row['_level'] = 0
    total_row['_type'] = 'total'

    # Rename for display
    total_row = total_row.rename(columns={'pct_of_total': '% of queries', 'cumulative_pct': 'cumulative %'})

    # Format the display dataframe
    display_total = total_row.copy()
    display_total['Total'] = '▶ ' + display_total['dimension_value']

    # Format numeric columns using dynamic formatters
    pct_decimals = st.session_state.get('percentage_decimals', 2)
    display_total['queries'] = display_total['queries'].apply(fmt_whole_number)
    display_total['search_term_count'] = display_total['search_term_count'].apply(fmt_whole_number)
    display_total['average_queries_per_day'] = display_total['average_queries_per_day'].apply(fmt_decimal)
    display_total['average_queries_per_search_term_per_day'] = display_total['average_queries_per_search_term_per_day'].apply(fmt_decimal)
    display_total['search_terms_for_top_x_pct'] = display_total['search_terms_for_top_x_pct'].apply(fmt_whole_number)
    display_total['% of queries'] = display_total['% of queries'].apply(lambda x: f"{x:.{pct_decimals}f}%")
    display_total['cumulative %'] = display_total['cumulative %'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    display_total['ctr'] = display_total['ctr'].apply(fmt_percentage)
    display_total['a2c_rate'] = display_total['a2c_rate'].apply(fmt_percentage)
    display_total['conversion_rate'] = display_total['conversion_rate'].apply(fmt_percentage)
    display_total['pdp_conversion'] = display_total['pdp_conversion'].apply(fmt_percentage)
    display_total['revenue_per_query'] = display_total['revenue_per_query'].apply(fmt_currency)
    display_total['purchases'] = display_total['purchases'].apply(fmt_whole_number)
    display_total['gross_purchase'] = display_total['gross_purchase'].apply(fmt_currency)
    display_total['avg_order_value'] = display_total['avg_order_value'].apply(fmt_currency)

    # Format 1p/3p percentage columns if they exist
    if 'pct_clicks_1p' in display_total.columns:
        display_total['pct_clicks_1p'] = display_total['pct_clicks_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'pct_clicks_3p' in display_total.columns:
        display_total['pct_clicks_3p'] = display_total['pct_clicks_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'pct_a2c_1p' in display_total.columns:
        display_total['pct_a2c_1p'] = display_total['pct_a2c_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'pct_a2c_3p' in display_total.columns:
        display_total['pct_a2c_3p'] = display_total['pct_a2c_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'pct_purchases_1p' in display_total.columns:
        display_total['pct_purchases_1p'] = display_total['pct_purchases_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'pct_purchases_3p' in display_total.columns:
        display_total['pct_purchases_3p'] = display_total['pct_purchases_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
    if 'aov_1p' in display_total.columns:
        display_total['aov_1p'] = display_total['aov_1p'].apply(fmt_currency)
    if 'aov_3p' in display_total.columns:
        display_total['aov_3p'] = display_total['aov_3p'].apply(fmt_currency)

    # Select columns to display based on visible_columns
    display_cols = ['Total']
    display_cols.extend(st.session_state.visible_columns)

    # Remove duplicates while preserving order and filter to only existing columns
    display_cols = list(dict.fromkeys(display_cols))
    display_cols = [col for col in display_cols if col in display_total.columns]

    # Add drill-down indicator column
    row_id = 'total_row'
    is_expanded = row_id in st.session_state.pivot_expanded

    # Add expansion indicator as first column
    display_total_with_indicator = display_total.copy()
    display_total_with_indicator.insert(0, '', '🔽' if is_expanded else '▶️')

    # Display table with selection enabled
    selection = st.dataframe(
        display_total_with_indicator[[''] + display_cols],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key='total_pivot_table'
    )

    # Handle row selection for drill-down
    if selection and selection.selection.rows and len(selection.selection.rows) > 0:
        # Toggle expansion
        if is_expanded:
            del st.session_state.pivot_expanded[row_id]
        else:
            st.session_state.pivot_expanded[row_id] = {'type': 'search', 'page': 0}
        st.rerun()

    # Show expanded search terms if total row is expanded
    if 'total_row' in st.session_state.pivot_expanded:
        with st.spinner('Loading search terms...'):
            # Query all search terms with base filters only
            search_query = dp.build_drill_down_query(
                st.session_state.bq_params['table'],
                base_filters,
                {},  # No dimension filters
                None
            )
            search_terms = dp.execute_cached_query(st.session_state.bq_client, search_query, ttl=300)

            if not search_terms.empty:
                # Calculate derived metrics
                search_terms['ctr'] = np.where(search_terms['queries'] > 0, search_terms['queries_pdp'] / search_terms['queries'], 0)
                search_terms['a2c_rate'] = np.where(search_terms['queries'] > 0, search_terms['queries_a2c'] / search_terms['queries'], 0)
                search_terms['conversion_rate'] = np.where(search_terms['queries'] > 0, search_terms['purchases'] / search_terms['queries'], 0)
                search_terms['pdp_conversion'] = np.where(search_terms['queries_pdp'] > 0, search_terms['purchases'] / search_terms['queries_pdp'], 0)
                search_terms['revenue_per_query'] = np.where(search_terms['queries'] > 0, search_terms['gross_purchase'] / search_terms['queries'], 0)
                search_terms['avg_order_value'] = np.where(search_terms['purchases'] > 0, search_terms['gross_purchase'] / search_terms['purchases'], 0)

                # Calculate 1p/3p percentage metrics
                total_clicks_1p_3p = search_terms['products_pdp_1p'] + search_terms['products_pdp_3p']
                search_terms['pct_clicks_1p'] = np.where(total_clicks_1p_3p > 0,
                                                         (search_terms['products_pdp_1p'] / total_clicks_1p_3p * 100), 0)
                search_terms['pct_clicks_3p'] = np.where(total_clicks_1p_3p > 0,
                                                         (search_terms['products_pdp_3p'] / total_clicks_1p_3p * 100), 0)

                total_a2c_1p_3p = search_terms['products_a2c_1p'] + search_terms['products_a2c_3p']
                search_terms['pct_a2c_1p'] = np.where(total_a2c_1p_3p > 0,
                                                      (search_terms['products_a2c_1p'] / total_a2c_1p_3p * 100), 0)
                search_terms['pct_a2c_3p'] = np.where(total_a2c_1p_3p > 0,
                                                      (search_terms['products_a2c_3p'] / total_a2c_1p_3p * 100), 0)

                total_purchases_1p_3p = search_terms['purchases_1p'] + search_terms['purchases_3p']
                search_terms['pct_purchases_1p'] = np.where(total_purchases_1p_3p > 0,
                                                            (search_terms['purchases_1p'] / total_purchases_1p_3p * 100), 0)
                search_terms['pct_purchases_3p'] = np.where(total_purchases_1p_3p > 0,
                                                            (search_terms['purchases_3p'] / total_purchases_1p_3p * 100), 0)

                search_terms['aov_1p'] = np.where(search_terms['purchases_1p'] > 0,
                                                  (search_terms['gross_purchase_1p'] / search_terms['purchases_1p']), 0)
                search_terms['aov_3p'] = np.where(search_terms['purchases_3p'] > 0,
                                                  (search_terms['gross_purchase_3p'] / search_terms['purchases_3p']), 0)

                # Add calculated columns
                search_terms['search_term_count'] = 1  # Each row is one search term
                search_terms['average_queries_per_day'] = search_terms['queries'] / num_days
                search_terms['average_queries_per_search_term_per_day'] = search_terms['queries'] / num_days
                search_terms['search_terms_for_top_x_pct'] = 0  # Not applicable for individual search terms

                # Calculate percentages
                search_terms['pct_of_total'] = (search_terms['queries'] / kpis['total_queries'] * 100) if kpis['total_queries'] > 0 else 0
                total_queries_search = search_terms['queries'].sum()
                search_terms['cumulative_queries'] = search_terms['queries'].cumsum()
                search_terms['cumulative_pct'] = (search_terms['cumulative_queries'] / total_queries_search * 100) if total_queries_search > 0 else 0

                # Pagination
                search_page_size = 10
                expansion_info = st.session_state.pivot_expanded['total_row']
                search_current_page = expansion_info['page']
                search_total_pages = (len(search_terms) + search_page_size - 1) // search_page_size
                search_start_idx = search_current_page * search_page_size
                search_end_idx = min(search_start_idx + search_page_size, len(search_terms))
                search_page = search_terms.iloc[search_start_idx:search_end_idx].copy()

                # Format for display
                search_display = search_page.copy()
                search_display = search_display.rename(columns={'pct_of_total': '% of queries', 'cumulative_pct': 'cumulative %'})

                # Format columns
                # Format numeric columns using dynamic formatters
                pct_decimals = st.session_state.get('percentage_decimals', 2)
                search_display['queries'] = search_display['queries'].apply(fmt_whole_number)
                search_display['search_term_count'] = search_display['search_term_count'].apply(fmt_whole_number)
                search_display['average_queries_per_day'] = search_display['average_queries_per_day'].apply(fmt_decimal)
                search_display['average_queries_per_search_term_per_day'] = search_display['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                search_display['search_terms_for_top_x_pct'] = search_display['search_terms_for_top_x_pct'].apply(fmt_whole_number)
                search_display['% of queries'] = search_display['% of queries'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                search_display['cumulative %'] = search_display['cumulative %'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                search_display['ctr'] = search_display['ctr'].apply(fmt_percentage)
                search_display['a2c_rate'] = search_display['a2c_rate'].apply(fmt_percentage)
                search_display['conversion_rate'] = search_display['conversion_rate'].apply(fmt_percentage)
                search_display['pdp_conversion'] = search_display['pdp_conversion'].apply(fmt_percentage)
                search_display['revenue_per_query'] = search_display['revenue_per_query'].apply(fmt_currency)
                search_display['purchases'] = search_display['purchases'].apply(fmt_whole_number)
                search_display['gross_purchase'] = search_display['gross_purchase'].apply(fmt_currency)
                search_display['avg_order_value'] = search_display['avg_order_value'].apply(fmt_currency)

                # Select columns to display
                search_cols = ['search_term']
                search_cols.extend(st.session_state.visible_columns)

                st.dataframe(search_display[search_cols], use_container_width=True, hide_index=True)

                # Pagination controls
                if search_total_pages > 1:
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col1:
                        if search_current_page > 0:
                            if st.button("◀ Previous", key='total_search_prev'):
                                st.session_state.pivot_expanded['total_row']['page'] -= 1
                                st.rerun()
                    with col2:
                        st.write(f"Page {search_current_page + 1} of {search_total_pages} ({len(search_terms)} search terms)")
                    with col3:
                        if search_current_page < search_total_pages - 1:
                            if st.button("Next ▶", key='total_search_next'):
                                st.session_state.pivot_expanded['total_row']['page'] += 1
                                st.rerun()
            else:
                st.info("No search terms found.")

else:
    # Convert dimension names to keys
    selected_dimension_keys = [dimensions[name] for name in selected_dimension_names]

    # Initialize session state for inline expansion
    if 'pivot_expanded' not in st.session_state:
        st.session_state.pivot_expanded = {}  # {row_id: {'type': 'dimension'/'search', 'page': 0}}
    if 'pivot_selected_row' not in st.session_state:
        st.session_state.pivot_selected_row = None
    if 'search_term_input' not in st.session_state:
        st.session_state.search_term_input = ''
    if 'search_term_active' not in st.session_state:
        st.session_state.search_term_active = None
    if 'search_term_path' not in st.session_state:
        st.session_state.search_term_path = None

    # Search Term Finder
    st.markdown("""
    <div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 0.75rem 1rem; border-radius: 6px; margin-bottom: 1rem;">
        <p style="margin: 0; color: white; font-weight: 600; font-size: 0.9rem;">🔍 Search Term Finder</p>
        <p style="margin: 0.25rem 0 0 0; color: rgba(255, 255, 255, 0.9); font-size: 0.8rem;">Find and navigate to a specific search term in the pivot table</p>
    </div>
""", unsafe_allow_html=True)

    col_search_input, col_search_btn, col_clear_btn = st.columns([4, 1, 1])

    with col_search_input:
        search_term_input = st.text_input(
            "Search term (exact match)",
            value=st.session_state.search_term_input,
            key='search_term_input_field',
            placeholder="Enter exact search term..."
        )

    with col_search_btn:
        st.write("")  # Spacing
        if st.button("🔍 Find", key='search_btn', use_container_width=True):
            if search_term_input.strip():
                # Perform search
                with st.spinner('Searching for search term...'):
                    paths = dp.find_search_term_dimension_paths(
                        client=st.session_state.bq_client,
                        dataset_table=st.session_state.bq_params['table'],
                        search_term=search_term_input.strip(),
                        base_filters=base_filters,
                        dimension_keys=selected_dimension_keys
                    )

                    if paths:
                        # Found the search term - save to session state
                        st.session_state.search_term_input = search_term_input.strip()
                        st.session_state.search_term_active = search_term_input.strip()
                        st.session_state.search_term_path = paths[0]  # Use first match

                        # Clear existing expansions and set up new one
                        st.session_state.pivot_expanded = {}

                        # Build expansion path through hierarchy
                        # We'll expand each dimension level leading to the search term
                        for i, dim_key in enumerate(selected_dimension_keys):
                            if dim_key in paths[0]:
                                # Build row_id for this level using format: {dimension_key}_{dimension_value}
                                dimension_value = paths[0][dim_key]
                                row_id = f"{dim_key}_{dimension_value}"

                                # Determine expansion type and calculate correct page
                                if i == len(selected_dimension_keys) - 1:
                                    # Last dimension - expand to search terms
                                    # Need to calculate which page the search term is on

                                    # Build dimension filters up to this point
                                    dimension_filters = {}
                                    for j in range(i + 1):
                                        dimension_filters[selected_dimension_keys[j]] = paths[0][selected_dimension_keys[j]]

                                    # Fetch search terms to find page number
                                    drill_query = dp.build_drill_down_query(
                                        st.session_state.bq_params['table'],
                                        base_filters,
                                        dimension_filters,
                                        None
                                    )
                                    search_terms = dp.execute_cached_query(st.session_state.bq_client, drill_query, ttl=300)

                                    # Find the index of the search term
                                    page_size = 5
                                    search_term_idx = 0
                                    if not search_terms.empty and 'search_term' in search_terms.columns:
                                        search_term_positions = search_terms[search_terms['search_term'] == search_term_input.strip()].index
                                        if len(search_term_positions) > 0:
                                            search_term_idx = search_term_positions[0]

                                    # Calculate page number
                                    correct_page = search_term_idx // page_size

                                    st.session_state.pivot_expanded[row_id] = {'type': 'search', 'page': correct_page}
                                else:
                                    # Intermediate dimension - expand to next dimension
                                    st.session_state.pivot_expanded[row_id] = {'type': 'dimension', 'page': 0}

                        st.rerun()
                    else:
                        st.warning(f"Search term '{search_term_input.strip()}' not found in the current data.")
                        st.session_state.search_term_active = None
                        st.session_state.search_term_path = None

    with col_clear_btn:
        st.write("")  # Spacing
        if st.button("✕ Clear", key='clear_search_btn', use_container_width=True):
            st.session_state.search_term_input = ''
            st.session_state.search_term_active = None
            st.session_state.search_term_path = None
            st.session_state.pivot_expanded = {}
            st.rerun()

    # Display search status
    if st.session_state.search_term_active:
        path_display = " → ".join([f"{dim_key}: {st.session_state.search_term_path[dim_key]}"
                                   for dim_key in selected_dimension_keys
                                   if dim_key in st.session_state.search_term_path])
        st.success(f"✓ Found '{st.session_state.search_term_active}' at: {path_display}")

    # Current level is always 0 for main table (no navigation)
    current_level = 0

    # Date range visualization - show which dates are included/excluded
    total_days = (max_date - min_date).days + 1
    date_segments = []

    # Create list of all dates with their status (included or excluded)
    current_date = pd.to_datetime(min_date)
    max_date_pd = pd.to_datetime(max_date)

    while current_date <= max_date_pd:
        is_included = False

        # Check if date is in the main range (when not using advanced mode)
        if not use_advanced and start_date <= current_date <= end_date:
            is_included = True

        # Check additional date ranges (when using advanced mode)
        if use_advanced and st.session_state.additional_date_ranges:
            for date_range in st.session_state.additional_date_ranges:
                range_start = pd.to_datetime(date_range[0])
                range_end = pd.to_datetime(date_range[1])
                if range_start <= current_date <= range_end:
                    is_included = True
                    break

        # Check excluded date ranges (always applies)
        if is_included and st.session_state.excluded_date_ranges:
            for date_range in st.session_state.excluded_date_ranges:
                range_start = pd.to_datetime(date_range[0])
                range_end = pd.to_datetime(date_range[1])
                if range_start <= current_date <= range_end:
                    is_included = False
                    break

        date_segments.append({
            'date': current_date,
            'included': is_included
        })
        current_date += pd.Timedelta(days=1)

    # Group consecutive dates with same status into segments for efficient rendering
    visual_segments = []
    if date_segments:
        current_segment = {'start': date_segments[0]['date'], 'included': date_segments[0]['included'], 'count': 1}

        for segment in date_segments[1:]:
            if segment['included'] == current_segment['included']:
                current_segment['count'] += 1
            else:
                visual_segments.append(current_segment)
                current_segment = {'start': segment['date'], 'included': segment['included'], 'count': 1}
        visual_segments.append(current_segment)

    # Create HTML segments
    segment_html = ""
    for seg in visual_segments:
        width_pct = (seg['count'] / total_days) * 100
        color = "#10b981" if seg['included'] else "#d1d5db"  # green-500 : gray-300
        segment_html += f'<div style="width: {width_pct}%; background-color: {color}; height: 100%;"></div>'

    # Render date range visualization
    st.markdown(f"""
    <div style="margin-bottom: 1rem;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
            <span style="font-size: 0.875rem; font-weight: 600; color: #374151;">Date Range Coverage</span>
            <span style="font-size: 0.75rem; color: #6b7280;">{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}</span>
        </div>
        <div style="display: flex; width: 100%; height: 24px; border-radius: 4px; overflow: hidden; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);">
            {segment_html}
        </div>
        <div style="display: flex; justify-content: space-between; margin-top: 0.5rem; font-size: 0.75rem; color: #6b7280;">
            <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #10b981; border-radius: 2px; margin-right: 4px; vertical-align: middle;"></span>Included</span>
            <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #d1d5db; border-radius: 2px; margin-right: 4px; vertical-align: middle;"></span>Excluded</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

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
                pivot_data['pdp_conversion'] = np.where(pivot_data['queries_pdp'] > 0,
                                                     pivot_data['purchases'] / pivot_data['queries_pdp'], 0)
                pivot_data['revenue_per_query'] = np.where(pivot_data['queries'] > 0,
                                                           pivot_data['gross_purchase'] / pivot_data['queries'], 0)
                pivot_data['avg_order_value'] = np.where(pivot_data['purchases'] > 0,
                                                         pivot_data['gross_purchase'] / pivot_data['purchases'], 0)

                # Calculate 1p/3p percentages if columns are present
                if 'products_pdp_1p' in pivot_data.columns and 'products_pdp_3p' in pivot_data.columns:
                    total_clicks_1p_3p = pivot_data['products_pdp_1p'] + pivot_data['products_pdp_3p']
                    pivot_data['pct_clicks_1p'] = np.where(total_clicks_1p_3p > 0,
                                                           (pivot_data['products_pdp_1p'] / total_clicks_1p_3p * 100), 0)
                    pivot_data['pct_clicks_3p'] = np.where(total_clicks_1p_3p > 0,
                                                           (pivot_data['products_pdp_3p'] / total_clicks_1p_3p * 100), 0)

                if 'products_a2c_1p' in pivot_data.columns and 'products_a2c_3p' in pivot_data.columns:
                    total_a2c_1p_3p = pivot_data['products_a2c_1p'] + pivot_data['products_a2c_3p']
                    pivot_data['pct_a2c_1p'] = np.where(total_a2c_1p_3p > 0,
                                                        (pivot_data['products_a2c_1p'] / total_a2c_1p_3p * 100), 0)
                    pivot_data['pct_a2c_3p'] = np.where(total_a2c_1p_3p > 0,
                                                        (pivot_data['products_a2c_3p'] / total_a2c_1p_3p * 100), 0)

                if 'purchases_1p' in pivot_data.columns and 'purchases_3p' in pivot_data.columns:
                    total_purchases_1p_3p = pivot_data['purchases_1p'] + pivot_data['purchases_3p']
                    pivot_data['pct_purchases_1p'] = np.where(total_purchases_1p_3p > 0,
                                                              (pivot_data['purchases_1p'] / total_purchases_1p_3p * 100), 0)
                    pivot_data['pct_purchases_3p'] = np.where(total_purchases_1p_3p > 0,
                                                              (pivot_data['purchases_3p'] / total_purchases_1p_3p * 100), 0)

                if 'gross_purchase_1p' in pivot_data.columns and 'purchases_1p' in pivot_data.columns:
                    pivot_data['aov_1p'] = np.where(pivot_data['purchases_1p'] > 0,
                                                    (pivot_data['gross_purchase_1p'] / pivot_data['purchases_1p']), 0)

                if 'gross_purchase_3p' in pivot_data.columns and 'purchases_3p' in pivot_data.columns:
                    pivot_data['aov_3p'] = np.where(pivot_data['purchases_3p'] > 0,
                                                    (pivot_data['gross_purchase_3p'] / pivot_data['purchases_3p']), 0)

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

                # Calculate search terms for top X% only if column is visible (batch optimization)
                show_concentration_calc = ('search_terms_for_top_x_pct' in st.session_state.visible_columns)
                if show_concentration_calc:
                    dimension_values_list = pivot_data['dimension_value'].tolist()

                    # Check if there's an "Other" row and get its constituent values
                    other_values_dict = None
                    if 'Other' in dimension_values_list:
                        # Get the values that are NOT in the explicit list (i.e., the "Other" values)
                        explicit_values = [v for v in dimension_values_list if v != 'Other']
                        other_values = dp.get_other_dimension_values(
                            st.session_state.bq_client,
                            st.session_state.bq_params['table'],
                            base_filters,
                            current_dimension_key,
                            explicit_values
                        )
                        if other_values:
                            other_values_dict = {'Other': other_values}

                    concentration_dict = dp.calculate_batch_search_terms_concentration(
                        st.session_state.bq_client,
                        st.session_state.bq_params['table'],
                        base_filters,
                        current_dimension_key,
                        dimension_values_list,
                        search_term_concentration_pct,
                        other_values_dict
                    )
                    # Map the results to the dataframe
                    pivot_data['search_terms_for_top_x_pct'] = pivot_data['dimension_value'].map(
                        lambda val: concentration_dict.get(val, 0)
                    )
                else:
                    # Set to 0 if column not visible (won't be displayed anyway)
                    pivot_data['search_terms_for_top_x_pct'] = 0

                # Display level header
                st.subheader(f"📊 Level {current_level + 1}: {current_dimension_name}")

                if not pivot_data.empty:
                    # Format data for display with all dimension columns
                    display_data = pivot_data.copy()

                    # Add row tracking column (hidden from display but used for selection)
                    display_data['_row_id'] = display_data['dimension_value'].apply(lambda x: f"{current_dimension_key}_{x}")
                    display_data['_level'] = current_level
                    display_data['_type'] = 'dimension'

                    # Add columns for all dimension levels + search term
                    for i, dim_name in enumerate(selected_dimension_names):
                        if i == current_level:
                            # Current level - populate with data and add expansion indicator (except for Total)
                            display_data[dim_name] = display_data['dimension_value'].apply(
                                lambda x: str(x) if x == 'Total' else '▶ ' + str(x)
                            )
                        else:
                            # Other levels - empty for now
                            display_data[dim_name] = ''

                    # Add empty search term column
                    display_data['Search Term'] = ''

                    # Rename metric columns
                    display_data = display_data.rename(columns={
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
                        'avg_order_value': 'avg_order_value',
                        'pct_clicks_1p': 'pct_clicks_1p',
                        'pct_clicks_3p': 'pct_clicks_3p',
                        'pct_a2c_1p': 'pct_a2c_1p',
                        'pct_a2c_3p': 'pct_a2c_3p',
                        'pct_purchases_1p': 'pct_purchases_1p',
                        'pct_purchases_3p': 'pct_purchases_3p',
                        'aov_1p': 'aov_1p',
                        'aov_3p': 'aov_3p'
                    })

                    # Format columns for display using dynamic formatters
                    pct_decimals = st.session_state.get('percentage_decimals', 2)
                    display_formatted = display_data.copy()
                    display_formatted['queries'] = display_formatted['queries'].apply(fmt_whole_number)
                    if 'search_term_count' in display_formatted.columns:
                        display_formatted['search_term_count'] = display_formatted['search_term_count'].apply(fmt_whole_number)
                    if 'average_queries_per_day' in display_formatted.columns:
                        display_formatted['average_queries_per_day'] = display_formatted['average_queries_per_day'].apply(fmt_decimal)
                    if 'average_queries_per_search_term_per_day' in display_formatted.columns:
                        display_formatted['average_queries_per_search_term_per_day'] = display_formatted['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                    if 'search_terms_for_top_x_pct' in display_formatted.columns:
                        display_formatted['search_terms_for_top_x_pct'] = display_formatted['search_terms_for_top_x_pct'].apply(fmt_whole_number)
                    display_formatted['% of queries'] = display_formatted['% of queries'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    display_formatted['cumulative %'] = display_formatted['cumulative %'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    display_formatted['ctr'] = display_formatted['ctr'].apply(fmt_percentage)
                    display_formatted['a2c_rate'] = display_formatted['a2c_rate'].apply(fmt_percentage)
                    display_formatted['conversion_rate'] = display_formatted['conversion_rate'].apply(fmt_percentage)
                    if 'pdp_conversion' in display_formatted.columns:
                        display_formatted['pdp_conversion'] = display_formatted['pdp_conversion'].apply(fmt_percentage)
                    display_formatted['purchases'] = display_formatted['purchases'].apply(fmt_whole_number)
                    display_formatted['revenue_per_query'] = display_formatted['revenue_per_query'].apply(fmt_currency)
                    display_formatted['gross_purchase'] = display_formatted['gross_purchase'].apply(fmt_currency)
                    display_formatted['avg_order_value'] = display_formatted['avg_order_value'].apply(fmt_currency)

                    # Format 1p/3p percentage columns
                    if 'pct_clicks_1p' in display_formatted.columns:
                        display_formatted['pct_clicks_1p'] = display_formatted['pct_clicks_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    if 'pct_clicks_3p' in display_formatted.columns:
                        display_formatted['pct_clicks_3p'] = display_formatted['pct_clicks_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    if 'pct_a2c_1p' in display_formatted.columns:
                        display_formatted['pct_a2c_1p'] = display_formatted['pct_a2c_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    if 'pct_a2c_3p' in display_formatted.columns:
                        display_formatted['pct_a2c_3p'] = display_formatted['pct_a2c_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    if 'pct_purchases_1p' in display_formatted.columns:
                        display_formatted['pct_purchases_1p'] = display_formatted['pct_purchases_1p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")
                    if 'pct_purchases_3p' in display_formatted.columns:
                        display_formatted['pct_purchases_3p'] = display_formatted['pct_purchases_3p'].apply(lambda x: f"{x:.{pct_decimals}f}%" if pd.notna(x) else "")

                    # Format 1p/3p AOV columns
                    if 'aov_1p' in display_formatted.columns:
                        display_formatted['aov_1p'] = display_formatted['aov_1p'].apply(fmt_currency)
                    if 'aov_3p' in display_formatted.columns:
                        display_formatted['aov_3p'] = display_formatted['aov_3p'].apply(fmt_currency)

                    # Filter to only show selected columns + all dimension columns
                    # Dimension columns are always shown, tracking columns are hidden
                    dimension_cols = selected_dimension_names + ['Search Term']
                    tracking_cols = ['_row_id', '_level', '_type']

                    if len(st.session_state.visible_columns) == 0:
                        # [] = show only dimension columns
                        columns_to_show = [col for col in dimension_cols if col in display_formatted.columns]
                    else:
                        # Specific columns selected + dimension columns, exclude tracking
                        columns_to_show = dimension_cols + [col for col in st.session_state.visible_columns if col in display_formatted.columns and col not in dimension_cols]
                        columns_to_show = [col for col in columns_to_show if col not in tracking_cols]

                    # Remove duplicates while preserving order
                    seen = set()
                    columns_to_show = [col for col in columns_to_show if col in display_formatted.columns and not (col in seen or seen.add(col))]

                    # Create display version (without tracking columns) and keep full version for selection handling
                    display_formatted_full = display_formatted.copy()  # Keep tracking columns for selection

                    # Add drill-down indicator column
                    drill_indicators = []
                    for idx in range(len(pivot_data)):
                        base_value = pivot_data.iloc[idx]['dimension_value']
                        base_row_id = f"{current_dimension_key}_{base_value}"
                        is_total_row = base_value == 'Total'
                        is_expanded = base_row_id in st.session_state.pivot_expanded

                        if is_total_row:
                            drill_indicators.append("")  # No indicator for Total rows
                        elif is_expanded:
                            drill_indicators.append("🔽")
                        else:
                            drill_indicators.append("▶️")

                    display_formatted_full['🔍'] = drill_indicators

                    # Add drill-down column as first column in display
                    display_formatted = display_formatted[columns_to_show]  # Display without tracking
                    display_formatted.insert(0, '🔍', drill_indicators)

                    # Check if we have any base-level expansions
                    # Find ALL base rows that are expanded
                    expanded_base_rows = []
                    for idx in range(len(display_formatted)):
                        base_value = pivot_data.iloc[idx]['dimension_value']
                        base_row_id = f"{current_dimension_key}_{base_value}"
                        if base_row_id in st.session_state.pivot_expanded:
                            expanded_base_rows.append((idx, base_value, base_row_id))

                    # For now, support only first expansion (single expansion mode)
                    if len(expanded_base_rows) > 0:
                        expanded_row_idx, selected_value, row_id = expanded_base_rows[0]
                    else:
                        expanded_row_idx = None

                    if expanded_row_idx is not None:
                        expansion_info = st.session_state.pivot_expanded[row_id]

                        # Build combined dataframe with expanded rows inline
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
                                search_terms['pdp_conversion'] = np.where(search_terms['queries_pdp'] > 0,
                                                                       search_terms['purchases'] / search_terms['queries_pdp'], 0)
                                search_terms['revenue_per_query'] = np.where(search_terms['queries'] > 0,
                                                                             search_terms['gross_purchase'] / search_terms['queries'], 0)
                                search_terms['avg_order_value'] = np.where(search_terms['purchases'] > 0,
                                                                           search_terms['gross_purchase'] / search_terms['purchases'], 0)
                                search_terms['average_queries_per_day'] = search_terms['queries'] / num_days

                                # Calculate 1p/3p percentage metrics
                                total_clicks_1p_3p = search_terms['products_pdp_1p'] + search_terms['products_pdp_3p']
                                search_terms['pct_clicks_1p'] = np.where(total_clicks_1p_3p > 0,
                                                                         (search_terms['products_pdp_1p'] / total_clicks_1p_3p * 100), 0)
                                search_terms['pct_clicks_3p'] = np.where(total_clicks_1p_3p > 0,
                                                                         (search_terms['products_pdp_3p'] / total_clicks_1p_3p * 100), 0)

                                total_a2c_1p_3p = search_terms['products_a2c_1p'] + search_terms['products_a2c_3p']
                                search_terms['pct_a2c_1p'] = np.where(total_a2c_1p_3p > 0,
                                                                      (search_terms['products_a2c_1p'] / total_a2c_1p_3p * 100), 0)
                                search_terms['pct_a2c_3p'] = np.where(total_a2c_1p_3p > 0,
                                                                      (search_terms['products_a2c_3p'] / total_a2c_1p_3p * 100), 0)

                                total_purchases_1p_3p = search_terms['purchases_1p'] + search_terms['purchases_3p']
                                search_terms['pct_purchases_1p'] = np.where(total_purchases_1p_3p > 0,
                                                                            (search_terms['purchases_1p'] / total_purchases_1p_3p * 100), 0)
                                search_terms['pct_purchases_3p'] = np.where(total_purchases_1p_3p > 0,
                                                                            (search_terms['purchases_3p'] / total_purchases_1p_3p * 100), 0)

                                search_terms['aov_1p'] = np.where(search_terms['purchases_1p'] > 0,
                                                                  (search_terms['gross_purchase_1p'] / search_terms['purchases_1p']), 0)
                                search_terms['aov_3p'] = np.where(search_terms['purchases_3p'] > 0,
                                                                  (search_terms['gross_purchase_3p'] / search_terms['purchases_3p']), 0)

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

                                # Format search terms with separate columns
                                search_display = search_page.copy()

                                # Add row tracking columns (search terms aren't expandable, use unique ID)
                                search_display['_row_id'] = search_display['search_term'].apply(lambda x: f"search_{current_dimension_key}_{selected_value}_{x}")
                                search_display['_level'] = -1  # Special level for search terms
                                search_display['_type'] = 'search'

                                # Add columns for all dimension levels - all empty except Search Term
                                for dim_name in selected_dimension_names:
                                    search_display[dim_name] = ''

                                # Populate Search Term column with highlighting for matched search
                                if st.session_state.search_term_active:
                                    search_display['Search Term'] = search_display['search_term'].apply(
                                        lambda x: f"⭐ {x}" if str(x) == st.session_state.search_term_active else str(x)
                                    )
                                else:
                                    search_display['Search Term'] = search_display['search_term'].astype(str)

                                # Format numbers exactly like main table using dynamic formatters
                                pct_decimals = st.session_state.get('percentage_decimals', 2)
                                search_display['queries'] = search_display['queries'].apply(fmt_whole_number)
                                search_display['% of queries'] = search_display['pct_of_total'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                search_display['cumulative %'] = search_display['cumulative_pct'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                if 'average_queries_per_day' in search_display.columns:
                                    search_display['average_queries_per_day'] = search_display['average_queries_per_day'].apply(fmt_decimal)
                                if 'average_queries_per_search_term_per_day' in search_display.columns:
                                    search_display['average_queries_per_search_term_per_day'] = search_display['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                                search_display['ctr'] = search_display['ctr'].apply(fmt_percentage)
                                search_display['a2c_rate'] = search_display['a2c_rate'].apply(fmt_percentage)
                                search_display['conversion_rate'] = search_display['conversion_rate'].apply(fmt_percentage)
                                if 'pdp_conversion' in search_display.columns:
                                    search_display['pdp_conversion'] = search_display['pdp_conversion'].apply(fmt_percentage)
                                search_display['revenue_per_query'] = search_display['revenue_per_query'].apply(fmt_currency)
                                search_display['purchases'] = search_display['purchases'].apply(fmt_whole_number)
                                search_display['gross_purchase'] = search_display['gross_purchase'].apply(fmt_currency)
                                search_display['avg_order_value'] = search_display['avg_order_value'].apply(fmt_currency)

                                # Match exact columns from main table in same order
                                main_cols = display_formatted.columns.tolist()
                                # Ensure search_display has all columns (add missing ones with empty strings)
                                for col in main_cols:
                                    if col not in search_display.columns:
                                        search_display[col] = ''

                                # Save full version with tracking columns BEFORE filtering
                                search_display_full = search_display.copy()

                                # Reorder to match main table exactly (removes tracking columns)
                                search_display = search_display[main_cols]

                                # Change parent row's ▶ to ▼ to show it's expanded
                                parent_row = display_formatted.iloc[expanded_row_idx].copy()
                                parent_row[current_dimension_name] = parent_row[current_dimension_name].replace('▶ ', '▼ ')

                                # Also build full version with tracking columns for selection handling
                                parent_row_full = display_formatted_full.iloc[expanded_row_idx].copy()
                                parent_row_full[current_dimension_name] = parent_row_full[current_dimension_name].replace('▶ ', '▼ ')

                                # Build combined table with modified parent row
                                if expanded_row_idx > 0:
                                    combined_table = pd.concat([
                                        display_formatted.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row]),
                                        search_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        display_formatted_full.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row_full]),
                                        search_display_full,  # Use full version with tracking columns
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                else:
                                    combined_table = pd.concat([
                                        pd.DataFrame([parent_row]),
                                        search_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        pd.DataFrame([parent_row_full]),
                                        search_display_full,  # Use full version with tracking columns
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)

                                # Store full version for selection handling
                                st.session_state.pivot_combined_full = combined_table_full

                                # Display combined table with indicators inside
                                st.dataframe(
                                    combined_table,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=600,
                                    on_select="rerun",
                                    selection_mode="multi-row"
                                )

                                # Pagination controls
                                st.markdown(f"**Page {current_page + 1} of {total_pages}** (showing {start_idx + 1}-{end_idx} of {len(search_terms)} search terms)")
                                col_prev, col_next = st.columns(2)
                                with col_prev:
                                    if current_page > 0:
                                        if st.button("← Previous", key='prev_page', use_container_width=True):
                                            if row_id in st.session_state.pivot_expanded:
                                                st.session_state.pivot_expanded[row_id]['page'] = current_page - 1
                                                st.rerun()
                                with col_next:
                                    if current_page < total_pages - 1:
                                        if st.button("Next →", key='next_page', use_container_width=True):
                                            if row_id in st.session_state.pivot_expanded:
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
                        elif expansion_info['type'] == 'dimension':
                            # Dimension expansion - query next dimension level
                            next_level = current_level + 1

                            # Check if next level is valid
                            if next_level >= len(selected_dimension_keys):
                                st.warning(f"Invalid expansion detected. Clearing and reloading...")
                                # Clear invalid expansion from state
                                del st.session_state.pivot_expanded[row_id]
                                if row_id == st.session_state.pivot_selected_row:
                                    st.session_state.pivot_selected_row = None
                                st.rerun()

                            next_dimension_key = selected_dimension_keys[next_level]
                            next_dimension_name = selected_dimension_names[next_level]

                            # Query next dimension filtered by parent
                            next_sort_config = sort_configs[next_level] if next_level < len(sort_configs) else {'sort_by': 'queries', 'ascending': False}

                            next_query = dp.build_dimension_level_query(
                                st.session_state.bq_params['table'],
                                base_filters,
                                next_dimension_key,
                                parent_filters={current_dimension_key: selected_value},
                                cumulative_threshold=cumulative_threshold / 100.0,
                                sort_by=next_sort_config['sort_by'],
                                ascending=next_sort_config['ascending'],
                                volume_threshold=volume_threshold,
                                daily_volume_threshold=daily_volume_threshold
                            )
                            dimension_data = dp.execute_cached_query(st.session_state.bq_client, next_query, ttl=300)

                            if not dimension_data.empty:
                                # Calculate derived metrics
                                dimension_data['ctr'] = np.where(dimension_data['queries'] > 0,
                                                                 dimension_data['queries_pdp'] / dimension_data['queries'], 0)
                                dimension_data['a2c_rate'] = np.where(dimension_data['queries'] > 0,
                                                                      dimension_data['queries_a2c'] / dimension_data['queries'], 0)
                                dimension_data['conversion_rate'] = np.where(dimension_data['queries'] > 0,
                                                                             dimension_data['purchases'] / dimension_data['queries'], 0)
                                dimension_data['pdp_conversion'] = np.where(dimension_data['queries_pdp'] > 0,
                                                                         dimension_data['purchases'] / dimension_data['queries_pdp'], 0)
                                dimension_data['revenue_per_query'] = np.where(dimension_data['queries'] > 0,
                                                                               dimension_data['gross_purchase'] / dimension_data['queries'], 0)
                                dimension_data['avg_order_value'] = np.where(dimension_data['purchases'] > 0,
                                                                             dimension_data['gross_purchase'] / dimension_data['purchases'], 0)
                                if 'average_queries_per_day' not in dimension_data.columns:
                                    dimension_data['average_queries_per_day'] = dimension_data['queries'] / num_days
                                if 'average_queries_per_search_term_per_day' not in dimension_data.columns:
                                    if 'search_term_count' in dimension_data.columns:
                                        dimension_data['average_queries_per_search_term_per_day'] = dimension_data['queries'] / num_days / dimension_data['search_term_count']
                                    else:
                                        dimension_data['average_queries_per_search_term_per_day'] = 0

                                # Calculate percentages
                                dimension_data['pct_of_total'] = (dimension_data['queries'] / total_queries_all * 100) if total_queries_all > 0 else 0
                                total_queries_dim = dimension_data['queries'].sum()
                                dimension_data['cumulative_queries'] = dimension_data['queries'].cumsum()
                                dimension_data['cumulative_pct'] = (dimension_data['cumulative_queries'] / total_queries_dim * 100) if total_queries_dim > 0 else 0

                                # Paginate
                                page_size = 5
                                current_page = expansion_info['page']
                                total_pages = (len(dimension_data) + page_size - 1) // page_size
                                start_idx = current_page * page_size
                                end_idx = min(start_idx + page_size, len(dimension_data))
                                dimension_page = dimension_data.iloc[start_idx:end_idx].copy()

                                # Format dimension data for display with separate columns
                                dimension_display = dimension_page.copy()

                                # Add row tracking columns
                                dimension_display['_row_id'] = dimension_display['dimension_value'].apply(lambda x: f"{next_dimension_key}_{x}")
                                dimension_display['_level'] = next_level
                                dimension_display['_type'] = 'dimension'

                                # Add columns for all dimension levels
                                for i, dim_name in enumerate(selected_dimension_names):
                                    if i < current_level:
                                        # Parent dimensions - leave empty
                                        dimension_display[dim_name] = ''
                                    elif i == next_level:
                                        # Current child dimension - populate with data and add indicator
                                        # Check if this can be expanded further (but not for Total)
                                        if next_level < len(selected_dimension_keys) - 1:
                                            dimension_display[dim_name] = dimension_display['dimension_value'].apply(
                                                lambda x: str(x) if x == 'Total' else '▶ ' + str(x)
                                            )
                                        else:
                                            dimension_display[dim_name] = dimension_display['dimension_value'].astype(str)
                                    else:
                                        # Other dimensions - empty
                                        dimension_display[dim_name] = ''

                                # Add empty search term column
                                dimension_display['Search Term'] = ''

                                # Format numbers using dynamic formatters
                                pct_decimals = st.session_state.get('percentage_decimals', 2)
                                dimension_display['queries'] = dimension_display['queries'].apply(fmt_whole_number)
                                if 'search_term_count' in dimension_display.columns:
                                    dimension_display['search_term_count'] = dimension_display['search_term_count'].apply(fmt_whole_number)
                                if 'average_queries_per_day' in dimension_display.columns:
                                    dimension_display['average_queries_per_day'] = dimension_display['average_queries_per_day'].apply(fmt_decimal)
                                if 'average_queries_per_search_term_per_day' in dimension_display.columns:
                                    dimension_display['average_queries_per_search_term_per_day'] = dimension_display['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                                dimension_display['% of queries'] = dimension_display['pct_of_total'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                dimension_display['cumulative %'] = dimension_display['cumulative_pct'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                dimension_display['ctr'] = dimension_display['ctr'].apply(fmt_percentage)
                                dimension_display['a2c_rate'] = dimension_display['a2c_rate'].apply(fmt_percentage)
                                dimension_display['conversion_rate'] = dimension_display['conversion_rate'].apply(fmt_percentage)
                                if 'pdp_conversion' in dimension_display.columns:
                                    dimension_display['pdp_conversion'] = dimension_display['pdp_conversion'].apply(fmt_percentage)
                                dimension_display['revenue_per_query'] = dimension_display['revenue_per_query'].apply(fmt_currency)
                                dimension_display['purchases'] = dimension_display['purchases'].apply(fmt_whole_number)
                                dimension_display['gross_purchase'] = dimension_display['gross_purchase'].apply(fmt_currency)
                                dimension_display['avg_order_value'] = dimension_display['avg_order_value'].apply(fmt_currency)

                                # Match columns from main table
                                main_cols = display_formatted.columns.tolist()
                                for col in main_cols:
                                    if col not in dimension_display.columns:
                                        dimension_display[col] = ''

                                # Save full version with tracking columns BEFORE filtering
                                dimension_display_full = dimension_display.copy()

                                # Now filter for display (remove tracking columns)
                                dimension_display = dimension_display[main_cols]

                                # Change parent row's ▶ to ▼ to show it's expanded
                                parent_row = display_formatted.iloc[expanded_row_idx].copy()
                                parent_row[current_dimension_name] = parent_row[current_dimension_name].replace('▶ ', '▼ ')

                                # Also build full version with tracking columns for selection handling
                                parent_row_full = display_formatted_full.iloc[expanded_row_idx].copy()
                                parent_row_full[current_dimension_name] = parent_row_full[current_dimension_name].replace('▶ ', '▼ ')

                                # Build combined table with modified parent row
                                if expanded_row_idx > 0:
                                    combined_table = pd.concat([
                                        display_formatted.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row]),
                                        dimension_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        display_formatted_full.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row_full]),
                                        dimension_display_full,  # Use full version with tracking columns
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                else:
                                    combined_table = pd.concat([
                                        pd.DataFrame([parent_row]),
                                        dimension_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        pd.DataFrame([parent_row_full]),
                                        dimension_display_full,  # Use full version with tracking columns
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)

                                # Check if any of the child dimension rows are also expanded (nested expansion)
                                # Look for any row_ids in dimension_display_full that are in pivot_expanded
                                nested_expansion_idx = None
                                nested_expansion_row_id = None
                                for child_idx in range(len(dimension_display_full)):
                                    child_row_id = dimension_display_full.iloc[child_idx]['_row_id']
                                    if child_row_id in st.session_state.pivot_expanded:
                                        nested_expansion_idx = child_idx
                                        nested_expansion_row_id = child_row_id
                                        break  # For now, support only one nested expansion

                                if nested_expansion_idx is not None:
                                    # Implement full nested expansion building
                                    nested_expansion_info = st.session_state.pivot_expanded[nested_expansion_row_id]

                                    # Parse the nested row_id to extract dimension info
                                    # Format: {dimension_key}_{dimension_value}
                                    nested_row = dimension_display_full.iloc[nested_expansion_idx]
                                    child_dimension_value = nested_row['dimension_value']
                                    child_level = int(nested_row['_level'])
                                    child_dimension_key = selected_dimension_keys[child_level]

                                    # Skip expansion for "Other" rows (they aggregate multiple values and cannot be drilled down)
                                    if child_dimension_value != "Other":
                                        # Build accumulated parent filters (both original parent and child)
                                        accumulated_filters = {
                                            current_dimension_key: selected_value,
                                            child_dimension_key: child_dimension_value
                                        }

                                        # Determine what to expand (next dimension or search terms)
                                        if child_level < len(selected_dimension_keys) - 1:
                                            # Expand to next dimension level
                                            grandchild_level = child_level + 1
                                            grandchild_dimension_key = selected_dimension_keys[grandchild_level]
                                            grandchild_dimension_name = selected_dimension_names[grandchild_level]

                                            # Query for next dimension level
                                            grandchild_sort_config = sort_configs[grandchild_level] if grandchild_level < len(sort_configs) else {'sort_by': 'queries', 'ascending': False}

                                            grandchild_query = dp.build_dimension_level_query(
                                                st.session_state.bq_params['table'],
                                                base_filters,
                                                grandchild_dimension_key,
                                                parent_filters=accumulated_filters,
                                                cumulative_threshold=cumulative_threshold / 100.0,
                                                sort_by=grandchild_sort_config['sort_by'],
                                                ascending=grandchild_sort_config['ascending'],
                                                volume_threshold=volume_threshold,
                                                daily_volume_threshold=daily_volume_threshold
                                            )
                                            grandchild_data = dp.execute_cached_query(st.session_state.bq_client, grandchild_query, ttl=300)

                                            if not grandchild_data.empty:
                                                # Calculate derived metrics
                                                grandchild_data['ctr'] = np.where(grandchild_data['queries'] > 0,
                                                                                 grandchild_data['queries_pdp'] / grandchild_data['queries'], 0)
                                                grandchild_data['a2c_rate'] = np.where(grandchild_data['queries'] > 0,
                                                                                      grandchild_data['queries_a2c'] / grandchild_data['queries'], 0)
                                                grandchild_data['conversion_rate'] = np.where(grandchild_data['queries'] > 0,
                                                                                             grandchild_data['purchases'] / grandchild_data['queries'], 0)
                                                grandchild_data['pdp_conversion'] = np.where(grandchild_data['queries_pdp'] > 0,
                                                                                         grandchild_data['purchases'] / grandchild_data['queries_pdp'], 0)
                                                grandchild_data['revenue_per_query'] = np.where(grandchild_data['queries'] > 0,
                                                                                               grandchild_data['gross_purchase'] / grandchild_data['queries'], 0)
                                                grandchild_data['avg_order_value'] = np.where(grandchild_data['purchases'] > 0,
                                                                                             grandchild_data['gross_purchase'] / grandchild_data['purchases'], 0)
                                                if 'average_queries_per_day' not in grandchild_data.columns:
                                                    grandchild_data['average_queries_per_day'] = grandchild_data['queries'] / num_days
                                                if 'average_queries_per_search_term_per_day' not in grandchild_data.columns:
                                                    if 'search_term_count' in grandchild_data.columns:
                                                        grandchild_data['average_queries_per_search_term_per_day'] = grandchild_data['queries'] / num_days / grandchild_data['search_term_count']
                                                    else:
                                                        grandchild_data['average_queries_per_search_term_per_day'] = 0

                                                # Calculate percentages
                                                grandchild_data['pct_of_total'] = (grandchild_data['queries'] / total_queries_all * 100) if total_queries_all > 0 else 0
                                                total_queries_grandchild = grandchild_data['queries'].sum()
                                                grandchild_data['cumulative_queries'] = grandchild_data['queries'].cumsum()
                                                grandchild_data['cumulative_pct'] = (grandchild_data['cumulative_queries'] / total_queries_grandchild * 100) if total_queries_grandchild > 0 else 0

                                                # Paginate
                                                grandchild_page_size = 5
                                                grandchild_current_page = nested_expansion_info['page']
                                                grandchild_total_pages = (len(grandchild_data) + grandchild_page_size - 1) // grandchild_page_size
                                                grandchild_start_idx = grandchild_current_page * grandchild_page_size
                                                grandchild_end_idx = min(grandchild_start_idx + grandchild_page_size, len(grandchild_data))
                                                grandchild_page = grandchild_data.iloc[grandchild_start_idx:grandchild_end_idx].copy()

                                                # Format grandchild dimension data
                                                grandchild_display = grandchild_page.copy()

                                                # Add row tracking columns
                                                grandchild_display['_row_id'] = grandchild_display['dimension_value'].apply(lambda x: f"{grandchild_dimension_key}_{x}")
                                                grandchild_display['_level'] = grandchild_level
                                                grandchild_display['_type'] = 'dimension'

                                                # Add columns for all dimension levels
                                                for i, dim_name in enumerate(selected_dimension_names):
                                                    if i < child_level:
                                                        grandchild_display[dim_name] = ''
                                                    elif i == grandchild_level:
                                                        # Grandchild dimension - check if expandable
                                                        if grandchild_level < len(selected_dimension_keys) - 1:
                                                            grandchild_display[dim_name] = '▶ ' + grandchild_display['dimension_value'].astype(str)
                                                        else:
                                                            grandchild_display[dim_name] = grandchild_display['dimension_value'].astype(str)
                                                    else:
                                                        grandchild_display[dim_name] = ''

                                                grandchild_display['Search Term'] = ''

                                                # Format numbers using dynamic formatters
                                                pct_decimals = st.session_state.get('percentage_decimals', 2)
                                                grandchild_display['queries'] = grandchild_display['queries'].apply(fmt_whole_number)
                                                if 'search_term_count' in grandchild_display.columns:
                                                    grandchild_display['search_term_count'] = grandchild_display['search_term_count'].apply(fmt_whole_number)
                                                if 'average_queries_per_day' in grandchild_display.columns:
                                                    grandchild_display['average_queries_per_day'] = grandchild_display['average_queries_per_day'].apply(fmt_decimal)
                                                if 'average_queries_per_search_term_per_day' in grandchild_display.columns:
                                                    grandchild_display['average_queries_per_search_term_per_day'] = grandchild_display['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                                                grandchild_display['% of queries'] = grandchild_display['pct_of_total'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                                grandchild_display['cumulative %'] = grandchild_display['cumulative_pct'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                                grandchild_display['ctr'] = grandchild_display['ctr'].apply(fmt_percentage)
                                                grandchild_display['a2c_rate'] = grandchild_display['a2c_rate'].apply(fmt_percentage)
                                                grandchild_display['conversion_rate'] = grandchild_display['conversion_rate'].apply(fmt_percentage)
                                                if 'pdp_conversion' in grandchild_display.columns:
                                                    grandchild_display['pdp_conversion'] = grandchild_display['pdp_conversion'].apply(fmt_percentage)
                                                grandchild_display['revenue_per_query'] = grandchild_display['revenue_per_query'].apply(fmt_currency)
                                                grandchild_display['purchases'] = grandchild_display['purchases'].apply(fmt_whole_number)
                                                grandchild_display['gross_purchase'] = grandchild_display['gross_purchase'].apply(fmt_currency)
                                                grandchild_display['avg_order_value'] = grandchild_display['avg_order_value'].apply(fmt_currency)

                                                # Match columns from main table
                                                for col in main_cols:
                                                    if col not in grandchild_display.columns:
                                                        grandchild_display[col] = ''

                                                grandchild_display_full = grandchild_display.copy()
                                                grandchild_display = grandchild_display[main_cols]

                                                # Update child row to show it's expanded (▶ to ▼)
                                                nested_child_row = dimension_display.iloc[nested_expansion_idx].copy()
                                                nested_child_row[next_dimension_name] = nested_child_row[next_dimension_name].replace('▶ ', '▼ ')

                                                nested_child_row_full = dimension_display_full.iloc[nested_expansion_idx].copy()
                                                nested_child_row_full[next_dimension_name] = nested_child_row_full[next_dimension_name].replace('▶ ', '▼ ')

                                                # Rebuild dimension_display with grandchildren inserted
                                                if nested_expansion_idx > 0:
                                                    dimension_display = pd.concat([
                                                        dimension_display.iloc[:nested_expansion_idx],
                                                        pd.DataFrame([nested_child_row]),
                                                        grandchild_display,
                                                        dimension_display.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                    dimension_display_full = pd.concat([
                                                        dimension_display_full.iloc[:nested_expansion_idx],
                                                        pd.DataFrame([nested_child_row_full]),
                                                        grandchild_display_full,
                                                        dimension_display_full.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                else:
                                                    dimension_display = pd.concat([
                                                        pd.DataFrame([nested_child_row]),
                                                        grandchild_display,
                                                        dimension_display.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                    dimension_display_full = pd.concat([
                                                        pd.DataFrame([nested_child_row_full]),
                                                        grandchild_display_full,
                                                        dimension_display_full.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)

                                        else:
                                            # Expand to search terms (leaf level)
                                            search_query = dp.build_drill_down_query(
                                                st.session_state.bq_params['table'],
                                                base_filters,
                                                accumulated_filters,
                                                None
                                            )
                                            grandchild_search_terms = dp.execute_cached_query(st.session_state.bq_client, search_query, ttl=300)

                                            if not grandchild_search_terms.empty:
                                                # Calculate derived metrics
                                                grandchild_search_terms['ctr'] = np.where(grandchild_search_terms['queries'] > 0,
                                                                                         grandchild_search_terms['queries_pdp'] / grandchild_search_terms['queries'], 0)
                                                grandchild_search_terms['a2c_rate'] = np.where(grandchild_search_terms['queries'] > 0,
                                                                                              grandchild_search_terms['queries_a2c'] / grandchild_search_terms['queries'], 0)
                                                grandchild_search_terms['conversion_rate'] = np.where(grandchild_search_terms['queries'] > 0,
                                                                                                     grandchild_search_terms['purchases'] / grandchild_search_terms['queries'], 0)
                                                grandchild_search_terms['pdp_conversion'] = np.where(grandchild_search_terms['queries_pdp'] > 0,
                                                                                                 grandchild_search_terms['purchases'] / grandchild_search_terms['queries_pdp'], 0)
                                                grandchild_search_terms['revenue_per_query'] = np.where(grandchild_search_terms['queries'] > 0,
                                                                                                       grandchild_search_terms['gross_purchase'] / grandchild_search_terms['queries'], 0)
                                                grandchild_search_terms['avg_order_value'] = np.where(grandchild_search_terms['purchases'] > 0,
                                                                                                     grandchild_search_terms['gross_purchase'] / grandchild_search_terms['purchases'], 0)
                                                if 'average_queries_per_day' not in grandchild_search_terms.columns:
                                                    grandchild_search_terms['average_queries_per_day'] = grandchild_search_terms['queries'] / num_days
                                                if 'average_queries_per_search_term_per_day' not in grandchild_search_terms.columns:
                                                    grandchild_search_terms['average_queries_per_search_term_per_day'] = grandchild_search_terms['queries'] / num_days

                                                # Calculate 1p/3p percentage metrics
                                                total_clicks_1p_3p = grandchild_search_terms['products_pdp_1p'] + grandchild_search_terms['products_pdp_3p']
                                                grandchild_search_terms['pct_clicks_1p'] = np.where(total_clicks_1p_3p > 0,
                                                                                                    (grandchild_search_terms['products_pdp_1p'] / total_clicks_1p_3p * 100), 0)
                                                grandchild_search_terms['pct_clicks_3p'] = np.where(total_clicks_1p_3p > 0,
                                                                                                    (grandchild_search_terms['products_pdp_3p'] / total_clicks_1p_3p * 100), 0)

                                                total_a2c_1p_3p = grandchild_search_terms['products_a2c_1p'] + grandchild_search_terms['products_a2c_3p']
                                                grandchild_search_terms['pct_a2c_1p'] = np.where(total_a2c_1p_3p > 0,
                                                                                                 (grandchild_search_terms['products_a2c_1p'] / total_a2c_1p_3p * 100), 0)
                                                grandchild_search_terms['pct_a2c_3p'] = np.where(total_a2c_1p_3p > 0,
                                                                                                 (grandchild_search_terms['products_a2c_3p'] / total_a2c_1p_3p * 100), 0)

                                                total_purchases_1p_3p = grandchild_search_terms['purchases_1p'] + grandchild_search_terms['purchases_3p']
                                                grandchild_search_terms['pct_purchases_1p'] = np.where(total_purchases_1p_3p > 0,
                                                                                                       (grandchild_search_terms['purchases_1p'] / total_purchases_1p_3p * 100), 0)
                                                grandchild_search_terms['pct_purchases_3p'] = np.where(total_purchases_1p_3p > 0,
                                                                                                       (grandchild_search_terms['purchases_3p'] / total_purchases_1p_3p * 100), 0)

                                                grandchild_search_terms['aov_1p'] = np.where(grandchild_search_terms['purchases_1p'] > 0,
                                                                                             (grandchild_search_terms['gross_purchase_1p'] / grandchild_search_terms['purchases_1p']), 0)
                                                grandchild_search_terms['aov_3p'] = np.where(grandchild_search_terms['purchases_3p'] > 0,
                                                                                             (grandchild_search_terms['gross_purchase_3p'] / grandchild_search_terms['purchases_3p']), 0)

                                                # Calculate percentages
                                                grandchild_search_terms['pct_of_total'] = (grandchild_search_terms['queries'] / total_queries_all * 100) if total_queries_all > 0 else 0
                                                total_queries_search = grandchild_search_terms['queries'].sum()
                                                grandchild_search_terms['cumulative_queries'] = grandchild_search_terms['queries'].cumsum()
                                                grandchild_search_terms['cumulative_pct'] = (grandchild_search_terms['cumulative_queries'] / total_queries_search * 100) if total_queries_search > 0 else 0

                                                # Paginate search terms
                                                search_page_size = 5
                                                search_current_page = nested_expansion_info['page']
                                                search_total_pages = (len(grandchild_search_terms) + search_page_size - 1) // search_page_size
                                                search_start_idx = search_current_page * search_page_size
                                                search_end_idx = min(search_start_idx + search_page_size, len(grandchild_search_terms))
                                                search_page = grandchild_search_terms.iloc[search_start_idx:search_end_idx].copy()

                                                # Format search terms display
                                                grandchild_search_display = search_page.copy()

                                                # Add row tracking columns
                                                grandchild_search_display['_row_id'] = grandchild_search_display['search_term'].apply(
                                                    lambda x: f"search_{child_dimension_key}_{child_dimension_value}_{x}"
                                                )
                                                grandchild_search_display['_level'] = -1
                                                grandchild_search_display['_type'] = 'search'

                                                # Add columns for all dimension levels
                                                for dim_name in selected_dimension_names:
                                                    grandchild_search_display[dim_name] = ''

                                                # Populate Search Term column with highlighting for matched search
                                                if st.session_state.search_term_active:
                                                    grandchild_search_display['Search Term'] = grandchild_search_display['search_term'].apply(
                                                        lambda x: f"⭐ {x}" if str(x) == st.session_state.search_term_active else str(x)
                                                    )
                                                else:
                                                    grandchild_search_display['Search Term'] = grandchild_search_display['search_term'].astype(str)

                                                # Format numbers using dynamic formatters
                                                pct_decimals = st.session_state.get('percentage_decimals', 2)
                                                grandchild_search_display['queries'] = grandchild_search_display['queries'].apply(fmt_whole_number)
                                                grandchild_search_display['% of queries'] = grandchild_search_display['pct_of_total'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                                grandchild_search_display['cumulative %'] = grandchild_search_display['cumulative_pct'].apply(lambda x: f"{x:.{pct_decimals}f}%")
                                                if 'average_queries_per_day' in grandchild_search_display.columns:
                                                    grandchild_search_display['average_queries_per_day'] = grandchild_search_display['average_queries_per_day'].apply(fmt_decimal)
                                                if 'average_queries_per_search_term_per_day' in grandchild_search_display.columns:
                                                    grandchild_search_display['average_queries_per_search_term_per_day'] = grandchild_search_display['average_queries_per_search_term_per_day'].apply(fmt_decimal)
                                                grandchild_search_display['ctr'] = grandchild_search_display['ctr'].apply(fmt_percentage)
                                                grandchild_search_display['a2c_rate'] = grandchild_search_display['a2c_rate'].apply(fmt_percentage)
                                                grandchild_search_display['conversion_rate'] = grandchild_search_display['conversion_rate'].apply(fmt_percentage)
                                                if 'pdp_conversion' in grandchild_search_display.columns:
                                                    grandchild_search_display['pdp_conversion'] = grandchild_search_display['pdp_conversion'].apply(fmt_percentage)
                                                grandchild_search_display['revenue_per_query'] = grandchild_search_display['revenue_per_query'].apply(fmt_currency)
                                                grandchild_search_display['purchases'] = grandchild_search_display['purchases'].apply(fmt_whole_number)
                                                grandchild_search_display['gross_purchase'] = grandchild_search_display['gross_purchase'].apply(fmt_currency)
                                                grandchild_search_display['avg_order_value'] = grandchild_search_display['avg_order_value'].apply(fmt_currency)

                                                # Match columns from main table
                                                for col in main_cols:
                                                    if col not in grandchild_search_display.columns:
                                                        grandchild_search_display[col] = ''

                                                grandchild_search_display_full = grandchild_search_display.copy()
                                                grandchild_search_display = grandchild_search_display[main_cols]

                                                # Update child row to show it's expanded
                                                nested_child_row = dimension_display.iloc[nested_expansion_idx].copy()
                                                nested_child_row[next_dimension_name] = nested_child_row[next_dimension_name].replace('▶ ', '▼ ')

                                                nested_child_row_full = dimension_display_full.iloc[nested_expansion_idx].copy()
                                                nested_child_row_full[next_dimension_name] = nested_child_row_full[next_dimension_name].replace('▶ ', '▼ ')

                                                # Rebuild dimension_display with search terms inserted
                                                if nested_expansion_idx > 0:
                                                    dimension_display = pd.concat([
                                                        dimension_display.iloc[:nested_expansion_idx],
                                                        pd.DataFrame([nested_child_row]),
                                                        grandchild_search_display,
                                                        dimension_display.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                    dimension_display_full = pd.concat([
                                                        dimension_display_full.iloc[:nested_expansion_idx],
                                                        pd.DataFrame([nested_child_row_full]),
                                                        grandchild_search_display_full,
                                                        dimension_display_full.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                else:
                                                    dimension_display = pd.concat([
                                                        pd.DataFrame([nested_child_row]),
                                                        grandchild_search_display,
                                                        dimension_display.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)
                                                    dimension_display_full = pd.concat([
                                                        pd.DataFrame([nested_child_row_full]),
                                                        grandchild_search_display_full,
                                                        dimension_display_full.iloc[nested_expansion_idx+1:]
                                                    ], ignore_index=True)

                                # Now rebuild combined_table with the updated dimension_display that includes nested expansions
                                if expanded_row_idx > 0:
                                    combined_table = pd.concat([
                                        display_formatted.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row]),
                                        dimension_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        display_formatted_full.iloc[:expanded_row_idx],
                                        pd.DataFrame([parent_row_full]),
                                        dimension_display_full,
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                else:
                                    combined_table = pd.concat([
                                        pd.DataFrame([parent_row]),
                                        dimension_display,
                                        display_formatted.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)
                                    combined_table_full = pd.concat([
                                        pd.DataFrame([parent_row_full]),
                                        dimension_display_full,
                                        display_formatted_full.iloc[expanded_row_idx+1:]
                                    ], ignore_index=True)

                                # Store full version for selection handling
                                st.session_state.pivot_combined_full = combined_table_full

                                # Display combined table with indicators inside
                                st.dataframe(
                                    combined_table,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=600,
                                    on_select="rerun",
                                    selection_mode="multi-row"
                                )

                                # Pagination controls
                                st.markdown(f"**Page {current_page + 1} of {total_pages}** (showing {start_idx + 1}-{end_idx} of {len(dimension_data)} {next_dimension_name} values)")
                                col_prev, col_next = st.columns(2)
                                with col_prev:
                                    if current_page > 0:
                                        if st.button("← Previous", key='prev_page_dim', use_container_width=True):
                                            st.session_state.pivot_expanded[row_id]['page'] = current_page - 1
                                            st.rerun()
                                with col_next:
                                    if current_page < total_pages - 1:
                                        if st.button("Next →", key='next_page_dim', use_container_width=True):
                                            st.session_state.pivot_expanded[row_id]['page'] = current_page + 1
                                            st.rerun()
                            else:
                                # No dimension data - show regular table
                                st.dataframe(
                                    display_formatted,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=600
                                )
                                st.info(f"No {next_dimension_name} data found for this selection")

                    else:
                        # No expansion - display full table with drill-down indicators inside
                        st.caption("💡 Use the checkbox to select rows for trend analysis. Click on the 🔍 column to expand/collapse: ▶️ (expand) or 🔽 (collapse).")

                        selection = st.dataframe(
                            display_formatted,
                            use_container_width=True,
                            hide_index=True,
                            height=600,
                            on_select="rerun",
                            selection_mode="multi-row",
                            key='pivot_table_main'
                        )

                        # Handle row clicks in the drill-down column (single click to toggle)
                        if selection and selection.selection.rows and len(selection.selection.rows) > 0:
                            # Get the last selected row (most recent click)
                            selected_idx = selection.selection.rows[-1]
                            row_id = display_formatted_full.iloc[selected_idx]['_row_id']
                            selected_value = pivot_data.iloc[selected_idx]['dimension_value']
                            is_total_row = selected_value == 'Total'

                            # Check if this is a new selection (different from last processed)
                            if 'last_clicked_row' not in st.session_state or st.session_state.last_clicked_row != row_id:
                                st.session_state.last_clicked_row = row_id

                                if not is_total_row:
                                    is_expanded = row_id in st.session_state.pivot_expanded

                                    # Toggle expansion
                                    if is_expanded:
                                        # Collapse
                                        del st.session_state.pivot_expanded[row_id]
                                        st.session_state.pivot_selected_row = None
                                    else:
                                        # Expand
                                        if current_level < len(selected_dimension_keys) - 1:
                                            expansion_type = 'dimension'
                                        else:
                                            expansion_type = 'search'

                                        st.session_state.pivot_expanded[row_id] = {'type': expansion_type, 'page': 0}
                                        st.session_state.pivot_selected_row = row_id
                                    st.rerun()

                else:
                    st.info("No data available for this dimension with current filters.")

            else:
                st.info("You've reached the end of the dimension hierarchy. Use the 'Show Search Terms' option to explore search terms.")

    except Exception as e:
        st.error(f"Error loading pivot table: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# Section 8: Pivot Row Trend Analysis (Multi-Row Comparison)
st.divider()
if len(st.session_state.pivot_rows_for_trend) > 0:
    num_rows = len(st.session_state.pivot_rows_for_trend)
    st.header(f"📈 Trend Analysis - Comparing {num_rows} Row{'s' if num_rows > 1 else ''}")

    # Display list of rows being compared with remove buttons
    st.markdown("**Comparing:**")
    cols_per_row = 4
    for i in range(0, num_rows, cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < num_rows:
                with col:
                    row_data = st.session_state.pivot_rows_for_trend[idx]
                    if st.button(f"✕ {row_data['display_name']}", key=f"remove_trend_{idx}", use_container_width=True):
                        st.session_state.pivot_rows_for_trend.pop(idx)
                        st.rerun()

    # Clear all button
    if st.button("✕ Clear All", key='clear_all_trends'):
        st.session_state.pivot_rows_for_trend = []
        st.session_state.selected_rows_for_comparison = {}
        st.rerun()

    # Time granularity, search term level, and metric selector
    col_freq, col_search_term_level, col_metrics = st.columns([1, 1, 2])

    with col_freq:
        trend_freq_options = {
            'Daily': 'D',
            'Weekly': 'W',
            'Monthly': 'M'
        }
        trend_freq_label = st.selectbox(
            "Time Granularity",
            options=list(trend_freq_options.keys()),
            index=0,
            key='trend_freq'
        )
        trend_freq = trend_freq_options[trend_freq_label]

    with col_search_term_level:
        search_term_level_options = {
            'Individual Search Term': 'individual',
            'All Search Terms': 'aggregated'
        }
        search_term_level_label = st.selectbox(
            "Search Term Level",
            options=list(search_term_level_options.keys()),
            index=0,
            key='search_term_level'
        )
        search_term_level = search_term_level_options[search_term_level_label]

    with col_metrics:
        # Metric selector for trends
        if st.session_state.visible_columns is None or len(st.session_state.visible_columns) == 0:
            available_trend_metrics = TRENDABLE_METRICS
        else:
            available_trend_metrics = [m for m in TRENDABLE_METRICS if m in st.session_state.visible_columns]

        default_metrics = ['queries', 'revenue_per_query']
        default_metrics = [m for m in default_metrics if m in available_trend_metrics]
        if not default_metrics and available_trend_metrics:
            default_metrics = [available_trend_metrics[0]]

        trend_metrics = st.multiselect(
            "Select Metrics to Display",
            options=available_trend_metrics,
            default=default_metrics,
            key='trend_metrics'
        )

    # Chart Grouping UI
    if trend_metrics and len(trend_metrics) > 1:
        with st.expander("Group Metrics in Combined Charts", expanded=False):
            st.markdown("""
            Drag metrics into groups to combine them in a single chart with dual Y-axes.
            Metrics in the same group will share a chart, while ungrouped metrics display separately.
            """)

            # Get metric names for display
            metric_names = {
                'queries': 'Total Queries',
                'revenue_per_query': 'Revenue per Query',
                'ctr': 'Click-Through Rate',
                'conversion_rate': 'Conversion Rate',
                'a2c_rate': 'Add-to-Cart Rate',
                'pdp_conversion': 'PDP Conversion Rate',
                'purchases': 'Total Purchases',
                'gross_purchase': 'Gross Revenue',
                'avg_order_value': 'Average Order Value',
                'pct_clicks_1p': '% Clicks 1P',
                'pct_clicks_3p': '% Clicks 3P',
                'pct_a2c_1p': '% A2C 1P',
                'pct_a2c_3p': '% A2C 3P',
                'pct_purchases_1p': '% Purchases 1P',
                'pct_purchases_3p': '% Purchases 3P',
                'aov_1p': 'AOV 1P',
                'aov_3p': 'AOV 3P'
            }

            # Initialize or reset chart groups based on selected metrics
            if 'last_trend_metrics' not in st.session_state or st.session_state.last_trend_metrics != trend_metrics:
                st.session_state.last_trend_metrics = trend_metrics
                st.session_state.metric_chart_groups = []

            # UI for creating/managing groups
            col_add, col_clear = st.columns([3, 1])
            with col_add:
                new_group_metrics = st.multiselect(
                    "Create a new chart group",
                    options=trend_metrics,
                    format_func=lambda x: metric_names.get(x, x),
                    key='new_group_metrics'
                )
            with col_clear:
                st.write("")  # Spacing
                st.write("")  # Spacing
                if st.button("Add Group", disabled=len(new_group_metrics) < 2):
                    st.session_state.metric_chart_groups.append(new_group_metrics)
                    st.rerun()

            # Display existing groups
            if st.session_state.metric_chart_groups:
                st.markdown("**Current Chart Groups:**")
                for idx, group in enumerate(st.session_state.metric_chart_groups):
                    col_group, col_remove = st.columns([5, 1])
                    with col_group:
                        group_display = " + ".join([metric_names.get(m, m) for m in group])
                        st.markdown(f"**Group {idx + 1}:** {group_display}")
                    with col_remove:
                        if st.button("Remove", key=f"remove_group_{idx}"):
                            st.session_state.metric_chart_groups.pop(idx)
                            st.rerun()
            else:
                st.info("No chart groups created. Add at least 2 metrics to create a combined chart.")

    if trend_metrics:
        # Query trend data for all selected rows
        with st.spinner(f'Loading trend data for {num_rows} row(s)...'):
            try:
                all_trend_data = []

                for row_data in st.session_state.pivot_rows_for_trend:
                    # Create a copy of dimension filters to modify
                    dimension_filters = row_data['dimension_filters'].copy()

                    # Remove search_term filter if aggregated level is selected
                    if search_term_level == 'aggregated' and 'search_term' in dimension_filters:
                        del dimension_filters['search_term']

                    query = dp.build_timeseries_query_with_dimensions(
                        st.session_state.bq_params['table'],
                        base_filters,
                        dimension_filters,
                        freq=trend_freq
                    )
                    ts_data = dp.execute_cached_query(st.session_state.bq_client, query, ttl=300)

                    if len(ts_data) > 0:
                        # Calculate derived metrics
                        ts_data['ctr'] = np.where(ts_data['queries'] > 0, ts_data['queries_pdp'] / ts_data['queries'], 0)
                        ts_data['a2c_rate'] = np.where(ts_data['queries'] > 0, ts_data['queries_a2c'] / ts_data['queries'], 0)
                        ts_data['conversion_rate'] = np.where(ts_data['queries'] > 0, ts_data['purchases'] / ts_data['queries'], 0)
                        ts_data['pdp_conversion'] = np.where(ts_data['queries_pdp'] > 0, ts_data['purchases'] / ts_data['queries_pdp'], 0)
                        ts_data['revenue_per_query'] = np.where(ts_data['queries'] > 0, ts_data['gross_purchase'] / ts_data['queries'], 0)
                        ts_data['avg_order_value'] = np.where(ts_data['purchases'] > 0, ts_data['gross_purchase'] / ts_data['purchases'], 0)

                        # Calculate 1p/3p percentage metrics if columns exist
                        if 'products_pdp_1p' in ts_data.columns and 'products_pdp_3p' in ts_data.columns:
                            total_clicks_1p_3p = ts_data['products_pdp_1p'] + ts_data['products_pdp_3p']
                            ts_data['pct_clicks_1p'] = np.where(total_clicks_1p_3p > 0,
                                                                (ts_data['products_pdp_1p'] / total_clicks_1p_3p * 100), 0)
                            ts_data['pct_clicks_3p'] = np.where(total_clicks_1p_3p > 0,
                                                                (ts_data['products_pdp_3p'] / total_clicks_1p_3p * 100), 0)

                        if 'products_a2c_1p' in ts_data.columns and 'products_a2c_3p' in ts_data.columns:
                            total_a2c_1p_3p = ts_data['products_a2c_1p'] + ts_data['products_a2c_3p']
                            ts_data['pct_a2c_1p'] = np.where(total_a2c_1p_3p > 0,
                                                             (ts_data['products_a2c_1p'] / total_a2c_1p_3p * 100), 0)
                            ts_data['pct_a2c_3p'] = np.where(total_a2c_1p_3p > 0,
                                                             (ts_data['products_a2c_3p'] / total_a2c_1p_3p * 100), 0)

                        if 'purchases_1p' in ts_data.columns and 'purchases_3p' in ts_data.columns:
                            total_purchases_1p_3p = ts_data['purchases_1p'] + ts_data['purchases_3p']
                            ts_data['pct_purchases_1p'] = np.where(total_purchases_1p_3p > 0,
                                                                   (ts_data['purchases_1p'] / total_purchases_1p_3p * 100), 0)
                            ts_data['pct_purchases_3p'] = np.where(total_purchases_1p_3p > 0,
                                                                   (ts_data['purchases_3p'] / total_purchases_1p_3p * 100), 0)

                        if 'gross_purchase_1p' in ts_data.columns and 'purchases_1p' in ts_data.columns:
                            ts_data['aov_1p'] = np.where(ts_data['purchases_1p'] > 0,
                                                         (ts_data['gross_purchase_1p'] / ts_data['purchases_1p']), 0)

                        if 'gross_purchase_3p' in ts_data.columns and 'purchases_3p' in ts_data.columns:
                            ts_data['aov_3p'] = np.where(ts_data['purchases_3p'] > 0,
                                                         (ts_data['gross_purchase_3p'] / ts_data['purchases_3p']), 0)

                        all_trend_data.append({
                            'data': ts_data,
                            'label': row_data['display_name']
                        })

                if len(all_trend_data) > 0:
                    # Display trend charts using multi-row function
                    metric_names = {
                        'queries': 'Total Queries',
                        'revenue_per_query': 'Revenue per Query',
                        'ctr': 'Click-Through Rate',
                        'conversion_rate': 'Conversion Rate',
                        'a2c_rate': 'Add-to-Cart Rate',
                        'pdp_conversion': 'PDP Conversion Rate',
                        'purchases': 'Total Purchases',
                        'gross_purchase': 'Gross Revenue',
                        'avg_order_value': 'Average Order Value',
                        'pct_clicks_1p': '% Clicks 1P',
                        'pct_clicks_3p': '% Clicks 3P',
                        'pct_a2c_1p': '% A2C 1P',
                        'pct_a2c_3p': '% A2C 3P',
                        'pct_purchases_1p': '% Purchases 1P',
                        'pct_purchases_3p': '% Purchases 3P',
                        'aov_1p': 'AOV 1P',
                        'aov_3p': 'AOV 3P'
                    }

                    # Track which metrics have been rendered as part of a group
                    metrics_rendered = set()

                    # First, render any grouped metrics
                    for group in st.session_state.metric_chart_groups:
                        # Only render if all metrics in the group are selected
                        if all(m in trend_metrics for m in group):
                            # Create combined chart with dual Y-axes
                            group_title = " + ".join([metric_names.get(m, m) for m in group])
                            if num_rows > 1:
                                title = f"{group_title} - Comparison"
                            else:
                                title = f"{group_title} - {all_trend_data[0]['label']}"

                            fig = viz.create_combined_metrics_chart(
                                all_trend_data,
                                group,
                                metric_names,
                                title
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Mark these metrics as rendered
                            metrics_rendered.update(group)

                    # Then render individual charts for ungrouped metrics
                    for metric in trend_metrics:
                        if metric not in metrics_rendered:
                            if num_rows == 1:
                                # Single row - use regular chart
                                fig = viz.create_trend_chart(
                                    all_trend_data[0]['data'],
                                    metric,
                                    title=f"{metric_names.get(metric, metric)} Over Time - {all_trend_data[0]['label']}",
                                    yaxis_title=metric_names.get(metric, metric),
                                    comparison_df=None
                                )
                            else:
                                # Multiple rows - use multi-row comparison chart
                                fig = viz.create_multi_row_trend_chart(
                                    all_trend_data,
                                    metric,
                                    title=f"{metric_names.get(metric, metric)} Over Time - Comparison",
                                    yaxis_title=metric_names.get(metric, metric)
                                )
                            st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No data available for the selected rows.")

            except Exception as e:
                st.error(f"Error loading trend data: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
    else:
        st.info("Select at least one metric to display trends.")
else:
    # Show instruction
    st.markdown("""
    <div style="background: #f3f4f6; padding: 1rem; border-radius: 6px; border-left: 4px solid #3b82f6;">
        <p style="margin: 0; color: #374151; font-size: 0.9rem;">
            💡 <strong>Tip:</strong> Use the pin buttons (📍) next to rows in the pivot table above to select multiple rows for comparison, then click "Compare Trends" to see how metrics change over time.
        </p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.divider()
st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
