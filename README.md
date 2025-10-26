# Search Analytics Dashboard

An interactive dashboard for analyzing customer search behavior in your ecommerce platform.

## Overview

This Streamlit-based dashboard provides comprehensive analytics on search queries, including:

- **Overview KPIs**: Total queries, revenue, conversion rates, and key metrics
- **Trend Analysis**: Time-series visualizations with period comparison capabilities
- **Channel Performance**: Compare metrics across different channels (App/Web)
- **Search Query Length Analysis**: Performance breakdown by number of words in search queries
- **Attribute Analysis**:
  - Aggregate view by number of attributes
  - Individual attribute breakdown (categoria, tipo, genero, marca, color, material, talla, modelo)
- **Search Term Explorer**: Multi-level drill-down to individual search terms

## Currency Conversion

All monetary values are **automatically converted from CLP to USD** using daily exchange rates:
- Fetches real-time exchange rates from Frankfurter API
- Caches rates locally for better performance
- Falls back to default rate (950 CLP/USD) when API is unavailable
- All revenue metrics displayed with **$ symbol**

## Key Metrics Calculated

- **Click-Through Rate (CTR)**: queries_pdp / queries - **displayed as %**
- **Add-to-Cart Rate**: queries_a2c / queries - **displayed as %**
- **Conversion Rate**: purchases / queries - **displayed as %**
- **Revenue per Query**: gross_purchase / queries - **displayed as $**
- **Average Order Value**: gross_purchase / purchases - **displayed as $**

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the dashboard with:
```bash
streamlit run dashboard.py
```

The dashboard will open in your default browser at `http://localhost:8501`

## Data Format

The dashboard expects a CSV file with the following columns:

- `country`: Country code
- `channel`: Channel (e.g., App, Web)
- `date`: Date of the search
- `search_term`: The actual search query
- `n_words_normalized`: Number of words in the search term
- `n_attributes`: Total number of attributes identified
- `attr_categoria`, `attr_tipo`, `attr_genero`, `attr_marca`, `attr_color`, `attr_material`, `attr_talla`, `attr_modelo`: Binary flags for identified attributes
- `visits`: Number of visits
- `queries`: Number of queries
- `visits_pdp`: Product detail page visits
- `queries_pdp`: Queries that led to PDP
- `visits_a2c`: Add-to-cart visits
- `queries_a2c`: Queries that led to add-to-cart
- `purchases`: Number of purchases
- `gross_purchase`: Revenue generated

## Features

### Filters (Sidebar)

- **Date Range**: Select specific date ranges to analyze
- **Period Comparison**: Compare metrics across different time periods
- **Country**: Filter by country
- **Channel**: Filter by channel (App/Web)
- **Individual Attributes**: Toggle filters for specific attributes
- **Number of Attributes**: Filter by attribute count range

### Interactive Visualizations

- Time-series trend charts with customizable granularity (daily/weekly/monthly)
- Channel performance comparison charts
- Attribute performance analysis
- Top search terms by various metrics
- Sortable and filterable data tables

### Export Capabilities

- Download filtered search term data as CSV
- All tables support sorting and filtering

## Dashboard Statistics

Current dataset includes:
- **66,190 data rows**
- **51,152 unique search terms**
- **284,884 total queries**
- **$271,281.60 total revenue (USD)**
- **1.65% overall conversion rate**
- **48.68% average CTR**
- **$0.95 revenue per query**

## File Structure

- `dashboard.py`: Main Streamlit application
- `data_processing.py`: Data loading and metric calculation functions
- `visualizations.py`: Chart creation and visualization functions
- `requirements.txt`: Python dependencies
- `bquxjob_6268ac9c_19a163e3339.csv`: Sample data file

## Tips for Analysis

1. **Use Period Comparison**: Enable period comparison to understand trends and changes over time
2. **Filter by Attributes**: Analyze how searches with specific attributes (e.g., color, brand) perform differently
3. **Explore Top Terms**: Use the search term explorer to identify high-value search queries
4. **Channel Analysis**: Compare App vs Web performance to optimize channel-specific strategies
5. **Drill Down**: Start with high-level metrics and drill down to individual search terms for detailed insights

## Technical Details

- **Framework**: Streamlit 1.31+
- **Data Processing**: Pandas 2.0+
- **Visualizations**: Plotly 5.18+
- **Python Version**: 3.9+

## Support

For issues or questions, please refer to the Streamlit documentation at https://docs.streamlit.io
