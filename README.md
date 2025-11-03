# Search Analytics Dashboard

An interactive dashboard for analyzing customer search behavior in your ecommerce platform.

## Overview

This modern React + Python dashboard provides comprehensive analytics on search queries, including:

- **Overview KPIs**: Total queries, revenue, conversion rates, and key metrics
- **Trend Analysis**: Time-series visualizations with interactive charts
- **Channel Performance**: Compare metrics across different channels (App/Web)
- **Search Query Length Analysis**: Performance breakdown by number of words in search queries
- **Attribute Analysis**:
  - Aggregate view by number of attributes
  - Individual attribute breakdown (categoria, tipo, genero, marca, color, material, talla, modelo)
- **Hierarchical Pivot Table**: Multi-level drill-down with expandable rows
- **Advanced Filtering**: Date range, countries, channels, and attribute filters

## Architecture

**Frontend**: Next.js 14 + React + TypeScript
**Backend**: Python FastAPI
**Data Source**: Google BigQuery
**Deployment**: Docker Compose

## Quick Start

### One-Command Start

```bash
docker-compose up --build
```

Then open:
- **Dashboard**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs

### Stopping the Application

```bash
docker-compose down
```

## Key Metrics Calculated

- **Click-Through Rate (CTR)**: queries_pdp / queries
- **Add-to-Cart Rate**: queries_a2c / queries
- **Conversion Rate**: purchases / queries
- **PDP Conversion**: purchases / queries_pdp
- **Revenue per Query**: gross_purchase / queries
- **Average Order Value**: gross_purchase / purchases

## BigQuery Configuration

On first run:
1. Navigate to the "Info" tab in the dashboard
2. Choose authentication method:
   - **Application Default Credentials (ADC)**: Run `gcloud auth application-default login` first
   - **Service Account JSON**: Paste your service account JSON
3. Enter your BigQuery project ID, dataset, and table name
4. (Optional) Set date range limits for access control
5. Click "Connect to BigQuery"

## Data Format

The dashboard expects a BigQuery table with the following columns:

- `date`: DATE - Date of the search
- `country`: STRING - Country code
- `channel`: STRING - Channel (e.g., App, Web)
- `search_term`: STRING - The actual search query
- `n_words_normalized`: INT64 - Number of words in the search term
- `n_attributes`: INT64 - Total number of attributes identified
- `attr_categoria`, `attr_tipo`, `attr_genero`, `attr_marca`, `attr_color`, `attr_material`, `attr_talla`, `attr_modelo`: BOOLEAN - Attribute flags
- `queries`: INT64 - Number of queries
- `queries_pdp`: INT64 - Queries that led to PDP
- `queries_a2c`: INT64 - Queries that led to add-to-cart
- `purchases`: INT64 - Number of purchases
- `gross_purchase`: FLOAT64 - Revenue generated

## Features

### Filter Sidebar
- Date range picker (start & end dates)
- Country and channel dropdowns
- Number of attributes range (min/max)
- 8 individual attribute checkboxes
- Apply/Reset buttons with global state management

### Dashboard Sections
1. **Overview**: 7 KPI cards with key performance indicators
2. **Trends**: Time-series charts for queries, purchases, and conversion rates
3. **Channels**: Channel comparison with 3 detailed charts
4. **Attributes**: 8 charts analyzing performance by attributes and word count
5. **Search Terms**: Sortable table with CSV export (top 100 terms)
6. **Pivot Table**: Hierarchical drill-down with expandable rows
7. **Info**: BigQuery configuration and connection management

### Advanced Features
- Column visibility management
- CSV data export
- Sortable tables (click column headers)
- Responsive mobile design
- Professional loading and error states

## Development

### Backend Development
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend Development
```bash
cd frontend
npm install
npm run dev  # Runs on port 3000
```

## Project Structure

```
├── docker-compose.yml          # Orchestrates frontend + backend
├── backend/                    # Python FastAPI service
│   ├── main.py                # API endpoints
│   ├── config.py              # Configuration management
│   ├── models/
│   │   └── schemas.py         # Pydantic models
│   └── services/
│       ├── bigquery_service.py  # BigQuery client
│       └── data_service.py      # Business logic
│
└── frontend/                   # Next.js React app
    ├── app/
    │   └── page.tsx           # Main dashboard
    ├── components/
    │   ├── layout/            # Layout components
    │   └── sections/          # Dashboard sections
    └── lib/
        ├── api.ts             # API client
        └── contexts/          # React contexts
```

## Documentation

- `CLAUDE.md` - Comprehensive developer guide for working with this codebase
- `QUICKSTART.md` - Quick start guide for running the application
- `MIGRATION_README.md` - Architecture details and migration information
- `FEATURES_COMPLETE.md` - Complete feature list

## Technical Stack

- **Frontend**: Next.js 14, React 18, TypeScript, Tailwind CSS, Recharts, React Query
- **Backend**: Python 3.9+, FastAPI, Pydantic
- **Data**: Google BigQuery, Pandas, NumPy
- **Deployment**: Docker, Docker Compose

## Support

For detailed architecture information and development guidance, see `CLAUDE.md`.

For BigQuery setup and troubleshooting, visit the Info tab in the dashboard at http://localhost:3000.
