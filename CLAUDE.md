# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A search analytics dashboard for analyzing customer search behavior in ecommerce platforms. The application queries BigQuery for search data and provides interactive visualizations through a modern React frontend.

**Stack:** Next.js 14 (React + TypeScript) frontend + FastAPI (Python) backend + Google BigQuery

## Running the Application

### Quick Start (Recommended)
```bash
# Start everything with Docker
docker-compose up --build

# Access:
# - Frontend Dashboard: http://localhost:3000
# - Backend API: http://localhost:8000
# - API Documentation: http://localhost:8000/docs
```

### Development Mode (Without Docker)

**Backend:**
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev  # Runs on port 3000
```

**Frontend Build:**
```bash
cd frontend
npm run build  # Production build
npm start      # Run production server
```

## Architecture Overview

### Data Flow
1. **BigQuery** stores search analytics data (no local CSV files)
2. **FastAPI backend** (`backend/`) queries BigQuery and serves REST API
3. **Next.js frontend** (`frontend/`) fetches data via API and renders dashboard
4. **Docker Compose** orchestrates both services with hot-reload for development

### Key Architecture Patterns

#### BigQuery Service Layer
- **No hardcoded credentials**: Configuration happens via UI or environment variables
- **Two authentication methods**:
  - Application Default Credentials (ADC) - uses `gcloud auth application-default login`
  - Service Account JSON - uploaded via UI
- **Date limit enforcement**: Server-side date clamping prevents unauthorized date range access
- **Config persistence**: Saved to `/app/config/bigquery_config.json` in Docker volume

#### Frontend State Management
- **React Context** (`frontend/lib/contexts/filter-context.tsx`): Global filter state shared across all tabs
- **React Query** (`@tanstack/react-query`): API caching, loading states, error handling
- **Filter propagation**: When filters change in sidebar, all dashboard sections automatically refetch data

#### Pivot Table Architecture
- **Hierarchical drill-down**: Users can expand rows to see child dimensions or search terms
- **Dimension hierarchy**: Start with high-level dimension (e.g., n_words) → drill into search terms
- **Pagination for children**: Expanded rows paginate through search terms (100 per page)
- **Row expansion state**: Tracked in parent component to persist expand/collapse across re-renders

#### Dynamic Schema System (NEW)
The application uses a **fully dynamic schema system** that automatically detects and adapts to any BigQuery table structure without code changes.

**Key Features:**
- **Auto-Detection**: Automatically classifies columns as metrics or dimensions based on type and naming
- **Custom Calculated Metrics**: Users can create formulas like `{queries_pdp} / {queries}` via API
- **Dynamic SQL Generation**: All queries built dynamically from schema configuration
- **API-Driven Schema Management**: Full CRUD operations for metrics and dimensions
- **Backward Compatible**: Default schema maintains original hardcoded metrics/dimensions

**Schema Configuration Files:**
- **Schema Storage**: `/app/config/schema_config.json` (persisted in Docker volume)
- **Auto-Created**: Schema automatically detected and created on first BigQuery connection
- **Format**: JSON with base metrics, calculated metrics, and dimensions

**Schema Architecture:**
```
SchemaConfig
├── base_metrics[]         # Direct column aggregations (SUM, AVG, COUNT, etc.)
│   ├── id, display_name
│   ├── column_name, aggregation
│   └── format_type, decimal_places, category
├── calculated_metrics[]   # Computed from base metrics
│   ├── id, display_name
│   ├── formula: "{metric1} / {metric2}"
│   ├── sql_expression: "SAFE_DIVIDE(SUM(col1), SUM(col2))"
│   └── depends_on[], format_type
└── dimensions[]           # Grouping/filtering columns
    ├── id, column_name, display_name
    ├── data_type, filter_type
    └── is_filterable, is_groupable
```

**Formula Syntax:**
- Reference metrics: `{metric_id}`
- Operations: `+`, `-`, `*`, `/` (division auto-converted to SAFE_DIVIDE)
- Example: `{purchases} / {queries}` → `SAFE_DIVIDE(SUM(purchases), SUM(queries))`

**Auto-Detection Logic:**
- **Numeric columns** with keywords (queries, purchases, revenue) → Base Metrics (SUM aggregation)
- **String/categorical columns** → Dimensions (STRING type)
- **Boolean/attr_ prefix columns** → Boolean filter dimensions
- **DATE columns** → Date dimensions with date_range filter

**API Endpoints** (24 new endpoints):
```bash
# Schema Management
GET  /api/schema                    # Get current schema
POST /api/schema/detect             # Auto-detect from BigQuery
POST /api/schema/reset              # Reset to defaults

# Base Metrics CRUD
GET    /api/metrics/base
POST   /api/metrics/base
PUT    /api/metrics/base/{id}
DELETE /api/metrics/base/{id}

# Calculated Metrics CRUD
GET    /api/metrics/calculated
POST   /api/metrics/calculated       # Includes formula validation
POST   /api/metrics/validate-formula # Test formula before saving

# Dimensions CRUD
GET    /api/dimensions
POST   /api/dimensions
PUT    /api/dimensions/{id}
DELETE /api/dimensions/{id}
GET    /api/dimensions/filterable   # Get filterable dimensions only
GET    /api/dimensions/groupable    # Get groupable dimensions only
```

**Dynamic Query Building:**
All BigQuery queries now built dynamically:
1. `BigQueryService._build_metric_select_clause()` - Generates SELECT with all metrics from schema
2. `BigQueryService._load_schema()` - Loads schema on service initialization
3. `DataService._compute_calculated_metrics()` - Evaluates formulas post-aggregation

**Example Workflow:**
1. Connect to BigQuery → Schema auto-detected and saved
2. View metrics via `GET /api/metrics/base` and `/api/metrics/calculated`
3. Create custom metric: `POST /api/metrics/calculated` with `{"formula": "{revenue} / {queries}"}`
4. All dashboard queries automatically include new metric
5. Frontend fetches metrics dynamically (no hardcoded lists needed)

### Critical Files

**Backend:**
- `backend/main.py` - FastAPI app with all API endpoints (includes 24 schema management endpoints)
- `backend/services/bigquery_service.py` - BigQuery client with dynamic SQL generation (refactored for schema)
- `backend/services/data_service.py` - Business logic layer, metric calculations (includes calculated metrics support)
- `backend/services/schema_service.py` - **NEW** Schema auto-detection, persistence, CRUD (450 lines)
- `backend/services/metric_service.py` - **NEW** Metric management and formula parsing (350 lines)
- `backend/services/dimension_service.py` - **NEW** Dimension management (100 lines)
- `backend/config.py` - Configuration management (includes `SCHEMA_CONFIG_FILE` path)
- `backend/models/schemas.py` - Pydantic models including 15 new schema models (BaseMetric, CalculatedMetric, etc.)

**Frontend:**
- `frontend/app/page.tsx` - Main dashboard page with tab navigation
- `frontend/lib/contexts/filter-context.tsx` - Global filter state (React Context)
- `frontend/lib/api.ts` - API client functions for all backend endpoints
- `frontend/components/sections/pivot-table-section.tsx` - Complex pivot table with drill-down
- `frontend/components/layout/filter-sidebar.tsx` - Slide-out filter panel

**Config:**
- `docker-compose.yml` - Service orchestration, mounts gcloud credentials for ADC

## BigQuery Configuration

### First-Time Setup
1. Navigate to "Info" tab in the dashboard (http://localhost:3000)
2. Choose authentication method:
   - **ADC**: Run `gcloud auth application-default login` on host machine first
   - **Service Account**: Paste service account JSON
3. Enter: Project ID, Dataset, Table
4. (Optional) Set date limits to restrict access to specific date range
5. Click "Connect to BigQuery"

### Date Limit Feature
- Admins can set `allowed_min_date` and `allowed_max_date` in BigQuery config
- Backend automatically clamps any date range queries to these limits
- Prevents users from accessing data outside allowed range
- Configured in `backend/config.py`, enforced in `BigQueryService._clamp_dates()`

### Environment Variables (Alternative to UI Config)
```bash
BIGQUERY_PROJECT_ID=your-project
BIGQUERY_DATASET=your_dataset
BIGQUERY_TABLE=your_table
# For service account JSON:
BIGQUERY_CREDENTIALS_PATH=/path/to/service-account.json
```

## Data Model

### Expected BigQuery Table Schema
```
- date: DATE
- country: STRING
- channel: STRING (e.g., 'App', 'Web')
- search_term: STRING
- n_words_normalized: INT64
- n_attributes: INT64
- attr_categoria: BOOLEAN
- attr_tipo: BOOLEAN
- attr_genero: BOOLEAN
- attr_marca: BOOLEAN
- attr_color: BOOLEAN
- attr_material: BOOLEAN
- attr_talla: BOOLEAN
- attr_modelo: BOOLEAN
- queries: INT64
- queries_pdp: INT64
- queries_a2c: INT64
- purchases: INT64
- gross_purchase: FLOAT64
```

### Key Metrics Calculated
- **CTR**: `queries_pdp / queries`
- **Add-to-Cart Rate**: `queries_a2c / queries`
- **Conversion Rate**: `purchases / queries`
- **PDP Conversion**: `purchases / queries_pdp`
- **Revenue per Query**: `gross_purchase / queries`
- **Average Order Value (AOV)**: `gross_purchase / purchases`

## API Endpoints

All endpoints accept filter query parameters:
- `start_date`, `end_date`: Date range (YYYY-MM-DD)
- `country`, `channel`: Single value filters
- `n_attributes_min`, `n_attributes_max`: Range filter
- `attr_categoria`, `attr_tipo`, etc.: Boolean filters for 8 attributes

**Main Endpoints:**
- `GET /api/overview` - KPI metrics
- `GET /api/trends?granularity=daily` - Time-series data
- `GET /api/breakdown/{dimension}?limit=20` - Dimension breakdown (channel, n_words, n_attributes)
- `GET /api/search-terms?limit=100&sort_by=queries` - Top search terms
- `GET /api/filters/options` - Available filter values
- `GET /api/pivot?dimension=n_words` - Pivot table data
- `GET /api/pivot/{dimension}/{value}/children` - Drill-down data
- `POST /api/bigquery/configure` - Configure BigQuery connection
- `POST /api/bigquery/disconnect` - Disconnect and clear credentials

## Dashboard Features

### Main Sections
1. **Overview**: 7 KPI cards (Queries, Revenue, CVR, CTR, Unique Terms, Rev/Query, AOV)
2. **Trends**: Time-series charts for queries, purchases, conversion rates
3. **Channels**: Channel comparison (KPI cards + 3 charts)
4. **Attributes**: Performance by # of attributes and # of words (8 charts total)
5. **Search Terms**: Top 100 search terms table with sorting and CSV export
6. **Pivot Table**: Hierarchical drill-down table with expandable rows
7. **Info**: BigQuery configuration and connection management

### Filter Sidebar
- Slide-out panel (click "Filters" button in header)
- Date range picker
- Country & Channel dropdowns
- Number of attributes range (min/max)
- 8 individual attribute checkboxes
- Apply/Reset buttons
- Filters persist in React Context and apply to all sections

### Pivot Table Features
- Select dimension to group by (n_words, n_attributes, channel, country)
- Click row to expand and see search terms for that dimension value
- Paginate through expanded search terms (100 per page)
- Column visibility management (multiselect which metrics to show)
- Cumulative percentage calculation
- Export capabilities

## Adding New Features

### Adding a New API Endpoint
1. Add route handler in `backend/main.py`
2. Add business logic function in `backend/services/data_service.py`
3. Add Pydantic models in `backend/models/schemas.py` if needed
4. Add client function in `frontend/lib/api.ts`
5. Use with `useQuery` in React component

### Adding a New Dashboard Section
1. Create component in `frontend/components/sections/`
2. Use `useFilters()` hook to get current filter state
3. Fetch data with `useQuery` from React Query
4. Add tab to navigation in `frontend/app/page.tsx`

### Adding a New Filter
1. Update `FilterParams` model in `backend/models/schemas.py`
2. Update `build_filter_clause()` in `backend/services/bigquery_service.py`
3. Add filter UI control in `frontend/components/layout/filter-sidebar.tsx`
4. Update `FilterState` type in `frontend/lib/contexts/filter-context.tsx`

## Important Notes

### Docker Volumes
- `bigquery_config` volume persists BigQuery configuration across container restarts
- `~/.config/gcloud` mounted read-only for ADC authentication

### Port Configuration
- Frontend: 3000 (configurable in `docker-compose.yml`)
- Backend: 8000 (configurable in `docker-compose.yml`)
- Backend must be accessible at URL specified in `NEXT_PUBLIC_API_URL` env var

### Common Troubleshooting
- **"BigQuery not configured"**: Configure via Info tab or set env vars
- **CORS errors**: Check `allow_origins` in `backend/main.py` includes your frontend URL
- **Date clamp issues**: Check `allowed_min_date`/`allowed_max_date` in BigQuery config
- **ADC not working**: Run `gcloud auth application-default login` on host machine
