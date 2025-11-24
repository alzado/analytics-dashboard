# Search Analytics Dashboard

> A powerful, fully dynamic analytics platform for analyzing customer search behavior in ecommerce platforms with drag-and-drop dashboards, multi-table support, and real-time BigQuery integration.

[![Tech Stack](https://img.shields.io/badge/Next.js-14-black)](https://nextjs.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green)](https://fastapi.tiangolo.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Integrated-orange)](https://cloud.google.com/bigquery)

## Overview

This modern analytics platform provides comprehensive search analytics with a unique **dynamic schema system** that automatically adapts to any BigQuery table structure - no hardcoded columns required.

### Key Features

🔄 **Dynamic Schema System**
- Automatically detects your BigQuery table structure
- No code changes needed for different schemas
- Create custom calculated metrics with formulas like `{revenue} / {queries}`
- Formula validation and testing before deployment

📊 **Custom Dashboards & Widgets**
- Drag-and-drop dashboard builder
- Create unlimited custom visualizations
- Resize and position widgets freely
- Supports pivot tables and charts

🗂️ **Multi-Table Support**
- Connect to multiple BigQuery tables
- Each widget can query different tables
- Switch between tables dynamically
- Independent schemas per table

📈 **Hierarchical Pivot Tables**
- Multi-level drill-down analysis
- Expandable rows with search term details
- Paginated child data (100 items per page)
- Dynamic column selection

💰 **Query Logging & Cost Tracking**
- Monitor every BigQuery query
- Track bytes processed and billed
- Usage statistics dashboard
- Cost optimization insights

🔍 **Advanced Analytics**
- Overview KPIs with trend indicators
- Time-series visualizations
- Channel performance comparison
- Attribute analysis (8+ dimensions)
- Custom dimensions with SQL formulas

## Architecture

**Frontend**: Next.js 14 + React + TypeScript + Tailwind CSS
**Backend**: FastAPI (Python) + Google BigQuery
**State Management**: React Query + React Context
**Deployment**: Docker Compose with hot-reload

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   Next.js UI    │ ───▶ │  FastAPI Backend │ ───▶ │  Google BigQuery│
│  (Port 3000)    │ HTTP │   (Port 8000)    │ SQL  │   (Cloud Data)  │
└─────────────────┘      └──────────────────┘      └─────────────────┘
         │                        │
         │                        ▼
         │              ┌──────────────────┐
         │              │ Dynamic Schema    │
         │              │ • Auto-detection  │
         └─────────────▶│ • Custom metrics  │
                        │ • Formula engine  │
                        └──────────────────┘
```

## Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed
- **Google Cloud account** with BigQuery access
- (Optional) **gcloud CLI** for Application Default Credentials

### One-Command Start

```bash
docker-compose up --build
```

Then open:
- **Dashboard**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs

### First-Time Setup

1. Navigate to the **"Info"** tab at http://localhost:3000
2. Choose your authentication method:
   - **Application Default Credentials (ADC)**: Run `gcloud auth application-default login` first
   - **Service Account JSON**: Paste your service account JSON directly
3. Enter your BigQuery details:
   - **Project ID**: Your GCP project ID
   - **Dataset**: BigQuery dataset name
   - **Table**: Table name to analyze
4. (Optional) Set **date range limits** for access control
5. Click **"Connect to BigQuery"**
6. Schema will be **auto-detected** and saved automatically

The system will automatically:
- Detect all columns in your table
- Classify numeric columns as metrics
- Classify string/date columns as dimensions
- Create default calculated metrics (CTR, conversion rates, etc.)
- Save configuration for future use

### Stopping the Application

```bash
docker-compose down
```

## Features in Detail

### 1. Dynamic Schema System

The dashboard automatically adapts to **any BigQuery table structure** without code changes.

#### Auto-Detection

When you connect to a BigQuery table, the system:
1. Scans all columns in your table
2. Classifies columns based on data type:
   - **Numeric columns** → Base Metrics (SUM aggregation)
   - **String columns** → Dimensions (grouping/filtering)
   - **DATE columns** → Date dimensions
   - **BOOLEAN columns** → Boolean filters
3. Saves schema to `/app/config/schema_config.json`

#### Custom Calculated Metrics

Create formulas using existing metrics:

**Formula Syntax**:
- Reference metrics: `{metric_id}`
- Operators: `+`, `-`, `*`, `/` (automatically converts to SAFE_DIVIDE)
- Example: `{purchases} / {queries}` → `SAFE_DIVIDE(SUM(purchases), SUM(queries))`

**Common Examples**:
```javascript
// Conversion Rate
{purchases} / {queries}

// Revenue per Query
{gross_purchase} / {queries}

// Average Order Value
{gross_purchase} / {purchases}

// Click-Through Rate
{queries_pdp} / {queries}
```

#### Schema Management API

```bash
# Get current schema
GET /api/schema

# Auto-detect from BigQuery
POST /api/schema/detect

# Reset to defaults
POST /api/schema/reset

# Create custom calculated metric
POST /api/metrics/calculated
{
  "display_name": "Conversion Rate",
  "formula": "{purchases} / {queries}",
  "format_type": "percentage"
}

# Validate formula before saving
POST /api/metrics/validate-formula
{
  "formula": "{revenue} / {queries}"
}
```

### 2. Custom Dashboards & Widgets

Build custom dashboards with drag-and-drop widgets.

#### Creating a Dashboard

1. Navigate to **"Dashboards"** tab
2. Click **"New Dashboard"**
3. Enter name and description
4. Toggle **Edit Mode** to add widgets

#### Widget Types

**Pivot Table Widget**:
- Top 10 rows with totals
- Select dimensions (checkboxes)
- Select metrics (checkboxes)
- Automatic aggregation

**Chart Widget**:
- Bar or Line charts
- Multiple metrics on same chart
- X-axis dimension selection
- Interactive tooltips

#### Dashboard Features

- **12-column grid layout** with responsive breakpoints
- **Drag-and-drop positioning** (react-grid-layout)
- **Resizable widgets** (drag corners)
- **Auto-save layout** (persists on change)
- **Edit mode toggle** (view vs edit)
- **Widget configuration** (gear icon)
- **Date range picker** (per widget)

#### Multi-Table Support

Each widget can query a different BigQuery table:
1. Connect multiple tables via **"Info"** tab → **"Table Registry"**
2. When creating a widget, select which table to query
3. Each table has its own independent schema
4. Switch active table for dashboard-wide queries

### 3. Pivot Table Analysis

Powerful hierarchical drill-down with expandable rows.

#### Features

- **Dimension Selection**: Group by any dimension (n_words, channel, country, etc.)
- **Metric Selection**: Choose which metrics to display (multiselect)
- **Drill-Down**: Click row to expand → see search terms for that group
- **Pagination**: Navigate through expanded rows (100 per page)
- **Cumulative %**: Automatically calculated for each row
- **Total Row**: Sticky footer with aggregated totals
- **Column Visibility**: Hide/show columns dynamically

#### Usage Example

1. Navigate to **"Pivot Table"** tab
2. Select dimension (e.g., "Number of Words")
3. Select metrics to display (e.g., Queries, Revenue, CTR)
4. Click on a row (e.g., "3 words") to expand
5. View top search terms for that group
6. Use pagination to browse more terms

### 4. Query Logging & Cost Tracking

Monitor BigQuery usage and optimize costs.

#### Tracked Metrics

- **Query Type**: kpi, trends, search_terms, pivot, etc.
- **Bytes Processed**: Raw data scanned
- **Bytes Billed**: Rounded to 10MB minimum (BigQuery pricing)
- **Execution Time**: Milliseconds per query
- **Timestamp**: When query was executed
- **Parameters**: Filters, dimensions used

#### Usage Dashboard

View query logs and statistics:
- **Query History**: Table with all queries
- **Usage Stats**: Aggregate metrics
- **Daily Trends**: Time-series charts
- **Cost Analysis**: Estimated costs (based on $5/TB on-demand pricing)

#### API Endpoints

```bash
# Get query history
GET /api/logs/queries?limit=100

# Get aggregate usage stats
GET /api/logs/usage/stats

# Get daily usage time-series
GET /api/logs/usage/daily

# Clear old logs
DELETE /api/logs/clear?days=30
```

### 5. Advanced Filtering

Global filters applied across all dashboard sections.

#### Filter Types

**Date Range**:
- Start date and end date picker
- Enforced by server-side date clamping (if limits configured)

**Single-Value Filters**:
- Country (dropdown)
- Channel (dropdown)

**Range Filter**:
- Number of attributes (min/max)

**Boolean Filters**:
- 8 individual attribute checkboxes (categoria, tipo, genero, marca, color, material, talla, modelo)

#### How Filters Work

1. Click **"Filters"** button in header (slide-out panel)
2. Set your filter criteria
3. Click **"Apply Filters"**
4. All dashboard sections automatically refetch data with new filters
5. Filters persist in React Context across tab navigation

### 6. Standard Dashboard Sections

#### Overview
- 7 KPI cards: Queries, Revenue, Conversion Rate, CTR, Unique Terms, Rev/Query, AOV
- Trend indicators (up/down arrows)
- Percentage changes

#### Trends
- Time-series charts for queries, purchases, conversion rates
- Granularity: daily, weekly, monthly
- Interactive tooltips with exact values

#### Channels
- Channel comparison (App vs Web)
- KPI cards per channel
- 3 detailed comparison charts

#### Attributes
- Performance by number of attributes (0-8)
- Performance by number of words (1-10+)
- Individual attribute breakdown charts

#### Search Terms
- Top 100 search terms table
- Sortable columns (click headers)
- CSV export functionality
- Metrics: queries, purchases, revenue, CTR, conversion rate

## BigQuery Configuration

### Authentication Methods

#### Method 1: Application Default Credentials (ADC)

**Recommended for local development**

1. Install gcloud CLI: https://cloud.google.com/sdk/docs/install
2. Authenticate on your host machine:
   ```bash
   gcloud auth application-default login
   ```
3. Start Docker containers (credentials auto-mounted via volume)
4. In dashboard "Info" tab, select **"Use Application Default Credentials"**
5. Enter Project ID, Dataset, Table
6. Click **"Connect to BigQuery"**

**Pros**:
- No credential files to manage
- Uses your personal GCP credentials
- Easy credential rotation

**Cons**:
- Requires gcloud CLI installation
- Not suitable for production deployment

#### Method 2: Service Account JSON

**Recommended for production**

1. Create a service account in GCP Console
2. Grant **BigQuery Data Viewer** and **BigQuery Job User** roles
3. Download JSON key file
4. In dashboard "Info" tab, select **"Upload Service Account JSON"**
5. Paste JSON content
6. Enter Project ID, Dataset, Table
7. Click **"Connect to BigQuery"**

**Pros**:
- Works in any environment
- Suitable for production
- Fine-grained permissions

**Cons**:
- Must manage credential files securely
- Credential rotation requires manual update

### Date Range Limits (Access Control)

Restrict users to specific date ranges:

1. In BigQuery configuration, set:
   - **Allowed Min Date**: Earliest date users can query (e.g., 2024-01-01)
   - **Allowed Max Date**: Latest date users can query (e.g., 2024-12-31)
2. Server automatically clamps any date range queries to these limits
3. Users cannot access data outside the allowed range

**Use Cases**:
- Demo environments (limit to sample data period)
- Data privacy compliance (restrict historical data)
- Cost control (prevent large date range queries)

### Environment Variables (Alternative)

Instead of UI configuration, you can set environment variables in `docker-compose.yml`:

```yaml
backend:
  environment:
    BIGQUERY_PROJECT_ID: your-gcp-project-id
    BIGQUERY_DATASET: your_dataset_name
    BIGQUERY_TABLE: your_table_name
    # For service account (optional):
    BIGQUERY_CREDENTIALS_PATH: /app/credentials/service-account.json
```

## Configuration Reference

### Docker Compose Ports

**Default Ports** (configurable in `docker-compose.yml`):
- Frontend: `3000`
- Backend: `8000`

To change ports:
```yaml
frontend:
  ports:
    - "3001:3000"  # Host port 3001 → Container port 3000
backend:
  ports:
    - "8001:8000"  # Host port 8001 → Container port 8000
```

### Docker Volumes

**Persistent Data**:
- `bigquery_config`: BigQuery connection settings
- `~/.config/gcloud`: gcloud credentials (ADC mode, read-only)

**Configuration Files** (created automatically in volume):
```
/app/config/
├── bigquery_config.json        # BigQuery connection
├── schema_config.json          # Dynamic schema
├── custom_dimensions.json      # Custom dimensions
├── dashboards/                 # Dashboard configs
│   └── dashboard_{id}.json
└── query_logs.db              # SQLite usage logs
```

### BigQuery Table Schema

The dashboard works with **any table structure**, but here's a typical ecommerce search analytics schema:

```sql
CREATE TABLE `project.dataset.search_analytics` (
  date DATE,                      -- Date of the search
  country STRING,                 -- Country code (e.g., 'US', 'MX')
  channel STRING,                 -- Channel (e.g., 'App', 'Web')
  search_term STRING,             -- The actual search query
  n_words_normalized INT64,       -- Number of words in search
  n_attributes INT64,             -- Total attributes identified

  -- Boolean attribute flags
  attr_categoria BOOLEAN,
  attr_tipo BOOLEAN,
  attr_genero BOOLEAN,
  attr_marca BOOLEAN,
  attr_color BOOLEAN,
  attr_material BOOLEAN,
  attr_talla BOOLEAN,
  attr_modelo BOOLEAN,

  -- Metrics
  queries INT64,                  -- Number of queries
  queries_pdp INT64,              -- Queries → Product Detail Page
  queries_a2c INT64,              -- Queries → Add to Cart
  purchases INT64,                -- Number of purchases
  gross_purchase FLOAT64          -- Revenue generated
);
```

**Note**: Your table doesn't need to match this schema. The dynamic schema system will automatically detect and adapt to your columns.

## Cost Optimization

BigQuery charges based on data processed. Here's how to minimize costs:

### Understanding BigQuery Pricing

**On-Demand Pricing**: $5 per TB of data processed
- Minimum 10 MB per query (rounds up)
- First 1 TB per month is free
- Costs can add up quickly with large tables

### Optimization Tips

1. **Use Date Filters**: Always filter by date range to reduce data scanned
   ```sql
   WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
   ```

2. **Limit Date Ranges**: Set `allowed_min_date` and `allowed_max_date` in config

3. **Partition Tables**: Use date partitioning for large tables
   ```sql
   CREATE TABLE dataset.table
   PARTITION BY date
   AS SELECT ...
   ```

4. **Cluster Tables**: Add clustering on frequently filtered columns
   ```sql
   CREATE TABLE dataset.table
   PARTITION BY date
   CLUSTER BY country, channel
   AS SELECT ...
   ```

5. **Monitor Usage**: Check query logs in dashboard to identify expensive queries

6. **Use Calculated Metrics**: Compute complex metrics in the app (free) rather than BigQuery
   - Example: `{purchases} / {queries}` computed post-aggregation

7. **Limit Search Terms Queries**: The search terms table can scan lots of data
   - Use the `limit` parameter (default: 100)
   - Filter by specific dimensions first

### Estimated Costs

For a typical 100 GB table:
- **Overview query**: ~1 GB scanned = $0.005
- **Trends query**: ~5 GB scanned = $0.025
- **Search terms query**: ~10 GB scanned = $0.05
- **Pivot table query**: ~2 GB scanned = $0.01

**Daily usage estimate** (10 dashboard views): ~$1-2/day = $30-60/month

**For detailed cost analysis**, see `BIGQUERY_COST_GUIDE.md`

## Development

### Running Without Docker

**Backend**:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend**:
```bash
cd frontend
npm install
npm run dev  # Runs on port 3000
```

**Frontend Production Build**:
```bash
cd frontend
npm run build  # Production build
npm start      # Run production server
```

### Project Structure

```
├── docker-compose.yml          # Orchestrates frontend + backend
│
├── backend/                    # Python FastAPI service (7,460 lines)
│   ├── main.py                # 50+ API endpoints (1,521 lines)
│   ├── config.py              # Multi-table registry, dashboard registry
│   ├── requirements.txt       # 9 dependencies
│   ├── models/
│   │   └── schemas.py         # 60+ Pydantic models (615 lines)
│   └── services/
│       ├── bigquery_service.py      # Dynamic SQL generation (1,321 lines)
│       ├── data_service.py          # Business logic (1,499 lines)
│       ├── schema_service.py        # Schema auto-detection (853 lines)
│       ├── metric_service.py        # Custom metrics (795 lines)
│       ├── dimension_service.py     # Dimension management (98 lines)
│       ├── custom_dimension_service.py  # Custom dimensions (149 lines)
│       ├── query_logger.py          # Usage tracking (389 lines)
│       └── date_resolver.py         # Date parsing (219 lines)
│
└── frontend/                   # Next.js React app
    ├── app/
    │   └── page.tsx           # Main router (4 tabs)
    ├── components/
    │   ├── dashboards/        # Custom dashboard UI (5 components)
    │   ├── layout/            # Main layout with tabs (1 component)
    │   ├── modals/            # Configuration dialogs (5 modals)
    │   ├── pivot/             # Pivot table UI (3 components)
    │   ├── sections/          # Main dashboard views (6 sections)
    │   ├── widgets/           # Dashboard widgets (4 widgets)
    │   └── ui/                # Shared UI components
    ├── hooks/                 # Custom React hooks
    ├── lib/
    │   ├── api.ts            # API client (~800 lines)
    │   ├── types.ts          # TypeScript types
    │   └── contexts/         # React contexts (filter state, etc.)
    └── package.json          # 23 dependencies
```

### Adding Features

#### Add New API Endpoint

1. Add route handler in `backend/main.py`:
   ```python
   @app.get("/api/my-endpoint")
   async def my_endpoint(filters: FilterParams = Depends()):
       return data_service.get_my_data(filters)
   ```

2. Add business logic in `backend/services/data_service.py`:
   ```python
   def get_my_data(self, filters):
       query = bigquery_service.build_my_query(filters)
       return bigquery_service.execute_query(query)
   ```

3. Add client function in `frontend/lib/api.ts`:
   ```typescript
   export const getMyData = async (filters) => {
     const response = await fetch(`${API_URL}/api/my-endpoint?${buildQueryString(filters)}`);
     return response.json();
   };
   ```

4. Use in React component:
   ```typescript
   const { data, isLoading } = useQuery({
     queryKey: ['myData', filters],
     queryFn: () => getMyData(filters)
   });
   ```

#### Add New Dashboard Section

1. Create component in `frontend/components/sections/my-section.tsx`
2. Use `useFilters()` hook to get filter state
3. Fetch data with `useQuery` from React Query
4. Add tab to navigation in `frontend/app/page.tsx`

#### Add New Filter

1. Update `FilterParams` model in `backend/models/schemas.py`
2. Update `build_filter_clause()` in `backend/services/bigquery_service.py`
3. Add UI control in `frontend/components/layout/filter-sidebar.tsx`
4. Update `FilterState` type in `frontend/lib/contexts/filter-context.tsx`

## Troubleshooting

### Connection Issues

**Problem**: "BigQuery not configured" error

**Solutions**:
1. Navigate to Info tab and configure BigQuery
2. Check environment variables in `docker-compose.yml`
3. Verify gcloud credentials: `gcloud auth application-default print-access-token`
4. Check Docker volume mounts for credentials

---

**Problem**: "Permission denied" or "Access denied" errors

**Solutions**:
1. Verify service account has **BigQuery Data Viewer** role
2. Verify service account has **BigQuery Job User** role
3. Check project ID matches the one in GCP Console
4. Ensure dataset/table exists and is accessible

---

### Authentication Issues

**Problem**: ADC not working (Application Default Credentials)

**Solutions**:
1. Run `gcloud auth application-default login` on host machine
2. Verify `~/.config/gcloud` directory exists on host
3. Check Docker volume mount in `docker-compose.yml`:
   ```yaml
   volumes:
     - ~/.config/gcloud:/root/.config/gcloud:ro
   ```
4. Restart containers: `docker-compose down && docker-compose up`

---

**Problem**: Service account JSON not accepted

**Solutions**:
1. Verify JSON is valid (paste in JSON validator)
2. Ensure JSON contains `type`, `project_id`, `private_key` fields
3. Check for extra characters or formatting issues
4. Try downloading a fresh JSON from GCP Console

---

### Data Issues

**Problem**: "No data found" or empty charts

**Solutions**:
1. Verify date range includes data (check in BigQuery Console)
2. Check filters aren't too restrictive (reset filters)
3. Verify table name is correct (case-sensitive)
4. Check date column format matches `DATE` type
5. Look for errors in browser console (F12)

---

**Problem**: Schema detection fails

**Solutions**:
1. Verify table exists in BigQuery
2. Check table has at least one row of data
3. Ensure service account can read table schema
4. Try manual schema reset: `POST /api/schema/reset`
5. Check backend logs: `docker-compose logs backend`

---

### Performance Issues

**Problem**: Queries are slow (>5 seconds)

**Solutions**:
1. Add date partitioning to BigQuery table
2. Add clustering on frequently filtered columns
3. Reduce date range in filters
4. Check query logs for bytes processed
5. Consider upgrading to BigQuery Flat-Rate pricing

---

**Problem**: High BigQuery costs

**Solutions**:
1. Review query logs to identify expensive queries
2. Implement date range limits in config
3. Use partitioned and clustered tables
4. Limit search terms queries (reduce `limit` parameter)
5. See **Cost Optimization** section above

---

### Docker Issues

**Problem**: Port already in use (3000 or 8000)

**Solutions**:
1. Change ports in `docker-compose.yml`:
   ```yaml
   frontend:
     ports:
       - "3001:3000"
   ```
2. Stop other services using those ports
3. Check for zombie processes: `lsof -i :3000`

---

**Problem**: Containers crash on startup

**Solutions**:
1. Check logs: `docker-compose logs backend` and `docker-compose logs frontend`
2. Verify `requirements.txt` and `package.json` are valid
3. Rebuild without cache: `docker-compose build --no-cache`
4. Check disk space: `df -h`
5. Increase Docker memory limit (Settings → Resources)

---

### CORS Issues

**Problem**: "CORS policy blocked" errors

**Solutions**:
1. Verify `NEXT_PUBLIC_API_URL` matches backend URL
2. Check `allow_origins` in `backend/main.py` includes frontend URL:
   ```python
   app.add_middleware(
       CORSMiddleware,
       allow_origins=["http://localhost:3000"],
   )
   ```
3. Restart backend container after changes

---

### Debug Mode

Enable detailed logging:

**Backend**:
```bash
docker-compose logs -f backend
```

**Frontend**:
```bash
docker-compose logs -f frontend
```

**BigQuery Query Logs**:
View in dashboard → Query Logs section

## API Reference

The backend exposes 50+ REST API endpoints. Full interactive documentation available at:

**http://localhost:8000/docs** (Swagger UI)

### Major Endpoint Groups

#### BigQuery Configuration
- `POST /api/bigquery/configure` - Connect to BigQuery
- `POST /api/bigquery/disconnect` - Disconnect and clear credentials

#### Schema Management (24 endpoints)
- `GET /api/schema` - Get current schema
- `POST /api/schema/detect` - Auto-detect from BigQuery
- `POST /api/schema/reset` - Reset to defaults
- `GET/POST/PUT/DELETE /api/metrics/base` - Base metrics CRUD
- `GET/POST/PUT/DELETE /api/metrics/calculated` - Calculated metrics CRUD
- `POST /api/metrics/validate-formula` - Test formula
- `GET/POST/PUT/DELETE /api/dimensions` - Dimensions CRUD

#### Dashboard & Widgets (8 endpoints)
- `GET/POST /api/dashboards` - List/create dashboards
- `GET/PUT/DELETE /api/dashboards/{id}` - Dashboard CRUD
- `POST /api/dashboards/{id}/widgets` - Add widget
- `PUT/DELETE /api/dashboards/{id}/widgets/{widget_id}` - Widget CRUD

#### Multi-Table Support (9 endpoints)
- `GET/POST /api/tables` - List/create tables
- `GET/PUT/DELETE /api/tables/{id}` - Table CRUD
- `POST /api/tables/{id}/activate` - Switch active table
- `POST /api/tables/copy-schema` - Copy schema between tables

#### Data Queries
- `GET /api/overview` - KPI metrics
- `GET /api/trends?granularity=daily` - Time-series data
- `GET /api/breakdown/{dimension}?limit=20` - Dimension breakdown
- `GET /api/search-terms?limit=100&sort_by=queries` - Top search terms
- `GET /api/pivot?dimension=n_words` - Pivot table data
- `GET /api/pivot/{dimension}/{value}/children` - Drill-down data

#### Query Logging (4 endpoints)
- `GET /api/logs/queries` - Query history
- `GET /api/logs/usage/stats` - Aggregate stats
- `GET /api/logs/usage/daily` - Daily usage
- `DELETE /api/logs/clear` - Clear old logs

#### Filters
- `GET /api/filters/options` - Available filter values

#### Custom Dimensions (4 endpoints)
- `GET/POST /api/custom-dimensions` - List/create
- `PUT/DELETE /api/custom-dimensions/{id}` - CRUD

**All endpoints accept filter query parameters**:
- `start_date`, `end_date`: Date range (YYYY-MM-DD)
- `country`, `channel`: Single value filters
- `n_attributes_min`, `n_attributes_max`: Range filter
- `attr_categoria`, `attr_tipo`, etc.: Boolean filters

## FAQ

### Can I use this with other databases besides BigQuery?

Currently, the dashboard is designed specifically for BigQuery. However, the architecture is modular:
- Backend services are in `services/` directory
- Creating a new data service for another database (PostgreSQL, MySQL, Snowflake) would require:
  1. Implementing a new service similar to `bigquery_service.py`
  2. Updating `data_service.py` to use the new service
  3. Ensuring the service supports the same query patterns

### Can I connect to multiple BigQuery projects?

Yes! The multi-table support allows you to connect to tables in different projects:
1. Navigate to Info tab → Table Registry
2. Add multiple tables with different project IDs
3. Each widget can query a different project/dataset/table

### How much does BigQuery cost for typical usage?

For a 100 GB table with 10 dashboard views per day:
- **Cost per day**: $1-2
- **Cost per month**: $30-60
- **First 1 TB/month is free**, so small-scale usage may be free

See **Cost Optimization** section for tips to reduce costs.

### Is my data secure?

Yes:
- Credentials stored only in Docker volumes (not in codebase)
- Service account JSON secured in container
- No data cached or stored (queries BigQuery on-demand)
- CORS restrictions prevent unauthorized access
- Date limits enforce access control
- All communication over HTTP (HTTPS in production)

### Can I deploy this to production?

Yes, but consider:
1. Use **service account JSON** (not ADC)
2. Set up **HTTPS** with SSL certificates
3. Configure **firewall rules** to restrict access
4. Set **date range limits** for access control
5. Use **BigQuery Flat-Rate pricing** for predictable costs
6. Enable **authentication** (add auth middleware)
7. Use **managed database** for query logs (not SQLite)
8. Monitor **query logs** and set up alerts

### Why BigQuery instead of CSV files?

BigQuery offers several advantages:
- **Scalability**: Handle TB-scale datasets
- **Performance**: Parallel query execution
- **Real-time**: No ETL lag, query live data
- **Cost-effective**: Pay only for queries (on-demand)
- **Collaboration**: Shared data access
- **Security**: Fine-grained access control
- **Integration**: Part of Google Cloud ecosystem

### Can I customize the default metrics and dimensions?

Absolutely! You have several options:
1. **Let auto-detection run**, then add/remove metrics via API
2. **Create calculated metrics** with custom formulas
3. **Add custom dimensions** with SQL expressions
4. **Modify schema config file** directly (`/app/config/schema_config.json`)
5. **Use API endpoints** to programmatically update schema

### How do I export data?

Multiple export options:
1. **CSV Export**: Click "Export CSV" button in search terms table
2. **API Access**: Query any endpoint and save JSON response
3. **BigQuery Console**: Query directly in BigQuery for advanced exports
4. **Custom Integration**: Build on top of the REST API

## Additional Resources

### Documentation Files

- **CLAUDE.md** - Comprehensive developer guide for AI assistance
- **DASHBOARDS_FEATURE.md** - Detailed custom dashboards tutorial (510 lines)
- **BIGQUERY_COST_GUIDE.md** - Cost optimization guide (254 lines)
- **FEATURES_COMPLETE.md** - Feature completion checklist
- **MIGRATION_README.md** - Streamlit to Next.js migration details
- **QUICKSTART.md** - Quick start guide

### External Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [BigQuery Pricing](https://cloud.google.com/bigquery/pricing)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Next.js Documentation](https://nextjs.org/docs)
- [React Query Documentation](https://tanstack.com/query/latest)

### Tech Stack Links

- [Next.js 14](https://nextjs.org/)
- [React 18](https://react.dev/)
- [TypeScript](https://www.typescriptlang.org/)
- [Tailwind CSS](https://tailwindcss.com/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Pydantic](https://docs.pydantic.dev/)
- [Google Cloud BigQuery](https://cloud.google.com/bigquery)
- [Docker](https://www.docker.com/)
- [Recharts](https://recharts.org/)
- [React Query](https://tanstack.com/query/latest)

## Support

For detailed architecture information and development guidance, see **CLAUDE.md**.

For BigQuery setup and troubleshooting, visit the **Info** tab at http://localhost:3000.

For cost optimization strategies, see **BIGQUERY_COST_GUIDE.md**.

---

**Built with** Next.js, FastAPI, BigQuery, and Docker
