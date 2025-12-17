# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A search analytics dashboard for analyzing customer search behavior in ecommerce platforms. The application queries BigQuery for search data and provides interactive visualizations through a modern React frontend.

**Stack:** Next.js 14 (React + TypeScript) frontend + Django REST Framework (Python) backend + PostgreSQL + Google BigQuery

## Running the Application

```bash
# Start everything with Docker
docker-compose up --build

# Access:
# - Frontend Dashboard: http://localhost:3000
# - Backend API: http://localhost:8000
```

## Architecture Overview

### Data Flow
1. **BigQuery** stores search analytics data
2. **Django backend** (`backend_django/`) queries BigQuery and serves REST API
3. **PostgreSQL** stores application data (users, dashboards, schemas, rollups)
4. **Next.js frontend** (`frontend/`) fetches data via API and renders dashboard
5. **Docker Compose** orchestrates all services

### Project Structure
```
.
├── backend_django/          # Django REST Framework backend
│   ├── apps/
│   │   ├── analytics/       # BigQuery queries, pivot tables, significance tests
│   │   ├── dashboards/      # Dashboard and widget management
│   │   ├── schemas/         # Dynamic schema configuration
│   │   ├── rollups/         # Pre-aggregated rollup tables
│   │   ├── tables/          # BigQuery table configuration
│   │   ├── users/           # Authentication
│   │   ├── organizations/   # Multi-tenancy
│   │   └── credentials/     # GCP credential management
│   └── search_analytics/    # Django project settings
├── frontend/                # Next.js frontend
│   ├── app/                 # Pages (App Router)
│   ├── components/          # React components
│   ├── hooks/               # Custom hooks
│   └── lib/                 # API client, contexts
└── docker-compose.yml
```

### Key Backend Apps

**analytics/** - Core data querying
- `services/bigquery_service.py` - BigQuery client, SQL generation
- `services/data_service.py` - Business logic, metric calculations
- `services/query_router_service.py` - Routes queries to rollups or raw tables
- `services/statistical_service.py` - Significance testing (z-test, proportion tests)
- `views.py` - API endpoints for pivot, significance tests

**schemas/** - Dynamic schema system
- Auto-detects columns from BigQuery tables
- Supports base metrics (SUM, COUNT, etc.) and calculated metrics (formulas)
- Dimensions with filter types (select, multi_select, date_range, boolean)

**rollups/** - Pre-aggregation system
- Creates pre-aggregated tables for faster queries
- Query router automatically selects optimal rollup based on dimensions/metrics

**dashboards/** - Dashboard management
- Widgets with chart configurations
- Standalone widgets (drafts) and dashboard-attached widgets

### Key Frontend Components

- `components/sections/pivot-table-section.tsx` - Main pivot table with multi-table mode
- `components/pivot/` - Pivot configuration panels
- `components/widgets/chart-widget.tsx` - Chart rendering
- `hooks/use-schema.ts` - Schema data fetching
- `lib/api.ts` - API client

## Key Features

### Multi-Table Pivot Mode
Compare metrics across different dimension filters (e.g., control vs treatment groups):
- First column defines the dimension values
- Additional columns show metrics for different filters
- Supports significance testing between columns

### Significance Testing
Statistical comparison between groups:
- Z-test for continuous metrics
- Proportion test for percentage metrics
- Shows p-values and confidence indicators

### Rollup System
Pre-aggregated tables for performance:
- Query router scores rollups based on dimensions/metrics needed
- Falls back to raw table when no suitable rollup exists
- Supports re-aggregation across dates for SUM metrics

### Dynamic Schema
- Auto-detection from BigQuery table structure
- Custom calculated metrics with formula syntax: `{metric1} / {metric2}`
- Configurable dimensions with filter types

## Common Tasks

### Adding a New API Endpoint
1. Add view in `backend_django/apps/<app>/views.py`
2. Add URL in `backend_django/apps/<app>/urls.py`
3. Add serializer if needed in `serializers.py`
4. Add client function in `frontend/lib/api.ts`

### Working with BigQuery
- Service: `backend_django/apps/analytics/services/bigquery_service.py`
- Uses Application Default Credentials (ADC)
- Run `gcloud auth application-default login` on host machine

### Database Migrations
```bash
docker-compose exec backend_django python manage.py makemigrations
docker-compose exec backend_django python manage.py migrate
```

## Environment Variables

Key variables in `docker-compose.yml`:
- `POSTGRES_*` - Database connection
- `DJANGO_SECRET_KEY` - Django secret
- `GOOGLE_APPLICATION_CREDENTIALS` - GCP credentials path
- `NEXT_PUBLIC_API_URL` - Backend URL for frontend
