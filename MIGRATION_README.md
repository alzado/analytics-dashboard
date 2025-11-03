# Search Analytics Dashboard - Next.js Migration

This document describes the new React/Next.js dashboard that replaces the Streamlit version.

## Architecture

### Stack
- **Frontend**: Next.js 14 + React + TypeScript
- **Styling**: Tailwind CSS
- **Charts**: Recharts
- **State Management**: React Query (@tanstack/react-query)
- **Backend**: Python FastAPI
- **Deployment**: Docker Compose

### Project Structure

```
├── docker-compose.yml          # Orchestrates frontend + backend
├── backend/                    # Python FastAPI service
│   ├── Dockerfile
│   ├── main.py                # FastAPI app with endpoints
│   ├── requirements.txt
│   ├── models/
│   │   └── schemas.py         # Pydantic models
│   └── services/
│       └── data_service.py    # Data processing logic
│
└── frontend/                   # Next.js app
    ├── Dockerfile
    ├── package.json
    ├── app/
    │   ├── layout.tsx         # Root layout
    │   ├── page.tsx           # Main dashboard page
    │   └── globals.css        # Global styles
    ├── components/
    │   ├── layout/
    │   │   └── dashboard-layout.tsx
    │   ├── sections/
    │   │   ├── overview-section.tsx
    │   │   ├── trends-section.tsx
    │   │   └── search-terms-section.tsx
    │   └── ui/
    │       └── kpi-card.tsx
    └── lib/
        ├── types.ts           # TypeScript interfaces
        ├── api.ts             # API client functions
        └── providers/
            └── query-provider.tsx
```

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Your CSV data file (`bquxjob_6268ac9c_19a163e3339.csv`) in the root directory

### Running the Application

1. **Start all services with Docker Compose:**
   ```bash
   docker-compose up --build
   ```

   This will start:
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - Backend API docs: http://localhost:8000/docs

2. **Access the dashboard:**
   Open http://localhost:3000 in your browser

### Development Mode

For development without Docker:

**Backend:**
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

## Features Implemented

### ✅ Core Features
- **Docker Setup**: Complete containerization with docker-compose
- **Backend API**: FastAPI with endpoints for:
  - Overview metrics (`/api/overview`)
  - Trend data (`/api/trends`)
  - Dimension breakdown (`/api/breakdown/{dimension}`)
  - Search terms (`/api/search-terms`)
  - Filter options (`/api/filters/options`)
- **Frontend Framework**: Next.js 14 with App Router
- **Data Fetching**: React Query for caching and state management
- **TypeScript**: Full type safety across the application
- **Responsive Layout**: Mobile-friendly navigation
- **Dashboard Sections**:
  - **Overview**: KPI cards showing key metrics
  - **Trends**: Time-series charts for queries, purchases, conversion rates
  - **Search Terms**: Table of top search terms with metrics

### 🚧 Features to Add/Enhance

**High Priority:**
1. **Filters Sidebar**:
   - Date range picker
   - Country/Channel dropdowns
   - Attribute checkboxes
   - Apply/Reset buttons

2. **Additional Charts**:
   - Channel performance comparison
   - N-words analysis
   - N-attributes breakdown
   - Heatmaps for cross-tab analysis

3. **Enhanced Tables**:
   - Sorting
   - Pagination
   - Export to CSV

**Medium Priority:**
4. **Custom Dashboards**:
   - Drag-and-drop widget builder
   - Save layouts to localStorage
   - Widget configuration

5. **Period Comparison**:
   - Compare two date ranges
   - Show deltas in KPI cards

**Low Priority:**
6. **Advanced Features**:
   - Real-time updates
   - User authentication
   - Saved views
   - Annotations on charts

## API Endpoints

### Backend Endpoints

**GET `/api/overview`**
- Query params: `start_date`, `end_date`, `country`, `channel`, `n_attributes_min`, `n_attributes_max`
- Returns: Overview metrics

**GET `/api/trends`**
- Query params: same as overview + `granularity` (daily/weekly/monthly)
- Returns: Array of time-series data points

**GET `/api/breakdown/{dimension}`**
- Dimensions: `channel`, `n_words`, `n_attributes`
- Query params: filters + `limit`
- Returns: Array of dimension breakdowns

**GET `/api/search-terms`**
- Query params: filters + `limit`, `sort_by`
- Returns: Array of search term metrics

**GET `/api/filters/options`**
- Returns: Available filter values (countries, channels, date range, attributes)

## Component Guide

### Adding a New Dashboard Section

1. Create a new section component in `frontend/components/sections/`:

```typescript
'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchYourData } from '@/lib/api'

export function YourSection() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['your-data-key'],
    queryFn: () => fetchYourData({}),
  })

  if (isLoading) return <div>Loading...</div>
  if (error) return <div>Error loading data</div>

  return (
    <div>
      {/* Your component JSX */}
    </div>
  )
}
```

2. Add the section to `frontend/app/page.tsx`:

```typescript
{activeTab === 'your-tab' && <YourSection />}
```

3. Add the tab to the navigation in `dashboard-layout.tsx`

### Creating a Chart Component

Use Recharts for visualizations:

```typescript
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

export function MyChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="value" stroke="#3b82f6" strokeWidth={2} />
      </LineChart>
    </ResponsiveContainer>
  )
}
```

## Migration from Streamlit

### What Changed
- **No more st.cache_data**: Replaced with React Query caching
- **No more st.session_state**: Use React state (`useState`, `useContext`)
- **No more st.sidebar**: Custom sidebar component with Tailwind
- **No more st.dataframe**: Custom table components or libraries like TanStack Table

### Mapping Streamlit → React

| Streamlit | React Equivalent |
|-----------|-----------------|
| `st.title()` | `<h1 className="text-2xl font-bold">` |
| `st.header()` | `<h2 className="text-lg font-semibold">` |
| `st.metric()` | `<KPICard />` component |
| `st.plotly_chart()` | Recharts components |
| `st.dataframe()` | `<table>` or TanStack Table |
| `st.selectbox()` | `<select>` with `onChange` |
| `st.date_input()` | date picker library (e.g., react-datepicker) |

## Troubleshooting

### Backend not connecting
- Check if backend container is running: `docker-compose ps`
- Check backend logs: `docker-compose logs backend`
- Verify data file is mounted: `docker-compose exec backend ls -la /app/data/`

### Frontend build errors
- Clear node_modules: `rm -rf frontend/node_modules frontend/.next`
- Rebuild: `docker-compose up --build frontend`

### CORS errors
- Backend CORS middleware is configured to allow `localhost:3000`
- If using different port, update `backend/main.py` CORS settings

### Data not loading
- Check if CSV file path is correct in `docker-compose.yml`
- Verify backend startup logs: should see "Data loaded: X rows"

## Next Steps

1. **Add Filters**: Implement the filter sidebar component
2. **More Charts**: Add remaining visualizations from Streamlit version
3. **Testing**: Add unit tests and E2E tests
4. **Production Build**: Create production Docker images
5. **Deployment**: Set up CI/CD pipeline

## Resources

- [Next.js Documentation](https://nextjs.org/docs)
- [React Query Documentation](https://tanstack.com/query/latest)
- [Recharts Documentation](https://recharts.org)
- [Tailwind CSS Documentation](https://tailwindcss.com/docs)
- [FastAPI Documentation](https://fastapi.tiangolo.com)

## Support

For questions or issues:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review Docker logs: `docker-compose logs`
3. Check API docs: http://localhost:8000/docs
