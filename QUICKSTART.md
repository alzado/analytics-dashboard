# Quick Start Guide

## 🚀 Running the New Dashboard

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

## 📁 What Was Created

### Backend (Python FastAPI)
✅ Complete REST API with 5 endpoints
✅ Data processing service with filtering
✅ Pydantic models for type safety
✅ CORS configuration
✅ Docker containerization

### Frontend (Next.js + React) - FEATURE COMPLETE!
✅ **5 Dashboard Sections**:
  - Overview (7 KPI cards)
  - Trends (2 time-series charts)
  - Channels (3 comparison charts)
  - Attributes (8 analysis charts)
  - Search Terms (sortable table with export)

✅ **Complete Filter System**:
  - Date range picker
  - Country & Channel dropdowns
  - Number of attributes range
  - 8 Individual attribute checkboxes
  - Slide-out filter sidebar
  - Global filter state management

✅ **Advanced Features**:
  - CSV export from tables
  - Column sorting (click to sort)
  - 15+ Interactive Recharts
  - React Query caching
  - TypeScript throughout
  - Fully mobile-responsive
  - Professional loading & error states

### Infrastructure
✅ Docker Compose orchestration
✅ Hot-reload for development
✅ React Context for global state
✅ Comprehensive documentation

## ✨ All Core Features Complete!

### ✅ Implemented
- [x] **Filter Sidebar**: Full filter controls with date, country, channel, attributes
- [x] **All Charts**: Channel comparison, n-words, n-attributes analysis (15+ charts!)
- [x] **Table Features**: Sorting by multiple columns, CSV export
- [x] **Mobile Responsive**: Works beautifully on all screen sizes
- [x] **Professional UI**: Slide-out sidebar, loading states, error handling

### 🎯 Optional Enhancements (Future)
- [ ] **Period Comparison**: Side-by-side date range comparison
- [ ] **Custom Dashboards**: Drag-and-drop widget configuration
- [ ] **Real-time Updates**: WebSocket for live data
- [ ] **Authentication**: User login and saved views
- [ ] **Advanced Analytics**: Cohort analysis, funnels

## 🛠️ Quick Modifications

### Add a New KPI Card

Edit `frontend/components/sections/overview-section.tsx`:

```typescript
<KPICard
  title="Your Metric"
  value={`${metrics.your_value}`}
  trend="+5.2%"
/>
```

### Add a New Chart

1. Create component in `frontend/components/charts/`
2. Import Recharts components
3. Fetch data with React Query
4. Add to relevant section

### Add New API Endpoint

1. Add route to `backend/main.py`
2. Add function to `backend/services/data_service.py`
3. Update `frontend/lib/api.ts` with client function
4. Use in component with `useQuery`

## 📊 All Features

### 1. Overview Tab
- 7 KPI cards: Queries, Revenue, CVR, CTR, Unique Terms, Rev/Query, AOV
- Color-coded trends
- Responsive grid layout

### 2. Trends Tab
- Query and purchase trends over time
- Conversion rate and CTR trends
- Interactive line charts with date formatting

### 3. Channels Tab ⭐ NEW!
- Channel KPI cards with detailed metrics
- CTR comparison bar chart
- Conversion rate comparison chart
- Revenue by channel visualization

### 4. Attributes Tab ⭐ NEW!
- **Performance by # Attributes** (3 charts):
  - Query distribution pie chart
  - Conversion rate by attributes
  - Revenue by attributes
- **Performance by # Words** (4 charts):
  - Query volume by word count
  - CTR by word count
  - Conversion rate analysis
  - Revenue per query analysis

### 5. Search Terms Tab
- Top 100 search terms
- **Sortable columns**: Click to sort by Queries, Purchases, or Revenue
- **CSV Export button**: Download filtered data
- Hover effects and professional styling

### Filter Sidebar ⭐ NEW!
- Click "Filters" button in header
- Date range picker (start & end)
- Country dropdown
- Channel dropdown
- Number of attributes (min/max)
- 8 Attribute checkboxes
- Apply/Reset buttons
- **All sections auto-update** when filters change!

## 🐛 Troubleshooting

**Nothing loads in browser:**
```bash
docker-compose logs frontend
docker-compose logs backend
```

**Backend can't find data:**
- Check `docker-compose.yml` volume mount for CSV file
- Verify CSV filename matches

**Port already in use:**
```bash
# Change ports in docker-compose.yml
ports:
  - "3001:3000"  # Frontend
  - "8001:8000"  # Backend
```

## 📚 Learn More

See `MIGRATION_README.md` for:
- Detailed architecture
- API documentation
- Component guide
- Streamlit → React migration tips

## 🎉 You're Ready!

Your new dashboard is faster, more dynamic, and mobile-friendly. Start by running:

```bash
docker-compose up --build
```

Then visit http://localhost:3000 to see your data visualized in a modern React interface!
