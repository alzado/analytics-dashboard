# BigQuery Query Cost Guide

## Understanding BigQuery Data Consumption

BigQuery charges based on **bytes scanned**, not bytes returned. This guide explains which operations in this application consume more data.

---

## Query Types (Ranked by Cost)

### 🔴 MOST EXPENSIVE: `query_all` (SELECT *)
**What it does:** Returns ALL columns and ALL matching rows
**SQL Pattern:** `SELECT * FROM table WHERE filters`
**Why expensive:** Scans every single column in your table
**Data scanned:** Full table scan (all columns × filtered rows)
**Used by:** Export features, bulk data retrieval

**Example:**
- Table has 50 columns
- Query matches 1M rows
- **Scans:** All 50 columns × 1M rows = HUGE data consumption

**💡 Tip:** Use filters to reduce rows. But even with filters, SELECT * scans all columns.

---

### 🟠 EXPENSIVE: `search_terms` Query
**What it does:** Returns individual search terms with their metrics
**SQL Pattern:**
```sql
SELECT
  search_term,
  SUM(queries), SUM(queries_pdp), SUM(queries_a2c),
  SUM(purchases), SUM(gross_purchase), ...
FROM table
WHERE filters
GROUP BY search_term
ORDER BY metric DESC
LIMIT 1000
```
**Why expensive:**
- Scans all rows (even with filters)
- Groups by high-cardinality dimension (search_term)
- Returns many rows (up to 1000)

**Data scanned:** All metric columns × filtered rows
**Used by:** "Search Terms" tab, pivot table drill-downs

**💡 Tip:**
- Use date filters to reduce rows scanned
- Limit results (LIMIT 100 vs 1000)
- Filter by country/channel to scan fewer rows

---

### 🟡 MODERATE: `trends` & `breakdown` Queries
**What it does:** Aggregates data by dimension (date, channel, n_words, etc.)
**SQL Pattern:**
```sql
SELECT
  dimension,
  SUM(queries), SUM(queries_pdp), ...
FROM table
WHERE filters
GROUP BY dimension
```
**Why moderate:**
- Scans all rows (but filtered)
- Only scans needed columns (not SELECT *)
- Returns aggregated results (fewer rows)

**Data scanned:** Selected columns × filtered rows
**Used by:** Trends charts, breakdown by channel/attributes

**💡 Tip:** Date filters have the biggest impact on reducing scanned bytes.

---

### 🟢 CHEAP: `kpi` Metrics
**What it does:** Single aggregation across entire filtered dataset
**SQL Pattern:**
```sql
SELECT
  SUM(queries) as total_queries,
  SUM(purchases) as total_purchases,
  COUNT(DISTINCT search_term) as unique_terms,
  ...
FROM table
WHERE filters
```
**Why cheap:**
- Scans all filtered rows (same as others)
- But returns only 1 row with totals
- Efficient aggregation

**Data scanned:** Selected columns × filtered rows
**Used by:** Overview/KPI cards on dashboard

**💡 Note:** Still scans all filtered rows, but very efficient for single totals.

---

### 🟢 VERY CHEAP: `dimension_values` Query
**What it does:** Gets list of unique values for dropdowns
**SQL Pattern:**
```sql
SELECT DISTINCT dimension
FROM table
WHERE filters
ORDER BY dimension
```
**Why very cheap:**
- Only scans 1 column
- Returns deduplicated list
- Usually cached

**Data scanned:** 1 column × filtered rows
**Used by:** Filter dropdowns (country, channel, etc.)

---

## Cost Optimization Tips

### 1. **Date Filtering is KEY** 🗓️
BigQuery scans are row-based. Fewer rows = lower cost.

**Example:**
- Full year: Scans 365 days of data
- 1 month: Scans 30 days of data (12x cheaper!)
- 1 week: Scans 7 days of data (52x cheaper!)

### 2. **Use Partitioned Tables** (if possible) 📊
If your BigQuery table is partitioned by `date`:
- Queries with date filters ONLY scan relevant partitions
- Massive cost savings (can be 10-100x cheaper)

**Check if your table is partitioned:**
```sql
SELECT
  table_name,
  partition_field,
  partition_type
FROM `project.dataset.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'your_table'
```

### 3. **Add Filters Early** 🔍
Every filter reduces rows scanned:
- Date filter: Usually reduces by 90%+
- Country filter: Reduces by 50-90%
- Channel filter: Reduces by ~50%

**Example Query Cost:**
- No filters: Scans 100M rows
- + Date filter (30 days): Scans 8M rows (12x cheaper)
- + Country filter (Chile): Scans 800K rows (125x cheaper!)

### 4. **Avoid `SELECT *`** ⚠️
Only select columns you need:

**Bad (expensive):**
```sql
SELECT * FROM table  -- Scans all 50 columns
```

**Good (cheaper):**
```sql
SELECT search_term, queries, purchases  -- Scans only 3 columns
FROM table
```

### 5. **Cache Results** 💾
BigQuery caches query results for 24 hours:
- Same query = FREE (no scan)
- Slight filter change = Full scan

**Pro tip:** Use consistent date ranges during analysis sessions.

---

## Real-World Cost Examples

Assume table with 50 columns, 100M total rows, 10GB total size:

| Operation | Rows Scanned | Columns Scanned | Data Scanned | Relative Cost |
|-----------|-------------|-----------------|--------------|---------------|
| KPI (full table) | 100M | 10 | 2GB | 🟡 Moderate |
| KPI (30 days) | 8M | 10 | 160MB | 🟢 Low |
| Trends (30 days) | 8M | 10 | 160MB | 🟢 Low |
| Search Terms (30 days, limit 1000) | 8M | 10 | 160MB | 🟢 Low |
| Query All (30 days) | 8M | 50 | **800MB** | 🔴 High |
| Query All (full year) | 100M | 50 | **10GB** | 🔴 VERY High |

---

## Monitoring Your Usage

### In Application Logs
The application logs every query with:
- `bytes_processed`: Actual data scanned
- `bytes_billed`: What BigQuery charges for (rounded up to 10MB minimum)
- `execution_time`: Query duration
- `query_type`: Operation type (kpi, trends, search_terms, etc.)

**Check logs:**
```bash
# In Docker
docker-compose logs backend | grep "bytes_billed"

# Or check the database (if query logging is enabled)
```

### In BigQuery Console
1. Go to BigQuery Console
2. Click "Query History"
3. See bytes scanned per query
4. Filter by date to see your usage

---

## Summary: What Consumes Data?

**❌ High Data Consumption:**
- SELECT * queries (export/bulk operations)
- Queries without date filters
- Querying full table history
- Search terms with no filters

**✅ Low Data Consumption:**
- Aggregated queries (SUM, COUNT)
- Queries with date filters
- Dimension breakdowns
- KPI metrics
- Filter dropdown queries

**🎯 Best Practice:**
Always use date filters! This single habit can reduce costs by 10-100x.

---

## Questions?

**Q: Why do logs show different queries use different data?**
A: It depends on:
1. Number of rows matched by filters
2. Number of columns selected
3. Whether table is partitioned

**Q: Why does the same query sometimes use cache vs scan?**
A: BigQuery caches results for 24 hours. If ANY filter changes (even by 1 day), it's a new query.

**Q: How can I reduce costs?**
A: Use shorter date ranges during exploration. Only extend to full dataset when needed.
