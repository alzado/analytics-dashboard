"""
Data service - BigQuery implementation only.
All data is fetched from BigQuery on-demand.
"""
from typing import List, Dict, Optional
import numpy as np
import math
import pandas as pd
from models.schemas import (
    FilterParams,
    OverviewMetrics,
    TrendData,
    DimensionBreakdown,
    SearchTermData,
    FilterOptions,
    PivotRow,
    PivotChildRow,
    PivotResponse
)
from services.bigquery_service import get_bigquery_service
from services.custom_dimension_service import get_custom_dimension_service
from config import CUSTOM_DIMENSIONS_FILE


def safe_float(value: float) -> float:
    """Convert a value to float, replacing NaN and infinity with 0."""
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return float(value)


def _get_all_metrics_for_pivot(table_id: Optional[str] = None, requested_metrics: Optional[List[str]] = None):
    """
    Load all base and calculated metrics from schema service for pivot table.

    Args:
        table_id: Optional table ID to load schema for
        requested_metrics: Optional list of metric IDs to filter (if None, returns all)

    Returns dict with:
    - base_metrics: List of base metric definitions
    - calculated_metrics: List of calculated metric definitions
    - all_metric_ids: List of all metric IDs
    - primary_sort_metric: ID of the metric to sort by (from schema config)
    - schema_config: Full schema config object
    """
    try:
        from services.schema_service import SchemaService
        bq_service = get_bigquery_service(table_id)
        if bq_service and bq_service.client:
            schema_service = SchemaService(bq_service.client, table_id=bq_service.table_id)
            schema_config = schema_service.load_schema()

            # Access Pydantic model attributes directly (not .get() method)
            all_base_metrics = schema_config.base_metrics if schema_config else []
            all_calculated_metrics = schema_config.calculated_metrics if schema_config else []

            # Filter metrics if requested_metrics is provided
            if requested_metrics:
                requested_set = set(requested_metrics)

                # Keep only requested metrics
                base_metrics = [m for m in all_base_metrics if m.id in requested_set]
                calculated_metrics = [m for m in all_calculated_metrics if m.id in requested_set]

                # Also include dependencies (both base and calculated) that requested metrics depend on
                # This is crucial for metrics like "ctr_per_day" that depend on "ctr" (calculated)
                processed = set()  # Track processed metrics to avoid infinite loops
                to_process = list(calculated_metrics)

                while to_process:
                    calc_metric = to_process.pop(0)
                    if calc_metric.id in processed:
                        continue
                    processed.add(calc_metric.id)

                    if calc_metric.depends_on:
                        for dep_id in calc_metric.depends_on:
                            # Add dependency if not already included
                            if dep_id not in requested_set:
                                # Check if it's a base metric
                                dep_metric = next((m for m in all_base_metrics if m.id == dep_id), None)
                                if dep_metric and dep_metric not in base_metrics:
                                    base_metrics.append(dep_metric)
                                    requested_set.add(dep_id)
                                else:
                                    # Check if it's a calculated metric
                                    dep_calc_metric = next((m for m in all_calculated_metrics if m.id == dep_id), None)
                                    if dep_calc_metric and dep_calc_metric not in calculated_metrics:
                                        calculated_metrics.append(dep_calc_metric)
                                        requested_set.add(dep_id)
                                        # Process this calculated metric's dependencies too
                                        to_process.append(dep_calc_metric)
            else:
                base_metrics = all_base_metrics
                calculated_metrics = all_calculated_metrics

            # Get all metric IDs
            all_metric_ids = [m.id for m in base_metrics] + [m.id for m in calculated_metrics]

            # Get primary sort metric from schema config, or use first visible base metric
            primary_sort_metric = schema_config.primary_sort_metric if schema_config else None

            # If primary sort metric is set but not in the filtered metrics, we need to either:
            # 1. Include it in the filtered list, OR
            # 2. Change to a metric that IS in the filtered list
            if primary_sort_metric and primary_sort_metric not in all_metric_ids:
                # Option 1: Add the primary sort metric to the filtered list
                primary_sort_base_metric = next((m for m in all_base_metrics if m.id == primary_sort_metric), None)
                if primary_sort_base_metric:
                    base_metrics.append(primary_sort_base_metric)
                    all_metric_ids.append(primary_sort_metric)
                else:
                    # Option 2: If primary sort metric doesn't exist, use first available metric
                    primary_sort_metric = None

            if not primary_sort_metric:
                # Fallback: use first visible metric with lowest sort_order from base or calculated
                all_metrics = base_metrics + calculated_metrics
                if all_metrics:
                    visible = [m for m in all_metrics if m.is_visible_by_default]
                    if visible:
                        primary_sort_metric = sorted(visible, key=lambda m: m.sort_order)[0].id
                    else:
                        primary_sort_metric = all_metrics[0].id

            # Get avg per day metric from schema config, or use primary sort metric as fallback
            avg_per_day_metric = schema_config.avg_per_day_metric if schema_config else None

            # If avg_per_day_metric is set but not in the filtered metrics, add it
            if avg_per_day_metric and avg_per_day_metric not in all_metric_ids:
                avg_per_day_base_metric = next((m for m in all_base_metrics if m.id == avg_per_day_metric), None)
                if avg_per_day_base_metric:
                    base_metrics.append(avg_per_day_base_metric)
                    all_metric_ids.append(avg_per_day_metric)
                else:
                    # If metric doesn't exist, use primary sort metric as fallback
                    avg_per_day_metric = None

            if not avg_per_day_metric:
                avg_per_day_metric = primary_sort_metric  # Use same metric as sort metric

            return {
                'base_metrics': base_metrics,
                'calculated_metrics': calculated_metrics,
                'all_metric_ids': all_metric_ids,
                'primary_sort_metric': primary_sort_metric,
                'avg_per_day_metric': avg_per_day_metric,
                'schema_config': schema_config
            }
    except Exception as e:
        print(f"Warning: Could not load metrics for pivot: {e}")

    # Return empty structure if loading fails
    return {
        'base_metrics': [],
        'calculated_metrics': [],
        'all_metric_ids': [],
        'primary_sort_metric': None,
        'avg_per_day_metric': None,
        'schema_config': None
    }


def _load_metrics_data(bq_service):
    """
    Load all base and calculated metrics from schema service.
    This is an alias for _get_all_metrics_for_pivot() for backward compatibility.
    """
    table_id = bq_service.table_id if bq_service else None
    return _get_all_metrics_for_pivot(table_id=table_id)


def _build_pivot_select_clause(metrics_data):
    """
    Build dynamic SQL SELECT clause for pivot table queries.
    Uses a two-tier approach to avoid redundant calculations:
    - Base metrics are calculated with aggregations
    - Calculated metrics reference base metric aliases (no re-aggregation)

    Args:
        metrics_data: Dict from _get_all_metrics_for_pivot()

    Returns:
        Tuple of (base_select_items, calculated_select_items) where calculated metrics
        reference base metrics by their aliases
    """
    print(f"DEBUG _build_pivot_select_clause called with {len(metrics_data.get('calculated_metrics', []))} calculated metrics")
    base_select_items = []
    calculated_select_items = []

    # Separate calculated metrics into categories for proper SQL generation:
    # - Simple: no dependencies, computed in base query
    # - Intermediate: dependencies on calculated metrics that are used by days_in_range metrics
    # - Complex: all other calculated metrics
    #
    # Key insight: When a metric like "ctr_per_day = ctr / days_in_range" exists,
    # the "ctr" metric must be computed in the base query so it can be referenced by alias
    simple_calculated_metrics = []
    complex_calculated_metrics = []

    # First pass: identify metrics that depend on days_in_range
    metrics_with_days_in_range = set()
    for metric in metrics_data['calculated_metrics']:
        if metric.depends_on and 'days_in_range' in metric.depends_on:
            metrics_with_days_in_range.add(metric.id)

    # Second pass: find all calculated metric dependencies of days_in_range metrics
    # These need to be computed in the base query so they can be referenced
    must_be_in_base_query = set()
    for metric in metrics_data['calculated_metrics']:
        if metric.id in metrics_with_days_in_range and metric.depends_on_calculated:
            for dep_id in metric.depends_on_calculated:
                must_be_in_base_query.add(dep_id)

    # Third pass: classify metrics
    for metric in metrics_data['calculated_metrics']:
        # Skip days_in_range metrics themselves - they go to complex
        if metric.id in metrics_with_days_in_range:
            complex_calculated_metrics.append(metric)
        # Check if this calculated metric has no dependencies (simple aggregation)
        elif not metric.depends_on or len(metric.depends_on) == 0:
            simple_calculated_metrics.append(metric)
        # Check if this metric must be in base query (dependency of days_in_range metric)
        elif metric.id in must_be_in_base_query:
            simple_calculated_metrics.append(metric)
        else:
            complex_calculated_metrics.append(metric)

    # Add true base metrics
    for metric in metrics_data['base_metrics']:
        # Use aggregation from schema with actual column name from BigQuery
        agg = metric.aggregation
        col = metric.column_name
        if metric.is_system and metric.id == 'days_in_range':
            # Special handling for virtual metric - use DATE_DIFF instead of whatever aggregation is in schema
            base_select_items.append(f"DATE_DIFF(MAX(date), MIN(date), DAY) + 1 as {metric.id}")
        else:
            base_select_items.append(f"{agg}({col}) as {metric.id}")

    # Add simple calculated metrics to base items (they're really just aggregations)
    for metric in simple_calculated_metrics:
        base_select_items.append(f"{metric.sql_expression} as {metric.id}")

    # Create a combined list of all "base-level" metrics (true base + simple calculated)
    # This is used later when selecting from subquery
    all_base_metrics = list(metrics_data['base_metrics']) + simple_calculated_metrics

    # No need to track extra columns anymore since days_in_range is now a base metric in schema
    extra_base_columns = []

    # Build complex calculated metrics that reference base metrics by alias (not by re-aggregating)
    # We only process complex calculated metrics here (simple ones are already in base_select_items)
    for metric in complex_calculated_metrics:
        # Convert sql_expression to reference aliases instead of aggregations
        sql_expr = metric.sql_expression

        # Replace true base metrics with their aliases
        for base_metric in metrics_data['base_metrics']:
            # Pattern to match: AGG(column_name) where AGG is the aggregation function
            agg_func = base_metric.aggregation
            col_name = base_metric.column_name

            if agg_func == 'COUNT_DISTINCT':
                pattern = f"COUNT(DISTINCT {col_name})"
            elif agg_func == 'COUNT':
                # Handle both COUNT(column) and COUNT(*)
                pattern1 = f"COUNT({col_name})"
                pattern2 = "COUNT(*)"
                sql_expr = sql_expr.replace(pattern1, base_metric.id)
                sql_expr = sql_expr.replace(pattern2, base_metric.id)
                continue  # Skip the regular replacement below
            else:
                pattern = f"{agg_func}({col_name})"

            # Replace with just the metric alias
            sql_expr = sql_expr.replace(pattern, base_metric.id)

        # Also replace simple calculated metrics (like queries = COUNT(*))
        for simple_metric in simple_calculated_metrics:
            # Replace the full SQL expression with the metric ID
            sql_expr = sql_expr.replace(simple_metric.sql_expression, simple_metric.id)

        # Special handling for days_in_range virtual metric
        sql_expr = sql_expr.replace("DATE_DIFF(MAX(date), MIN(date), DAY) + 1", "days_in_range")

        calculated_select_items.append(f"({sql_expr}) as {metric.id}")

    # Return base select items, calculated select items, the list of all base-level metrics,
    # and extra columns that need to be selected from inner query (like days_in_range)
    return base_select_items, calculated_select_items, all_base_metrics, extra_base_columns


def _extract_metrics_from_row(row, metrics_data):
    """
    Extract all metric values from a DataFrame row into a dictionary.

    Args:
        row: pandas Series with metric columns
        metrics_data: Dict from _get_all_metrics_for_pivot()

    Returns:
        Dict with metric_id -> value mapping
    """
    metrics = {}

    # Extract all metrics from the row
    for metric_id in metrics_data['all_metric_ids']:
        if metric_id in row:
            value = row[metric_id]
            # Handle pandas dtypes - convert to int or float
            if pd.isna(value):
                metrics[metric_id] = 0.0
            elif 'int' in str(row[metric_id].__class__.__name__).lower():
                metrics[metric_id] = int(value)
            else:
                metrics[metric_id] = safe_float(value)

    return metrics


def _get_schema_config():
    """Load schema configuration from SchemaService."""
    try:
        from services.schema_service import SchemaService
        bq_service = get_bigquery_service()
        if bq_service and bq_service.client:
            schema_service = SchemaService(bq_service.client, table_id=bq_service.table_id)
            return schema_service.load_schema()
    except Exception as e:
        print(f"Warning: Could not load schema: {e}")
    return None


def _compute_calculated_metrics(base_metrics: Dict, schema_config=None) -> Dict:
    """
    Compute calculated metrics from base metrics using schema formulas.

    Args:
        base_metrics: Dictionary of base metric values from BigQuery
        schema_config: Schema configuration with calculated metric definitions

    Returns:
        Dictionary of calculated metric values
    """
    calculated = {}

    if not schema_config or not schema_config.calculated_metrics:
        return calculated

    # For each calculated metric in schema, compute its value
    for calc_metric in schema_config.calculated_metrics:
        try:
            # Build expression by replacing metric IDs with their values
            expression = calc_metric.sql_expression

            # Simple approach: replace AGG(column) patterns with actual values
            # This works for post-aggregation calculations
            for base_metric in schema_config.base_metrics:
                # Replace patterns like "SUM(queries)" with the actual value
                patterns = [
                    f"{base_metric.aggregation}({base_metric.column_name})",
                    base_metric.id
                ]
                for pattern in patterns:
                    if pattern in expression:
                        value = base_metrics.get(base_metric.id, 0)
                        expression = expression.replace(pattern, str(value))

            # Handle SAFE_DIVIDE specially
            if "SAFE_DIVIDE" in expression:
                # Extract numerator and denominator from SAFE_DIVIDE(num, denom)
                import re
                match = re.match(r'SAFE_DIVIDE\((.*?),\s*(.*?)\)', expression)
                if match:
                    try:
                        numerator = float(eval(match.group(1)))
                        denominator = float(eval(match.group(2)))
                        result = numerator / denominator if denominator != 0 else 0
                    except:
                        result = 0
                else:
                    result = 0
            else:
                # Evaluate the expression
                try:
                    result = eval(expression)
                except:
                    result = 0

            calculated[calc_metric.id] = safe_float(result)

        except Exception as e:
            print(f"Warning: Could not compute calculated metric '{calc_metric.id}': {e}")
            calculated[calc_metric.id] = 0

    return calculated


def _query_custom_dimension_pivot(
    custom_dim_id: str,
    filters: FilterParams,
    limit: int
) -> Optional[PivotResponse]:
    """Query pivot data for a custom dimension (e.g., date ranges)"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        return None

    # Load the custom dimension
    cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
    custom_dim = cd_service.get_by_id(custom_dim_id)

    if not custom_dim:
        return None

    # Load all metrics dynamically from schema
    metrics_data = _get_all_metrics_for_pivot()
    base_select_items, calculated_select_items, all_base_metrics, extra_base_columns = _build_pivot_select_clause(metrics_data)

    # Determine metric for sorting and avg per day
    if metrics_data['primary_sort_metric']:
        primary_sort = metrics_data['primary_sort_metric']
    elif metrics_data['base_metrics']:
        primary_sort = metrics_data['base_metrics'][0].id
    else:
        raise ValueError("No metrics available in schema")
    avg_per_day_metric = metrics_data['avg_per_day_metric'] or primary_sort
    avg_per_day_key = f"{avg_per_day_metric}_per_day"

    # For date_range type custom dimensions, run a query for each date range value
    if custom_dim.type == "date_range":
        all_rows = []

        # Initialize base metric totals dynamically
        base_metric_totals = {metric.id: 0 for metric in metrics_data['base_metrics']}

        # Track actual min/max dates across all queries for num_days calculation
        overall_min_date = None
        overall_max_date = None

        for value in custom_dim.values:
            # Resolve dates (relative to absolute if needed)
            from .date_resolver import resolve_relative_date

            if value.date_range_type == 'relative' and value.relative_date_preset:
                # Resolve relative date to absolute
                start_date, end_date = resolve_relative_date(value.relative_date_preset)
            else:
                # Use absolute dates (handle empty strings as None)
                start_date = value.start_date if value.start_date else None
                end_date = value.end_date if value.end_date else None

            # Create modified filters with this date range
            value_filters = FilterParams(**filters.dict())
            value_filters.start_date = start_date
            value_filters.end_date = end_date

            # Build filter clause for this date range
            # Note: We've already resolved relative dates to absolute above (lines 334-340)
            # So we pass date_range_type='absolute' and the resolved dates
            where_clause = bq_service.build_filter_clause(
                start_date=start_date,
                end_date=end_date,
                dimension_filters=filters.dimension_filters,
                date_range_type='absolute',
                relative_date_preset=None
            )

            # Query for this date range with dynamic metrics using subquery pattern
            base_select_str = ',\n                    '.join(base_select_items)

            if calculated_select_items:
                # List base metric aliases to select from inner query (including extra columns like days_in_range)
                base_metric_aliases = [metric.id for metric in all_base_metrics] + extra_base_columns
                base_aliases_str = ', '.join(base_metric_aliases)
                calculated_select_str = ',\n                '.join(calculated_select_items)

                query = f"""
                    SELECT
                        {base_aliases_str},
                        {calculated_select_str},
                        min_date,
                        max_date
                    FROM (
                        SELECT
                            {base_select_str},
                            MIN(date) as min_date,
                            MAX(date) as max_date
                        FROM `{bq_service.table_path}`
                        {where_clause}
                    )
                """
            else:
                # If no calculated metrics, use base metrics directly
                query = f"""
                    SELECT
                        {base_select_str},
                        MIN(date) as min_date,
                        MAX(date) as max_date
                    FROM `{bq_service.table_path}`
                    {where_clause}
                """

            df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

            if not df.empty:
                row_data = df.iloc[0]

                # Extract all metrics dynamically
                metrics = _extract_metrics_from_row(row_data, metrics_data)

                # Track date range for avg_per_day calculation
                min_date = row_data['min_date'] if 'min_date' in row_data else None
                max_date = row_data['max_date'] if 'max_date' in row_data else None

                if min_date and max_date:
                    num_days = (max_date - min_date).days + 1
                else:
                    num_days = 1

                # Update overall date range
                if overall_min_date is None or (min_date and min_date < overall_min_date):
                    overall_min_date = min_date
                if overall_max_date is None or (max_date and max_date > overall_max_date):
                    overall_max_date = max_date

                # Add avg per day as a computed metric
                avg_per_day_value = metrics.get(avg_per_day_metric, 0)
                metrics[avg_per_day_key] = safe_float(avg_per_day_value / num_days) if num_days > 0 and avg_per_day_value > 0 else 0.0

                # Add row
                all_rows.append(PivotRow(
                    dimension_value=value.label,
                    metrics=metrics,
                    percentage_of_total=0.0,  # Will calculate after
                    has_children=False
                ))

                # Accumulate base metric totals
                for metric in metrics_data['base_metrics']:
                    base_metric_totals[metric.id] += metrics.get(metric.id, 0)

        # Query for "Other" - dates not in any defined date range
        # Build exclusion conditions for all date ranges
        # Resolve relative dates to absolute dates for exclusions
        date_exclusions = []
        for value in custom_dim.values:
            if value.date_range_type == 'relative' and value.relative_date_preset:
                start_date, end_date = resolve_relative_date(value.relative_date_preset)
            else:
                start_date = value.start_date
                end_date = value.end_date
            date_exclusions.append(f"NOT (date BETWEEN '{start_date}' AND '{end_date}')")

        date_exclusion_clause = " AND ".join(date_exclusions) if date_exclusions else ""

        # Build base filter clause
        base_where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Add date exclusions to WHERE clause
        if date_exclusion_clause:
            if base_where_clause:
                combined_where_clause = f"{base_where_clause} AND ({date_exclusion_clause})"
            else:
                combined_where_clause = f"WHERE ({date_exclusion_clause})"
        else:
            combined_where_clause = base_where_clause

        # Query for "Other" dates with dynamic metrics
        other_query = f"""
            SELECT
                {select_clause},
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_service.table_path}`
            {combined_where_clause}
        """

        other_df = bq_service._execute_and_log_query(other_query, query_type="pivot", endpoint="data_service")

        if not other_df.empty:
            other_row_data = other_df.iloc[0]

            # Extract all metrics dynamically
            other_metrics = _extract_metrics_from_row(other_row_data, metrics_data)

            # Track date range for avg_per_day calculation
            other_min_date = other_row_data['min_date'] if 'min_date' in other_row_data else None
            other_max_date = other_row_data['max_date'] if 'max_date' in other_row_data else None

            if other_min_date and other_max_date:
                other_num_days = (other_max_date - other_min_date).days + 1
            else:
                other_num_days = 1

            # Update overall date range
            if overall_min_date is None or (other_min_date and other_min_date < overall_min_date):
                overall_min_date = other_min_date
            if overall_max_date is None or (other_max_date and other_max_date > overall_max_date):
                overall_max_date = other_max_date

            # Add avg per day as a computed metric
            other_avg_per_day_value = other_metrics.get(avg_per_day_metric, 0)
            other_metrics[avg_per_day_key] = safe_float(other_avg_per_day_value / other_num_days) if other_num_days > 0 and other_avg_per_day_value > 0 else 0.0

            # Add "Other" row
            all_rows.append(PivotRow(
                dimension_value="Other",
                metrics=other_metrics,
                percentage_of_total=0.0,  # Will calculate after
                has_children=False
            ))

            # Accumulate "Other" totals
            for metric in metrics_data['base_metrics']:
                base_metric_totals[metric.id] += other_metrics.get(metric.id, 0)

        # Calculate percentage of total for each row
        total_primary_metric = base_metric_totals.get(primary_sort, 0)
        for row in all_rows:
            row_primary_value = row.metrics.get(primary_sort, 0)
            row.percentage_of_total = safe_float(row_primary_value / total_primary_metric * 100) if total_primary_metric > 0 else 0.0

        # Calculate overall num_days
        if overall_min_date and overall_max_date:
            overall_num_days = (overall_max_date - overall_min_date).days + 1
        else:
            overall_num_days = 1

        # Compute calculated metrics from base metric totals
        calculated_totals = _compute_calculated_metrics(base_metric_totals, metrics_data['schema_config'])

        # Combine all totals
        total_metrics = {**base_metric_totals, **calculated_totals}

        # Add avg per day to total metrics
        total_avg_per_day_value = total_metrics.get(avg_per_day_metric, 0)
        total_metrics[avg_per_day_key] = safe_float(total_avg_per_day_value / overall_num_days) if overall_num_days > 0 else 0.0

        # Create total row
        total_row = PivotRow(
            dimension_value="Total",
            metrics=total_metrics,
            percentage_of_total=100.0,
            has_children=False
        )

        # Get available dimensions from schema
        dimension_map = {}
        if bq_service.schema_config and bq_service.schema_config.dimensions:
            for dim in bq_service.schema_config.dimensions:
                dimension_map[dim.id] = dim.column_name

        return PivotResponse(
            rows=all_rows,
            total=total_row,
            available_dimensions=list(dimension_map.keys()),
            dimension_metadata={
                "id": custom_dim.id,
                "name": custom_dim.name,
                "type": custom_dim.type,
                "is_custom": True
            }
        )

    # metric_condition custom dimensions are not supported (removed - too hardcoded for search_term)
    elif custom_dim.type == "metric_condition":
        raise ValueError(f"Custom dimension type 'metric_condition' is not supported. This feature was removed because it contained hardcoded references to 'search_term' column.")


    return None


def get_overview_metrics(filters: FilterParams) -> OverviewMetrics:
    """Get overview metrics from BigQuery - fully dynamic"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # query_kpi_metrics already returns all base + calculated metrics dynamically
    metrics_dict = bq_service.query_kpi_metrics(filters=filters)

    # Return metrics as-is in dynamic format
    return OverviewMetrics(metrics=metrics_dict)


def get_trend_data(filters: FilterParams, granularity: str = "daily") -> List[TrendData]:
    """Get time series trend data from BigQuery - fully dynamic"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_timeseries(
        filters=filters,
        granularity=granularity
    )

    if df.empty:
        return []

    # Get metrics data for dynamic extraction
    metrics_data = _load_metrics_data(bq_service)

    # Fill NaN with 0 for numeric columns only
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Convert to response format with dynamic metrics
    result = []
    for _, row in df.iterrows():
        # Extract all metrics dynamically
        metrics = _extract_metrics_from_row(row, metrics_data)

        result.append(TrendData(
            date=row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else None,
            metrics=metrics
        ))

    return result


def get_dimension_breakdown(dimension: str, filters: FilterParams, limit: int = 20) -> List[DimensionBreakdown]:
    """Get breakdown by dimension from BigQuery - fully dynamic"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_dimension_breakdown(
        dimension=dimension,
        filters=filters,
        limit=limit
    )

    if df.empty:
        return []

    # Get metrics data for dynamic extraction
    metrics_data = _load_metrics_data(bq_service)

    # Get primary sort metric for percentage calculation
    if metrics_data.get('primary_sort_metric'):
        primary_sort = metrics_data['primary_sort_metric']
    elif metrics_data.get('base_metrics'):
        primary_sort = metrics_data['base_metrics'][0].id
    else:
        raise ValueError("No metrics available in schema")

    # Calculate total for percentage
    total_primary = df[primary_sort].sum() if primary_sort in df.columns else 0

    # Fill NaN with 0 for numeric columns only
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Convert to response format with dynamic metrics
    result = []
    for _, row in df.iterrows():
        # Extract all metrics dynamically
        metrics = _extract_metrics_from_row(row, metrics_data)

        # Calculate percentage of total
        primary_value = metrics.get(primary_sort, 0)
        percentage = (primary_value / total_primary * 100) if total_primary > 0 else 0

        result.append(DimensionBreakdown(
            dimension_value=str(row['dimension_value']),
            metrics=metrics,
            percentage_of_total=float(percentage)
        ))

    return result


def get_search_terms(filters: FilterParams, limit: int = 100, sort_by: str = "queries") -> List[SearchTermData]:
    """Get top search terms from BigQuery - fully dynamic"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_search_terms(
        filters=filters,
        limit=limit,
        sort_by=sort_by
    )

    if df.empty:
        return []

    # Get metrics data for dynamic extraction
    metrics_data = _load_metrics_data(bq_service)

    # Fill NaN with 0 for numeric columns only
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Convert to response format with dynamic metrics
    result = []
    for _, row in df.iterrows():
        # Extract all metrics dynamically
        metrics = _extract_metrics_from_row(row, metrics_data)

        result.append(SearchTermData(
            search_term=str(row['search_term']),
            metrics=metrics
        ))

    return result


def get_filter_options() -> FilterOptions:
    """Get available filter options from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Query for distinct countries
    countries_query = f"SELECT DISTINCT country FROM `{bq_service.table_path}` WHERE country IS NOT NULL ORDER BY country"
    countries_df = bq_service._execute_and_log_query(countries_query, query_type="pivot", endpoint="data_service")
    countries = countries_df['country'].tolist()

    # Query for distinct channels
    channels_query = f"SELECT DISTINCT channel FROM `{bq_service.table_path}` WHERE channel IS NOT NULL ORDER BY channel"
    channels_df = bq_service._execute_and_log_query(channels_query, query_type="pivot", endpoint="data_service")
    channels = channels_df['channel'].tolist()

    # Query for date range
    date_query = f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM `{bq_service.table_path}`"
    date_df = bq_service._execute_and_log_query(date_query, query_type="pivot", endpoint="data_service")

    min_date = date_df['min_date'].iloc[0]
    max_date = date_df['max_date'].iloc[0]

    date_range = {
        'min': min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
        'max': max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None
    }

    attributes = ['categoria', 'tipo', 'genero', 'marca', 'color', 'material', 'talla', 'modelo']

    return FilterOptions(
        countries=countries,
        channels=channels,
        date_range=date_range,
        attributes=attributes
    )


def get_pivot_data(dimensions: List[str], filters: FilterParams, limit: int = 50, offset: int = 0, dimension_values: Optional[List[str]] = None, table_id: Optional[str] = None, skip_count: bool = False, metrics: Optional[List[str]] = None) -> PivotResponse:
    """Get hierarchical pivot table data by dimensions from BigQuery

    Args:
        dimensions: List of dimension IDs to group by
        filters: Filter parameters
        limit: Maximum number of rows to return (ignored if dimension_values is provided)
        offset: Number of rows to skip (ignored if dimension_values is provided)
        dimension_values: Optional list of specific dimension values to fetch (for multi-table matching)
        table_id: Optional table ID for multi-table widget support (defaults to active table)
        skip_count: Skip the count query (for initial loads or when total count not needed)
        metrics: Optional list of metric IDs to calculate (defaults to all metrics if None)
    """
    bq_service = get_bigquery_service(table_id)
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # If metrics list is explicitly empty (not None), return empty response
    if metrics is not None and len(metrics) == 0:
        return PivotResponse(
            rows=[],
            total={},
            total_count=0,
            has_more=False
        )

    # Load all metrics dynamically from schema, optionally filtered by requested metrics
    metrics_data = _get_all_metrics_for_pivot(table_id, requested_metrics=metrics)
    base_select_items, calculated_select_items, all_base_metrics, extra_base_columns = _build_pivot_select_clause(metrics_data)

    # Check if any dimension is a custom dimension (starts with "custom_")
    if dimensions and len(dimensions) > 0:
        # Check if mixing custom and regular dimensions (not allowed)
        has_custom = any(d.startswith("custom_") for d in dimensions)
        has_regular = any(not d.startswith("custom_") for d in dimensions)

        if has_custom and has_regular:
            raise ValueError("Cannot mix custom dimensions with regular dimensions in row dimensions. Use custom dimensions as table dimensions (columns) only.")

        if has_custom:
            # Must be the first (and only) dimension for now
            first_dim = dimensions[0]
            if first_dim.startswith("custom_"):
                # Extract custom dimension ID
                custom_dim_id = first_dim.replace("custom_", "")
                # Use custom dimension query logic
                result = _query_custom_dimension_pivot(custom_dim_id, filters, limit)
                if result:
                    return result
                else:
                    raise ValueError(f"Custom dimension {custom_dim_id} not found")

    # Build dimension map dynamically from schema
    dimension_map = {}
    if metrics_data['schema_config'] and metrics_data['schema_config'].dimensions:
        for dim in metrics_data['schema_config'].dimensions:
            # Map dimension ID to column name
            dimension_map[dim.id] = dim.column_name

    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    # Use cached method to avoid redundant queries for same table/filters/dimensions
    min_date, max_date, num_days = bq_service.get_date_range_cached(
        start_date=filters.start_date,
        end_date=filters.end_date,
        dimension_filters=filters.dimension_filters,
        dimensions=dimensions,
        date_range_type=filters.date_range_type,
        relative_date_preset=filters.relative_date_preset
    )

    # If no dimensions provided, return aggregated totals as a single row
    if not dimensions:
        # Build filter clause (include n_words and n_attributes for exact filtering)
        where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            date_range_type=filters.date_range_type,
            relative_date_preset=filters.relative_date_preset
        )

        # Query for aggregated totals with subquery pattern
        # Inner query calculates base metrics, outer query calculates calculated metrics
        base_select_str = ',\n                '.join(base_select_items)

        if calculated_select_items:
            # List base metric aliases to select from inner query (including extra columns like days_in_range)
            base_metric_aliases = [metric.id for metric in all_base_metrics] + extra_base_columns
            print(f"DEBUG: base_metric_aliases = {base_metric_aliases}")
            print(f"DEBUG: all_base_metrics = {[m.id for m in all_base_metrics]}")
            base_aliases_str = ', '.join(base_metric_aliases)
            calculated_select_str = ',\n            '.join(calculated_select_items)
            print(f"DEBUG: calculated_select_items = {calculated_select_items}")

            query = f"""
                SELECT
                    {base_aliases_str},
                    {calculated_select_str}
                FROM (
                    SELECT
                        {base_select_str}
                    FROM `{bq_service.table_path}`
                    {where_clause}
                )
            """
        else:
            # If no calculated metrics, just use base metrics directly
            query = f"""
                SELECT
                    {base_select_str}
                FROM `{bq_service.table_path}`
                {where_clause}
            """

        df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

        if df.empty:
            # Return empty response with zero totals - use empty metrics dict
            empty_metrics = {metric_id: 0 for metric_id in metrics_data['all_metric_ids']}
            total_row = PivotRow(
                dimension_value="Total",
                metrics=empty_metrics,
                percentage_of_total=100.0,
                has_children=False
            )
            return PivotResponse(
                rows=[total_row],
                total=total_row,
                available_dimensions=list(dimension_map.keys()),
                total_count=1
            )

        # Extract all metrics dynamically from the row
        row_data = df.iloc[0]
        metrics = _extract_metrics_from_row(row_data, metrics_data)

        # Add avg per day as a computed metric
        if metrics_data['avg_per_day_metric']:
            avg_per_day_metric = metrics_data['avg_per_day_metric']
        elif metrics_data['primary_sort_metric']:
            avg_per_day_metric = metrics_data['primary_sort_metric']
        elif metrics_data['base_metrics']:
            avg_per_day_metric = metrics_data['base_metrics'][0].id
        else:
            raise ValueError("No metrics available in schema")
        avg_per_day_key = f"{avg_per_day_metric}_per_day"
        avg_per_day_value = metrics.get(avg_per_day_metric, 0)
        metrics[avg_per_day_key] = safe_float(avg_per_day_value / num_days) if num_days > 0 and avg_per_day_value > 0 else 0.0

        total_row = PivotRow(
            dimension_value="All Data",
            metrics=metrics,
            percentage_of_total=100.0,
            has_children=False
        )
        return PivotResponse(
            rows=[total_row],
            total=total_row,
            available_dimensions=list(dimension_map.keys()),
            total_count=1
        )

    # Build filter clause
    where_clause = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        dimension_filters=filters.dimension_filters,
        date_range_type=filters.date_range_type,
        relative_date_preset=filters.relative_date_preset
    )

    # Map all dimensions to their column names
    group_cols = [dimension_map.get(dim, dim) for dim in dimensions]

    # Quote column names if they contain special characters (like hyphens in custom dimensions)
    quoted_group_cols = [bq_service._quote_column_name(col) for col in group_cols]
    group_by_clause = ", ".join(quoted_group_cols)

    # Build SELECT clause for dimension values
    # For multiple dimensions, concat them with " - " separator
    if len(quoted_group_cols) > 1:
        # Convert each column to string and join with separator
        cast_cols = [f"CAST({col} AS STRING)" for col in quoted_group_cols]
        separator = ', " - ", '
        concat_args = separator.join(cast_cols)
        dim_value_clause = f"CONCAT({concat_args}) as dimension_value"
    else:
        # Check if dimension is DATE type and cast to STRING
        dim_def = next((d for d in metrics_data['schema_config'].dimensions if d.column_name == group_cols[0]), None)
        if dim_def and dim_def.data_type == 'DATE':
            dim_value_clause = f"CAST({quoted_group_cols[0]} AS STRING) as dimension_value"
        else:
            dim_value_clause = f"{quoted_group_cols[0]} as dimension_value"

    # Query for pivot data with dynamic metrics
    # Determine which metric to sort by
    if metrics_data['primary_sort_metric']:
        primary_sort = metrics_data['primary_sort_metric']
    elif metrics_data['base_metrics']:
        primary_sort = metrics_data['base_metrics'][0].id
    else:
        raise ValueError("No metrics available in schema")

    # Get the metric to use for avg per day calculation
    avg_per_day_metric = metrics_data['avg_per_day_metric'] or primary_sort
    avg_per_day_key = f"{avg_per_day_metric}_per_day"

    # Split into two queries for better performance:
    # 1. Main query: Get top N rows (no window functions)
    # 2. Totals query: Get aggregated totals separately

    # Count query - get total count of dimension values for pagination
    # Skip if not needed (e.g., initial load or when pagination info not required)
    if skip_count and offset == 0:
        # Skip count query on initial load to save BigQuery queries
        total_count = -1  # Sentinel value indicating count was skipped
    else:
        # Use cached count method - will cache results for same filters/dimensions
        # Use APPROX_COUNT_DISTINCT for very large datasets (faster but slightly less accurate)
        use_approx = False  # Set to True if you have millions of unique dimension combinations
        total_count = bq_service.get_count_cached(
            group_cols=group_cols,
            start_date=filters.start_date,
            end_date=filters.end_date,
            dimension_filters=filters.dimension_filters,
            use_approx=use_approx
        )

    # Build additional filter for specific dimension values if provided
    dimension_values_filter = ""
    if dimension_values and len(dimension_values) > 0:
        # Escape single quotes in dimension values
        escaped_values = [value.replace("'", "\\'") for value in dimension_values]
        values_list = "', '".join(escaped_values)
        # Add to where clause
        dimension_column = group_cols[0]  # Use the first (and should be only) dimension column
        # Use WHERE if there's no existing where clause, otherwise use AND
        connector = "AND" if where_clause else "WHERE"
        dimension_values_filter = f"{connector} {dimension_column} IN ('{values_list}')"

    # Main query - get top N dimension values with offset (or specific values if provided)
    # Use subquery pattern to avoid redundant metric calculations
    base_select_str = ',\n                '.join(base_select_items)

    if calculated_select_items:
        # List base metric aliases to select from inner query (including extra columns like days_in_range)
        base_metric_aliases = [metric.id for metric in all_base_metrics] + extra_base_columns
        base_aliases_str = ', '.join(base_metric_aliases)
        calculated_select_str = ',\n            '.join(calculated_select_items)

        if dimension_values and len(dimension_values) > 0:
            # When fetching specific dimension values, don't use LIMIT/OFFSET and sort alphabetically for consistent ordering
            query = f"""
                SELECT
                    dimension_value,
                    {base_aliases_str},
                    {calculated_select_str}
                FROM (
                    SELECT
                        {dim_value_clause},
                        {base_select_str}
                    FROM `{bq_service.table_path}`
                    {where_clause}
                    {dimension_values_filter}
                    GROUP BY {group_by_clause}
                )
                ORDER BY dimension_value
            """
        else:
            # Normal top-N query
            query = f"""
                SELECT
                    dimension_value,
                    {base_aliases_str},
                    {calculated_select_str}
                FROM (
                    SELECT
                        {dim_value_clause},
                        {base_select_str}
                    FROM `{bq_service.table_path}`
                    {where_clause}
                    GROUP BY {group_by_clause}
                )
                ORDER BY {primary_sort} DESC
                LIMIT {limit}
                OFFSET {offset}
            """
    else:
        # If no calculated metrics, use base metrics directly
        if dimension_values and len(dimension_values) > 0:
            query = f"""
                SELECT
                    {dim_value_clause},
                    {base_select_str}
                FROM `{bq_service.table_path}`
                {where_clause}
                {dimension_values_filter}
                GROUP BY {group_by_clause}
                ORDER BY {group_by_clause}
            """
        else:
            query = f"""
                SELECT
                    {dim_value_clause},
                    {base_select_str}
                FROM `{bq_service.table_path}`
                {where_clause}
                GROUP BY {group_by_clause}
                ORDER BY {primary_sort} DESC
                LIMIT {limit}
                OFFSET {offset}
            """

    df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

    # Totals query - get overall totals in a separate query with subquery pattern
    if calculated_select_items:
        totals_query = f"""
            SELECT
                {base_aliases_str},
                {calculated_select_str}
            FROM (
                SELECT
                    {base_select_str}
                FROM `{bq_service.table_path}`
                {where_clause}
            )
        """
    else:
        totals_query = f"""
            SELECT
                {base_select_str}
            FROM `{bq_service.table_path}`
            {where_clause}
        """

    totals_df = bq_service._execute_and_log_query(totals_query, query_type="pivot_totals", endpoint="data_service")

    if df.empty:
        # Return empty response with zero totals - use empty metrics dict
        empty_metrics = {metric_id: 0 for metric_id in metrics_data['all_metric_ids']}
        empty_metrics[avg_per_day_key] = 0.0  # Add avg per day metric
        total_row = PivotRow(
            dimension_value="Total",
            metrics=empty_metrics,
            percentage_of_total=100.0,
            has_children=False
        )
        return PivotResponse(
            rows=[],
            total=total_row,
            available_dimensions=list(dimension_map.keys()),
            total_count=total_count
        )

    # Extract totals from the separate totals query
    if totals_df.empty:
        empty_metrics = {metric_id: 0 for metric_id in metrics_data['all_metric_ids']}
        total_primary_metric = 0
        total_metrics_from_query = empty_metrics
    else:
        totals_row = totals_df.iloc[0]
        # Extract ALL metrics (base + calculated) from totals query, just like we do for rows
        total_metrics_from_query = _extract_metrics_from_row(totals_row, metrics_data)

        # Get total for primary sort metric for percentage calculation
        total_primary_metric = total_metrics_from_query.get(primary_sort, 0)

    # Fill NaN and infinity with 0 for all metric columns
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # Convert to PivotRow objects using dynamic metrics
    rows = []
    for _, row in df.iterrows():
        # Extract all metrics dynamically from this row
        metrics = _extract_metrics_from_row(row, metrics_data)

        # Add avg per day as a computed metric
        avg_per_day_value = metrics.get(avg_per_day_metric, 0)
        metrics[avg_per_day_key] = safe_float(avg_per_day_value / num_days) if num_days > 0 and avg_per_day_value > 0 else 0.0

        # Calculate percentage of total based on primary sort metric
        primary_metric_value = metrics.get(primary_sort, 0)
        percentage_of_total = safe_float((primary_metric_value / total_primary_metric * 100)) if total_primary_metric > 0 else 0.0

        rows.append(PivotRow(
            dimension_value=str(row['dimension_value']),
            metrics=metrics,
            percentage_of_total=percentage_of_total,
            has_children=False
        ))

    # Use metrics extracted from totals query (already includes both base and calculated metrics)
    total_metrics = total_metrics_from_query

    # Add avg per day to total metrics
    total_avg_per_day_value = total_metrics.get(avg_per_day_metric, 0)
    total_metrics[avg_per_day_key] = safe_float(total_avg_per_day_value / num_days) if num_days > 0 else 0.0

    total_row = PivotRow(
        dimension_value="Total",
        metrics=total_metrics,
        percentage_of_total=100.0,
        has_children=False
    )

    return PivotResponse(
        rows=rows,
        total=total_row,
        available_dimensions=list(dimension_map.keys()),
        total_count=total_count
    )


def get_pivot_children(
    dimension: str,
    value: str,
    filters: FilterParams,
    limit: int = 100,
    offset: int = 0
) -> List[PivotChildRow]:
    """Get child rows (search terms) for a specific dimension value from BigQuery

    If dimension is empty string, fetches all search terms without dimension filtering
    """

    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Load all metrics dynamically from schema
    metrics_data = _get_all_metrics_for_pivot()
    base_select_items, calculated_select_items, all_base_metrics, extra_base_columns = _build_pivot_select_clause(metrics_data)
    if metrics_data['primary_sort_metric']:
        primary_sort = metrics_data['primary_sort_metric']
    elif metrics_data['base_metrics']:
        primary_sort = metrics_data['base_metrics'][0].id
    else:
        raise ValueError("No metrics available in schema")
    avg_per_day_metric = metrics_data['avg_per_day_metric'] or primary_sort

    # Check if this is a custom dimension (starts with "custom_")
    if dimension and dimension.startswith("custom_"):
        # Extract custom dimension ID
        custom_dim_id = dimension.replace("custom_", "")

        # Load the custom dimension
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        custom_dim = cd_service.get_by_id(custom_dim_id)

        if not custom_dim:
            raise ValueError(f"Custom dimension {custom_dim_id} not found")

        if custom_dim.type == "date_range":
            # Find the date range value that matches the label
            date_range_value = None
            for val in custom_dim.values:
                if val.label == value:
                    date_range_value = val
                    break

            if not date_range_value:
                raise ValueError(f"Value '{value}' not found in custom dimension {custom_dim.name}")

            # Override filters with this date range
            # Resolve relative dates to absolute dates if needed
            if date_range_value.date_range_type == 'relative' and date_range_value.relative_date_preset:
                start_date, end_date = resolve_relative_date(date_range_value.relative_date_preset)
            else:
                start_date = date_range_value.start_date
                end_date = date_range_value.end_date

            filters = FilterParams(**filters.dict())
            filters.start_date = start_date
            filters.end_date = end_date

            # Set dimension to empty string so the rest of the function handles it as a non-dimension query
            dimension = ""

        elif custom_dim.type == "metric_condition":
            raise ValueError(f"Custom dimension type 'metric_condition' is not supported for children queries. This feature was removed because it contained hardcoded references to 'search_term' column.")


    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    # Use cached method to avoid redundant queries for same table/filters/dimensions
    min_date, max_date, num_days = bq_service.get_date_range_cached(
        start_date=filters.start_date,
        end_date=filters.end_date,
        dimension_filters=filters.dimension_filters,
        dimensions=dimensions,
        date_range_type=filters.date_range_type,
        relative_date_preset=filters.relative_date_preset
    )

    # Build base filter clause using the centralized method
    where_clause = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        dimension_filters=filters.dimension_filters,
        date_range_type=filters.date_range_type,
        relative_date_preset=filters.relative_date_preset
    )

    # Add dimension filter only if dimension is specified
    if dimension:  # If dimension is provided (not empty string)
        # Map dimension ID to column name using schema
        group_col = dimension
        is_numeric = False

        if metrics_data['schema_config'] and metrics_data['schema_config'].dimensions:
            dim_def = next((d for d in metrics_data['schema_config'].dimensions if d.id == dimension), None)
            if dim_def:
                group_col = dim_def.column_name
                # Numeric dimensions: INTEGER, FLOAT
                is_numeric = dim_def.data_type in ['INTEGER', 'FLOAT']

        # Numeric dimensions don't need quotes, string dimensions do
        dimension_condition = f"{group_col} = {value}" if is_numeric else f"{group_col} = '{value}'"

        # Append dimension filter to existing WHERE clause
        if where_clause:
            where_clause = f"{where_clause} AND {dimension_condition}"
        else:
            where_clause = f"WHERE {dimension_condition}"

    # First, get the grand total queries for percentage calculation (same filters, no dimension restriction)
    base_where_clause = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        dimension_filters=filters.dimension_filters,
        date_range_type=filters.date_range_type,
        relative_date_preset=filters.relative_date_preset
    )

    # Get grand total for percentage calculation (using primary sort metric)
    total_query = f"""
        SELECT SUM({primary_sort}) as total_primary_metric
        FROM `{bq_service.table_path}`
        {base_where_clause}
    """

    total_df = bq_service._execute_and_log_query(total_query, query_type="pivot", endpoint="data_service")
    grand_total_primary = float(total_df['total_primary_metric'].iloc[0]) if not total_df.empty and total_df['total_primary_metric'].iloc[0] is not None else 0

    # Query for search terms within this dimension value using dynamic metrics
    # Use subquery pattern to avoid redundant metric calculations
    base_select_str = ',\n                '.join(base_select_items)

    if calculated_select_items:
        # List base metric aliases to select from inner query (including extra columns like days_in_range)
        base_metric_aliases = [metric.id for metric in all_base_metrics] + extra_base_columns
        base_aliases_str = ', '.join(base_metric_aliases)
        calculated_select_str = ',\n            '.join(calculated_select_items)

        query = f"""
            SELECT
                search_term,
                {base_aliases_str},
                {calculated_select_str}
            FROM (
                SELECT
                    search_term,
                    {base_select_str}
                FROM `{bq_service.table_path}`
                {where_clause}
                GROUP BY search_term
            )
            ORDER BY {primary_sort} DESC
            LIMIT {limit}
            OFFSET {offset}
        """
    else:
        # If no calculated metrics, use base metrics directly
        query = f"""
            SELECT
                search_term,
                {base_select_str}
            FROM `{bq_service.table_path}`
            {where_clause}
            GROUP BY search_term
            ORDER BY {primary_sort} DESC
            LIMIT {limit}
            OFFSET {offset}
        """

    df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

    if df.empty:
        return []

    # Fill NaN and infinity with 0 for all metric columns
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # Convert to PivotChildRow objects using dynamic metrics
    children = []
    for _, row in df.iterrows():
        # Extract all metrics dynamically from this row
        metrics = _extract_metrics_from_row(row, metrics_data)

        # Calculate avg_per_day - uses the configured metric
        avg_per_day_value = metrics.get(avg_per_day_metric, 0)
        avg_queries_per_day = safe_float(avg_per_day_value / num_days) if num_days > 0 and avg_per_day_value > 0 else 0.0

        # Calculate percentage of total based on primary sort metric
        primary_metric_value = metrics.get(primary_sort, 0)
        percentage_of_total = safe_float((primary_metric_value / grand_total_primary * 100)) if grand_total_primary > 0 else 0.0

        children.append(PivotChildRow(
            search_term=str(row['search_term']),
            metrics=metrics,
            avg_queries_per_day=avg_queries_per_day,
            percentage_of_total=percentage_of_total
        ))

    return children


def get_dimension_values(dimension: str, filters: FilterParams, table_id: Optional[str] = None) -> List[str]:
    """Get distinct values for a dimension from BigQuery or custom dimensions"""
    # Handle custom dimensions
    if dimension.startswith("custom_"):
        from services.custom_dimension_service import get_custom_dimension_service
        from config import CUSTOM_DIMENSIONS_FILE

        custom_dim_id = dimension.replace("custom_", "")
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        custom_dim = cd_service.get_by_id(custom_dim_id)

        if custom_dim is None:
            raise ValueError(f"Custom dimension {custom_dim_id} not found")

        # Return the labels from the custom dimension values
        return [value.label for value in custom_dim.values]

    # Handle standard BigQuery dimensions
    bq_service = get_bigquery_service(table_id)
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    return bq_service.query_dimension_values(
        dimension=dimension,
        filters=filters
    )
