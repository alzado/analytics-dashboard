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

    # For date_range type custom dimensions, run a query for each date range value
    if custom_dim.type == "date_range":
        all_rows = []
        total_queries_sum = 0
        total_queries_pdp_sum = 0
        total_queries_a2c_sum = 0
        total_purchases_sum = 0
        total_revenue_sum = 0.0
        total_search_term_count = 0

        # Track actual min/max dates across all queries for num_days calculation
        overall_min_date = None
        overall_max_date = None

        for value in custom_dim.values:
            # Create modified filters with this date range
            value_filters = FilterParams(**filters.dict())
            value_filters.start_date = value.start_date
            value_filters.end_date = value.end_date

            # Build filter clause for this date range
            where_clause = bq_service.build_filter_clause(
                start_date=value.start_date,
                end_date=value.end_date,
                country=filters.country,
                channel=filters.channel,
                gcategory=filters.gcategory,
                query_intent_classification=filters.query_intent_classification,
                n_words_normalized=filters.n_words_normalized,
                n_attributes=filters.n_attributes,
                n_attributes_min=filters.n_attributes_min,
                n_attributes_max=filters.n_attributes_max
            )

            # Query for this date range
            query = f"""
                SELECT
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(gross_purchase) as revenue,
                    COUNT(DISTINCT search_term) as search_term_count,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM `{bq_service.table_path}`
                {where_clause}
            """

            df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

            if not df.empty and df['queries'].iloc[0] > 0:
                row_data = df.iloc[0]
                queries = int(row_data['queries'])
                queries_pdp = int(row_data['queries_pdp'])
                queries_a2c = int(row_data['queries_a2c'])
                purchases = int(row_data['purchases'])
                revenue = safe_float(row_data['revenue'])
                search_term_count = int(row_data['search_term_count'])

                # Track date range for avg_queries_per_day calculation
                min_date = row_data['min_date']
                max_date = row_data['max_date']

                if min_date and max_date:
                    num_days = (max_date - min_date).days + 1
                else:
                    num_days = 1

                # Update overall date range
                if overall_min_date is None or (min_date and min_date < overall_min_date):
                    overall_min_date = min_date
                if overall_max_date is None or (max_date and max_date > overall_max_date):
                    overall_max_date = max_date

                # Calculate metrics
                ctr = safe_float(queries_pdp / queries) if queries > 0 else 0.0
                a2c_rate = safe_float(queries_a2c / queries) if queries > 0 else 0.0
                conversion_rate = safe_float(purchases / queries) if queries > 0 else 0.0
                pdp_conversion = safe_float(purchases / queries_pdp) if queries_pdp > 0 else 0.0
                revenue_per_query = safe_float(revenue / queries) if queries > 0 else 0.0
                aov = safe_float(revenue / purchases) if purchases > 0 else 0.0
                avg_queries_per_day = safe_float(queries / num_days) if num_days > 0 else 0.0

                # Add row
                all_rows.append(PivotRow(
                    dimension_value=value.label,
                    queries=queries,
                    queries_pdp=queries_pdp,
                    queries_a2c=queries_a2c,
                    purchases=purchases,
                    revenue=revenue,
                    ctr=ctr,
                    a2c_rate=a2c_rate,
                    conversion_rate=conversion_rate,
                    pdp_conversion=pdp_conversion,
                    revenue_per_query=revenue_per_query,
                    aov=aov,
                    avg_queries_per_day=avg_queries_per_day,
                    percentage_of_total=0.0,  # Will calculate after
                    search_term_count=search_term_count,
                    has_children=True
                ))

                # Accumulate totals
                total_queries_sum += queries
                total_queries_pdp_sum += queries_pdp
                total_queries_a2c_sum += queries_a2c
                total_purchases_sum += purchases
                total_revenue_sum += revenue
                total_search_term_count += search_term_count

        # Query for "Other" - dates not in any defined date range
        # Build exclusion conditions for all date ranges
        date_exclusions = []
        for value in custom_dim.values:
            date_exclusions.append(f"NOT (date BETWEEN '{value.start_date}' AND '{value.end_date}')")

        date_exclusion_clause = " AND ".join(date_exclusions) if date_exclusions else ""

        # Build base filter clause
        base_where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            country=filters.country,
            channel=filters.channel,
            gcategory=filters.gcategory,
            query_intent_classification=filters.query_intent_classification,
            n_words_normalized=filters.n_words_normalized,
            n_attributes=filters.n_attributes,
            n_attributes_min=filters.n_attributes_min,
            n_attributes_max=filters.n_attributes_max
        )

        # Add date exclusions to WHERE clause
        if date_exclusion_clause:
            if base_where_clause:
                combined_where_clause = f"{base_where_clause} AND ({date_exclusion_clause})"
            else:
                combined_where_clause = f"WHERE ({date_exclusion_clause})"
        else:
            combined_where_clause = base_where_clause

        # Query for "Other" dates
        other_query = f"""
            SELECT
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_service.table_path}`
            {combined_where_clause}
        """

        other_df = bq_service._execute_and_log_query(other_query, query_type="pivot", endpoint="data_service")

        if not other_df.empty and other_df['queries'].iloc[0] > 0:
            other_row_data = other_df.iloc[0]
            other_queries = int(other_row_data['queries'])
            other_queries_pdp = int(other_row_data['queries_pdp'])
            other_queries_a2c = int(other_row_data['queries_a2c'])
            other_purchases = int(other_row_data['purchases'])
            other_revenue = safe_float(other_row_data['revenue'])
            other_search_term_count = int(other_row_data['search_term_count'])

            # Track date range for avg_queries_per_day calculation
            other_min_date = other_row_data['min_date']
            other_max_date = other_row_data['max_date']

            if other_min_date and other_max_date:
                other_num_days = (other_max_date - other_min_date).days + 1
            else:
                other_num_days = 1

            # Update overall date range
            if overall_min_date is None or (other_min_date and other_min_date < overall_min_date):
                overall_min_date = other_min_date
            if overall_max_date is None or (other_max_date and other_max_date > overall_max_date):
                overall_max_date = other_max_date

            # Calculate metrics for "Other"
            other_ctr = safe_float(other_queries_pdp / other_queries) if other_queries > 0 else 0.0
            other_a2c_rate = safe_float(other_queries_a2c / other_queries) if other_queries > 0 else 0.0
            other_conversion_rate = safe_float(other_purchases / other_queries) if other_queries > 0 else 0.0
            other_pdp_conversion = safe_float(other_purchases / other_queries_pdp) if other_queries_pdp > 0 else 0.0
            other_revenue_per_query = safe_float(other_revenue / other_queries) if other_queries > 0 else 0.0
            other_aov = safe_float(other_revenue / other_purchases) if other_purchases > 0 else 0.0
            other_avg_queries_per_day = safe_float(other_queries / other_num_days) if other_num_days > 0 else 0.0

            # Add "Other" row
            all_rows.append(PivotRow(
                dimension_value="Other",
                queries=other_queries,
                queries_pdp=other_queries_pdp,
                queries_a2c=other_queries_a2c,
                purchases=other_purchases,
                revenue=other_revenue,
                ctr=other_ctr,
                a2c_rate=other_a2c_rate,
                conversion_rate=other_conversion_rate,
                pdp_conversion=other_pdp_conversion,
                revenue_per_query=other_revenue_per_query,
                aov=other_aov,
                avg_queries_per_day=other_avg_queries_per_day,
                percentage_of_total=0.0,  # Will calculate after
                search_term_count=other_search_term_count,
                has_children=True
            ))

            # Accumulate "Other" totals
            total_queries_sum += other_queries
            total_queries_pdp_sum += other_queries_pdp
            total_queries_a2c_sum += other_queries_a2c
            total_purchases_sum += other_purchases
            total_revenue_sum += other_revenue
            total_search_term_count += other_search_term_count

        # Calculate percentage of total for each row
        for row in all_rows:
            row.percentage_of_total = safe_float(row.queries / total_queries_sum * 100) if total_queries_sum > 0 else 0.0

        # Calculate overall num_days
        if overall_min_date and overall_max_date:
            overall_num_days = (overall_max_date - overall_min_date).days + 1
        else:
            overall_num_days = 1

        # Create total row
        total_row = PivotRow(
            dimension_value="Total",
            queries=total_queries_sum,
            queries_pdp=total_queries_pdp_sum,
            queries_a2c=total_queries_a2c_sum,
            purchases=total_purchases_sum,
            revenue=total_revenue_sum,
            ctr=safe_float(total_queries_pdp_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            a2c_rate=safe_float(total_queries_a2c_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            conversion_rate=safe_float(total_purchases_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            pdp_conversion=safe_float(total_purchases_sum / total_queries_pdp_sum) if total_queries_pdp_sum > 0 else 0.0,
            revenue_per_query=safe_float(total_revenue_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            aov=safe_float(total_revenue_sum / total_purchases_sum) if total_purchases_sum > 0 else 0.0,
            avg_queries_per_day=safe_float(total_queries_sum / overall_num_days) if overall_num_days > 0 else 0.0,
            percentage_of_total=100.0,
            search_term_count=total_search_term_count,
            has_children=False
        )

        # Get available dimensions (include built-in ones)
        dimension_map = {
            'n_words_normalized': 'n_words_normalized',
            'n_attributes': 'n_attributes',
            'channel': 'channel',
            'country': 'country',
            'gcategory_name': 'gcategory_name'
        }

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

    # For metric_condition type custom dimensions
    elif custom_dim.type == "metric_condition":
        all_rows = []

        # First, calculate the metric for all search terms
        where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            country=filters.country,
            channel=filters.channel,
            gcategory=filters.gcategory,
            query_intent_classification=filters.query_intent_classification,
            n_words_normalized=filters.n_words_normalized,
            n_attributes=filters.n_attributes,
            n_attributes_min=filters.n_attributes_min,
            n_attributes_max=filters.n_attributes_max
        )

        # Get metric column name - need to handle calculated metrics
        metric_name = custom_dim.metric

        # Map metric names to SQL expressions
        metric_expression_map = {
            'queries': 'SUM(queries)',
            'purchases': 'SUM(purchases)',
            'revenue': 'SUM(gross_purchase)',
            'queries_pdp': 'SUM(queries_pdp)',
            'queries_a2c': 'SUM(queries_a2c)',
            'ctr': 'SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))',
            'conversion_rate': 'SAFE_DIVIDE(SUM(purchases), SUM(queries))',
            'a2c_rate': 'SAFE_DIVIDE(SUM(queries_a2c), SUM(queries))',
            'pdp_conversion': 'SAFE_DIVIDE(SUM(purchases), SUM(queries_pdp))',
            'revenue_per_query': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(queries))',
            'aov': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(purchases))',
            'avg_queries_per_day': 'SAFE_DIVIDE(SUM(queries), COUNT(DISTINCT date))'
        }

        metric_expression = metric_expression_map.get(metric_name, f'SUM({metric_name})')

        # Track totals for the overall dataset
        total_queries_sum = 0
        total_queries_pdp_sum = 0
        total_queries_a2c_sum = 0
        total_purchases_sum = 0
        total_revenue_sum = 0.0
        total_search_term_count = 0
        overall_min_date = None
        overall_max_date = None

        # Query each metric dimension value
        for value in custom_dim.metric_values:
            # Build condition SQL from the value's conditions
            conditions_list = [
                {
                    'operator': cond.operator,
                    'value': cond.value,
                    'value_max': cond.value_max
                }
                for cond in value.conditions
            ]
            # Use "metric_value" as the column name in HAVING clause since that's what we alias it to in SELECT
            metric_condition_sql = bq_service.build_metric_condition_sql("metric_value", conditions_list)

            # Build query with metric condition as a subquery filter
            query = f"""
                WITH search_term_metrics AS (
                    SELECT
                        search_term,
                        SUM(queries) as queries,
                        SUM(queries_pdp) as queries_pdp,
                        SUM(queries_a2c) as queries_a2c,
                        SUM(purchases) as purchases,
                        SUM(gross_purchase) as revenue,
                        {metric_expression} as metric_value,
                        MIN(date) as min_date,
                        MAX(date) as max_date
                    FROM `{bq_service.table_path}`
                    {where_clause}
                    GROUP BY search_term
                    HAVING {metric_condition_sql}
                )
                SELECT
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(revenue) as revenue,
                    COUNT(DISTINCT search_term) as search_term_count,
                    MIN(min_date) as min_date,
                    MAX(max_date) as max_date
                FROM search_term_metrics
            """

            df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

            if not df.empty and df['queries'].iloc[0] > 0:
                row_data = df.iloc[0]
                queries = int(row_data['queries'])
                queries_pdp = int(row_data['queries_pdp'])
                queries_a2c = int(row_data['queries_a2c'])
                purchases = int(row_data['purchases'])
                revenue = safe_float(row_data['revenue'])
                search_term_count = int(row_data['search_term_count'])

                # Track date range
                min_date = row_data['min_date']
                max_date = row_data['max_date']
                if min_date and max_date:
                    num_days = (max_date - min_date).days + 1
                else:
                    num_days = 1

                # Calculate metrics
                ctr = safe_float(queries_pdp / queries) if queries > 0 else 0.0
                a2c_rate = safe_float(queries_a2c / queries) if queries > 0 else 0.0
                conversion_rate = safe_float(purchases / queries) if queries > 0 else 0.0
                pdp_conversion = safe_float(purchases / queries_pdp) if queries_pdp > 0 else 0.0
                revenue_per_query = safe_float(revenue / queries) if queries > 0 else 0.0
                aov = safe_float(revenue / purchases) if purchases > 0 else 0.0
                avg_queries_per_day = safe_float(queries / num_days) if num_days > 0 else 0.0

                all_rows.append(PivotRow(
                    dimension_value=value.label,
                    queries=queries,
                    queries_pdp=queries_pdp,
                    queries_a2c=queries_a2c,
                    purchases=purchases,
                    revenue=revenue,
                    ctr=ctr,
                    a2c_rate=a2c_rate,
                    conversion_rate=conversion_rate,
                    pdp_conversion=pdp_conversion,
                    revenue_per_query=revenue_per_query,
                    aov=aov,
                    avg_queries_per_day=avg_queries_per_day,
                    percentage_of_total=0.0,  # Will calculate after
                    search_term_count=search_term_count,
                    has_children=True
                ))

        # Query for "Other" group (data NOT matching any conditions)
        # Build negated conditions (NOT matching any of the defined conditions)
        if True:  # Always create "Other" category
            all_conditions = []
            for value in custom_dim.metric_values:
                conditions_list = [
                    {
                        'operator': cond.operator,
                        'value': cond.value,
                        'value_max': cond.value_max
                    }
                    for cond in value.conditions
                ]
                # Use "metric_value" as the column name in HAVING clause since that's what we alias it to in SELECT
                value_condition = bq_service.build_metric_condition_sql("metric_value", conditions_list)
                all_conditions.append(f"({value_condition})")

            negated_condition = " OR ".join(all_conditions)

            query = f"""
                WITH search_term_metrics AS (
                    SELECT
                        search_term,
                        SUM(queries) as queries,
                        SUM(queries_pdp) as queries_pdp,
                        SUM(queries_a2c) as queries_a2c,
                        SUM(purchases) as purchases,
                        SUM(gross_purchase) as revenue,
                        {metric_expression} as metric_value,
                        MIN(date) as min_date,
                        MAX(date) as max_date
                    FROM `{bq_service.table_path}`
                    {where_clause}
                    GROUP BY search_term
                    HAVING NOT ({negated_condition})
                )
                SELECT
                    SUM(queries) as queries,
                    SUM(queries_pdp) as queries_pdp,
                    SUM(queries_a2c) as queries_a2c,
                    SUM(purchases) as purchases,
                    SUM(revenue) as revenue,
                    COUNT(DISTINCT search_term) as search_term_count,
                    MIN(min_date) as min_date,
                    MAX(max_date) as max_date
                FROM search_term_metrics
            """

            df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

            if not df.empty and df['queries'].iloc[0] > 0:
                row_data = df.iloc[0]
                queries = int(row_data['queries'])
                queries_pdp = int(row_data['queries_pdp'])
                queries_a2c = int(row_data['queries_a2c'])
                purchases = int(row_data['purchases'])
                revenue = safe_float(row_data['revenue'])
                search_term_count = int(row_data['search_term_count'])

                min_date = row_data['min_date']
                max_date = row_data['max_date']
                if min_date and max_date:
                    num_days = (max_date - min_date).days + 1
                else:
                    num_days = 1

                ctr = safe_float(queries_pdp / queries) if queries > 0 else 0.0
                a2c_rate = safe_float(queries_a2c / queries) if queries > 0 else 0.0
                conversion_rate = safe_float(purchases / queries) if queries > 0 else 0.0
                pdp_conversion = safe_float(purchases / queries_pdp) if queries_pdp > 0 else 0.0
                revenue_per_query = safe_float(revenue / queries) if queries > 0 else 0.0
                aov = safe_float(revenue / purchases) if purchases > 0 else 0.0
                avg_queries_per_day = safe_float(queries / num_days) if num_days > 0 else 0.0

                all_rows.append(PivotRow(
                    dimension_value="Other",
                    queries=queries,
                    queries_pdp=queries_pdp,
                    queries_a2c=queries_a2c,
                    purchases=purchases,
                    revenue=revenue,
                    ctr=ctr,
                    a2c_rate=a2c_rate,
                    conversion_rate=conversion_rate,
                    pdp_conversion=pdp_conversion,
                    revenue_per_query=revenue_per_query,
                    aov=aov,
                    avg_queries_per_day=avg_queries_per_day,
                    percentage_of_total=0.0,
                    search_term_count=search_term_count,
                    has_children=True
                ))

        # Calculate total (overall dataset, not sum of groups since there can be overlap)
        total_query = f"""
            SELECT
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM `{bq_service.table_path}`
            {where_clause}
        """

        total_df = bq_service._execute_and_log_query(total_query, query_type="pivot", endpoint="data_service")

        if not total_df.empty:
            total_data = total_df.iloc[0]
            total_queries_sum = int(total_data['queries'])
            total_queries_pdp_sum = int(total_data['queries_pdp'])
            total_queries_a2c_sum = int(total_data['queries_a2c'])
            total_purchases_sum = int(total_data['purchases'])
            total_revenue_sum = safe_float(total_data['revenue'])
            total_search_term_count = int(total_data['search_term_count'])
            overall_min_date = total_data['min_date']
            overall_max_date = total_data['max_date']

        # Calculate percentage for each row
        for row in all_rows:
            row.percentage_of_total = safe_float(row.queries / total_queries_sum * 100) if total_queries_sum > 0 else 0.0

        # Calculate overall num_days
        if overall_min_date and overall_max_date:
            overall_num_days = (overall_max_date - overall_min_date).days + 1
        else:
            overall_num_days = 1

        # Create total row
        total_row = PivotRow(
            dimension_value="Total",
            queries=total_queries_sum,
            queries_pdp=total_queries_pdp_sum,
            queries_a2c=total_queries_a2c_sum,
            purchases=total_purchases_sum,
            revenue=total_revenue_sum,
            ctr=safe_float(total_queries_pdp_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            a2c_rate=safe_float(total_queries_a2c_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            conversion_rate=safe_float(total_purchases_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            pdp_conversion=safe_float(total_purchases_sum / total_queries_pdp_sum) if total_queries_pdp_sum > 0 else 0.0,
            revenue_per_query=safe_float(total_revenue_sum / total_queries_sum) if total_queries_sum > 0 else 0.0,
            aov=safe_float(total_revenue_sum / total_purchases_sum) if total_purchases_sum > 0 else 0.0,
            avg_queries_per_day=safe_float(total_queries_sum / overall_num_days) if overall_num_days > 0 else 0.0,
            percentage_of_total=100.0,
            search_term_count=total_search_term_count,
            has_children=False
        )

        dimension_map = {
            'n_words_normalized': 'n_words_normalized',
            'n_attributes': 'n_attributes',
            'channel': 'channel',
            'country': 'country',
            'gcategory_name': 'gcategory_name'
        }

        return PivotResponse(
            rows=all_rows,
            total=total_row,
            available_dimensions=list(dimension_map.keys()),
            dimension_metadata={
                "id": custom_dim.id,
                "name": custom_dim.name,
                "type": custom_dim.type,
                "metric": custom_dim.metric,
                "is_custom": True
            }
        )

    return None


def get_overview_metrics(filters: FilterParams) -> OverviewMetrics:
    """Get overview metrics from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    metrics_dict = bq_service.query_kpi_metrics(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    # Calculate derived metrics
    queries = metrics_dict.get('queries', 0)
    queries_pdp = metrics_dict.get('queries_pdp', 0)
    queries_a2c = metrics_dict.get('queries_a2c', 0)
    purchases = metrics_dict.get('purchases', 0)
    revenue = metrics_dict.get('revenue', 0)

    # Calculate number of days for avg_queries_per_day
    num_days = 1
    if filters.start_date and filters.end_date:
        from datetime import datetime
        start = datetime.strptime(filters.start_date, '%Y-%m-%d')
        end = datetime.strptime(filters.end_date, '%Y-%m-%d')
        num_days = (end - start).days + 1

    return OverviewMetrics(
        queries=queries,
        queries_pdp=queries_pdp,
        queries_a2c=queries_a2c,
        purchases=purchases,
        revenue=revenue,
        ctr=queries_pdp / queries if queries > 0 else 0,
        a2c_rate=queries_a2c / queries if queries > 0 else 0,
        conversion_rate=purchases / queries if queries > 0 else 0,
        pdp_conversion=purchases / queries_pdp if queries_pdp > 0 else 0,
        revenue_per_query=revenue / queries if queries > 0 else 0,
        aov=revenue / purchases if purchases > 0 else 0,
        avg_queries_per_day=queries / num_days if num_days > 0 else 0,
        unique_search_terms=metrics_dict.get('unique_search_terms', 0)
    )


def get_trend_data(filters: FilterParams, granularity: str = "daily") -> List[TrendData]:
    """Get time series trend data from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_timeseries(
        granularity=granularity,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(TrendData(
            date=row['date'].strftime('%Y-%m-%d'),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            a2c_rate=float(row['a2c_rate']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            revenue_per_query=float(row['revenue_per_query'])
        ))

    return result


def get_dimension_breakdown(dimension: str, filters: FilterParams, limit: int = 20) -> List[DimensionBreakdown]:
    """Get breakdown by dimension from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_dimension_breakdown(
        dimension=dimension,
        limit=limit,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    total_queries = df['queries'].sum()

    # Calculate number of days for avg_queries_per_day
    num_days = 1
    if filters.start_date and filters.end_date:
        from datetime import datetime
        start = datetime.strptime(filters.start_date, '%Y-%m-%d')
        end = datetime.strptime(filters.end_date, '%Y-%m-%d')
        num_days = (end - start).days + 1

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0
    df['percentage_of_total'] = (df['queries'] / total_queries * 100) if total_queries > 0 else 0

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(DimensionBreakdown(
            dimension_value=str(row['dimension_value']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            a2c_rate=float(row['a2c_rate']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            revenue_per_query=float(row['revenue_per_query']),
            avg_queries_per_day=float(row['avg_queries_per_day']),
            percentage_of_total=float(row['percentage_of_total'])
        ))

    return result


def get_search_terms(filters: FilterParams, limit: int = 100, sort_by: str = "queries") -> List[SearchTermData]:
    """Get top search terms from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    df = bq_service.query_search_terms(
        limit=limit,
        sort_by=sort_by,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0

    # Fill NaN with 0
    df = df.fillna(0)

    # Convert to response format
    result = []
    for _, row in df.iterrows():
        result.append(SearchTermData(
            search_term=str(row['search_term']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=float(row['revenue']),
            ctr=float(row['ctr']),
            conversion_rate=float(row['conversion_rate']),
            pdp_conversion=float(row['pdp_conversion']),
            avg_queries_per_day=float(row['avg_queries_per_day']),
            n_words=int(row['n_words_normalized']),
            n_attributes=int(row['n_attributes'])
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

    date_range = {
        'min': date_df['min_date'].iloc[0].strftime('%Y-%m-%d'),
        'max': date_df['max_date'].iloc[0].strftime('%Y-%m-%d')
    }

    attributes = ['categoria', 'tipo', 'genero', 'marca', 'color', 'material', 'talla', 'modelo']

    return FilterOptions(
        countries=countries,
        channels=channels,
        date_range=date_range,
        attributes=attributes
    )


def get_pivot_data(dimensions: List[str], filters: FilterParams, limit: int = 50) -> PivotResponse:
    """Get hierarchical pivot table data by dimensions from BigQuery"""
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    # Check if any dimension is a custom dimension (starts with "custom_")
    if dimensions and len(dimensions) > 0:
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

    # Dimension map for available dimensions
    dimension_map = {
        'n_words_normalized': 'n_words_normalized',
        'n_attributes': 'n_attributes',
        'channel': 'channel',
        'country': 'country',
        'gcategory_name': 'gcategory_name'
    }

    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    where_clause_for_dates = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        query_intent_classification=filters.query_intent_classification,
        n_words_normalized=filters.n_words_normalized,
        n_attributes=filters.n_attributes,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )
    date_range_query = f"""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM `{bq_service.table_path}`
        {where_clause_for_dates}
    """
    date_range_df = bq_service._execute_and_log_query(date_range_query, query_type="pivot", endpoint="data_service")

    # Calculate number of days from actual data
    num_days = 1
    if not date_range_df.empty and date_range_df['min_date'].iloc[0] is not None and date_range_df['max_date'].iloc[0] is not None:
        min_date = date_range_df['min_date'].iloc[0]
        max_date = date_range_df['max_date'].iloc[0]
        num_days = (max_date - min_date).days + 1

    # If no dimensions provided, return aggregated totals as a single row
    if not dimensions:
        # Build filter clause (include n_words and n_attributes for exact filtering)
        where_clause = bq_service.build_filter_clause(
            start_date=filters.start_date,
            end_date=filters.end_date,
            country=filters.country,
            channel=filters.channel,
            gcategory=filters.gcategory,
            n_words_normalized=filters.n_words_normalized,
            n_attributes=filters.n_attributes,
            n_attributes_min=filters.n_attributes_min,
            n_attributes_max=filters.n_attributes_max
        )

        # Query for aggregated totals
        query = f"""
            SELECT
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count
            FROM `{bq_service.table_path}`
            {where_clause}
        """

        df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

        if df.empty or df['queries'].iloc[0] == 0:
            # Return empty response with zero totals
            total_row = PivotRow(
                dimension_value="Total",
                queries=0,
                queries_pdp=0,
                queries_a2c=0,
                purchases=0,
                revenue=0.0,
                ctr=0.0,
                a2c_rate=0.0,
                conversion_rate=0.0,
                pdp_conversion=0.0,
                revenue_per_query=0.0,
                aov=0.0,
                avg_queries_per_day=0.0,
                percentage_of_total=100.0,
                search_term_count=0,
                has_children=False
            )
            return PivotResponse(
                rows=[total_row],
                total=total_row,
                available_dimensions=list(dimension_map.keys())
            )

        # Calculate metrics
        row_data = df.iloc[0]
        queries = int(row_data['queries'])
        queries_pdp = int(row_data['queries_pdp'])
        queries_a2c = int(row_data['queries_a2c'])
        purchases = int(row_data['purchases'])
        revenue = safe_float(row_data['revenue'])
        search_term_count = int(row_data['search_term_count'])

        ctr = safe_float(queries_pdp / queries) if queries > 0 else 0.0
        a2c_rate = safe_float(queries_a2c / queries) if queries > 0 else 0.0
        conversion_rate = safe_float(purchases / queries) if queries > 0 else 0.0
        pdp_conversion = safe_float(purchases / queries_pdp) if queries_pdp > 0 else 0.0
        revenue_per_query = safe_float(revenue / queries) if queries > 0 else 0.0
        aov = safe_float(revenue / purchases) if purchases > 0 else 0.0
        avg_queries_per_day = safe_float(queries / num_days) if num_days > 0 else 0.0

        total_row = PivotRow(
            dimension_value="All Data",
            queries=queries,
            queries_pdp=queries_pdp,
            queries_a2c=queries_a2c,
            purchases=purchases,
            revenue=revenue,
            ctr=ctr,
            a2c_rate=a2c_rate,
            conversion_rate=conversion_rate,
            pdp_conversion=pdp_conversion,
            revenue_per_query=revenue_per_query,
            aov=aov,
            avg_queries_per_day=avg_queries_per_day,
            percentage_of_total=100.0,
            search_term_count=search_term_count,
            has_children=False
        )
        return PivotResponse(
            rows=[total_row],
            total=total_row,
            available_dimensions=list(dimension_map.keys())
        )

    # Build filter clause
    where_clause = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        query_intent_classification=filters.query_intent_classification,
        n_words_normalized=filters.n_words_normalized,
        n_attributes=filters.n_attributes,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )

    # Map all dimensions to their column names
    group_cols = [dimension_map.get(dim, dim) for dim in dimensions]
    group_by_clause = ", ".join(group_cols)

    # Build SELECT clause for dimension values
    # For multiple dimensions, concat them with " - " separator
    if len(group_cols) > 1:
        # Convert each column to string and join with separator
        cast_cols = [f"CAST({col} AS STRING)" for col in group_cols]
        separator = ', " - ", '
        concat_args = separator.join(cast_cols)
        dim_value_clause = f"CONCAT({concat_args}) as dimension_value"
    else:
        dim_value_clause = f"{group_cols[0]} as dimension_value"

    # Query for pivot data
    query = f"""
        WITH grouped_data AS (
            SELECT
                {dim_value_clause},
                SUM(queries) as queries,
                SUM(queries_pdp) as queries_pdp,
                SUM(queries_a2c) as queries_a2c,
                SUM(purchases) as purchases,
                SUM(gross_purchase) as revenue,
                COUNT(DISTINCT search_term) as search_term_count
            FROM `{bq_service.table_path}`
            {where_clause}
            GROUP BY {group_by_clause}
            ORDER BY queries DESC
            LIMIT {limit}
        ),
        total_data AS (
            SELECT SUM(queries) as total_queries
            FROM `{bq_service.table_path}`
            {where_clause}
        )
        SELECT
            grouped_data.*,
            total_data.total_queries
        FROM grouped_data
        CROSS JOIN total_data
    """

    df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

    if df.empty:
        # Return empty response with zero totals
        total_row = PivotRow(
            dimension_value="Total",
            queries=0,
            queries_pdp=0,
            queries_a2c=0,
            purchases=0,
            revenue=0.0,
            ctr=0.0,
            a2c_rate=0.0,
            conversion_rate=0.0,
            pdp_conversion=0.0,
            revenue_per_query=0.0,
            aov=0.0,
            avg_queries_per_day=0.0,
            percentage_of_total=100.0,
            search_term_count=0,
            has_children=False
        )
        return PivotResponse(
            rows=[],
            total=total_row,
            available_dimensions=list(dimension_map.keys())
        )

    total_queries = df['total_queries'].iloc[0]

    # Calculate metrics for each row
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['a2c_rate'] = df['queries_a2c'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['revenue_per_query'] = df['revenue'] / df['queries']
    df['aov'] = df['revenue'] / df['purchases']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0
    df['percentage_of_total'] = (df['queries'] / total_queries * 100) if total_queries > 0 else 0
    df['has_children'] = True  # All dimension rows have search terms as children

    # Fill NaN and infinity with 0
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # Convert to PivotRow objects
    rows = []
    for _, row in df.iterrows():
        rows.append(PivotRow(
            dimension_value=str(row['dimension_value']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            queries_a2c=int(row['queries_a2c']),
            purchases=int(row['purchases']),
            revenue=safe_float(row['revenue']),
            ctr=safe_float(row['ctr']),
            a2c_rate=safe_float(row['a2c_rate']),
            conversion_rate=safe_float(row['conversion_rate']),
            pdp_conversion=safe_float(row['pdp_conversion']),
            revenue_per_query=safe_float(row['revenue_per_query']),
            aov=safe_float(row['aov']),
            avg_queries_per_day=safe_float(row['avg_queries_per_day']),
            percentage_of_total=safe_float(row['percentage_of_total']),
            search_term_count=int(row['search_term_count']),
            has_children=True
        ))

    # Calculate totals
    total_row = PivotRow(
        dimension_value="Total",
        queries=int(df['queries'].sum()),
        queries_pdp=int(df['queries_pdp'].sum()),
        queries_a2c=int(df['queries_a2c'].sum()),
        purchases=int(df['purchases'].sum()),
        revenue=safe_float(df['revenue'].sum()),
        ctr=safe_float(df['queries_pdp'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        a2c_rate=safe_float(df['queries_a2c'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        conversion_rate=safe_float(df['purchases'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        pdp_conversion=safe_float(df['purchases'].sum() / df['queries_pdp'].sum()) if df['queries_pdp'].sum() > 0 else 0.0,
        revenue_per_query=safe_float(df['revenue'].sum() / df['queries'].sum()) if df['queries'].sum() > 0 else 0.0,
        aov=safe_float(df['revenue'].sum() / df['purchases'].sum()) if df['purchases'].sum() > 0 else 0.0,
        avg_queries_per_day=safe_float(df['queries'].sum() / num_days) if num_days > 0 else 0.0,
        percentage_of_total=100.0,
        search_term_count=int(df['search_term_count'].sum()),
        has_children=False
    )

    return PivotResponse(
        rows=rows,
        total=total_row,
        available_dimensions=list(dimension_map.keys())
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
            filters = FilterParams(**filters.dict())
            filters.start_date = date_range_value.start_date
            filters.end_date = date_range_value.end_date

            # Set dimension to empty string so the rest of the function handles it as a non-dimension query
            dimension = ""

        elif custom_dim.type == "metric_condition":
            # For metric_condition type, we need to filter search terms based on metric conditions
            # The children query will be built differently - we'll return early

            # Find the metric value that matches the label
            metric_value = None
            for val in custom_dim.metric_values:
                if val.label == value:
                    metric_value = val
                    break

            if not metric_value:
                raise ValueError(f"Value '{value}' not found in custom dimension {custom_dim.name}")

            # Get metric expression map
            metric_expression_map = {
                'queries': 'SUM(queries)',
                'purchases': 'SUM(purchases)',
                'revenue': 'SUM(gross_purchase)',
                'queries_pdp': 'SUM(queries_pdp)',
                'queries_a2c': 'SUM(queries_a2c)',
                'ctr': 'SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))',
                'conversion_rate': 'SAFE_DIVIDE(SUM(purchases), SUM(queries))',
                'a2c_rate': 'SAFE_DIVIDE(SUM(queries_a2c), SUM(queries))',
                'pdp_conversion': 'SAFE_DIVIDE(SUM(purchases), SUM(queries_pdp))',
                'revenue_per_query': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(queries))',
                'aov': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(purchases))',
                'avg_queries_per_day': 'SAFE_DIVIDE(SUM(queries), COUNT(DISTINCT date))'
            }

            metric_expression = metric_expression_map.get(custom_dim.metric, f'SUM({custom_dim.metric})')

            # Build condition SQL
            conditions_list = [
                {
                    'operator': cond.operator,
                    'value': cond.value,
                    'value_max': cond.value_max
                }
                for cond in metric_value.conditions
            ]
            metric_condition_sql = bq_service.build_metric_condition_sql("metric_value", conditions_list)

            # Build filter clause
            try:
                where_clause = bq_service.build_filter_clause(
                    start_date=filters.start_date,
                    end_date=filters.end_date,
                    country=filters.country,
                    channel=filters.channel,
                    gcategory=filters.gcategory,
                    query_intent_classification=filters.query_intent_classification,
                    n_words_normalized=filters.n_words_normalized,
                    n_attributes=filters.n_attributes,
                    n_attributes_min=filters.n_attributes_min,
                    n_attributes_max=filters.n_attributes_max
                )
            except Exception as e:
                raise

            # Query for search terms that match the metric condition
            query = f"""
                WITH search_term_metrics AS (
                    SELECT
                        search_term,
                        SUM(queries) as queries,
                        SUM(queries_pdp) as queries_pdp,
                        SUM(purchases) as purchases,
                        SUM(gross_purchase) as revenue,
                        {metric_expression} as metric_value,
                        MIN(date) as min_date,
                        MAX(date) as max_date
                    FROM `{bq_service.table_path}`
                    {where_clause}
                    GROUP BY search_term
                    HAVING {metric_condition_sql}
                )
                SELECT
                    search_term,
                    queries,
                    queries_pdp,
                    purchases,
                    revenue,
                    min_date,
                    max_date
                FROM search_term_metrics
                ORDER BY queries DESC
                LIMIT {limit}
                OFFSET {offset}
            """

            try:
                df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")
            except Exception as e:
                raise

            if df.empty:
                return []

            # Calculate date range for avg_queries_per_day
            if not df.empty and df['min_date'].iloc[0] is not None and df['max_date'].iloc[0] is not None:
                min_date = df['min_date'].min()
                max_date = df['max_date'].max()
                num_days = (max_date - min_date).days + 1
            else:
                num_days = 1

            # Get grand total for percentage calculation
            total_query = f"""
                SELECT SUM(queries) as total_queries
                FROM `{bq_service.table_path}`
                {where_clause}
            """
            total_df = bq_service._execute_and_log_query(total_query, query_type="pivot", endpoint="data_service")
            grand_total_queries = float(total_df['total_queries'].iloc[0]) if not total_df.empty and total_df['total_queries'].iloc[0] is not None else 0

            # Drop date columns (they have db_dtypes that don't support fillna with numeric values)
            df = df.drop(columns=['min_date', 'max_date'])

            # Calculate metrics
            try:
                df['ctr'] = df['queries_pdp'] / df['queries']
                df['conversion_rate'] = df['purchases'] / df['queries']
                df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
                # Use pandas division (will handle division by zero with NaN, which fillna will fix)
                df['avg_queries_per_day'] = df['queries'] / num_days
                df['percentage_of_total'] = df['queries'] / grand_total_queries
                df['aov'] = df['revenue'] / df['purchases']

                # Fill NaN and infinity with 0
                df = df.fillna(0)
                df = df.replace([np.inf, -np.inf], 0)
            except Exception as e:
                raise

            # Convert to PivotChildRow objects
            children = []
            for idx, row in df.iterrows():
                try:
                    children.append(PivotChildRow(
                        search_term=str(row['search_term']),
                        queries=int(row['queries']),
                        queries_pdp=int(row['queries_pdp']),
                        purchases=int(row['purchases']),
                        revenue=safe_float(row['revenue']),
                        ctr=safe_float(row['ctr']),
                        conversion_rate=safe_float(row['conversion_rate']),
                        pdp_conversion=safe_float(row['pdp_conversion']),
                        avg_queries_per_day=safe_float(row['avg_queries_per_day']),
                        percentage_of_total=safe_float(row['percentage_of_total']),
                        aov=safe_float(row['aov'])
                    ))
                except Exception as e:
                    raise

            return children

    # Query actual date range from filtered data for accurate avg_queries_per_day calculation
    where_clause_for_dates = bq_service.build_filter_clause(
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        query_intent_classification=filters.query_intent_classification,
        n_words_normalized=filters.n_words_normalized,
        n_attributes=filters.n_attributes,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )
    date_range_query = f"""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM `{bq_service.table_path}`
        {where_clause_for_dates}
    """
    date_range_df = bq_service._execute_and_log_query(date_range_query, query_type="pivot", endpoint="data_service")

    # Calculate number of days from actual data
    num_days = 1
    if not date_range_df.empty and date_range_df['min_date'].iloc[0] is not None and date_range_df['max_date'].iloc[0] is not None:
        min_date = date_range_df['min_date'].iloc[0]
        max_date = date_range_df['max_date'].iloc[0]
        num_days = (max_date - min_date).days + 1

    # Build base filter clause
    where_conditions = []

    # Apply same date clamping as parent query to ensure consistency
    clamped_start, clamped_end = bq_service._clamp_dates(filters.start_date, filters.end_date)

    if clamped_start and clamped_end:
        where_conditions.append(f"date BETWEEN '{clamped_start}' AND '{clamped_end}'")
    elif clamped_start:
        where_conditions.append(f"date >= '{clamped_start}'")
    elif clamped_end:
        where_conditions.append(f"date <= '{clamped_end}'")
    if filters.country:
        where_conditions.append(f"country = '{filters.country}'")
    if filters.channel:
        where_conditions.append(f"channel = '{filters.channel}'")
    if filters.gcategory:
        where_conditions.append(f"gcategory_name = '{filters.gcategory}'")
    if filters.query_intent_classification:
        where_conditions.append(f"query_intent_classification = '{filters.query_intent_classification}'")
    if filters.n_attributes_min is not None:
        where_conditions.append(f"n_attributes >= {filters.n_attributes_min}")
    if filters.n_attributes_max is not None:
        where_conditions.append(f"n_attributes <= {filters.n_attributes_max}")
    # Exact dimension filters (for table dimensions in pivot)
    if filters.n_words_normalized is not None:
        where_conditions.append(f"n_words_normalized = {filters.n_words_normalized}")
    if filters.n_attributes is not None:
        where_conditions.append(f"n_attributes = {filters.n_attributes}")

    # Add dimension filter only if dimension is specified
    if dimension:  # If dimension is provided (not empty string)
        # Built-in dimensions - map to actual column names
        dimension_map = {
            'n_words_normalized': 'n_words_normalized',  # Actual column name in BigQuery
            'n_attributes': 'n_attributes',
            'channel': 'channel',
            'country': 'country',
            'gcategory_name': 'gcategory_name'
        }
        group_col = dimension_map.get(dimension, dimension)

        # Numeric dimensions don't need quotes, string dimensions do
        numeric_dimensions = {'n_words_normalized', 'n_attributes', 'n_words_normalized'}
        if dimension in numeric_dimensions or group_col in numeric_dimensions:
            where_conditions.append(f"{group_col} = {value}")
        else:
            where_conditions.append(f"{group_col} = '{value}'")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # First, get the grand total queries for percentage calculation (same filters, no dimension restriction)
    base_where_conditions = []
    if clamped_start and clamped_end:
        base_where_conditions.append(f"date BETWEEN '{clamped_start}' AND '{clamped_end}'")
    elif clamped_start:
        base_where_conditions.append(f"date >= '{clamped_start}'")
    elif clamped_end:
        base_where_conditions.append(f"date <= '{clamped_end}'")
    if filters.country:
        base_where_conditions.append(f"country = '{filters.country}'")
    if filters.channel:
        base_where_conditions.append(f"channel = '{filters.channel}'")
    if filters.gcategory:
        base_where_conditions.append(f"gcategory_name = '{filters.gcategory}'")
    if filters.query_intent_classification:
        base_where_conditions.append(f"query_intent_classification = '{filters.query_intent_classification}'")
    if filters.n_attributes_min is not None:
        base_where_conditions.append(f"n_attributes >= {filters.n_attributes_min}")
    if filters.n_attributes_max is not None:
        base_where_conditions.append(f"n_attributes <= {filters.n_attributes_max}")
    # Exact dimension filters (for table dimensions in pivot)
    if filters.n_words_normalized is not None:
        base_where_conditions.append(f"n_words_normalized = {filters.n_words_normalized}")
    if filters.n_attributes is not None:
        base_where_conditions.append(f"n_attributes = {filters.n_attributes}")

    base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""

    total_query = f"""
        SELECT SUM(queries) as total_queries
        FROM `{bq_service.table_path}`
        {base_where_clause}
    """

    total_df = bq_service._execute_and_log_query(total_query, query_type="pivot", endpoint="data_service")
    grand_total_queries = float(total_df['total_queries'].iloc[0]) if not total_df.empty and total_df['total_queries'].iloc[0] is not None else 0

    # Query for search terms within this dimension value
    query = f"""
        SELECT
            search_term,
            SUM(queries) as queries,
            SUM(queries_pdp) as queries_pdp,
            SUM(purchases) as purchases,
            SUM(gross_purchase) as revenue
        FROM `{bq_service.table_path}`
        {where_clause}
        GROUP BY search_term
        ORDER BY queries DESC
        LIMIT {limit}
        OFFSET {offset}
    """

    df = bq_service._execute_and_log_query(query, query_type="pivot", endpoint="data_service")

    if df.empty:
        return []

    # Calculate rates
    df['ctr'] = df['queries_pdp'] / df['queries']
    df['conversion_rate'] = df['purchases'] / df['queries']
    df['pdp_conversion'] = df['purchases'] / df['queries_pdp']
    df['avg_queries_per_day'] = df['queries'] / num_days if num_days > 0 else 0

    # Calculate percentage of total (relative to grand total)
    df['percentage_of_total'] = df['queries'] / grand_total_queries if grand_total_queries > 0 else 0

    # Calculate AOV (Average Order Value)
    df['aov'] = df['revenue'] / df['purchases']

    # Fill NaN and infinity with 0
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    # Convert to PivotChildRow objects
    children = []
    for _, row in df.iterrows():
        children.append(PivotChildRow(
            search_term=str(row['search_term']),
            queries=int(row['queries']),
            queries_pdp=int(row['queries_pdp']),
            purchases=int(row['purchases']),
            revenue=safe_float(row['revenue']),
            ctr=safe_float(row['ctr']),
            conversion_rate=safe_float(row['conversion_rate']),
            pdp_conversion=safe_float(row['pdp_conversion']),
            avg_queries_per_day=safe_float(row['avg_queries_per_day']),
            percentage_of_total=safe_float(row['percentage_of_total']),
            aov=safe_float(row['aov'])
        ))

    return children


def get_dimension_values(dimension: str, filters: FilterParams) -> List[str]:
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
    bq_service = get_bigquery_service()
    if bq_service is None:
        raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

    return bq_service.query_dimension_values(
        dimension=dimension,
        start_date=filters.start_date,
        end_date=filters.end_date,
        country=filters.country,
        channel=filters.channel,
        gcategory=filters.gcategory,
        query_intent_classification=filters.query_intent_classification,
        n_attributes_min=filters.n_attributes_min,
        n_attributes_max=filters.n_attributes_max
    )
