"""
Data service - Business logic layer for analytics.
Handles pivot table data, metrics calculations, and data transformations.

Django port of the FastAPI data_service.py with rollup routing support.
"""
import math
import re
import logging
from typing import List, Dict, Optional, Any, Tuple

import pandas as pd

from apps.tables.models import BigQueryTable
from .bigquery_service import BigQueryService
from .query_router_service import QueryRouterService, RouteDecision

logger = logging.getLogger(__name__)


def safe_float(value: float) -> float:
    """Convert a value to float, replacing NaN and infinity with 0."""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return 0.0
    return float(value)


class DataService:
    """Service for data operations and transformations."""

    def __init__(self, bigquery_table: BigQueryTable, user=None):
        self.bigquery_table = bigquery_table
        self.bq_service = BigQueryService(bigquery_table, user)

    def get_pivot_data(
        self,
        dimensions: List[str],
        filters: Dict,
        limit: int = 50,
        offset: int = 0,
        dimension_values: Optional[List[str]] = None,
        skip_count: bool = False,
        metrics: Optional[List[str]] = None,
        require_rollup: bool = True
    ) -> Dict[str, Any]:
        """
        Get pivot table data grouped by dimensions.

        Args:
            dimensions: List of dimension column names to group by
            filters: Filter parameters (start_date, end_date, dimension_filters, etc.)
            limit: Max rows to return
            offset: Rows to skip
            dimension_values: Specific dimension values to fetch (for multi-table matching)
            skip_count: If True, skip the count query
            metrics: Optional list of metric IDs to calculate
            require_rollup: If True, require rollup (error if not available)

        Returns:
            Dict with rows, total, available_dimensions matching frontend expectations
        """
        # Load metrics configuration first (needed for routing)
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])

        # =================================================================
        # ROLLUP ROUTING: Check if a suitable rollup exists for this query
        # =================================================================
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []
        route_decision = self.route_query(
            dimensions=dimensions,
            metrics=metric_ids,
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Pivot routing: dims={dimensions}, filters={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            all_required_dims = list(set((dimensions or []) + filter_dims))

            # Get available rollups sorted by closeness to the requested configuration
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=dimensions or [],
                    query_metrics=metric_ids,
                    query_filters=filters.get('dimension_filters')
                )

            return {
                'rows': [],
                'total': None,
                'available_dimensions': self._get_available_dimensions(),
                'total_count': 0,
                'error': f"No suitable rollup found. Query dimensions: {dimensions or []}, Filter dimensions: {filter_dims}. Reason: {route_decision.reason}. Create a rollup with dimensions {all_required_dims} to enable this query.",
                'error_type': 'rollup_required',
                'available_rollups': available_rollups
            }

        # Determine table path (use rollup if available, otherwise base table)
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        # Query pivot data
        df = self.bq_service.query_pivot_data(
            dimensions=dimensions,
            filters=filters,
            limit=limit,
            offset=offset,
            metrics=metrics,
            table_path=table_path,
            dimension_values=dimension_values
        )

        # Calculate grand totals for percentage calculations
        grand_totals = self._calculate_totals(df, metrics_data)

        # Build response rows
        rows = []
        for idx, row in df.iterrows():
            row_data = self._build_pivot_row(row, dimensions, metrics_data, grand_totals)
            rows.append(row_data)

        # Build the total row (aggregated totals for footer)
        total_row = self._build_total_row(df, dimensions, metrics_data)

        # Get total count (unless skipped) - use same table as main query
        total_count = len(df) if skip_count else self._get_total_count(dimensions, filters, table_path)

        # Get available dimensions from schema
        available_dimensions = self._get_available_dimensions()

        return {
            'rows': rows,
            'total': total_row,
            'available_dimensions': available_dimensions,
            'total_count': total_count,
        }

    def get_pivot_children(
        self,
        dimension: str,
        value: str,
        filters: Dict,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get child rows (search terms) for a specific dimension value.

        Args:
            dimension: Parent dimension name
            value: Parent dimension value to filter by
            filters: Filter parameters
            limit: Max rows to return
            offset: Rows to skip

        Returns:
            List of child row dicts with search_term and metrics
        """
        # Add parent dimension filter
        child_filters = filters.copy()
        if 'dimension_filters' not in child_filters:
            child_filters['dimension_filters'] = {}

        if dimension and value:
            child_filters['dimension_filters'][dimension] = [value]

        # Query with search_term as dimension
        df = self.bq_service.query_pivot_data(
            dimensions=['search_term'],
            filters=child_filters,
            limit=limit,
            offset=offset
        )

        metrics_data = self._get_metrics_config()
        totals = self._calculate_totals(df, metrics_data)

        rows = []
        for idx, row in df.iterrows():
            row_data = {
                'search_term': str(row.get('search_term', '')),
                'metrics': self._extract_metrics(row, metrics_data, totals)
            }
            rows.append(row_data)

        return rows

    def get_dimension_values(
        self,
        dimension: str,
        filters: Dict,
        limit: int = 1000,
        require_rollup: bool = False,
        pivot_dimensions: Optional[List[str]] = None
    ) -> List[str]:
        """
        Get distinct values for a dimension.

        Args:
            dimension: Dimension column name
            filters: Filter parameters
            limit: Max values to return
            require_rollup: If True, require rollup (error if not available).
                           Defaults to False to allow fallback to raw table.
            pivot_dimensions: List of dimensions in current pivot context

        Returns:
            List of distinct string values
        """
        # Build the full list of dimensions needed for routing:
        # - The dimension we're querying for distinct values
        # - Plus any pivot context dimensions (e.g., if pivot has "query" as row dimension)
        # - Plus any filter dimensions
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []
        pivot_dims = pivot_dimensions or []

        # Combine all required dimensions (deduplicated)
        all_dimensions = list(set([dimension] + pivot_dims + filter_dims))

        route_decision = self.route_query(
            dimensions=all_dimensions,
            metrics=[],  # No metrics needed for distinct values
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Dimension values routing: dim={dimension}, pivot_dims={pivot_dims}, "
            f"all_dims={all_dimensions}, use_rollup={route_decision.use_rollup}, "
            f"reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, raise error
        if require_rollup and not route_decision.use_rollup:
            raise ValueError(
                f"No suitable rollup found for dimension '{dimension}' with pivot context {pivot_dims}. "
                f"Required dimensions: {all_dimensions}. "
                f"Reason: {route_decision.reason}"
            )

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        return self.bq_service.query_dimension_values(
            dimension=dimension,
            filters=filters,
            limit=limit,
            table_path=table_path
        )

    def _get_metrics_config(self) -> Dict[str, Any]:
        """Load metrics configuration from schema."""
        try:
            from apps.schemas.models import SchemaConfig

            schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()

            if not schema_config:
                return {'calculated_metrics': [], 'all_metric_ids': []}

            calculated_metrics = list(schema_config.calculated_metrics.all())

            return {
                'calculated_metrics': calculated_metrics,
                'all_metric_ids': [m.metric_id for m in calculated_metrics],
                'schema_config': schema_config
            }
        except Exception as e:
            logger.warning(f"Could not load metrics config: {e}")
            return {'calculated_metrics': [], 'all_metric_ids': []}

    def _calculate_totals(
        self,
        df: pd.DataFrame,
        metrics_data: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate total values for percentage calculations."""
        totals = {}

        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in df.columns:
                totals[metric_id] = safe_float(df[metric_id].sum())

        return totals

    def _build_pivot_row(
        self,
        row: pd.Series,
        dimensions: List[str],
        metrics_data: Dict[str, Any],
        totals: Dict[str, float]
    ) -> Dict[str, Any]:
        """Build a pivot row response dict matching frontend PivotRow interface."""
        # Create combined dimension value string (e.g., "Channel A - Country B")
        dim_parts = []
        for dim in dimensions:
            if dim in row.index:
                val = row[dim]
                if pd.notna(val):
                    dim_parts.append(str(val))
        dimension_value = " - ".join(dim_parts) if dim_parts else "All"

        # Extract metrics
        metrics = self._extract_metrics(row, metrics_data, totals)

        # Calculate percentage of total based on first volume metric
        percentage_of_total = 0.0
        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in metrics and metric_id in totals and totals[metric_id] > 0:
                percentage_of_total = (metrics[metric_id] / totals[metric_id]) * 100
                break

        return {
            'dimension_value': dimension_value,
            'metrics': metrics,
            'percentage_of_total': percentage_of_total,
            'search_term_count': 1,  # Each row represents one dimension combination
            'has_children': len(dimensions) > 0,  # Can drill down if dimensions selected
        }

    def _build_total_row(
        self,
        df: pd.DataFrame,
        dimensions: List[str],
        metrics_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the total row with aggregated metrics for footer."""
        if df.empty:
            return {
                'dimension_value': 'Total',
                'metrics': {},
                'percentage_of_total': 100.0,
                'search_term_count': 0,
                'has_children': False,
            }

        # Calculate totals by summing all rows
        total_metrics = {}
        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in df.columns:
                total_metrics[metric_id] = safe_float(df[metric_id].sum())

        # Add percentage metrics (all should be 100% for the total row)
        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in total_metrics:
                pct_key = f"{metric_id}_pct"
                total_metrics[pct_key] = 100.0  # Total is always 100%

        return {
            'dimension_value': 'Total',
            'metrics': total_metrics,
            'percentage_of_total': 100.0,
            'search_term_count': len(df),
            'has_children': False,
        }

    def _get_available_dimensions(self) -> List[str]:
        """Get list of available dimension IDs from schema."""
        try:
            from apps.schemas.models import SchemaConfig

            schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()

            if not schema_config:
                return []

            return [d.dimension_id for d in schema_config.dimensions.filter(is_groupable=True)]
        except Exception as e:
            logger.warning(f"Could not get available dimensions: {e}")
            return []

    def _extract_metrics(
        self,
        row: pd.Series,
        metrics_data: Dict[str, Any],
        totals: Dict[str, float]
    ) -> Dict[str, Any]:
        """Extract metric values from a row."""
        metrics = {}

        # Extract all metric values
        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in row.index:
                value = row[metric_id]
                if pd.isna(value):
                    metrics[metric_id] = 0.0
                elif 'int' in str(type(value).__name__).lower():
                    metrics[metric_id] = int(value)
                else:
                    metrics[metric_id] = safe_float(value)

        # Add percentage metrics
        for metric_id in metrics_data.get('all_metric_ids', []):
            if metric_id in metrics:
                pct_key = f"{metric_id}_pct"
                row_value = metrics.get(metric_id, 0)
                total_value = totals.get(metric_id, 0)
                if total_value > 0:
                    metrics[pct_key] = round(safe_float((row_value / total_value * 100)), 2)
                else:
                    metrics[pct_key] = 0.0

        return metrics

    def _get_total_count(
        self,
        dimensions: List[str],
        filters: Dict,
        table_path: Optional[str] = None
    ) -> int:
        """Get total count of distinct dimension combinations.

        Args:
            dimensions: List of dimension column names
            filters: Filter parameters
            table_path: Override table path (for rollup queries). Defaults to base table.

        Returns:
            Count of distinct dimension combinations
        """
        if not dimensions:
            return 1

        # Use provided table_path or default to base table
        query_table = table_path if table_path else self.bq_service.table_path

        where_clause = self.bq_service.build_filter_clause(
            start_date=filters.get('start_date'),
            end_date=filters.get('end_date'),
            dimension_filters=filters.get('dimension_filters'),
            date_range_type=filters.get('date_range_type', 'absolute'),
            relative_date_preset=filters.get('relative_date_preset')
        )

        dim_columns = ", ".join(dimensions)

        query = f"""
            SELECT COUNT(*) as total
            FROM (
                SELECT {dim_columns}
                FROM `{query_table}`
                {where_clause}
                GROUP BY {dim_columns}
            )
        """

        df = self.bq_service.execute_query(
            query=query,
            query_type='count',
            endpoint='/api/pivot',
            filters=filters
        )

        return int(df['total'].iloc[0]) if len(df) > 0 else 0

    def _get_query_router(self) -> Optional[QueryRouterService]:
        """Get a query router service instance."""
        try:
            from apps.schemas.models import SchemaConfig
            from apps.rollups.models import RollupConfig

            schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()

            if not schema_config:
                return None

            rollup_config = None
            try:
                rollup_config = RollupConfig.objects.get(
                    bigquery_table=self.bigquery_table
                )
            except RollupConfig.DoesNotExist:
                pass

            return QueryRouterService(
                rollup_config=rollup_config,
                schema_config=schema_config,
                source_project_id=self.bigquery_table.project_id,
                source_dataset=self.bigquery_table.dataset
            )
        except Exception as e:
            logger.warning(f"Could not create query router: {e}")
            return None

    def route_query(
        self,
        dimensions: List[str],
        metrics: List[str],
        filters: Optional[Dict[str, List[str]]] = None,
        require_rollup: bool = False
    ) -> RouteDecision:
        """
        Route a query to the optimal data source.

        Args:
            dimensions: Dimensions to group by
            metrics: Metrics to aggregate
            filters: Dimension filters
            require_rollup: Require a rollup match

        Returns:
            RouteDecision with routing info
        """
        router = self._get_query_router()
        if not router:
            return RouteDecision(
                use_rollup=False,
                reason="No query router available (missing schema or rollup config)"
            )

        return router.route_query(
            query_dimensions=dimensions,
            query_metrics=metrics,
            query_filters=filters,
            require_rollup=require_rollup
        )

    def _compute_calculated_metrics(
        self,
        df: pd.DataFrame,
        metrics_data: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Compute calculated metrics in Python from volume metrics.

        Conversion metrics (non-volume) are computed after querying volumes.

        Args:
            df: DataFrame with volume metrics
            metrics_data: Metrics configuration

        Returns:
            DataFrame with calculated metrics added
        """
        if df.empty:
            return df

        calculated_metrics = metrics_data.get('calculated_metrics', [])

        for metric in calculated_metrics:
            if metric.category == 'volume':
                continue

            metric_id = metric.metric_id
            formula = metric.formula
            depends_on = metric.depends_on or []

            # Check all dependencies are available
            if not all(dep in df.columns for dep in depends_on):
                continue

            try:
                result_values = self._evaluate_formula(
                    df=df,
                    formula=formula,
                    depends_on=depends_on
                )
                df[metric_id] = result_values
            except Exception as e:
                logger.warning(f"Failed to compute {metric_id}: {e}")
                df[metric_id] = 0.0

        return df

    def _evaluate_formula(
        self,
        df: pd.DataFrame,
        formula: str,
        depends_on: List[str]
    ) -> pd.Series:
        """
        Evaluate a metric formula.

        Args:
            df: DataFrame with source metrics
            formula: Formula like "{numerator} / {denominator}"
            depends_on: List of metric IDs the formula depends on

        Returns:
            Series with computed values
        """
        expr = formula

        for dep in depends_on:
            pattern = r'\{' + re.escape(dep) + r'\}'
            replacement = f"df['{dep}']"
            expr = re.sub(pattern, replacement, expr)

        expr = re.sub(
            r'(\S+)\s*/\s*(\S+)',
            r'safe_divide(\1, \2)',
            expr
        )

        def safe_divide(num, denom):
            result = num / denom
            result = result.replace([float('inf'), float('-inf')], 0.0)
            result = result.fillna(0.0)
            return result

        try:
            return eval(expr, {'df': df, 'safe_divide': safe_divide})
        except Exception as e:
            logger.warning(f"Formula evaluation failed for '{formula}': {e}")
            return pd.Series([0.0] * len(df))

    def _get_baseline_totals(
        self,
        metrics: List[str],
        filters: Dict
    ) -> Optional[Dict[str, float]]:
        """
        Get baseline totals from the date-only rollup.

        Used to detect metric inflation when grouping by dimensions.

        Args:
            metrics: Metric IDs to fetch
            filters: Filter parameters

        Returns:
            Dict of metric_id -> total value, or None if no baseline available
        """
        router = self._get_query_router()
        if not router:
            return None

        baseline_path = router.get_baseline_rollup_path()
        if not baseline_path:
            return None

        try:
            return self.bq_service.query_rollup_aggregates(
                rollup_table_path=baseline_path,
                metric_ids=metrics,
                start_date=filters.get('start_date'),
                end_date=filters.get('end_date'),
                dimension_filters=filters.get('dimension_filters'),
                date_range_type=filters.get('date_range_type', 'absolute'),
                relative_date_preset=filters.get('relative_date_preset')
            )
        except Exception as e:
            logger.warning(f"Failed to get baseline totals: {e}")
            return None

    def _compare_metrics_with_baseline(
        self,
        df: pd.DataFrame,
        metrics: List[str],
        filters: Dict,
        inflation_threshold: float = 0.01
    ) -> Dict[str, Any]:
        """
        Compare metrics against baseline to detect inflation.

        Args:
            df: DataFrame with metric values
            metrics: Metric IDs to compare
            filters: Filter parameters
            inflation_threshold: Threshold for flagging inflation

        Returns:
            Dict with comparison results
        """
        baseline_totals = self._get_baseline_totals(metrics, filters)
        if not baseline_totals:
            return {'has_baseline': False}

        result = {
            'has_baseline': True,
            'comparisons': {},
            'any_inflated': False
        }

        for metric_id in metrics:
            baseline_value = baseline_totals.get(metric_id, 0)
            current_value = df[metric_id].sum() if metric_id in df.columns else 0

            if baseline_value > 0:
                ratio = current_value / baseline_value
                is_inflated = ratio > (1 + inflation_threshold)

                result['comparisons'][metric_id] = {
                    'baseline': baseline_value,
                    'current': current_value,
                    'ratio': ratio,
                    'is_inflated': is_inflated
                }

                if is_inflated:
                    result['any_inflated'] = True

        return result

    def get_overview_metrics(self, filters: Dict) -> Dict[str, Any]:
        """
        Get overview KPI metrics.

        Args:
            filters: Filter parameters

        Returns:
            Dict with KPI metric values
        """
        return self.bq_service.query_kpi_metrics(filters)

    def get_trends_data(
        self,
        filters: Dict,
        granularity: str = 'daily'
    ) -> List[Dict[str, Any]]:
        """
        Get time-series trends data.

        Args:
            filters: Filter parameters
            granularity: 'daily', 'weekly', or 'monthly'

        Returns:
            List of data points with date and metrics
        """
        df = self.bq_service.query_timeseries(filters, granularity)

        metrics_data = self._get_metrics_config()
        df = self._compute_calculated_metrics(df, metrics_data)

        rows = []
        for idx, row in df.iterrows():
            row_data = {
                'date': str(row['date']) if pd.notna(row.get('date')) else None
            }

            for col in df.columns:
                if col != 'date':
                    value = row[col]
                    if pd.isna(value):
                        row_data[col] = 0.0
                    elif 'int' in str(type(value).__name__).lower():
                        row_data[col] = int(value)
                    else:
                        row_data[col] = safe_float(value)

            rows.append(row_data)

        return rows

    def get_dimension_breakdown(
        self,
        dimension: str,
        filters: Dict,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get breakdown by dimension.

        Args:
            dimension: Dimension to break down by
            filters: Filter parameters
            limit: Max rows to return

        Returns:
            List of breakdown rows
        """
        df = self.bq_service.query_dimension_breakdown(dimension, filters, limit)

        metrics_data = self._get_metrics_config()
        df = self._compute_calculated_metrics(df, metrics_data)

        rows = []
        for idx, row in df.iterrows():
            row_data = {
                'dimension_value': str(row.get('dimension_value', ''))
            }

            for col in df.columns:
                if col != 'dimension_value':
                    value = row[col]
                    if pd.isna(value):
                        row_data[col] = 0.0
                    elif 'int' in str(type(value).__name__).lower():
                        row_data[col] = int(value)
                    else:
                        row_data[col] = safe_float(value)

            rows.append(row_data)

        return rows

    def get_search_terms(
        self,
        filters: Dict,
        limit: int = 100,
        sort_by: str = 'queries'
    ) -> List[Dict[str, Any]]:
        """
        Get search terms data.

        Args:
            filters: Filter parameters
            limit: Max rows to return
            sort_by: Metric to sort by

        Returns:
            List of search term rows
        """
        df = self.bq_service.query_search_terms(filters, limit, sort_by)

        metrics_data = self._get_metrics_config()
        df = self._compute_calculated_metrics(df, metrics_data)

        rows = []
        for idx, row in df.iterrows():
            row_data = {
                'search_term': str(row.get('search_term', ''))
            }

            for col in df.columns:
                if col != 'search_term':
                    value = row[col]
                    if pd.isna(value):
                        row_data[col] = 0.0
                    elif 'int' in str(type(value).__name__).lower():
                        row_data[col] = int(value)
                    else:
                        row_data[col] = safe_float(value)

            rows.append(row_data)

        return rows

    def get_filter_options(self, filters: Optional[Dict] = None) -> Dict[str, List[str]]:
        """
        Get available filter options for dimensions.

        Args:
            filters: Optional existing filters to respect

        Returns:
            Dict of dimension -> list of values
        """
        options = {}

        metrics_data = self._get_metrics_config()
        schema_config = metrics_data.get('schema_config')

        if not schema_config:
            return options

        try:
            filterable_dims = schema_config.dimensions.filter(is_filterable=True)

            for dim in filterable_dims:
                values = self.bq_service.query_dimension_values(
                    dimension=dim.column_name,
                    filters=filters or {},
                    limit=100
                )
                options[dim.dimension_id] = values

        except Exception as e:
            logger.warning(f"Failed to get filter options: {e}")

        return options
