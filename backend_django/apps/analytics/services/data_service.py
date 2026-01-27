"""
Data service - Business logic layer for analytics.
Handles pivot table data, metrics calculations, and data transformations.

Django port of the FastAPI data_service.py with rollup routing support.
"""
import math
import re
import logging
from typing import List, Dict, Optional, Any, Tuple, Union

import pandas as pd

from apps.tables.models import BigQueryTable
from .bigquery_service import BigQueryService
from .query_router_service import QueryRouterService, RouteDecision
from .post_processing_service import PostProcessingService

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
        require_rollup: bool = True,
        custom_dimension_id: Optional[str] = None,
        custom_metric_ids: Optional[List[str]] = None
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
            custom_dimension_id: Optional ID of a custom dimension to apply bucketing
            custom_metric_ids: Optional list of custom metric IDs to apply re-aggregation

        Returns:
            Dict with rows, total, available_dimensions matching frontend expectations
        """
        # Load metrics configuration first (needed for routing)
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])

        # =================================================================
        # EXTRACT CUSTOM DIMENSIONS: Parse custom_<uuid> from dimensions array
        # =================================================================
        # If custom_dimension_id not explicitly provided, extract from dimensions
        if not custom_dimension_id:
            custom_dims_in_request = [d for d in (dimensions or []) if d.startswith('custom_')]
            if custom_dims_in_request:
                # Extract UUID from first custom dimension (format: custom_<uuid>)
                custom_dimension_id = custom_dims_in_request[0].replace('custom_', '', 1)
                logger.info(f"Extracted custom dimension ID from dimensions: {custom_dimension_id}")

        # =================================================================
        # ROLLUP ROUTING: Check if a suitable rollup exists for this query
        # =================================================================
        # Filter out custom dimensions from routing - they are virtual/computed
        # dimensions applied post-fetch and don't exist in rollups
        routable_dimensions = [d for d in (dimensions or []) if not d.startswith('custom_')]
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []
        # Also filter custom dimensions from filter dims
        routable_filter_dims = {k: v for k, v in (filters.get('dimension_filters') or {}).items() if not k.startswith('custom_')}
        route_decision = self.route_query(
            dimensions=routable_dimensions,
            metrics=metric_ids,
            filters=routable_filter_dims if routable_filter_dims else None,
            require_rollup=require_rollup
        )

        logger.info(
            f"Pivot routing: dims={dimensions}, filters={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            # Use routable dimensions (excluding custom_*) for error message
            routable_filter_dims_list = [k for k in filter_dims if not k.startswith('custom_')]
            all_required_dims = list(set(routable_dimensions + routable_filter_dims_list))

            # Get available rollups sorted by closeness to the requested configuration
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=routable_dimensions,
                    query_metrics=metric_ids,
                    query_filters=routable_filter_dims if routable_filter_dims else None
                )

            return {
                'rows': [],
                'total': None,
                'available_dimensions': self._get_available_dimensions(),
                'total_count': 0,
                'error': f"No suitable rollup found. Query dimensions: {routable_dimensions}, Filter dimensions: {routable_filter_dims_list}. Reason: {route_decision.reason}. Create a rollup with dimensions {all_required_dims} to enable this query.",
                'error_type': 'rollup_required',
                'required_dimensions': all_required_dims,
                'available_rollups': available_rollups
            }

        # Determine table path (use rollup if available, otherwise base table)
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        # =================================================================
        # CUSTOM DIMENSION HANDLING
        # =================================================================
        # Look up custom dimension info for BigQuery-based bucketing
        custom_dimension_info = None
        custom_dim_type = None
        custom_dim_metric = None  # Track metric referenced by custom dimension
        if custom_dimension_id:
            schema_config = metrics_data.get('schema_config')
            if schema_config:
                try:
                    from apps.schemas.models import CustomDimension
                    custom_dim = schema_config.custom_dimensions.get(id=custom_dimension_id)
                    custom_dim_type = custom_dim.dimension_type

                    # For metric_condition type, prepare info for BigQuery SQL
                    if custom_dim_type == 'metric_condition':
                        custom_dim_metric = custom_dim.get_source_metric()
                        custom_dimension_info = {
                            'id': str(custom_dim.id),
                            'metric': custom_dim_metric,
                            'conditions': custom_dim.values_json or []
                        }
                        logger.info(f"Using BigQuery bucketing for custom dimension '{custom_dim.name}'")
                except Exception as e:
                    logger.error(f"Error loading custom dimension {custom_dimension_id}: {e}")

        # =================================================================
        # CUSTOM METRICS HANDLING
        # =================================================================
        # Load custom metrics info for BigQuery-based computation
        # Include any custom metrics referenced by custom dimensions
        custom_metrics_info = None
        schema_config = metrics_data.get('schema_config')
        if schema_config:
            try:
                from apps.schemas.models import CustomMetric

                # Build list of custom metric IDs to compute
                cm_ids_to_load = set(custom_metric_ids or [])

                # If custom dimension references a custom metric, include it
                if custom_dim_metric:
                    # Check if it's a custom metric (not a base metric)
                    cm_by_metric_id = schema_config.custom_metrics.filter(
                        metric_id=custom_dim_metric
                    ).first()
                    if cm_by_metric_id:
                        cm_ids_to_load.add(custom_dim_metric)
                        logger.info(f"Auto-including custom metric '{custom_dim_metric}' for custom dimension")

                # Load custom metrics
                if cm_ids_to_load:
                    custom_metrics = list(schema_config.custom_metrics.filter(
                        metric_id__in=cm_ids_to_load
                    ))
                    if custom_metrics:
                        custom_metrics_info = [
                            {
                                'metric_id': cm.metric_id,
                                'source_metric': cm.source_metric,
                                'aggregation_type': cm.aggregation_type
                            }
                            for cm in custom_metrics
                        ]
                        logger.info(f"Using BigQuery computation for custom metrics: {[cm.metric_id for cm in custom_metrics]}")
            except Exception as e:
                logger.error(f"Error loading custom metrics: {e}")

        # Query pivot data - use routable_dimensions (excluding custom_*) for BigQuery
        # metric_condition custom dimensions are handled in BigQuery SQL
        # Custom metrics are computed in BigQuery as well
        df = self.bq_service.query_pivot_data(
            dimensions=routable_dimensions,
            filters=filters,
            limit=limit,
            offset=offset,
            metrics=metrics,
            table_path=table_path,
            dimension_values=dimension_values,
            custom_dimension=custom_dimension_info,
            custom_metrics=custom_metrics_info
        )

        # =================================================================
        # POST-PROCESSING: Apply custom dimensions (date_range only)
        # =================================================================
        # Skip post-processing for metric_condition (already handled in BigQuery)
        # Custom metrics are now computed in BigQuery, no post-processing needed
        custom_dim_col = f"custom_{custom_dimension_id}" if custom_dimension_id else None
        if custom_dimension_id and custom_dim_type != 'metric_condition':
            # Only do post-processing for non-metric_condition types (date_range, metric_bucket)
            df, custom_dim_col = self._apply_post_processing(
                df=df,
                metrics_data=metrics_data,
                dimensions=dimensions,
                custom_dimension_id=custom_dimension_id,
                custom_metric_ids=None  # Custom metrics handled by BigQuery
            )

        # Compute calculated metrics (conversion rates, etc.) from volume metrics
        df = self._compute_calculated_metrics(df, metrics_data)

        # Calculate grand totals for percentage calculations (includes ALL rows, even NULL dimensions)
        grand_totals = self._calculate_totals(df, metrics_data)

        # Build response rows (include all rows, NULL/empty dimensions displayed as "(null)"/"(empty)")
        rows = []
        for idx, row in df.iterrows():
            row_data = self._build_pivot_row(row, dimensions, metrics_data, grand_totals, custom_metric_ids)
            rows.append(row_data)

        # Build the total row (aggregated totals for footer - includes all data)
        total_row = self._build_total_row(df, dimensions, metrics_data, custom_metric_ids)

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

    def get_dimension_values(
        self,
        dimension: str,
        filters: Dict,
        limit: int = 1000,
        require_rollup: bool = True,
        pivot_dimensions: Optional[List[str]] = None,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get distinct values for a dimension.

        Args:
            dimension: Dimension column name
            filters: Filter parameters
            limit: Max values to return
            require_rollup: If True, require rollup (error if not available).
                           Defaults to True - always use rollups.
            pivot_dimensions: List of dimensions in current pivot context
            search: Optional search string to filter values (case-insensitive)

        Returns:
            Dict with 'values' key containing list of strings, or 'error' key if rollup not found
        """
        # Check if this is a joined dimension - if so, query the lookup table directly
        joined_dim_info = self._get_joined_dimension_info(dimension)
        if joined_dim_info:
            return self._get_joined_dimension_values(joined_dim_info, limit, search)

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

        # If require_rollup and no rollup found, return error response (like pivot does)
        if require_rollup and not route_decision.use_rollup:
            # Get available rollups for debugging
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=all_dimensions,
                    query_metrics=[],
                    query_filters=filters.get('dimension_filters')
                )

            return {
                'values': [],
                'error': f"No suitable rollup found. Required dimensions: {all_dimensions}. "
                         f"Create a rollup with these dimensions to enable this query.",
                'error_type': 'rollup_required',
                'required_dimensions': all_dimensions,
                'available_rollups': available_rollups
            }

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        values = self.bq_service.query_dimension_values(
            dimension=dimension,
            filters=filters,
            limit=limit,
            table_path=table_path,
            search=search
        )

        return {'values': values}

    def _get_joined_dimension_info(self, dimension_id: str) -> Optional[Dict]:
        """Check if a dimension is a joined dimension and return its info."""
        try:
            from apps.schemas.models import SchemaConfig, JoinedDimensionStatus

            schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()

            if not schema_config:
                return None

            # Search for this dimension in joined dimension sources
            for source in schema_config.joined_dimension_sources.filter(
                status=JoinedDimensionStatus.READY
            ).prefetch_related('columns'):
                for col in source.columns.all():
                    if col.dimension_id == dimension_id:
                        return {
                            'column': col,
                            'source': source,
                            'lookup_table_path': source.bq_table_path,
                            'source_column_name': col.source_column_name
                        }

            return None
        except Exception as e:
            logger.warning(f"Error checking for joined dimension: {e}")
            return None

    def _get_joined_dimension_values(
        self, joined_info: Dict, limit: int = 1000, search: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get distinct values for a joined dimension from its lookup table.

        Args:
            joined_info: Dict with column, source, lookup_table_path, source_column_name
            limit: Max values to return
            search: Optional search string to filter values (case-insensitive LIKE)
        """
        try:
            lookup_table = joined_info['lookup_table_path']
            column_name = joined_info['source_column_name']

            # Build WHERE clause
            where_clauses = [f"{column_name} IS NOT NULL"]

            # Add search filter if provided
            if search and search.strip():
                # Escape special characters in search string
                escaped_search = search.replace("'", "''").replace("\\", "\\\\")
                # Use LOWER for case-insensitive search
                where_clauses.append(
                    f"LOWER(CAST({column_name} AS STRING)) LIKE LOWER('%{escaped_search}%')"
                )

            where_clause = " AND ".join(where_clauses)

            query = f"""
                SELECT DISTINCT {column_name} as value
                FROM `{lookup_table}`
                WHERE {where_clause}
                ORDER BY value
                LIMIT {limit}
            """

            df = self.bq_service.execute_query(
                query=query,
                query_type='joined_dimension_values',
                endpoint=f'/api/pivot/dimension/{joined_info["column"].dimension_id}/values',
                filters={}
            )

            values = df['value'].tolist() if not df.empty else []
            return {'values': [str(v) for v in values]}
        except Exception as e:
            logger.error(f"Error fetching joined dimension values: {e}")
            return {
                'values': [],
                'error': f"Failed to fetch joined dimension values: {str(e)}"
            }

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

    def _calculate_num_days(self, filters: Dict[str, Any]) -> int:
        """
        Calculate the number of days in the date range.

        Args:
            filters: Filter parameters with start_date and end_date

        Returns:
            Number of days in the range (inclusive), minimum 1
        """
        from datetime import datetime

        start_date = filters.get('start_date')
        end_date = filters.get('end_date')

        if not start_date or not end_date:
            return 1

        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            num_days = (end - start).days + 1  # +1 for inclusive
            return max(1, num_days)
        except (ValueError, TypeError):
            return 1

    def _apply_post_processing(
        self,
        df: pd.DataFrame,
        metrics_data: Dict[str, Any],
        dimensions: List[str],
        custom_dimension_id: Optional[str] = None,
        custom_metric_ids: Optional[List[str]] = None,
        num_days: int = 1
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Apply custom dimensions and metrics as post-processing transformations.

        Args:
            df: Input DataFrame from BigQuery
            metrics_data: Metrics configuration dict
            dimensions: Current query dimensions
            custom_dimension_id: ID of custom dimension to apply (for bucketing)
            custom_metric_ids: List of custom metric IDs to apply (for re-aggregation)
            num_days: Number of days in the date range (for avg_per_day calculation)

        Returns:
            Tuple of (transformed DataFrame, custom dimension column name if grouped)
        """
        schema_config = metrics_data.get('schema_config')
        if not schema_config:
            logger.warning("No schema config found for post-processing")
            return df, None

        post_processor = PostProcessingService()
        custom_dim_col = None

        # Apply custom dimensions (bucketing)
        if custom_dimension_id:
            from apps.schemas.models import CustomDimension
            try:
                custom_dims = list(schema_config.custom_dimensions.filter(
                    id=custom_dimension_id
                ))
                if custom_dims:
                    df, custom_dim_col = post_processor.apply_custom_dimensions(
                        df=df,
                        custom_dimensions=custom_dims,
                        group_by_custom_id=custom_dimension_id,
                        existing_dimensions=dimensions
                    )
                    logger.info(
                        f"Applied custom dimension {custom_dimension_id}, "
                        f"result col: {custom_dim_col}"
                    )
            except Exception as e:
                logger.error(f"Error applying custom dimension: {e}")

        # Apply custom metrics (re-aggregation)
        if custom_metric_ids:
            from apps.schemas.models import CustomMetric
            try:
                custom_metrics = list(schema_config.custom_metrics.filter(
                    metric_id__in=custom_metric_ids
                ))
                if custom_metrics:
                    df = post_processor.apply_custom_metrics(
                        df=df,
                        custom_metrics=custom_metrics,
                        current_dimensions=dimensions,
                        num_days=num_days
                    )
                    logger.info(f"Applied custom metrics: {custom_metric_ids} (num_days={num_days})")
            except Exception as e:
                logger.error(f"Error applying custom metrics: {e}")

        return df, custom_dim_col

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
        totals: Dict[str, float],
        custom_metric_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Build a pivot row response dict matching frontend PivotRow interface."""
        # Create combined dimension value string (e.g., "Channel A - Country B")
        # Keep raw values: NULL becomes "__NULL__" (converted to IS NULL in queries),
        # empty strings stay as '' (matches directly in queries)
        dim_parts = []
        for dim in dimensions:
            if dim in row.index:
                val = row[dim]
                if pd.isna(val):
                    dim_parts.append("__NULL__")
                else:
                    dim_parts.append(str(val))  # Empty strings stay as ''
        dimension_value = " - ".join(dim_parts) if dim_parts else "All"

        # Extract metrics (including custom metrics if provided)
        metrics = self._extract_metrics(row, metrics_data, totals, custom_metric_ids)

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
        metrics_data: Dict[str, Any],
        custom_metric_ids: Optional[List[str]] = None
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

        # Build list of all metric IDs (schema metrics + custom metrics)
        all_metric_ids = list(metrics_data.get('all_metric_ids', []))
        if custom_metric_ids:
            all_metric_ids.extend(custom_metric_ids)

        # Calculate totals by summing all rows
        total_metrics = {}
        for metric_id in all_metric_ids:
            if metric_id in df.columns:
                total_metrics[metric_id] = safe_float(df[metric_id].sum())

        # Add percentage metrics (all should be 100% for the total row) - only for schema metrics
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
            from apps.schemas.models import SchemaConfig, JoinedDimensionStatus

            schema_config = SchemaConfig.objects.filter(
                bigquery_table=self.bigquery_table
            ).first()

            if not schema_config:
                return []

            # Get regular dimensions
            dims = [d.dimension_id for d in schema_config.dimensions.filter(is_groupable=True)]

            # Get joined dimensions from ready sources
            for source in schema_config.joined_dimension_sources.filter(status=JoinedDimensionStatus.READY):
                for col in source.columns.filter(is_groupable=True):
                    dims.append(col.dimension_id)

            return dims
        except Exception as e:
            logger.warning(f"Could not get available dimensions: {e}")
            return []

    def _extract_metrics(
        self,
        row: pd.Series,
        metrics_data: Dict[str, Any],
        totals: Dict[str, float],
        custom_metric_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Extract metric values from a row."""
        metrics = {}

        # Build list of all metric IDs to extract (schema metrics + custom metrics)
        all_metric_ids = list(metrics_data.get('all_metric_ids', []))
        if custom_metric_ids:
            all_metric_ids.extend(custom_metric_ids)

        # Extract all metric values
        for metric_id in all_metric_ids:
            if metric_id in row.index:
                value = row[metric_id]
                if pd.isna(value):
                    metrics[metric_id] = 0.0
                elif 'int' in str(type(value).__name__).lower():
                    metrics[metric_id] = int(value)
                else:
                    metrics[metric_id] = safe_float(value)

        # Add percentage metrics (only for schema metrics, not custom)
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

            # If depends_on is empty, parse dependencies from formula
            # This handles cases where schema copy didn't preserve depends_on
            if not depends_on and formula:
                depends_on = re.findall(r'\{(\w+)\}', formula)

            # Check all dependencies are available
            missing_deps = [dep for dep in depends_on if dep not in df.columns]
            if missing_deps:
                logger.warning(f"Skipping metric '{metric_id}': missing dependencies {missing_deps}")
                continue

            try:
                result_values = self._evaluate_formula(
                    df=df,
                    formula=formula,
                    depends_on=depends_on
                )
                df[metric_id] = result_values
            except Exception as e:
                logger.warning(f"Failed to compute metric {metric_id}: {e}")
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

    def get_overview_metrics(
        self,
        filters: Dict,
        require_rollup: bool = True
    ) -> Dict[str, Any]:
        """
        Get overview KPI metrics.

        Args:
            filters: Filter parameters
            require_rollup: If True, require rollup (error if not available)

        Returns:
            Dict with KPI metric values, or error dict if rollup required but not found
        """
        # Load metrics configuration for routing
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []

        # Route query - no dimensions for totals, but need filter dimensions
        route_decision = self.route_query(
            dimensions=[],  # No grouping dimensions for totals
            metrics=metric_ids,
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Overview routing: filter_dims={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=[],
                    query_metrics=metric_ids,
                    query_filters=filters.get('dimension_filters')
                )

            return {
                'error': f"No suitable rollup found for overview metrics. Filter dimensions: {filter_dims}. Reason: {route_decision.reason}",
                'error_type': 'rollup_required',
                'required_dimensions': filter_dims,
                'available_rollups': available_rollups
            }

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        return self.bq_service.query_kpi_metrics(filters, table_path)

    def get_trends_data(
        self,
        filters: Dict,
        granularity: str = 'daily',
        require_rollup: bool = True
    ) -> Any:
        """
        Get time-series trends data.

        Args:
            filters: Filter parameters
            granularity: 'daily', 'weekly', or 'monthly'
            require_rollup: If True, require rollup (error if not available)

        Returns:
            List of data points with date and metrics, or error dict if rollup required but not found
        """
        # Load metrics configuration for routing
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []

        # Route query - need 'date' dimension for timeseries
        route_decision = self.route_query(
            dimensions=['date'],  # Timeseries needs date dimension
            metrics=metric_ids,
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Trends routing: filter_dims={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=['date'],
                    query_metrics=metric_ids,
                    query_filters=filters.get('dimension_filters')
                )

            all_required_dims = ['date'] + filter_dims
            return {
                'error': f"No suitable rollup found for trends data. Required dimensions: {all_required_dims}. Reason: {route_decision.reason}",
                'error_type': 'rollup_required',
                'required_dimensions': all_required_dims,
                'available_rollups': available_rollups
            }

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        df = self.bq_service.query_timeseries(filters, granularity, table_path)

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
        limit: int = 20,
        require_rollup: bool = True
    ) -> Any:
        """
        Get breakdown by dimension.

        Args:
            dimension: Dimension to break down by
            filters: Filter parameters
            limit: Max rows to return
            require_rollup: If True, require rollup (error if not available)

        Returns:
            List of breakdown rows, or error dict if rollup required but not found
        """
        # Load metrics configuration for routing
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []

        # Route query - need the breakdown dimension
        route_decision = self.route_query(
            dimensions=[dimension],
            metrics=metric_ids,
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Breakdown routing: dimension={dimension}, filter_dims={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            all_required_dims = list(set([dimension] + filter_dims))
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=[dimension],
                    query_metrics=metric_ids,
                    query_filters=filters.get('dimension_filters')
                )

            return {
                'error': f"No suitable rollup found for dimension breakdown. Required dimensions: {all_required_dims}. Reason: {route_decision.reason}",
                'error_type': 'rollup_required',
                'required_dimensions': all_required_dims,
                'available_rollups': available_rollups
            }

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        df = self.bq_service.query_dimension_breakdown(dimension, filters, limit, table_path)

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
        sort_by: str = 'queries',
        require_rollup: bool = True
    ) -> Any:
        """
        Get search terms data.

        Args:
            filters: Filter parameters
            limit: Max rows to return
            sort_by: Metric to sort by
            require_rollup: If True, require rollup (error if not available)

        Returns:
            List of search term rows, or error dict if rollup required but not found
        """
        # Load metrics configuration for routing
        metrics_data = self._get_metrics_config()
        metric_ids = metrics_data.get('all_metric_ids', [])
        filter_dims = list(filters.get('dimension_filters', {}).keys()) if filters.get('dimension_filters') else []

        # Route query - need 'search_term' dimension
        route_decision = self.route_query(
            dimensions=['search_term'],
            metrics=metric_ids,
            filters=filters.get('dimension_filters'),
            require_rollup=require_rollup
        )

        logger.info(
            f"Search terms routing: filter_dims={filter_dims}, "
            f"use_rollup={route_decision.use_rollup}, reason={route_decision.reason}"
        )

        # If require_rollup and no rollup found, return error response
        if require_rollup and not route_decision.use_rollup:
            all_required_dims = list(set(['search_term'] + filter_dims))
            available_rollups = []
            router = self._get_query_router()
            if router:
                available_rollups = router.find_suitable_rollups(
                    query_dimensions=['search_term'],
                    query_metrics=metric_ids,
                    query_filters=filters.get('dimension_filters')
                )

            return {
                'error': f"No suitable rollup found for search terms. Required dimensions: {all_required_dims}. Create a rollup with 'search_term' dimension to enable this query.",
                'error_type': 'rollup_required',
                'required_dimensions': all_required_dims,
                'available_rollups': available_rollups
            }

        # Determine table path
        table_path = route_decision.rollup_table_path if route_decision.use_rollup else None

        df = self.bq_service.query_search_terms(filters, limit, sort_by, table_path)

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
