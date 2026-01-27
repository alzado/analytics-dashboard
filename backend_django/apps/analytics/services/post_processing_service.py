"""
Post-processing service for applying custom dimensions and metrics.

This service applies transformations after data is fetched from rollups:
- Custom dimensions: Bucket data based on metric values
- Custom metrics: Re-aggregate metrics across excluded dimensions
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging

from apps.schemas.models import CustomDimension, CustomMetric

logger = logging.getLogger(__name__)


class PostProcessingService:
    """
    Apply custom dimensions and metrics as post-processing transformations.

    This service is called after fetching data from BigQuery/rollups to:
    1. Apply custom dimension bucketing (e.g., queries > 1000 = "High Volume")
    2. Apply custom metric re-aggregation (e.g., sum across dates)
    """

    def apply_custom_dimensions(
        self,
        df: pd.DataFrame,
        custom_dimensions: List[CustomDimension],
        group_by_custom_id: Optional[str] = None,
        existing_dimensions: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Apply custom dimension bucketing to the DataFrame.

        Args:
            df: Input DataFrame with metric columns
            custom_dimensions: List of CustomDimension objects to apply
            group_by_custom_id: If provided, group results by this custom dimension
            existing_dimensions: List of current dimension columns in df

        Returns:
            Tuple of (transformed DataFrame, custom dimension column name if grouped)
        """
        if df.empty:
            return df, None

        custom_dim_col = None

        for custom_dim in custom_dimensions:
            if custom_dim.dimension_type == 'metric_bucket':
                # Use format matching dimensions list: custom_<uuid>
                col_name = f'custom_{custom_dim.id}'
                source_metric = custom_dim.get_source_metric()

                if source_metric not in df.columns:
                    logger.warning(
                        f"Source metric '{source_metric}' not found in DataFrame "
                        f"for custom dimension '{custom_dim.name}'"
                    )
                    continue

                df[col_name] = self._apply_buckets(
                    df[source_metric],
                    custom_dim.values_json
                )

                # Track if this is the dimension we're grouping by
                if group_by_custom_id and str(custom_dim.id) == str(group_by_custom_id):
                    custom_dim_col = col_name

            elif custom_dim.dimension_type == 'date_range':
                # Use format matching dimensions list: custom_<uuid>
                col_name = f'custom_{custom_dim.id}'

                if 'date' not in df.columns:
                    logger.warning(
                        f"Date column not found for custom dimension '{custom_dim.name}'"
                    )
                    continue

                df[col_name] = self._apply_date_ranges(
                    df['date'],
                    custom_dim.values_json
                )

                if group_by_custom_id and str(custom_dim.id) == str(group_by_custom_id):
                    custom_dim_col = col_name

            elif custom_dim.dimension_type == 'metric_condition':
                # Use format matching dimensions list: custom_<uuid>
                col_name = f'custom_{custom_dim.id}'
                source_metric = custom_dim.get_source_metric()

                logger.info(f"Applying metric_condition dimension '{custom_dim.name}'")
                logger.info(f"  Source metric: {source_metric}")
                logger.info(f"  DataFrame columns: {list(df.columns)}")
                logger.info(f"  Values JSON: {custom_dim.values_json}")

                if source_metric not in df.columns:
                    logger.warning(
                        f"Source metric '{source_metric}' not found for "
                        f"custom dimension '{custom_dim.name}'"
                    )
                    continue

                logger.info(f"  Source metric values: {df[source_metric].tolist()[:5]}")

                df[col_name] = self._apply_metric_conditions(
                    df[source_metric],
                    custom_dim.values_json
                )

                logger.info(f"  Assigned labels: {df[col_name].unique().tolist()}")

                if group_by_custom_id and str(custom_dim.id) == str(group_by_custom_id):
                    custom_dim_col = col_name

        # If grouping by custom dimension, re-aggregate
        if custom_dim_col and custom_dim_col in df.columns:
            df = self._reaggregate_by_dimension(
                df,
                custom_dim_col,
                existing_dimensions or []
            )

        return df, custom_dim_col

    def apply_custom_metrics(
        self,
        df: pd.DataFrame,
        custom_metrics: List[CustomMetric],
        current_dimensions: List[str],
        num_days: int = 1
    ) -> pd.DataFrame:
        """
        Apply custom metric re-aggregation to the DataFrame.

        Args:
            df: Input DataFrame with metric and dimension columns
            custom_metrics: List of CustomMetric objects to apply
            current_dimensions: List of dimension columns currently in the query
            num_days: Number of days in the date range (for avg_per_day calculation)

        Returns:
            DataFrame with additional custom metric columns
        """
        if df.empty:
            return df

        for custom_metric in custom_metrics:
            source_metric = custom_metric.source_metric

            if source_metric not in df.columns:
                logger.warning(
                    f"Source metric '{source_metric}' not found for "
                    f"custom metric '{custom_metric.name}'"
                )
                continue

            # Handle avg_per_day specially - divide by number of days
            if custom_metric.aggregation_type == 'avg_per_day':
                if num_days > 0:
                    df[custom_metric.metric_id] = df[source_metric] / num_days
                else:
                    df[custom_metric.metric_id] = df[source_metric]
                logger.info(
                    f"Applied avg_per_day for '{custom_metric.name}': "
                    f"divided {source_metric} by {num_days} days"
                )
                continue

            # Determine grouping dimensions (exclude specified)
            group_dims = [
                d for d in current_dimensions
                if d not in custom_metric.exclude_dimensions
            ]

            # Check if re-aggregation is needed
            if set(group_dims) != set(current_dimensions):
                df = self._add_reaggregated_metric(
                    df,
                    source_metric,
                    custom_metric.metric_id,
                    group_dims,
                    custom_metric.aggregation_type
                )
            else:
                # No re-aggregation needed, just copy the column
                df[custom_metric.metric_id] = df[source_metric]

        return df

    def _apply_buckets(
        self,
        series: pd.Series,
        buckets: List[Dict[str, Any]]
    ) -> pd.Series:
        """
        Apply bucket conditions to create categorical labels.

        Bucket format:
        [
            {"label": "High", "min": 1000},
            {"label": "Medium", "min": 100, "max": 999},
            {"label": "Low", "max": 99}
        ]
        """
        result = pd.Series(['Other'] * len(series), index=series.index)

        # Sort buckets by min value (descending) to handle overlaps correctly
        sorted_buckets = sorted(
            buckets,
            key=lambda b: b.get('min', float('-inf')),
            reverse=True
        )

        for bucket in sorted_buckets:
            label = bucket.get('label', 'Unknown')
            conditions = []

            if 'min' in bucket:
                conditions.append(series >= bucket['min'])
            if 'max' in bucket:
                conditions.append(series <= bucket['max'])
            if 'equals' in bucket:
                conditions.append(series == bucket['equals'])

            if conditions:
                mask = conditions[0]
                for cond in conditions[1:]:
                    mask = mask & cond
                result.loc[mask] = label

        return result

    def _apply_date_ranges(
        self,
        date_series: pd.Series,
        date_ranges: List[Dict[str, Any]]
    ) -> pd.Series:
        """
        Apply date range conditions to create period labels.

        Date range format:
        [
            {"label": "Q1 2024", "start_date": "2024-01-01", "end_date": "2024-03-31"},
            {"label": "Q2 2024", "start_date": "2024-04-01", "end_date": "2024-06-30"}
        ]
        """
        result = pd.Series(['Other'] * len(date_series), index=date_series.index)

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(date_series):
            date_series = pd.to_datetime(date_series)

        for date_range in date_ranges:
            label = date_range.get('label', 'Unknown')
            start_date = date_range.get('start_date')
            end_date = date_range.get('end_date')

            if start_date and end_date:
                start = pd.to_datetime(start_date)
                end = pd.to_datetime(end_date)
                mask = (date_series >= start) & (date_series <= end)
                result.loc[mask] = label

        return result

    def _apply_metric_conditions(
        self,
        series: pd.Series,
        conditions_list: List[Dict[str, Any]]
    ) -> pd.Series:
        """
        Apply metric conditions to create categorical labels.

        Conditions format:
        [
            {
                "label": "High CVR",
                "conditions": [{"operator": ">", "value": 0.1}]
            },
            {
                "label": "Low CVR",
                "conditions": [{"operator": "<=", "value": 0.1}]
            }
        ]
        """
        result = pd.Series(['Other'] * len(series), index=series.index)

        for condition_set in conditions_list:
            label = condition_set.get('label', 'Unknown')
            conditions = condition_set.get('conditions', [])

            mask = pd.Series([True] * len(series), index=series.index)

            for condition in conditions:
                operator = condition.get('operator', '>')
                value = condition.get('value')
                value_max = condition.get('value_max')

                if value is None:
                    continue

                if operator == '>':
                    mask = mask & (series > value)
                elif operator == '>=':
                    mask = mask & (series >= value)
                elif operator == '<':
                    mask = mask & (series < value)
                elif operator == '<=':
                    mask = mask & (series <= value)
                elif operator == '=':
                    mask = mask & (series == value)
                elif operator == '!=' or operator == '<>':
                    mask = mask & (series != value)
                elif operator == 'between' and value_max is not None:
                    mask = mask & (series >= value) & (series <= value_max)
                elif operator == 'is_null':
                    mask = mask & series.isna()
                elif operator == 'is_not_null':
                    mask = mask & series.notna()

            result.loc[mask] = label

        return result

    def _reaggregate_by_dimension(
        self,
        df: pd.DataFrame,
        group_col: str,
        existing_dimensions: List[str]
    ) -> pd.DataFrame:
        """
        Re-aggregate DataFrame by a new dimension column.

        Groups by the new dimension and sums all numeric columns.
        """
        # Find metric columns (numeric, not dimension columns)
        dimension_cols = set(existing_dimensions + [group_col])
        metric_cols = [
            col for col in df.columns
            if col not in dimension_cols
            and pd.api.types.is_numeric_dtype(df[col])
        ]

        if not metric_cols:
            logger.warning("No metric columns found for re-aggregation")
            return df

        # Group by the custom dimension and sum metrics
        # Keep original column name so _build_pivot_row can find it
        grouped = df.groupby(group_col, as_index=False)[metric_cols].sum()

        return grouped

    def _add_reaggregated_metric(
        self,
        df: pd.DataFrame,
        source_metric: str,
        target_metric_id: str,
        group_dims: List[str],
        aggregation_type: str
    ) -> pd.DataFrame:
        """
        Add a re-aggregated metric column to the DataFrame.

        This creates a new column with the metric aggregated across
        excluded dimensions.
        """
        if not group_dims:
            # No grouping dimensions, aggregate entire column
            agg_value = self._aggregate_series(df[source_metric], aggregation_type)
            df[target_metric_id] = agg_value
        else:
            # Group by remaining dimensions and aggregate
            agg_func = self._get_agg_function(aggregation_type)

            # Calculate aggregated values
            grouped = df.groupby(group_dims)[source_metric].transform(agg_func)
            df[target_metric_id] = grouped

        return df

    def _aggregate_series(self, series: pd.Series, aggregation_type: str) -> float:
        """Aggregate a series using the specified aggregation type."""
        if aggregation_type == 'sum':
            return series.sum()
        elif aggregation_type == 'avg':
            return series.mean()
        elif aggregation_type == 'max':
            return series.max()
        elif aggregation_type == 'min':
            return series.min()
        elif aggregation_type == 'count':
            return series.count()
        else:
            return series.sum()

    def _get_agg_function(self, aggregation_type: str):
        """Get the pandas aggregation function for the given type."""
        mapping = {
            'sum': 'sum',
            'avg': 'mean',
            'max': 'max',
            'min': 'min',
            'count': 'count',
        }
        return mapping.get(aggregation_type, 'sum')
