"""
Query Router Service for selecting optimal rollup tables.
Determines whether to query raw table or pre-aggregated rollups.

Note: Base metrics are DEPRECATED. All metrics should be calculated metrics.
"""
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field

from models.schemas import RollupDef, RollupConfig, SchemaConfig


@dataclass
class RouteDecision:
    """Result of routing decision."""
    use_rollup: bool
    rollup_id: Optional[str] = None
    rollup_table_path: Optional[str] = None
    needs_reaggregation: bool = False
    reason: str = ""
    metrics_available: List[str] = field(default_factory=list)
    metrics_unavailable: List[str] = field(default_factory=list)


class QueryRouterService:
    """Service for routing queries to optimal data sources."""

    def __init__(
        self,
        rollup_config: Optional[RollupConfig],
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str
    ):
        self.rollup_config = rollup_config
        self.schema_config = schema_config
        self.source_project_id = source_project_id
        self.source_dataset = source_dataset

    def _get_distinct_metrics(self, metric_ids: List[str]) -> Set[str]:
        """Get the set of metrics that behave like COUNT_DISTINCT when aggregated.

        This includes calculated metrics with category="volume" - these are pre-computed
        COUNT_DISTINCTs stored in rollups. While they CAN be summed, summing across dates
        causes inflation when entities appear on multiple dates.

        Returns:
            Set of metric IDs that may cause inflation when re-aggregated across dates.
        """
        distinct_metrics = set()
        for metric_id in metric_ids:
            # Check calculated metrics - volume metrics are stored COUNT DISTINCTs
            # These can be summed but may cause inflation when re-aggregating across dates
            calc_metric = next(
                (m for m in self.schema_config.calculated_metrics if m.id == metric_id),
                None
            )
            if calc_metric and calc_metric.category == "volume":
                distinct_metrics.add(metric_id)
        return distinct_metrics

    def _get_sum_metrics(self, metric_ids: List[str]) -> Set[str]:
        """Get the set of metrics that can be re-aggregated by summing."""
        sum_metrics = set()
        for metric_id in metric_ids:
            # Check calculated metrics - volume metrics can be summed
            calc_metric = next(
                (m for m in self.schema_config.calculated_metrics if m.id == metric_id),
                None
            )
            if calc_metric and calc_metric.category == "volume":
                sum_metrics.add(metric_id)
        return sum_metrics

    def _get_rollup_table_path(self, rollup: RollupDef) -> str:
        """Get the full BigQuery table path for a rollup."""
        # Use rollup-specific target, then global default, then source as fallback
        target_project = (
            rollup.target_project or
            (self.rollup_config.default_target_project if self.rollup_config else None) or
            self.source_project_id
        )
        target_dataset = (
            rollup.target_dataset or
            (self.rollup_config.default_target_dataset if self.rollup_config else None) or
            self.source_dataset
        )
        return f"{target_project}.{target_dataset}.{rollup.target_table_name}"

    def _get_rollup_metrics(self, rollup: RollupDef) -> Set[str]:
        """
        Get all metrics available in a rollup.

        Metrics are auto-derived from schema:
        1. Volume calculated metrics (stored in rollup)
        2. Conversion metrics (calculated in Python from available volumes)
        """
        available = set()

        # All volume calculated metrics are auto-included
        for m in self.schema_config.calculated_metrics:
            if m.category == "volume":
                available.add(m.id)

        # Conversion metrics can be calculated if their dependencies are available
        for calc_metric in self.schema_config.calculated_metrics:
            if calc_metric.category != "volume":
                # Check if all dependencies are available
                if calc_metric.depends_on and all(dep in available for dep in calc_metric.depends_on):
                    available.add(calc_metric.id)

        return available

    def find_simplest_rollup(self) -> Optional[RollupDef]:
        """
        Find rollup with only 'date' dimension (baseline) for metric comparison.

        Used to detect metric inflation from dimensional aggregation.
        The baseline rollup has only the date dimension for time-series totals.

        Returns:
            RollupDef with only 'date' dimension and 'ready' status, or None if not found
        """
        if not self.rollup_config or not self.rollup_config.rollups:
            return None

        # Find rollup with only 'date' dimension and ready status
        for rollup in self.rollup_config.rollups:
            if rollup.dimensions == ['date'] and rollup.status == "ready":
                return rollup

        return None

    def get_baseline_rollup_path(self) -> Optional[str]:
        """
        Get the table path for the baseline rollup (no dimensions).

        Returns:
            Full BigQuery table path for baseline rollup, or None if not found
        """
        baseline = self.find_simplest_rollup()
        if baseline:
            return self._get_rollup_table_path(baseline)
        return None

    def _score_rollup(
        self,
        rollup: RollupDef,
        query_dimensions: Set[str],
        query_metrics: Set[str],
        distinct_metrics: Set[str],
        filter_dimensions: Set[str]
    ) -> Tuple[int, bool, str]:
        """
        Score a rollup for a given query.

        Matching rules:
        1. Rollup must have all query dimensions
        2. Rollup must have all filter dimensions
        3. Rollup must have all required metrics
        4. SPECIAL CASE: When query has no dimensions (totals query), allow using
           a date-only rollup with re-aggregation (for non-DISTINCT metrics)
        5. For DISTINCT metrics, require exact dimension match (no re-aggregation)

        Returns:
            Tuple of (score, needs_reaggregation, reason)
            Score of -1 means rollup cannot be used
        """
        rollup_dims = set(rollup.dimensions)
        rollup_metrics = self._get_rollup_metrics(rollup)

        # Check if rollup is ready
        if rollup.status != "ready":
            return -1, False, f"Rollup status is '{rollup.status}', not 'ready'"

        # Check if rollup has all required query dimensions
        if not query_dimensions.issubset(rollup_dims):
            missing = query_dimensions - rollup_dims
            return -1, False, f"Missing dimensions: {missing}"

        # Check if rollup has all required filter dimensions
        if not filter_dimensions.issubset(rollup_dims):
            missing = filter_dimensions - rollup_dims
            return -1, False, f"Missing filter dimensions: {missing}"

        # Check if rollup has all required metrics
        if not query_metrics.issubset(rollup_metrics):
            missing = query_metrics - rollup_metrics
            return -1, False, f"Missing metrics: {missing}"

        # Check for extra dimensions in rollup
        # Extra dims are rollup dimensions that are NOT in query dimensions AND NOT in filter dimensions
        # Filter dimensions don't count as "extra" because we're filtering on them (not re-aggregating)
        extra_dims = rollup_dims - query_dimensions - filter_dimensions

        if extra_dims:
            # SPECIAL CASE: Allow re-aggregation when the only extra dimension is 'date'
            # This enables using rollups with date + other dimensions and summing across dates
            # Example: query for ['experiment_group'] can use rollup ['date', 'experiment_group']
            if extra_dims == {'date'}:
                # For DISTINCT metrics, allow re-aggregation but with lower score (will show warning in UI)
                # The values will be slightly inflated due to cross-date overlap, but this is often acceptable
                # Give lower score (80) for DISTINCT vs (100) for non-DISTINCT to prefer exact match when available
                if distinct_metrics:
                    return 80, True, "OK (re-aggregating COUNT DISTINCT across dates - may have slight inflation)"
                # Allow re-aggregation for non-DISTINCT metrics
                # Give slightly lower score than exact match (100 vs 150) to prefer exact match when available
                return 100, True, "OK (re-aggregating across dates)"

            # For other cases, strict mode - no re-aggregation allowed
            return -1, False, f"Rollup has extra dimensions: {extra_dims}. Exact match required (strict mode - no re-aggregation allowed)"

        # Exact dimension match - no re-aggregation needed
        score = 150
        needs_reaggregation = False

        return score, needs_reaggregation, "OK"

    def route_query(
        self,
        query_dimensions: List[str],
        query_metrics: List[str],
        query_filters: Optional[Dict[str, List[str]]] = None,
        require_rollup: bool = False
    ) -> RouteDecision:
        """
        Determine optimal data source for a query.

        Args:
            query_dimensions: Dimensions to group by
            query_metrics: Metrics to aggregate
            query_filters: Dimension filters applied
            require_rollup: If True, return error when no rollup matches (for DISTINCT metrics)

        Returns:
            RouteDecision with routing information
        """
        # No rollup config - use raw table
        if not self.rollup_config or not self.rollup_config.rollups:
            if require_rollup:
                return RouteDecision(
                    use_rollup=False,
                    reason="No rollups configured; query requires raw table"
                )
            return RouteDecision(
                use_rollup=False,
                reason="No rollups configured"
            )

        query_dims_set = set(query_dimensions)
        query_metrics_set = set(query_metrics)
        filter_dims_set = set(query_filters.keys()) if query_filters else set()

        # Identify COUNT DISTINCT metrics
        distinct_metrics = self._get_distinct_metrics(query_metrics)

        # If we have DISTINCT metrics, we MUST use a rollup with exact match
        # or fall back to raw table (which is expensive)
        has_distinct = bool(distinct_metrics)

        # Score all rollups
        scored_rollups: List[Tuple[int, RollupDef, bool, str]] = []

        for rollup in self.rollup_config.rollups:
            score, needs_reagg, reason = self._score_rollup(
                rollup,
                query_dims_set,
                query_metrics_set,
                distinct_metrics,
                filter_dims_set
            )
            if score >= 0:
                scored_rollups.append((score, rollup, needs_reagg, reason))

        # Sort by score (highest first)
        scored_rollups.sort(key=lambda x: x[0], reverse=True)

        # No suitable rollup found
        if not scored_rollups:
            if has_distinct and require_rollup:
                # User chose to error when no rollup for DISTINCT
                return RouteDecision(
                    use_rollup=False,
                    reason=f"No suitable rollup for COUNT_DISTINCT metrics {distinct_metrics} with dimensions {query_dims_set}. "
                           f"Create a rollup with exactly these dimensions to enable this query."
                )
            return RouteDecision(
                use_rollup=False,
                reason="No suitable rollup found; using raw table"
            )

        # Use best scoring rollup
        best_score, best_rollup, needs_reagg, _ = scored_rollups[0]

        return RouteDecision(
            use_rollup=True,
            rollup_id=best_rollup.id,
            rollup_table_path=self._get_rollup_table_path(best_rollup),
            needs_reaggregation=needs_reagg,
            reason=f"Using rollup '{best_rollup.id}' (score: {best_score})",
            metrics_available=list(self._get_rollup_metrics(best_rollup))
        )

    def find_suitable_rollups(
        self,
        query_dimensions: List[str],
        query_metrics: List[str],
        query_filters: Optional[Dict[str, List[str]]] = None
    ) -> List[Dict]:
        """
        Find all suitable rollups for a query (for debugging/UI).

        Returns:
            List of dicts with rollup info and scores
        """
        if not self.rollup_config or not self.rollup_config.rollups:
            return []

        query_dims_set = set(query_dimensions)
        query_metrics_set = set(query_metrics)
        filter_dims_set = set(query_filters.keys()) if query_filters else set()
        distinct_metrics = self._get_distinct_metrics(query_metrics)

        results = []
        for rollup in self.rollup_config.rollups:
            score, needs_reagg, reason = self._score_rollup(
                rollup,
                query_dims_set,
                query_metrics_set,
                distinct_metrics,
                filter_dims_set
            )
            results.append({
                "rollup_id": rollup.id,
                "display_name": rollup.display_name,
                "dimensions": rollup.dimensions,
                "status": rollup.status,
                "score": score,
                "can_use": score >= 0,
                "needs_reaggregation": needs_reagg if score >= 0 else None,
                "reason": reason
            })

        # Sort by score
        results.sort(key=lambda x: x["score"], reverse=True)
        return results

    def get_recommended_rollups(
        self,
        common_dimension_combinations: List[List[str]]
    ) -> List[Dict]:
        """
        Suggest rollups based on common dimension combinations.

        Args:
            common_dimension_combinations: List of dimension combos frequently queried

        Returns:
            List of recommended rollup configurations
        """
        recommendations = []
        existing_dims = set()

        if self.rollup_config:
            for rollup in self.rollup_config.rollups:
                existing_dims.add(tuple(sorted(rollup.dimensions)))

        for dims in common_dimension_combinations:
            dims_tuple = tuple(sorted(dims))
            if dims_tuple not in existing_dims:
                recommendations.append({
                    "dimensions": dims,
                    "suggested_id": "rollup_" + "_".join(sorted(dims)),
                    "reason": "Frequently queried dimension combination"
                })

        return recommendations
