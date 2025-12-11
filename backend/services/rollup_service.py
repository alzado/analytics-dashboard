"""
Rollup service for managing pre-aggregated tables.
Handles CRUD operations, SQL generation, and rollup table creation in BigQuery.

Key design principles:
- Metrics are AUTO-INCLUDED from schema (all base metrics + volume calculated metrics)
- Supports incremental refresh: only insert missing dates AND add missing metric columns
- Conversion metrics are NEVER stored in rollups (calculated in Python after query)
"""
import os
import json
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
from itertools import combinations

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def generate_dimension_combinations(dimensions: List[str]) -> List[List[str]]:
    """
    Generate all subsets of dimensions, always including 'date' as the baseline.

    This creates the power set for rollup generation. When creating a rollup
    with dimensions [A, B, C], this generates all possible combinations,
    always ensuring 'date' is included:
    - [date] (baseline - date only)
    - [date, A], [date, B], [date, C]
    - [date, A, B], [date, A, C], [date, B, C]
    - [date, A, B, C]

    Args:
        dimensions: List of dimension IDs (should include 'date')

    Returns:
        List of dimension combinations (each is a sorted list for consistency)
    """
    # Separate date from other dimensions
    other_dims = [d for d in dimensions if d != 'date']
    sorted_other_dims = sorted(other_dims)

    all_combinations = [['date']]  # Start with date-only baseline

    # Generate combinations of other dimensions (length 1 to n), always adding 'date'
    for r in range(1, len(sorted_other_dims) + 1):
        for combo in combinations(sorted_other_dims, r):
            # Always include 'date' with each combination
            all_combinations.append(['date'] + list(combo))

    return all_combinations

from models.schemas import (
    RollupDef, RollupConfig, RollupMetricDef, RollupCreate, RollupUpdate,
    RollupRefreshResponse, RollupListResponse, RollupPreviewSqlResponse,
    RollupStatusResponse, BaseMetric, SchemaConfig
)
from config import ROLLUPS_DIR


def generate_key_column_name(columns: List[str]) -> str:
    """
    Generate deterministic key column name from source columns.
    Same logic as OptimizedSourceService to ensure consistency.
    """
    sorted_cols = sorted([c.strip().lower() for c in columns])
    return "_key_" + "_".join(sorted_cols)


def replace_concat_with_keys(sql_expression: str, key_column_mapping: Dict[str, str]) -> str:
    """
    Replace CONCAT(...) patterns in SQL expression with precomputed key columns.

    Args:
        sql_expression: The original SQL expression with CONCAT patterns
        key_column_mapping: Dict mapping column combinations to key column names
                           e.g. {"query,visit_id": "_key_query_visit_id"}

    Returns:
        SQL expression with CONCAT patterns replaced by key columns
    """
    import re

    if not key_column_mapping or 'CONCAT' not in sql_expression.upper():
        return sql_expression

    result = sql_expression

    # Find all CONCAT expressions using balanced parentheses matching
    upper_sql = result.upper()
    replacements = []  # (start, end, replacement)

    start = 0
    while True:
        idx = upper_sql.find('CONCAT', start)
        if idx == -1:
            break

        # Find the opening paren after CONCAT
        paren_start = result.find('(', idx)
        if paren_start == -1:
            break

        # Count balanced parens to find the closing one
        depth = 1
        pos = paren_start + 1
        while pos < len(result) and depth > 0:
            if result[pos] == '(':
                depth += 1
            elif result[pos] == ')':
                depth -= 1
            pos += 1

        if depth == 0:
            concat_full = result[idx:pos]  # "CONCAT ( ... )"
            concat_content = result[paren_start + 1:pos - 1].strip()

            # Extract columns from COALESCE patterns within this CONCAT
            coalesce_pattern = r'COALESCE\s*\(\s*(?:CAST\s*\(\s*)?(\w+)'
            columns = re.findall(coalesce_pattern, concat_content, re.IGNORECASE)

            if len(columns) >= 2:
                # Check if we have a precomputed key for these columns
                sorted_cols = sorted([c.lower() for c in columns])
                lookup_key = ",".join(sorted_cols)

                if lookup_key in key_column_mapping:
                    key_col = key_column_mapping[lookup_key]
                    replacements.append((idx, pos, key_col))

        start = pos

    # Apply replacements in reverse order to preserve positions
    for start_pos, end_pos, replacement in reversed(replacements):
        result = result[:start_pos] + replacement + result[end_pos:]

    return result


class RollupService:
    """Service for managing pre-aggregated rollup tables."""

    def __init__(self, bigquery_client: Optional[bigquery.Client], table_id: str):
        self.client = bigquery_client
        self.table_id = table_id
        self._config_cache: Optional[RollupConfig] = None
        self._optimized_source_service = None

    def _get_optimized_source_info(
        self,
        source_table_path: str
    ) -> Tuple[str, Optional[Dict[str, str]]]:
        """
        Get optimized source table path and key column mapping if available.

        If an optimized source table exists and is ready, returns:
        - The optimized table path (to use as source)
        - Mapping of column combinations to key column names

        If not available, returns the original source path and None.

        Returns:
            Tuple of (source_path_to_use, key_column_mapping)
        """
        try:
            from services.optimized_source_service import OptimizedSourceService

            if self._optimized_source_service is None:
                self._optimized_source_service = OptimizedSourceService(
                    self.client, self.table_id
                )

            config = self._optimized_source_service.load_config()

            if config and config.status == "ready":
                optimized_path = self._optimized_source_service.get_optimized_table_path(config)
                if optimized_path:
                    # Build key column mapping
                    key_mapping = self._optimized_source_service.get_key_column_mapping()
                    return optimized_path, key_mapping

        except Exception as e:
            # If anything fails, fall back to original source
            print(f"Warning: Could not get optimized source info: {e}")

        return source_table_path, None

    def _get_config_path(self) -> Path:
        """Get the file path for rollup configuration."""
        rollups_dir = Path(ROLLUPS_DIR)
        rollups_dir.mkdir(parents=True, exist_ok=True)
        return rollups_dir / f"rollup_config_{self.table_id}.json"

    def load_config(self) -> RollupConfig:
        """Load rollup configuration from JSON file."""
        if self._config_cache is not None:
            return self._config_cache

        config_path = self._get_config_path()
        if config_path.exists():
            with open(config_path, 'r') as f:
                data = json.load(f)
                self._config_cache = RollupConfig(**data)
        else:
            # Create default config
            now = datetime.utcnow().isoformat() + "Z"
            self._config_cache = RollupConfig(
                rollups=[],
                default_target_dataset=None,
                version=1,
                created_at=now,
                updated_at=now
            )
            self.save_config(self._config_cache)

        return self._config_cache

    def save_config(self, config: RollupConfig) -> None:
        """Persist rollup configuration to file."""
        config_path = self._get_config_path()
        config.updated_at = datetime.utcnow().isoformat() + "Z"
        with open(config_path, 'w') as f:
            json.dump(config.model_dump(), f, indent=2)
        self._config_cache = config

    def _generate_rollup_id(self, dimensions: List[str]) -> str:
        """Generate rollup ID from dimensions, prefixed with table_id.

        For baseline rollups (date-only), returns '{table_id}_rollup_date'.
        """
        sorted_dims = sorted(dimensions)
        return f"{self.table_id}_rollup_" + "_".join(sorted_dims)

    def _generate_table_name(self, dimensions: List[str]) -> str:
        """Generate table name from dimensions."""
        return self._generate_rollup_id(dimensions)

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    def list_rollups(self) -> RollupListResponse:
        """List all rollup definitions."""
        config = self.load_config()
        return RollupListResponse(
            rollups=config.rollups,
            default_target_project=config.default_target_project,
            default_target_dataset=config.default_target_dataset
        )

    def get_rollup(self, rollup_id: str) -> Optional[RollupDef]:
        """Get a specific rollup by ID."""
        config = self.load_config()
        for rollup in config.rollups:
            if rollup.id == rollup_id:
                return rollup
        return None

    def create_rollup(self, data: RollupCreate, schema_config: SchemaConfig) -> List[RollupDef]:
        """Create rollup definitions for ALL dimension combinations.

        When given dimensions [A, B, C], creates 2^n rollups (8 total):
        - {table_id}_rollup_baseline (no dimensions - pure totals)
        - {table_id}_rollup_A, _B, _C
        - {table_id}_rollup_A_B, _A_C, _B_C
        - {table_id}_rollup_A_B_C

        Note: Metrics are auto-included from schema. Only dimensions need to be specified.

        Args:
            data: RollupCreate with dimensions and optional settings
            schema_config: Schema configuration for validation

        Returns:
            List of created RollupDef objects (one for each combination)
        """
        config = self.load_config()

        # Validate all dimensions exist in schema
        valid_dims = {d.id for d in schema_config.dimensions}
        for dim in data.dimensions:
            if dim not in valid_dims:
                raise ValueError(f"Unknown dimension: {dim}")

        now = datetime.utcnow().isoformat() + "Z"
        created_rollups = []

        # Ensure 'date' is always included in dimensions
        dims_with_date = list(data.dimensions)
        if 'date' not in dims_with_date:
            dims_with_date.insert(0, 'date')

        # Generate all combinations (power set) - always includes date as baseline
        dim_combinations = generate_dimension_combinations(dims_with_date)

        # Create a rollup for each combination
        for dims in dim_combinations:
            # Generate unique ID for this combination
            rollup_id = self._generate_rollup_id(dims)

            # Skip if already exists (idempotent behavior)
            if any(r.id == rollup_id for r in config.rollups):
                print(f"Rollup {rollup_id} already exists, skipping")
                continue

            # Generate table name
            table_name = self._generate_table_name(dims)

            # Generate display name - date-only is the baseline
            other_dims = [d for d in dims if d != 'date']
            if other_dims:
                dim_labels = ", ".join(other_dims)
                display_name = f"{data.display_name} [{dim_labels}]"
            else:
                display_name = f"{data.display_name} [Baseline]"

            # Generate description
            if other_dims:
                dim_labels = ", ".join(other_dims)
                description = data.description or f"Pre-aggregated table grouped by date, {dim_labels}"
            else:
                description = data.description or "Baseline rollup with date dimension only (for metric comparison)"

            rollup = RollupDef(
                id=rollup_id,
                display_name=display_name,
                description=description,
                dimensions=dims,
                metrics=None,  # Deprecated - metrics auto-derived from schema
                target_project=data.target_project,
                target_dataset=data.target_dataset,
                target_table_name=table_name,
                status="pending",
                created_at=now,
                updated_at=now
            )

            config.rollups.append(rollup)
            created_rollups.append(rollup)

        # Save all at once (atomic)
        self.save_config(config)

        return created_rollups

    def update_rollup(self, rollup_id: str, data: RollupUpdate, schema_config: SchemaConfig) -> RollupDef:
        """Update an existing rollup definition.

        Note: Metrics are auto-included from schema - updating metrics is not supported.
        """
        config = self.load_config()

        rollup_idx = None
        for i, r in enumerate(config.rollups):
            if r.id == rollup_id:
                rollup_idx = i
                break

        if rollup_idx is None:
            raise ValueError(f"Rollup with ID '{rollup_id}' not found")

        rollup = config.rollups[rollup_idx]

        # Apply updates
        if data.display_name is not None:
            rollup.display_name = data.display_name
        if data.description is not None:
            rollup.description = data.description
        if data.dimensions is not None:
            # Validate dimensions
            valid_dims = {d.id for d in schema_config.dimensions}
            for dim in data.dimensions:
                if dim not in valid_dims:
                    raise ValueError(f"Unknown dimension: {dim}")
            rollup.dimensions = data.dimensions
            # Mark as stale since dimensions changed
            rollup.status = "stale"
        # Note: metrics field is deprecated - metrics are auto-included from schema
        if data.target_project is not None:
            rollup.target_project = data.target_project
        if data.target_dataset is not None:
            rollup.target_dataset = data.target_dataset
        if data.target_table_name is not None:
            rollup.target_table_name = data.target_table_name

        rollup.updated_at = datetime.utcnow().isoformat() + "Z"
        config.rollups[rollup_idx] = rollup
        self.save_config(config)

        return rollup

    def delete_rollup(
        self,
        rollup_id: str,
        drop_table: bool = False,
        source_project_id: Optional[str] = None,
        source_dataset: Optional[str] = None
    ) -> bool:
        """Delete a rollup definition and optionally drop the BigQuery table."""
        config = self.load_config()

        rollup_idx = None
        rollup = None
        for i, r in enumerate(config.rollups):
            if r.id == rollup_id:
                rollup_idx = i
                rollup = r
                break

        if rollup_idx is None:
            raise ValueError(f"Rollup with ID '{rollup_id}' not found")

        # Drop BigQuery table if requested
        if drop_table and self.client and rollup:
            try:
                target_dataset = rollup.target_dataset or config.default_target_dataset or source_dataset
                table_path = f"{source_project_id}.{target_dataset}.{rollup.target_table_name}"
                self.client.delete_table(table_path, not_found_ok=True)
            except Exception as e:
                # Log but don't fail the delete
                print(f"Warning: Failed to drop table: {e}")

        # Remove from config
        config.rollups.pop(rollup_idx)
        self.save_config(config)

        return True

    def set_default_project(self, project: Optional[str]) -> RollupConfig:
        """Set the default target project for rollups."""
        config = self.load_config()
        config.default_target_project = project
        self.save_config(config)
        return config

    def set_default_dataset(self, dataset: Optional[str]) -> RollupConfig:
        """Set the default target dataset for rollups."""
        config = self.load_config()
        config.default_target_dataset = dataset
        self.save_config(config)
        return config

    # =========================================================================
    # Metrics Auto-Inclusion
    # =========================================================================

    def get_all_rollup_metrics(self, schema_config: SchemaConfig) -> List[str]:
        """
        Get all metric IDs that should be included in rollups.

        Returns IDs of:
        - All base metrics (except system metrics like days_in_range)
        - Volume calculated metrics (category="volume")

        Note: Conversion/rate metrics are NOT stored in rollups.
              They are calculated in Python after querying.
        """
        metric_ids = []

        # All base metrics (except system metrics)
        for m in schema_config.base_metrics:
            if not m.is_system:
                metric_ids.append(m.id)

        # Volume calculated metrics only
        for m in schema_config.calculated_metrics:
            if m.category == "volume":
                metric_ids.append(m.id)

        return metric_ids

    def get_rollup_metric_definitions(
        self,
        schema_config: SchemaConfig
    ) -> Tuple[List[BaseMetric], List]:
        """
        Get the actual metric definitions for rollup.

        Returns:
            Tuple of (base_metrics_list, volume_calc_metrics_list)
        """
        base_metrics = [m for m in schema_config.base_metrics if not m.is_system]
        volume_calc = [m for m in schema_config.calculated_metrics if m.category == "volume"]
        return base_metrics, volume_calc

    # =========================================================================
    # SQL Generation
    # =========================================================================

    def _build_aggregation_sql(
        self,
        metric: BaseMetric,
        alias: str,
        key_column_mapping: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build aggregation SQL for a base metric.

        Args:
            metric: The base metric definition
            alias: SQL alias for the result
            key_column_mapping: Optional mapping from column combination to precomputed key column.
                               Format: {"col1,col2": "_key_col1_col2"}

        If key_column_mapping is provided and contains a mapping for this metric's columns,
        uses the precomputed key column instead of CONCAT+COALESCE at runtime.
        """
        col = metric.column_name
        agg = metric.aggregation

        if agg == "COUNT_DISTINCT":
            # Handle multi-column COUNT DISTINCT
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]

                # Check if we have a precomputed key column
                if key_column_mapping:
                    # Generate the key name from sorted columns
                    key_name = generate_key_column_name(cols)
                    if key_name in key_column_mapping.values():
                        # Use precomputed key column directly
                        return f"COUNT(DISTINCT {key_name}) AS {alias}"
                    # Also check by the lookup format (sorted columns joined)
                    lookup = ",".join(sorted(cols))
                    if lookup in key_column_mapping:
                        key_name = key_column_mapping[lookup]
                        return f"COUNT(DISTINCT {key_name}) AS {alias}"

                # Fallback to CONCAT+COALESCE
                concat_parts = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in cols])
                return f"COUNT(DISTINCT CONCAT({concat_parts})) AS {alias}"
            return f"COUNT(DISTINCT {col}) AS {alias}"
        elif agg == "APPROX_COUNT_DISTINCT":
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]

                # Check if we have a precomputed key column
                if key_column_mapping:
                    key_name = generate_key_column_name(cols)
                    if key_name in key_column_mapping.values():
                        # Use precomputed key with FARM_FINGERPRINT for APPROX
                        return f"APPROX_COUNT_DISTINCT(FARM_FINGERPRINT({key_name})) AS {alias}"
                    lookup = ",".join(sorted(cols))
                    if lookup in key_column_mapping:
                        key_name = key_column_mapping[lookup]
                        return f"APPROX_COUNT_DISTINCT(FARM_FINGERPRINT({key_name})) AS {alias}"

                # Fallback to CONCAT+COALESCE
                concat_parts = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in cols])
                return f"APPROX_COUNT_DISTINCT(FARM_FINGERPRINT(CONCAT({concat_parts}))) AS {alias}"
            return f"APPROX_COUNT_DISTINCT({col}) AS {alias}"
        else:
            return f"{agg}({col}) AS {alias}"

    def _build_conditional_aggregation_sql(
        self,
        metric: BaseMetric,
        flag_column: str,
        alias: str
    ) -> str:
        """Build conditional aggregation SQL (where flag=1)."""
        col = metric.column_name
        agg = metric.aggregation

        if agg == "COUNT_DISTINCT":
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]
                concat_parts = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in cols])
                return f"COUNT(DISTINCT CASE WHEN {flag_column} = 1 THEN CONCAT({concat_parts}) END) AS {alias}"
            return f"COUNT(DISTINCT CASE WHEN {flag_column} = 1 THEN {col} END) AS {alias}"
        elif agg == "APPROX_COUNT_DISTINCT":
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]
                concat_parts = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in cols])
                return f"APPROX_COUNT_DISTINCT(CASE WHEN {flag_column} = 1 THEN FARM_FINGERPRINT(CONCAT({concat_parts})) END) AS {alias}"
            return f"APPROX_COUNT_DISTINCT(CASE WHEN {flag_column} = 1 THEN {col} END) AS {alias}"
        elif agg == "SUM":
            return f"SUM(CASE WHEN {flag_column} = 1 THEN {col} ELSE 0 END) AS {alias}"
        elif agg == "COUNT":
            return f"COUNTIF({flag_column} = 1) AS {alias}"
        elif agg == "AVG":
            return f"AVG(CASE WHEN {flag_column} = 1 THEN {col} END) AS {alias}"
        elif agg in ("MIN", "MAX"):
            return f"{agg}(CASE WHEN {flag_column} = 1 THEN {col} END) AS {alias}"
        else:
            return f"{agg}(CASE WHEN {flag_column} = 1 THEN {col} END) AS {alias}"

    def generate_create_sql(
        self,
        rollup: RollupDef,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str
    ) -> Tuple[str, str]:
        """
        Generate CREATE TABLE AS SELECT SQL for a rollup.

        Note: Metrics are auto-included from schema (all base + volume calculated).
        If an optimized source table exists, it will be used for better performance.

        Returns:
            Tuple of (sql, target_table_path)
        """
        config = self.load_config()

        # Check for optimized source table
        actual_source_path, key_column_mapping = self._get_optimized_source_info(source_table_path)

        # Build target table path
        target_project = rollup.target_project or config.default_target_project or source_project_id
        target_dataset = rollup.target_dataset or config.default_target_dataset or source_dataset
        target_table_path = f"{target_project}.{target_dataset}.{rollup.target_table_name}"

        # Get auto-included metrics
        base_metrics, volume_calc_metrics = self.get_rollup_metric_definitions(schema_config)

        # Build dimensions lookup
        dims_by_id = {d.id: d for d in schema_config.dimensions}

        # Build SELECT parts
        select_parts = []

        # Add dimensions
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                select_parts.append(f"    {dim.column_name} AS {dim_id}")

        # Add ALL base metrics (auto-included)
        # Pass key_column_mapping to use precomputed keys when available
        for metric in base_metrics:
            agg_sql = self._build_aggregation_sql(metric, metric.id, key_column_mapping)
            select_parts.append(f"    {agg_sql}")

        # Add ALL volume calculated metrics (auto-included)
        # Replace CONCAT patterns with precomputed keys when available
        for calc_metric in volume_calc_metrics:
            sql_expr = calc_metric.sql_expression
            if key_column_mapping:
                sql_expr = replace_concat_with_keys(sql_expr, key_column_mapping)
            select_parts.append(f"    {sql_expr} AS {calc_metric.id}")

        # Build GROUP BY
        group_by_parts = []
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                group_by_parts.append(dim.column_name)

        select_clause = ',\n'.join(select_parts)
        group_by_clause = ', '.join(group_by_parts)

        # Determine clustering columns (non-date dimensions, up to 4)
        non_date_dims = [d for d in rollup.dimensions if d != 'date']
        cluster_columns = non_date_dims[:4]  # BigQuery allows max 4 clustering columns
        cluster_clause = f"\nCLUSTER BY {', '.join(cluster_columns)}" if cluster_columns else ""

        # All rollups include 'date' dimension, so partition by date
        # Use actual_source_path which may be the optimized table
        sql = f"""CREATE OR REPLACE TABLE `{target_table_path}`
PARTITION BY date{cluster_clause}
AS
SELECT
{select_clause}
FROM `{actual_source_path}`
GROUP BY {group_by_clause}"""

        return sql, target_table_path

    def preview_sql(
        self,
        rollup_id: str,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str
    ) -> RollupPreviewSqlResponse:
        """Preview the SQL that would be generated for a rollup."""
        rollup = self.get_rollup(rollup_id)
        if not rollup:
            raise ValueError(f"Rollup with ID '{rollup_id}' not found")

        sql, target_table_path = self.generate_create_sql(
            rollup, source_table_path, schema_config, source_project_id, source_dataset
        )

        return RollupPreviewSqlResponse(
            rollup_id=rollup_id,
            sql=sql,
            target_table_path=target_table_path
        )

    # =========================================================================
    # Incremental Refresh - Missing Dates/Metrics Detection
    # =========================================================================

    def _get_target_table_path(
        self,
        rollup: RollupDef,
        source_project_id: str,
        source_dataset: str
    ) -> str:
        """Get full BigQuery path for rollup table."""
        config = self.load_config()
        target_project = rollup.target_project or config.default_target_project or source_project_id
        target_dataset = rollup.target_dataset or config.default_target_dataset or source_dataset
        return f"{target_project}.{target_dataset}.{rollup.target_table_name}"

    def _table_exists(self, table_path: str) -> bool:
        """Check if a BigQuery table exists."""
        if not self.client:
            return False
        try:
            self.client.get_table(table_path)
            return True
        except NotFound:
            return False

    def get_missing_dates(
        self,
        source_table_path: str,
        target_table_path: str
    ) -> List[str]:
        """
        Find dates that exist in source table but not in rollup table.

        Returns:
            List of date strings (YYYY-MM-DD) that need to be processed.
        """
        if not self.client:
            return []

        query = f"""
        SELECT DISTINCT CAST(src.date AS STRING) as missing_date
        FROM (SELECT DISTINCT date FROM `{source_table_path}`) src
        LEFT JOIN (SELECT DISTINCT date FROM `{target_table_path}`) tgt
        ON src.date = tgt.date
        WHERE tgt.date IS NULL
        ORDER BY missing_date
        """
        try:
            result = self.client.query(query).result()
            return [row.missing_date for row in result]
        except Exception as e:
            print(f"Error getting missing dates: {e}")
            return []

    def get_all_source_dates(self, source_table_path: str) -> List[str]:
        """
        Get all distinct dates from the source table.

        Returns:
            List of date strings (YYYY-MM-DD) sorted ascending.
        """
        if not self.client:
            return []

        query = f"""
        SELECT DISTINCT CAST(date AS STRING) as date_str
        FROM `{source_table_path}`
        ORDER BY date_str
        """
        try:
            result = self.client.query(query).result()
            return [row.date_str for row in result]
        except Exception as e:
            print(f"Error getting source dates: {e}")
            return []

    def get_missing_metrics(
        self,
        target_table_path: str,
        schema_config: SchemaConfig
    ) -> List[str]:
        """
        Find metrics in schema that don't exist as columns in rollup table.

        Returns:
            List of metric IDs that need to be added.
        """
        if not self.client:
            return []

        try:
            # Get current rollup table columns
            table = self.client.get_table(target_table_path)
            existing_columns = {field.name for field in table.schema}

            # Get all metrics that should be in rollup
            expected_metrics = set(self.get_all_rollup_metrics(schema_config))

            # Return metrics that are missing
            return sorted(list(expected_metrics - existing_columns))
        except NotFound:
            return []
        except Exception as e:
            print(f"Error getting missing metrics: {e}")
            return []

    def get_rollup_status(
        self,
        rollup_id: str,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str
    ) -> RollupStatusResponse:
        """
        Get detailed status of a rollup including what's missing.

        Returns:
            RollupStatusResponse with missing dates/metrics info.
        """
        rollup = self.get_rollup(rollup_id)
        if not rollup:
            raise ValueError(f"Rollup with ID '{rollup_id}' not found")

        target_table_path = self._get_target_table_path(rollup, source_project_id, source_dataset)
        table_exists = self._table_exists(target_table_path)

        if not table_exists:
            # Table doesn't exist - all dates/metrics are "missing"
            return RollupStatusResponse(
                rollup_id=rollup_id,
                table_exists=False,
                missing_dates=[],
                missing_dates_count=0,
                missing_metrics=[],
                missing_metrics_count=0,
                is_up_to_date=False
            )

        # Get missing dates and metrics
        missing_dates = self.get_missing_dates(source_table_path, target_table_path)
        missing_metrics = self.get_missing_metrics(target_table_path, schema_config)

        return RollupStatusResponse(
            rollup_id=rollup_id,
            table_exists=True,
            missing_dates=missing_dates,
            missing_dates_count=len(missing_dates),
            missing_metrics=missing_metrics,
            missing_metrics_count=len(missing_metrics),
            is_up_to_date=len(missing_dates) == 0 and len(missing_metrics) == 0
        )

    def add_missing_metric_columns(
        self,
        target_table_path: str,
        missing_metrics: List[str],
        schema_config: SchemaConfig
    ) -> None:
        """
        Add missing metric columns to rollup table using ALTER TABLE.

        Args:
            target_table_path: Full path to rollup table
            missing_metrics: List of metric IDs to add
            schema_config: Schema configuration
        """
        if not self.client or not missing_metrics:
            return

        # Build lookup for metrics
        base_metrics_by_id = {m.id: m for m in schema_config.base_metrics}
        calc_metrics_by_id = {m.id: m for m in schema_config.calculated_metrics}

        for metric_id in missing_metrics:
            # Determine data type
            base_metric = base_metrics_by_id.get(metric_id)
            if base_metric:
                if base_metric.data_type == "INTEGER":
                    bq_type = "INT64"
                elif base_metric.data_type == "FLOAT":
                    bq_type = "FLOAT64"
                else:
                    bq_type = "NUMERIC"
            else:
                # Calculated metrics are typically floats
                bq_type = "FLOAT64"

            # ALTER TABLE to add column
            alter_sql = f"ALTER TABLE `{target_table_path}` ADD COLUMN IF NOT EXISTS {metric_id} {bq_type}"
            try:
                self.client.query(alter_sql).result()
            except Exception as e:
                print(f"Warning: Failed to add column {metric_id}: {e}")

    def _build_metric_aggregation_for_update(
        self,
        metric_id: str,
        schema_config: SchemaConfig
    ) -> Optional[str]:
        """Build the aggregation expression for a single metric."""
        # Check base metrics
        for m in schema_config.base_metrics:
            if m.id == metric_id:
                return self._build_aggregation_sql(m, metric_id)

        # Check calculated metrics
        for m in schema_config.calculated_metrics:
            if m.id == metric_id and m.category == "volume":
                return f"{m.sql_expression} AS {metric_id}"

        return None

    def _backfill_metrics(
        self,
        target_table_path: str,
        source_table_path: str,
        rollup: RollupDef,
        missing_metrics: List[str],
        schema_config: SchemaConfig
    ) -> int:
        """
        Backfill newly added metric columns from source data.

        Uses UPDATE with JOIN to populate values.

        Returns:
            Number of rows updated.
        """
        if not self.client or not missing_metrics:
            return 0

        # Build dimensions lookup
        dims_by_id = {d.id: d for d in schema_config.dimensions}

        # Build dimension column list for JOIN
        dim_columns = []
        join_conditions = []
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                dim_columns.append(dim.column_name)
                join_conditions.append(f"target_tbl.{dim_id} = source_agg.{dim_id}")

        # Build aggregation expressions for missing metrics
        select_parts = []
        set_parts = []
        for metric_id in missing_metrics:
            agg_expr = self._build_metric_aggregation_for_update(metric_id, schema_config)
            if agg_expr:
                select_parts.append(f"    {agg_expr}")
                set_parts.append(f"target_tbl.{metric_id} = source_agg.{metric_id}")

        if not select_parts:
            return 0

        # Build dimension SELECT for subquery
        dim_select = ', '.join([f"{col} AS {dim_id}" for dim_id, col in
                                [(d, dims_by_id[d].column_name) for d in rollup.dimensions if d in dims_by_id]])

        group_by_clause = ', '.join(dim_columns)

        update_sql = f"""
UPDATE `{target_table_path}` target_tbl
SET {', '.join(set_parts)}
FROM (
    SELECT
        {dim_select},
{','.join(select_parts)}
    FROM `{source_table_path}`
    GROUP BY {group_by_clause}
) source_agg
WHERE {' AND '.join(join_conditions)}
"""
        try:
            job = self.client.query(update_sql)
            result = job.result()
            return job.num_dml_affected_rows or 0
        except Exception as e:
            print(f"Error backfilling metrics: {e}")
            raise

    def generate_incremental_insert_sql(
        self,
        rollup: RollupDef,
        source_table_path: str,
        target_table_path: str,
        schema_config: SchemaConfig,
        missing_dates: List[str]
    ) -> str:
        """
        Generate INSERT statement for missing dates only.

        If an optimized source table exists, it will be used for better performance.

        Returns:
            SQL string for INSERT statement.
        """
        # Check for optimized source table
        actual_source_path, key_column_mapping = self._get_optimized_source_info(source_table_path)

        # Get auto-included metrics
        base_metrics, volume_calc_metrics = self.get_rollup_metric_definitions(schema_config)

        # Build dimensions lookup
        dims_by_id = {d.id: d for d in schema_config.dimensions}

        # Build SELECT parts
        select_parts = []

        # Add dimensions
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                select_parts.append(f"    {dim.column_name} AS {dim_id}")

        # Add ALL base metrics (use key_column_mapping for precomputed keys)
        for metric in base_metrics:
            agg_sql = self._build_aggregation_sql(metric, metric.id, key_column_mapping)
            select_parts.append(f"    {agg_sql}")

        # Add ALL volume calculated metrics
        # Replace CONCAT patterns with precomputed keys when available
        for calc_metric in volume_calc_metrics:
            sql_expr = calc_metric.sql_expression
            if key_column_mapping:
                sql_expr = replace_concat_with_keys(sql_expr, key_column_mapping)
            select_parts.append(f"    {sql_expr} AS {calc_metric.id}")

        # Build GROUP BY
        group_by_parts = []
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                group_by_parts.append(dim.column_name)

        # Build date filter
        date_list = ", ".join([f"'{d}'" for d in missing_dates])

        select_clause = ',\n'.join(select_parts)
        group_by_clause = ', '.join(group_by_parts)

        # Use actual_source_path which may be the optimized table
        sql = f"""INSERT INTO `{target_table_path}`
SELECT
{select_clause}
FROM `{actual_source_path}`
WHERE date IN ({date_list})
GROUP BY {group_by_clause}"""

        return sql

    # =========================================================================
    # Execution
    # =========================================================================

    def refresh_rollup(
        self,
        rollup_id: str,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str,
        incremental: bool = False,
        force: bool = False,
        use_batched: bool = True,
        batch_size: int = 7
    ) -> RollupRefreshResponse:
        """
        Refresh a rollup table in BigQuery.

        Args:
            rollup_id: ID of the rollup to refresh
            source_table_path: Full path to source table
            schema_config: Current schema configuration
            source_project_id: BigQuery project ID
            source_dataset: Default dataset
            incremental: If True, only add missing dates and metrics
            force: Force refresh even if status is 'ready'
            use_batched: If True (default), use batched inserts for full refresh
                         to leverage partition pruning on source table
            batch_size: Number of dates per batch (default: 7)

        Returns:
            RollupRefreshResponse with execution details
        """
        if not self.client:
            return RollupRefreshResponse(
                success=False,
                message="BigQuery client not available",
                rollup_id=rollup_id,
                status="error"
            )

        rollup = self.get_rollup(rollup_id)
        if not rollup:
            return RollupRefreshResponse(
                success=False,
                message=f"Rollup with ID '{rollup_id}' not found",
                rollup_id=rollup_id,
                status="error"
            )

        target_table_path = self._get_target_table_path(rollup, source_project_id, source_dataset)
        table_exists = self._table_exists(target_table_path)

        # For incremental: if table doesn't exist, fall back to full refresh
        if incremental and not table_exists:
            incremental = False

        # Handle incremental refresh
        if incremental:
            return self._refresh_rollup_incremental(
                rollup, source_table_path, target_table_path, schema_config,
                source_project_id, source_dataset
            )

        # Full refresh - use batched mode by default to leverage partition pruning
        if use_batched and 'date' in rollup.dimensions:
            # Check if refresh is needed
            if not force and rollup.status == "ready":
                return RollupRefreshResponse(
                    success=True,
                    message="Rollup is already up to date",
                    rollup_id=rollup_id,
                    status="ready"
                )
            return self._refresh_rollup_batched(
                rollup, source_table_path, target_table_path, schema_config,
                source_project_id, source_dataset, batch_size
            )

        # Legacy full refresh (CREATE OR REPLACE) - full table scan
        # Check if refresh is needed
        if not force and rollup.status == "ready":
            return RollupRefreshResponse(
                success=True,
                message="Rollup is already up to date",
                rollup_id=rollup_id,
                status="ready"
            )

        # Update status to building
        config = self.load_config()
        for r in config.rollups:
            if r.id == rollup_id:
                r.status = "building"
                r.last_refresh_error = None
                break
        self.save_config(config)

        try:
            # Generate SQL
            sql, target_table_path = self.generate_create_sql(
                rollup, source_table_path, schema_config,
                source_project_id, source_dataset
            )

            # Execute query
            start_time = datetime.utcnow()
            job = self.client.query(sql)
            result = job.result()  # Wait for completion

            end_time = datetime.utcnow()
            execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

            # Get row count from the created table
            table = self.client.get_table(target_table_path)
            row_count = table.num_rows
            size_bytes = table.num_bytes

            # Update status to ready
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup_id:
                    r.status = "ready"
                    r.last_refresh_at = datetime.utcnow().isoformat() + "Z"
                    r.last_refresh_error = None
                    r.row_count = row_count
                    r.size_bytes = size_bytes
                    break
            self.save_config(config)

            return RollupRefreshResponse(
                success=True,
                message=f"Rollup table created successfully: {target_table_path}",
                rollup_id=rollup_id,
                status="ready",
                table_path=target_table_path,
                bytes_processed=job.total_bytes_processed,
                row_count=row_count,
                execution_time_ms=execution_time_ms
            )

        except Exception as e:
            # Update status to error
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup_id:
                    r.status = "error"
                    r.last_refresh_error = str(e)
                    break
            self.save_config(config)

            return RollupRefreshResponse(
                success=False,
                message=f"Failed to create rollup table: {str(e)}",
                rollup_id=rollup_id,
                status="error"
            )

    def _refresh_rollup_incremental(
        self,
        rollup: RollupDef,
        source_table_path: str,
        target_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str
    ) -> RollupRefreshResponse:
        """
        Perform incremental refresh: add missing dates and/or metrics.

        Note: Rollups without 'date' dimension do not support incremental refresh.
        All rollups should include 'date', so this check is a safety measure.

        Returns:
            RollupRefreshResponse with incremental refresh details.
        """
        # Rollups without date dimension need full refresh
        if 'date' not in rollup.dimensions:
            return RollupRefreshResponse(
                success=False,
                message="Incremental refresh not supported for rollups without date dimension. Use full refresh.",
                rollup_id=rollup.id,
                status=rollup.status
            )

        start_time = datetime.utcnow()
        total_bytes_processed = 0
        dates_processed = []
        metrics_processed = []

        try:
            # 1. Check for missing metrics (new columns needed)
            missing_metrics = self.get_missing_metrics(target_table_path, schema_config)
            if missing_metrics:
                # Add new metric columns
                self.add_missing_metric_columns(target_table_path, missing_metrics, schema_config)

                # Backfill data for new metrics
                self._backfill_metrics(
                    target_table_path, source_table_path,
                    rollup, missing_metrics, schema_config
                )
                metrics_processed = missing_metrics

            # 2. Check for missing dates
            missing_dates = self.get_missing_dates(source_table_path, target_table_path)
            if missing_dates:
                # Generate and execute INSERT for missing dates
                sql = self.generate_incremental_insert_sql(
                    rollup, source_table_path, target_table_path,
                    schema_config, missing_dates
                )
                job = self.client.query(sql)
                job.result()  # Wait for completion
                total_bytes_processed = job.total_bytes_processed or 0
                dates_processed = missing_dates

            end_time = datetime.utcnow()
            execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

            # Check if anything was done
            if not missing_metrics and not missing_dates:
                return RollupRefreshResponse(
                    success=True,
                    message="Rollup is up to date (no new dates or metrics)",
                    rollup_id=rollup.id,
                    status="ready",
                    table_path=target_table_path,
                    execution_time_ms=execution_time_ms,
                    dates_added=0,
                    metrics_added=0
                )

            # Get updated row count
            table = self.client.get_table(target_table_path)
            row_count = table.num_rows

            # Update status
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup.id:
                    r.status = "ready"
                    r.last_refresh_at = datetime.utcnow().isoformat() + "Z"
                    r.last_refresh_error = None
                    r.row_count = row_count
                    r.size_bytes = table.num_bytes
                    break
            self.save_config(config)

            # Build message
            parts = []
            if dates_processed:
                parts.append(f"{len(dates_processed)} date(s) added")
            if metrics_processed:
                parts.append(f"{len(metrics_processed)} metric(s) added")
            message = "Incremental refresh complete: " + ", ".join(parts)

            return RollupRefreshResponse(
                success=True,
                message=message,
                rollup_id=rollup.id,
                status="ready",
                table_path=target_table_path,
                bytes_processed=total_bytes_processed,
                row_count=row_count,
                execution_time_ms=execution_time_ms,
                dates_added=len(dates_processed),
                dates_processed=dates_processed if dates_processed else None,
                metrics_added=len(metrics_processed),
                metrics_processed=metrics_processed if metrics_processed else None
            )

        except Exception as e:
            # Update status to error
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup.id:
                    r.status = "error"
                    r.last_refresh_error = str(e)
                    break
            self.save_config(config)

            return RollupRefreshResponse(
                success=False,
                message=f"Incremental refresh failed: {str(e)}",
                rollup_id=rollup.id,
                status="error"
            )

    def _refresh_rollup_batched(
        self,
        rollup: RollupDef,
        source_table_path: str,
        target_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str,
        batch_size: int = 7
    ) -> RollupRefreshResponse:
        """
        Perform full refresh using batched inserts to leverage partition pruning.

        Instead of a single CREATE TABLE AS SELECT (full table scan), this:
        1. Creates an empty table with the correct schema
        2. Gets all dates from source table
        3. Inserts data in batches (default: 1 day at a time for optimal partition pruning)

        This is more efficient when source table is partitioned by date,
        as each INSERT only scans the relevant partition(s).

        Args:
            rollup: The rollup definition
            source_table_path: Full path to source table
            target_table_path: Full path to target rollup table
            schema_config: Current schema configuration
            source_project_id: BigQuery project ID
            source_dataset: Default dataset
            batch_size: Number of dates to process per batch (default: 7)

        Returns:
            RollupRefreshResponse with execution details
        """
        start_time = datetime.utcnow()
        total_bytes_processed = 0
        total_dates_processed = 0

        try:
            # Update status to building
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup.id:
                    r.status = "building"
                    r.last_refresh_error = None
                    break
            self.save_config(config)

            # 1. Drop existing table if it exists
            try:
                self.client.delete_table(target_table_path, not_found_ok=True)
            except Exception as e:
                print(f"Warning: Could not delete existing table: {e}")

            # 2. Create empty table with correct schema
            create_ddl = self._generate_create_table_ddl(
                rollup, target_table_path, schema_config
            )
            job = self.client.query(create_ddl)
            job.result()

            # 3. Get all dates from source
            all_dates = self.get_all_source_dates(source_table_path)
            if not all_dates:
                return RollupRefreshResponse(
                    success=False,
                    message="No dates found in source table",
                    rollup_id=rollup.id,
                    status="error"
                )

            # 4. Insert in batches
            for i in range(0, len(all_dates), batch_size):
                batch_dates = all_dates[i:i + batch_size]

                # Generate INSERT for this batch
                sql = self.generate_incremental_insert_sql(
                    rollup, source_table_path, target_table_path,
                    schema_config, batch_dates
                )

                job = self.client.query(sql)
                job.result()
                total_bytes_processed += job.total_bytes_processed or 0
                total_dates_processed += len(batch_dates)

            end_time = datetime.utcnow()
            execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

            # Get final table stats
            table = self.client.get_table(target_table_path)
            row_count = table.num_rows
            size_bytes = table.num_bytes

            # Update status to ready
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup.id:
                    r.status = "ready"
                    r.last_refresh_at = datetime.utcnow().isoformat() + "Z"
                    r.last_refresh_error = None
                    r.row_count = row_count
                    r.size_bytes = size_bytes
                    break
            self.save_config(config)

            return RollupRefreshResponse(
                success=True,
                message=f"Batched refresh complete: {total_dates_processed} dates in {(len(all_dates) + batch_size - 1) // batch_size} batches",
                rollup_id=rollup.id,
                status="ready",
                table_path=target_table_path,
                bytes_processed=total_bytes_processed,
                row_count=row_count,
                execution_time_ms=execution_time_ms,
                dates_added=total_dates_processed
            )

        except Exception as e:
            # Update status to error
            config = self.load_config()
            for r in config.rollups:
                if r.id == rollup.id:
                    r.status = "error"
                    r.last_refresh_error = str(e)
                    break
            self.save_config(config)

            return RollupRefreshResponse(
                success=False,
                message=f"Batched refresh failed: {str(e)}",
                rollup_id=rollup.id,
                status="error"
            )

    def _generate_create_table_ddl(
        self,
        rollup: RollupDef,
        target_table_path: str,
        schema_config: SchemaConfig
    ) -> str:
        """
        Generate CREATE TABLE DDL (empty table with schema) for a rollup.

        Returns:
            SQL DDL statement to create an empty partitioned/clustered table.
        """
        # Get auto-included metrics
        base_metrics, volume_calc_metrics = self.get_rollup_metric_definitions(schema_config)

        # Build dimensions lookup
        dims_by_id = {d.id: d for d in schema_config.dimensions}

        # Build column definitions
        column_defs = []

        # Add dimension columns
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                # Map data type to BigQuery type
                bq_type = self._get_bq_type(dim.data_type)
                column_defs.append(f"    {dim_id} {bq_type}")

        # Add base metric columns (all numeric)
        for metric in base_metrics:
            bq_type = "FLOAT64" if metric.data_type in ["FLOAT", "FLOAT64"] else "INT64"
            column_defs.append(f"    {metric.id} {bq_type}")

        # Add volume calculated metric columns (all numeric)
        for calc_metric in volume_calc_metrics:
            # Volume metrics are typically counts, so INT64
            column_defs.append(f"    {calc_metric.id} INT64")

        columns_clause = ',\n'.join(column_defs)

        # Determine clustering columns (non-date dimensions, up to 4)
        non_date_dims = [d for d in rollup.dimensions if d != 'date']
        cluster_columns = non_date_dims[:4]
        cluster_clause = f"\nCLUSTER BY {', '.join(cluster_columns)}" if cluster_columns else ""

        ddl = f"""CREATE TABLE `{target_table_path}` (
{columns_clause}
)
PARTITION BY date{cluster_clause}"""

        return ddl

    def _get_bq_type(self, data_type: str) -> str:
        """Map internal data type to BigQuery type."""
        type_map = {
            "STRING": "STRING",
            "INTEGER": "INT64",
            "INT64": "INT64",
            "FLOAT": "FLOAT64",
            "FLOAT64": "FLOAT64",
            "BOOLEAN": "BOOL",
            "BOOL": "BOOL",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }
        return type_map.get(data_type.upper(), "STRING")

    def refresh_all(
        self,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str,
        only_pending_or_stale: bool = True
    ) -> List[RollupRefreshResponse]:
        """
        Refresh all rollups (or only pending/stale ones).

        Returns:
            List of RollupRefreshResponse for each rollup
        """
        config = self.load_config()
        results = []

        for rollup in config.rollups:
            if only_pending_or_stale and rollup.status not in ("pending", "stale", "error"):
                continue

            result = self.refresh_rollup(
                rollup.id,
                source_table_path,
                schema_config,
                source_project_id,
                source_dataset,
                force=True
            )
            results.append(result)

        return results

    def check_table_exists(
        self,
        rollup_id: str,
        source_project_id: str,
        source_dataset: str
    ) -> bool:
        """Check if a rollup table exists in BigQuery."""
        if not self.client:
            return False

        rollup = self.get_rollup(rollup_id)
        if not rollup:
            return False

        config = self.load_config()
        target_dataset = rollup.target_dataset or config.default_target_dataset or source_dataset
        table_path = f"{source_project_id}.{target_dataset}.{rollup.target_table_name}"

        try:
            self.client.get_table(table_path)
            return True
        except NotFound:
            return False
