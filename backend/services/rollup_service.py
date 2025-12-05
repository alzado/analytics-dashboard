"""
Rollup service for managing pre-aggregated tables.
Handles CRUD operations, SQL generation, and rollup table creation in BigQuery.
"""
import os
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from models.schemas import (
    RollupDef, RollupConfig, RollupMetricDef, RollupCreate, RollupUpdate,
    RollupRefreshResponse, RollupListResponse, RollupPreviewSqlResponse,
    BaseMetric, SchemaConfig
)
from config import ROLLUPS_DIR


class RollupService:
    """Service for managing pre-aggregated rollup tables."""

    def __init__(self, bigquery_client: Optional[bigquery.Client], table_id: str):
        self.client = bigquery_client
        self.table_id = table_id
        self._config_cache: Optional[RollupConfig] = None

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
        """Generate rollup ID from dimensions."""
        sorted_dims = sorted(dimensions)
        return "rollup_" + "_".join(sorted_dims)

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
            default_target_dataset=config.default_target_dataset
        )

    def get_rollup(self, rollup_id: str) -> Optional[RollupDef]:
        """Get a specific rollup by ID."""
        config = self.load_config()
        for rollup in config.rollups:
            if rollup.id == rollup_id:
                return rollup
        return None

    def create_rollup(self, data: RollupCreate, schema_config: SchemaConfig) -> RollupDef:
        """Create a new rollup definition."""
        config = self.load_config()

        # Generate ID if not provided
        rollup_id = data.id or self._generate_rollup_id(data.dimensions)

        # Check for duplicate ID
        for existing in config.rollups:
            if existing.id == rollup_id:
                raise ValueError(f"Rollup with ID '{rollup_id}' already exists")

        # Validate dimensions exist in schema
        valid_dims = {d.id for d in schema_config.dimensions}
        for dim in data.dimensions:
            if dim not in valid_dims:
                raise ValueError(f"Unknown dimension: {dim}")

        # Validate metrics exist in schema
        valid_metrics = {m.id for m in schema_config.base_metrics}
        for metric_def in data.metrics:
            if metric_def.metric_id not in valid_metrics:
                raise ValueError(f"Unknown metric: {metric_def.metric_id}")

        now = datetime.utcnow().isoformat() + "Z"

        # Generate table name if not provided
        table_name = data.target_table_name or self._generate_table_name(data.dimensions)

        rollup = RollupDef(
            id=rollup_id,
            display_name=data.display_name,
            description=data.description,
            dimensions=data.dimensions,
            metrics=data.metrics,
            target_dataset=data.target_dataset,
            target_table_name=table_name,
            status="pending",
            created_at=now,
            updated_at=now
        )

        config.rollups.append(rollup)
        self.save_config(config)

        return rollup

    def update_rollup(self, rollup_id: str, data: RollupUpdate, schema_config: SchemaConfig) -> RollupDef:
        """Update an existing rollup definition."""
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
        if data.metrics is not None:
            # Validate metrics
            valid_metrics = {m.id for m in schema_config.base_metrics}
            for metric_def in data.metrics:
                if metric_def.metric_id not in valid_metrics:
                    raise ValueError(f"Unknown metric: {metric_def.metric_id}")
            rollup.metrics = data.metrics
            # Mark as stale since metrics changed
            rollup.status = "stale"
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

    def set_default_dataset(self, dataset: Optional[str]) -> RollupConfig:
        """Set the default target dataset for rollups."""
        config = self.load_config()
        config.default_target_dataset = dataset
        self.save_config(config)
        return config

    # =========================================================================
    # SQL Generation
    # =========================================================================

    def _build_aggregation_sql(self, metric: BaseMetric, alias: str) -> str:
        """Build aggregation SQL for a base metric."""
        col = metric.column_name
        agg = metric.aggregation

        if agg == "COUNT_DISTINCT":
            # Handle multi-column COUNT DISTINCT
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]
                concat_parts = ', '.join([f"COALESCE(CAST({c} AS STRING), '')" for c in cols])
                return f"COUNT(DISTINCT CONCAT({concat_parts})) AS {alias}"
            return f"COUNT(DISTINCT {col}) AS {alias}"
        elif agg == "APPROX_COUNT_DISTINCT":
            if ',' in col:
                cols = [c.strip() for c in col.split(',')]
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

        Returns:
            Tuple of (sql, target_table_path)
        """
        config = self.load_config()

        # Build target table path
        target_dataset = rollup.target_dataset or config.default_target_dataset or source_dataset
        target_table_path = f"{source_project_id}.{target_dataset}.{rollup.target_table_name}"

        # Build metrics lookup
        metrics_by_id = {m.id: m for m in schema_config.base_metrics}

        # Build dimensions lookup
        dims_by_id = {d.id: d for d in schema_config.dimensions}

        # Build SELECT parts
        select_parts = []

        # Add dimensions
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                select_parts.append(f"    {dim.column_name} AS {dim_id}")

        # Add metrics
        for metric_def in rollup.metrics:
            metric = metrics_by_id.get(metric_def.metric_id)
            if not metric:
                continue

            # Regular aggregation
            agg_sql = self._build_aggregation_sql(metric, metric_def.metric_id)
            select_parts.append(f"    {agg_sql}")

            # Conditional aggregation if requested
            if metric_def.include_conditional and metric_def.flag_column:
                flagged_alias = f"{metric_def.metric_id}_flagged"
                cond_sql = self._build_conditional_aggregation_sql(
                    metric, metric_def.flag_column, flagged_alias
                )
                select_parts.append(f"    {cond_sql}")

        # Build GROUP BY
        group_by_parts = []
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                group_by_parts.append(dim.column_name)

        select_clause = ',\n'.join(select_parts)
        group_by_clause = ', '.join(group_by_parts)
        sql = f"""CREATE OR REPLACE TABLE `{target_table_path}` AS
SELECT
{select_clause}
FROM `{source_table_path}`
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
    # Execution
    # =========================================================================

    def refresh_rollup(
        self,
        rollup_id: str,
        source_table_path: str,
        schema_config: SchemaConfig,
        source_project_id: str,
        source_dataset: str,
        force: bool = False
    ) -> RollupRefreshResponse:
        """
        Refresh (rebuild) a rollup table in BigQuery.

        Args:
            rollup_id: ID of the rollup to refresh
            source_table_path: Full path to source table
            schema_config: Current schema configuration
            source_project_id: BigQuery project ID
            source_dataset: Default dataset
            force: Force refresh even if status is 'ready'

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
