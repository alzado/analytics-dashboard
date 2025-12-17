"""
Rollup service for managing pre-aggregated tables in BigQuery.
Django port of the FastAPI rollup_service.py.

Key design principles:
- Metrics are AUTO-INCLUDED from schema (all base metrics + volume calculated metrics)
- Supports incremental refresh: only insert missing dates AND add missing metric columns
- Conversion metrics are NEVER stored in rollups (calculated in Python after query)
"""
import re
import time
import logging
from typing import List, Dict, Optional, Tuple, TYPE_CHECKING
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from django.utils import timezone

from .models import Rollup, RollupStatus, RollupConfig
from apps.schemas.models import SchemaConfig, CalculatedMetric, Dimension, OptimizedSourceConfig

if TYPE_CHECKING:
    from apps.tables.models import BigQueryTable

logger = logging.getLogger(__name__)


def generate_key_column_name(columns: List[str]) -> str:
    """Generate deterministic key column name from source columns."""
    sorted_cols = sorted([c.strip().lower() for c in columns])
    return "_key_" + "_".join(sorted_cols)


def replace_concat_with_keys(sql_expression: str, key_column_mapping: Dict[str, str]) -> str:
    """Replace CONCAT(...) patterns in SQL expression with precomputed key columns."""
    if not key_column_mapping or 'CONCAT' not in sql_expression.upper():
        return sql_expression

    result = sql_expression
    upper_sql = result.upper()
    replacements = []

    start = 0
    while True:
        idx = upper_sql.find('CONCAT', start)
        if idx == -1:
            break

        paren_start = result.find('(', idx)
        if paren_start == -1:
            break

        depth = 1
        pos = paren_start + 1
        while pos < len(result) and depth > 0:
            if result[pos] == '(':
                depth += 1
            elif result[pos] == ')':
                depth -= 1
            pos += 1

        if depth == 0:
            concat_content = result[paren_start + 1:pos - 1].strip()
            coalesce_pattern = r'COALESCE\s*\(\s*(?:CAST\s*\(\s*)?(\w+)'
            columns = re.findall(coalesce_pattern, concat_content, re.IGNORECASE)

            if len(columns) >= 2:
                sorted_cols = sorted([c.lower() for c in columns])
                lookup_key = ",".join(sorted_cols)

                if lookup_key in key_column_mapping:
                    key_col = key_column_mapping[lookup_key]
                    replacements.append((idx, pos, key_col))

        start = pos

    for start_pos, end_pos, replacement in reversed(replacements):
        result = result[:start_pos] + replacement + result[end_pos:]

    return result


class RollupService:
    """Service for managing pre-aggregated rollup tables in BigQuery."""

    def __init__(self, bigquery_client: bigquery.Client, bigquery_table: 'BigQueryTable'):
        self.client = bigquery_client
        self.bigquery_table = bigquery_table

    def _get_optimized_source_info(self) -> Tuple[str, Optional[Dict[str, str]]]:
        """Get optimized source table path and key column mapping if available."""
        try:
            config = self.bigquery_table.optimized_source_config
            if config and config.status == 'ready':
                optimized_path = config.optimized_table_path
                if optimized_path:
                    key_mapping = {}
                    for key in config.composite_key_mappings:
                        sorted_cols = sorted(key['source_columns'])
                        lookup = ",".join(sorted_cols)
                        key_mapping[lookup] = key['key_column_name']
                    return optimized_path, key_mapping
        except OptimizedSourceConfig.DoesNotExist:
            pass
        return self.bigquery_table.full_table_path, None

    def get_volume_metrics(self, schema_config: SchemaConfig) -> List[CalculatedMetric]:
        """Get all volume-category metrics from the schema."""
        return list(schema_config.calculated_metrics.filter(category='volume'))

    def get_all_dimensions(self, schema_config: SchemaConfig) -> Dict[str, Dimension]:
        """Get all dimensions as a dict keyed by dimension_id."""
        return {d.dimension_id: d for d in schema_config.dimensions.all()}

    def _build_aggregation_sql(
        self,
        metric: CalculatedMetric,
        key_column_mapping: Optional[Dict[str, str]] = None
    ) -> str:
        """Build aggregation SQL for a calculated metric."""
        sql_expr = metric.sql_expression
        if key_column_mapping:
            sql_expr = replace_concat_with_keys(sql_expr, key_column_mapping)
        return f"{sql_expr} AS {metric.metric_id}"

    def generate_create_sql(
        self,
        rollup: Rollup,
        schema_config: SchemaConfig
    ) -> Tuple[str, str]:
        """Generate CREATE TABLE AS SELECT SQL for a rollup."""
        # Check for optimized source table
        actual_source_path, key_column_mapping = self._get_optimized_source_info()

        target_path = rollup.full_rollup_path

        # Get volume metrics from schema
        volume_metrics = self.get_volume_metrics(schema_config)
        dims_by_id = self.get_all_dimensions(schema_config)

        # Build SELECT parts
        select_parts = []

        # Add dimensions
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                select_parts.append(f"    {dim.column_name} AS {dim_id}")

        # Add volume metrics
        for metric in volume_metrics:
            agg_sql = self._build_aggregation_sql(metric, key_column_mapping)
            select_parts.append(f"    {agg_sql}")

        # If no volume metrics, just add a row count
        if not volume_metrics:
            select_parts.append("    COUNT(*) AS row_count")

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
        cluster_columns = non_date_dims[:4]
        cluster_clause = f"\nCLUSTER BY {', '.join(cluster_columns)}" if cluster_columns else ""

        # All rollups include 'date' dimension, so partition by date
        sql = f"""CREATE OR REPLACE TABLE `{target_path}`
PARTITION BY date{cluster_clause}
AS
SELECT
{select_clause}
FROM `{actual_source_path}`
GROUP BY {group_by_clause}"""

        return sql, target_path

    def generate_incremental_insert_sql(
        self,
        rollup: Rollup,
        schema_config: SchemaConfig,
        missing_dates: List[str]
    ) -> str:
        """Generate INSERT statement for missing dates only."""
        actual_source_path, key_column_mapping = self._get_optimized_source_info()
        target_path = rollup.full_rollup_path

        volume_metrics = self.get_volume_metrics(schema_config)
        dims_by_id = self.get_all_dimensions(schema_config)

        # Build SELECT parts
        select_parts = []

        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                select_parts.append(f"    {dim.column_name} AS {dim_id}")

        for metric in volume_metrics:
            agg_sql = self._build_aggregation_sql(metric, key_column_mapping)
            select_parts.append(f"    {agg_sql}")

        if not volume_metrics:
            select_parts.append("    COUNT(*) AS row_count")

        group_by_parts = []
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                group_by_parts.append(dim.column_name)

        date_list = ", ".join([f"'{d}'" for d in missing_dates])

        select_clause = ',\n'.join(select_parts)
        group_by_clause = ', '.join(group_by_parts)

        sql = f"""INSERT INTO `{target_path}`
SELECT
{select_clause}
FROM `{actual_source_path}`
WHERE date IN ({date_list})
GROUP BY {group_by_clause}"""

        return sql

    def preview_sql(self, rollup: Rollup) -> Dict:
        """Preview the SQL that would be generated for a rollup."""
        try:
            schema_config = self.bigquery_table.schema_config
        except SchemaConfig.DoesNotExist:
            return {
                'rollup_id': str(rollup.id),
                'sql': '-- No schema configuration found',
                'target_table_path': rollup.full_rollup_path
            }

        create_sql, target_path = self.generate_create_sql(rollup, schema_config)

        # Generate a sample incremental SQL
        sample_dates = ['2025-01-01', '2025-01-02']
        incremental_sql = self.generate_incremental_insert_sql(rollup, schema_config, sample_dates)

        combined_sql = f"""-- CREATE TABLE SQL (Full Refresh)
{create_sql}

-- INCREMENTAL INSERT SQL (for new dates only)
{incremental_sql}"""

        return {
            'rollup_id': str(rollup.id),
            'sql': combined_sql,
            'target_table_path': target_path
        }

    def get_missing_dates(
        self,
        source_table_path: str,
        target_table_path: str
    ) -> List[str]:
        """Find dates that exist in source table but not in rollup table."""
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
            logger.warning(f"Error getting missing dates: {e}")
            return []

    def get_all_source_dates(self, source_table_path: str) -> List[str]:
        """Get all distinct dates from the source table."""
        query = f"""
        SELECT DISTINCT CAST(date AS STRING) as date_str
        FROM `{source_table_path}`
        ORDER BY date_str
        """
        try:
            result = self.client.query(query).result()
            return [row.date_str for row in result]
        except Exception as e:
            logger.warning(f"Error getting source dates: {e}")
            return []

    def _table_exists(self, table_path: str) -> bool:
        """Check if a BigQuery table exists."""
        try:
            self.client.get_table(table_path)
            return True
        except NotFound:
            return False

    def refresh_rollup(
        self,
        rollup: Rollup,
        incremental: bool = True,
        force: bool = False,
        batch_size: int = 7
    ) -> Dict:
        """
        Refresh a rollup table in BigQuery.

        Args:
            rollup: The Rollup model instance
            incremental: If True, only add missing dates
            force: Force refresh even if already up-to-date
            batch_size: Number of dates per batch for batched refresh

        Returns:
            Dict with success status, message, and stats
        """
        try:
            schema_config = self.bigquery_table.schema_config
        except SchemaConfig.DoesNotExist:
            rollup.mark_error("No schema configuration found")
            return {
                'success': False,
                'message': "No schema configuration found for this table",
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }

        rollup.mark_refreshing()
        target_path = rollup.full_rollup_path
        table_exists = self._table_exists(target_path)

        # If table doesn't exist, do full refresh
        if not table_exists:
            incremental = False

        try:
            if incremental:
                return self._refresh_incremental(rollup, schema_config, target_path)
            else:
                return self._refresh_batched(rollup, schema_config, target_path, batch_size)

        except Exception as e:
            logger.exception(f"Rollup refresh failed for {rollup.name}: {e}")
            rollup.mark_error(str(e))
            return {
                'success': False,
                'message': f"Refresh failed: {str(e)}",
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }

    def _refresh_incremental(
        self,
        rollup: Rollup,
        schema_config: SchemaConfig,
        target_path: str
    ) -> Dict:
        """Perform incremental refresh: add missing dates only."""
        actual_source_path, _ = self._get_optimized_source_info()

        # Get missing dates
        missing_dates = self.get_missing_dates(actual_source_path, target_path)

        if not missing_dates:
            rollup.mark_ready(
                row_count=rollup.row_count,
                size_bytes=rollup.size_bytes,
                duration_seconds=0
            )
            return {
                'success': True,
                'message': "Rollup is already up to date",
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }

        # Generate and execute INSERT
        sql = self.generate_incremental_insert_sql(rollup, schema_config, missing_dates)

        start_time = time.time()
        job = self.client.query(sql)
        job.result()
        duration_seconds = int(time.time() - start_time)
        bytes_processed = job.total_bytes_processed or 0

        # Get updated row count
        count_result = list(self.client.query(
            f"SELECT COUNT(*) as cnt FROM `{target_path}`"
        ).result())[0]
        row_count = count_result.cnt

        rollup.mark_ready(
            row_count=row_count,
            size_bytes=bytes_processed,
            duration_seconds=duration_seconds
        )

        return {
            'success': True,
            'message': f"Incremental refresh complete: {len(missing_dates)} date(s) added",
            'rollup_id': str(rollup.id),
            'status': rollup.status,
            'row_count': row_count,
            'bytes_processed': bytes_processed,
            'duration_seconds': duration_seconds,
            'dates_added': len(missing_dates)
        }

    def _refresh_batched(
        self,
        rollup: Rollup,
        schema_config: SchemaConfig,
        target_path: str,
        batch_size: int = 7
    ) -> Dict:
        """
        Perform full refresh using batched inserts to leverage partition pruning.

        Instead of a single CREATE TABLE AS SELECT (full table scan), this:
        1. Creates an empty table with the correct schema
        2. Gets all dates from source table
        3. Inserts data in batches
        """
        start_time = time.time()
        total_bytes_processed = 0

        # Drop existing table
        try:
            self.client.delete_table(target_path, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Could not delete existing table: {e}")

        # Create empty table with correct schema
        create_ddl = self._generate_create_table_ddl(rollup, schema_config, target_path)
        job = self.client.query(create_ddl)
        job.result()

        # Get all dates from source
        actual_source_path, _ = self._get_optimized_source_info()
        all_dates = self.get_all_source_dates(actual_source_path)

        if not all_dates:
            rollup.mark_error("No dates found in source table")
            return {
                'success': False,
                'message': "No dates found in source table",
                'rollup_id': str(rollup.id),
                'status': rollup.status
            }

        # Insert in batches
        for i in range(0, len(all_dates), batch_size):
            batch_dates = all_dates[i:i + batch_size]
            sql = self.generate_incremental_insert_sql(rollup, schema_config, batch_dates)
            job = self.client.query(sql)
            job.result()
            total_bytes_processed += job.total_bytes_processed or 0

        duration_seconds = int(time.time() - start_time)

        # Get final table stats
        table = self.client.get_table(target_path)
        row_count = table.num_rows
        size_bytes = table.num_bytes

        # Get date range
        date_result = list(self.client.query(
            f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM `{target_path}`"
        ).result())[0]

        rollup.min_date = date_result.min_date
        rollup.max_date = date_result.max_date
        rollup.mark_ready(
            row_count=row_count,
            size_bytes=size_bytes,
            duration_seconds=duration_seconds
        )

        num_batches = (len(all_dates) + batch_size - 1) // batch_size
        return {
            'success': True,
            'message': f"Batched refresh complete: {len(all_dates)} dates in {num_batches} batches",
            'rollup_id': str(rollup.id),
            'status': rollup.status,
            'row_count': row_count,
            'bytes_processed': total_bytes_processed,
            'duration_seconds': duration_seconds,
            'dates_added': len(all_dates)
        }

    def _generate_create_table_ddl(
        self,
        rollup: Rollup,
        schema_config: SchemaConfig,
        target_path: str
    ) -> str:
        """Generate CREATE TABLE DDL (empty table with schema) for a rollup."""
        volume_metrics = self.get_volume_metrics(schema_config)
        dims_by_id = self.get_all_dimensions(schema_config)

        column_defs = []

        # Add dimension columns
        for dim_id in rollup.dimensions:
            dim = dims_by_id.get(dim_id)
            if dim:
                bq_type = self._get_bq_type(dim.data_type)
                column_defs.append(f"    {dim_id} {bq_type}")

        # Add volume metric columns
        for metric in volume_metrics:
            column_defs.append(f"    {metric.metric_id} INT64")

        if not volume_metrics:
            column_defs.append("    row_count INT64")

        columns_clause = ',\n'.join(column_defs)

        non_date_dims = [d for d in rollup.dimensions if d != 'date']
        cluster_columns = non_date_dims[:4]
        cluster_clause = f"\nCLUSTER BY {', '.join(cluster_columns)}" if cluster_columns else ""

        ddl = f"""CREATE TABLE `{target_path}` (
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
        return type_map.get(data_type.upper() if data_type else "STRING", "STRING")

    def refresh_all_rollups(
        self,
        incremental: bool = True,
        only_pending_or_stale: bool = True
    ) -> Dict:
        """Refresh all rollups for this table."""
        rollups = self.bigquery_table.rollups.all()

        if only_pending_or_stale:
            rollups = rollups.filter(
                status__in=[RollupStatus.PENDING, RollupStatus.STALE, RollupStatus.ERROR]
            )

        results = []
        successful = 0
        failed = 0

        for rollup in rollups:
            result = self.refresh_rollup(rollup, incremental=incremental)
            results.append(result)
            if result['success']:
                successful += 1
            else:
                failed += 1

        return {
            'success': failed == 0,
            'total': len(results),
            'successful': successful,
            'failed': failed,
            'results': results
        }

    def delete_rollup(self, rollup: Rollup, drop_table: bool = False) -> Dict:
        """Delete a rollup and optionally the BigQuery table."""
        try:
            if drop_table:
                target_path = rollup.full_rollup_path
                try:
                    self.client.delete_table(target_path)
                    logger.info(f"Dropped rollup table: {target_path}")
                except NotFound:
                    pass
                except Exception as e:
                    return {
                        'success': False,
                        'message': f"Failed to drop table: {str(e)}"
                    }

            rollup.delete()
            return {
                'success': True,
                'message': "Rollup deleted successfully"
            }

        except Exception as e:
            logger.exception(f"Failed to delete rollup: {e}")
            return {
                'success': False,
                'message': f"Failed to delete: {str(e)}"
            }
