"""
Optimized Source Service for creating source tables with precomputed composite keys.

Django implementation of the FastAPI OptimizedSourceService.

This service creates an intermediate "optimized source table" that:
1. Precomputes all composite key columns (CONCAT(col1, col2, ...)) once
2. Adds partitioning by date
3. Adds clustering by high-cardinality dimensions

Rollups can then query this optimized table with simple COUNT(DISTINCT _key_column)
instead of computing CONCAT+COALESCE at runtime.
"""
import re
import time
import logging
from typing import Optional, List, Dict, Set, Tuple, TYPE_CHECKING
from datetime import datetime

from google.cloud import bigquery
from django.utils import timezone

from apps.schemas.models import (
    OptimizedSourceConfig, OptimizedSourceStatus,
    SchemaConfig, CalculatedMetric, Dimension
)

if TYPE_CHECKING:
    from apps.tables.models import BigQueryTable

logger = logging.getLogger(__name__)


class OptimizedSourceService:
    """Service for managing optimized source tables with precomputed composite keys."""

    def __init__(self, bigquery_client: bigquery.Client, bigquery_table: 'BigQueryTable'):
        self.client = bigquery_client
        self.bigquery_table = bigquery_table
        self.table_id = str(bigquery_table.id)

    @staticmethod
    def generate_key_column_name(columns: List[str]) -> str:
        """
        Generate deterministic key column name from source columns.

        Sorts columns alphabetically to ensure consistent naming regardless
        of order in metric definition.

        Example: ["visit_id", "query"] -> "_key_query_visit_id"
        """
        sorted_cols = sorted([c.strip().lower() for c in columns])
        return "_key_" + "_".join(sorted_cols)

    @staticmethod
    def _extract_concat_columns_from_sql(sql_expression: str) -> List[List[str]]:
        """
        Extract column names from CONCAT patterns in SQL expression.

        Parses patterns like:
        - CONCAT ( COALESCE ( visit_id , '' ) , COALESCE ( query , '' ) )
        - CONCAT ( COALESCE ( CAST ( visit_id AS STRING ) , '' ) , ... )

        Returns list of column lists (one list per CONCAT found).
        """
        results = []

        # Use balanced parentheses matching to extract CONCAT content
        upper_sql = sql_expression.upper()
        start = 0
        while True:
            idx = upper_sql.find('CONCAT', start)
            if idx == -1:
                break

            # Find the opening paren after CONCAT
            paren_start = sql_expression.find('(', idx)
            if paren_start == -1:
                break

            # Count balanced parens to find the closing one
            depth = 1
            pos = paren_start + 1
            while pos < len(sql_expression) and depth > 0:
                if sql_expression[pos] == '(':
                    depth += 1
                elif sql_expression[pos] == ')':
                    depth -= 1
                pos += 1

            if depth == 0:
                concat_content = sql_expression[paren_start + 1:pos - 1].strip()

                # Extract columns from COALESCE patterns within this CONCAT
                coalesce_pattern = r'COALESCE\s*\(\s*(?:CAST\s*\(\s*)?(\w+)'
                column_matches = re.findall(coalesce_pattern, concat_content, re.IGNORECASE)

                if len(column_matches) >= 2:  # Need at least 2 columns for a composite key
                    results.append(column_matches)

            start = pos

        return results

    def analyze_schema_for_composite_keys(
        self,
        schema_config: SchemaConfig
    ) -> List[Dict]:
        """
        Scan schema for metrics using multi-column COUNT_DISTINCT.

        Returns list of composite key mappings for each unique column combination.
        """
        # Track unique column combinations: key_name -> (source_columns, set of metric_ids)
        column_combinations: Dict[str, Tuple[List[str], Set[str]]] = {}

        # Check calculated_metrics for CONCAT patterns in sql_expression
        for metric in schema_config.calculated_metrics.all():
            sql_expr = metric.sql_expression or ''

            # Skip if no CONCAT in expression
            if 'CONCAT' not in sql_expr.upper():
                continue

            # Extract all CONCAT column combinations from this metric
            concat_columns_list = self._extract_concat_columns_from_sql(sql_expr)

            for columns in concat_columns_list:
                key_name = self.generate_key_column_name(columns)

                if key_name not in column_combinations:
                    column_combinations[key_name] = (columns, set())
                column_combinations[key_name][1].add(metric.metric_id)

        # Build mappings
        mappings = []
        for key_name, (source_columns, metric_ids) in column_combinations.items():
            mappings.append({
                'key_column_name': key_name,
                'source_columns': sorted(source_columns),
                'metric_ids': list(metric_ids)
            })

        return mappings

    def auto_detect_clustering_columns(
        self,
        schema_config: SchemaConfig,
        source_table_path: str,
        max_columns: int = 4
    ) -> List[str]:
        """
        Auto-detect high-cardinality dimensions for clustering.

        Strategy:
        1. Get groupable dimensions from schema (excluding date)
        2. Query approximate distinct counts for each
        3. Select top N by cardinality
        """
        # Get groupable dimensions (exclude date - it's the partition column)
        groupable_dims = list(
            schema_config.dimensions.filter(is_groupable=True)
            .exclude(column_name__iexact='date')
        )

        if not groupable_dims:
            return []

        # Build query to get cardinality of each dimension
        cardinality_parts = []
        for dim in groupable_dims:
            cardinality_parts.append(
                f"APPROX_COUNT_DISTINCT({dim.column_name}) AS {dim.dimension_id}_cardinality"
            )

        query = f"""
        SELECT {', '.join(cardinality_parts)}
        FROM `{source_table_path}`
        """

        try:
            result = self.client.query(query).result()
            row = list(result)[0]

            # Build list of (column_name, cardinality)
            dim_cardinalities = []
            for dim in groupable_dims:
                cardinality = getattr(row, f"{dim.dimension_id}_cardinality", 0)
                dim_cardinalities.append((dim.column_name, cardinality))

            # Sort by cardinality (descending) and take top N
            dim_cardinalities.sort(key=lambda x: x[1], reverse=True)
            return [d[0] for d in dim_cardinalities[:max_columns]]

        except Exception as e:
            logger.warning(f"Failed to auto-detect clustering columns: {e}")
            # Fallback: return first few groupable dimensions
            return [d.column_name for d in groupable_dims[:max_columns]]

    def _build_key_select_expression(self, columns: List[str]) -> str:
        """Build SQL expression for a composite key column."""
        parts = [f"COALESCE(CAST({col} AS STRING), '')" for col in columns]
        return f"CONCAT({', '.join(parts)})"

    def generate_create_sql(
        self,
        source_table_path: str,
        config: OptimizedSourceConfig
    ) -> str:
        """Generate CREATE TABLE SQL for optimized source table."""
        target_path = config.optimized_table_path

        # Build composite key SELECT expressions
        key_expressions = []
        for mapping in config.composite_key_mappings:
            expr = self._build_key_select_expression(mapping['source_columns'])
            key_expressions.append(f"    {expr} AS {mapping['key_column_name']}")

        # Build clustering clause
        cluster_clause = ""
        if config.clustering and config.clustering.get('columns'):
            cluster_clause = f"\nCLUSTER BY {', '.join(config.clustering['columns'])}"

        # Build SQL
        key_select = ""
        if key_expressions:
            key_select = ",\n" + ",\n".join(key_expressions)

        sql = f"""CREATE OR REPLACE TABLE `{target_path}`
PARTITION BY {config.partition_column}{cluster_clause}
AS
SELECT
    *{key_select}
FROM `{source_table_path}`"""

        return sql

    def generate_incremental_insert_sql(
        self,
        source_table_path: str,
        config: OptimizedSourceConfig,
        missing_dates: List[str]
    ) -> str:
        """Generate INSERT SQL for new dates only."""
        target_path = config.optimized_table_path

        # Build composite key SELECT expressions
        key_expressions = []
        for mapping in config.composite_key_mappings:
            expr = self._build_key_select_expression(mapping['source_columns'])
            key_expressions.append(f"    {expr} AS {mapping['key_column_name']}")

        # Format dates
        date_list = ", ".join([f"'{d}'" for d in missing_dates])

        # Build SQL
        key_select = ""
        if key_expressions:
            key_select = ",\n" + ",\n".join(key_expressions)

        sql = f"""INSERT INTO `{target_path}`
SELECT
    *{key_select}
FROM `{source_table_path}`
WHERE date IN ({date_list})"""

        return sql

    def check_staleness(
        self,
        source_table_path: str,
        schema_config: SchemaConfig,
        config: OptimizedSourceConfig
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Check if optimized source is stale.

        Returns:
            Tuple of (is_stale, stale_reasons, missing_keys)
        """
        stale_reasons = []
        missing_keys = []

        # Check for new composite keys in schema
        current_keys = self.analyze_schema_for_composite_keys(schema_config)
        current_key_names = {k['key_column_name'] for k in current_keys}
        existing_key_names = {k['key_column_name'] for k in config.composite_key_mappings}

        new_keys = current_key_names - existing_key_names
        if new_keys:
            stale_reasons.append(f"New composite keys in schema: {new_keys}")
            missing_keys.extend(new_keys)

        # Check for new dates in source table
        optimized_path = config.optimized_table_path
        if optimized_path:
            try:
                query = f"""
                SELECT
                    (SELECT MAX(date) FROM `{source_table_path}`) as source_max,
                    (SELECT MAX(date) FROM `{optimized_path}`) as optimized_max
                """
                result = list(self.client.query(query).result())[0]

                if result.source_max and result.optimized_max:
                    if result.source_max > result.optimized_max:
                        stale_reasons.append(
                            f"New dates in source: source max={result.source_max}, "
                            f"optimized max={result.optimized_max}"
                        )
            except Exception as e:
                if "Not found" in str(e):
                    stale_reasons.append("Optimized table does not exist in BigQuery")

        return len(stale_reasons) > 0, stale_reasons, missing_keys

    def analyze(
        self,
        source_table_path: str,
        schema_config: SchemaConfig
    ) -> Dict:
        """Analyze what would be created for an optimized source table."""
        composite_keys = self.analyze_schema_for_composite_keys(schema_config)
        recommended_clustering = self.auto_detect_clustering_columns(
            schema_config, source_table_path
        )

        metrics_with_keys = set()
        for key in composite_keys:
            metrics_with_keys.update(key['metric_ids'])

        return {
            'composite_keys': composite_keys,
            'recommended_clustering': recommended_clustering,
            'estimated_key_count': len(composite_keys),
            'metrics_with_composite_keys': list(metrics_with_keys)
        }

    def preview_sql(
        self,
        source_table_path: str,
        schema_config: SchemaConfig,
        auto_detect_clustering: bool = True,
        clustering_columns: Optional[List[str]] = None,
        target_project: Optional[str] = None,
        target_dataset: Optional[str] = None
    ) -> Dict:
        """Preview the SQL that would be generated."""
        composite_keys = self.analyze_schema_for_composite_keys(schema_config)

        # Determine clustering columns
        if clustering_columns:
            clustering_cols = clustering_columns[:4]
        elif auto_detect_clustering:
            clustering_cols = self.auto_detect_clustering_columns(
                schema_config, source_table_path
            )
        else:
            clustering_cols = []

        # Generate table name using table_id to avoid conflicts
        table_id_short = self.table_id.replace('-', '')[:8]
        optimized_table_name = f"optimized_source_{table_id_short}"

        # Create temporary config for SQL generation
        source_project = source_table_path.split('.')[0]
        source_dataset = source_table_path.split('.')[1]

        config = OptimizedSourceConfig(
            bigquery_table=self.bigquery_table,
            source_table_path=source_table_path,
            optimized_table_name=optimized_table_name,
            target_project=target_project or source_project,
            target_dataset=target_dataset or source_dataset,
            composite_key_mappings=composite_keys,
            clustering={
                'columns': clustering_cols,
                'auto_detected': auto_detect_clustering and not clustering_columns
            } if clustering_cols else None
        )

        sql = self.generate_create_sql(source_table_path, config)

        return {
            'sql': sql,
            'target_table_path': config.optimized_table_path,
            'composite_keys': [k['key_column_name'] for k in composite_keys],
            'clustering_columns': clustering_cols
        }

    def get_status(
        self,
        source_table_path: str,
        schema_config: SchemaConfig
    ) -> Dict:
        """Get current status of optimized source table."""
        try:
            config = self.bigquery_table.optimized_source_config
        except OptimizedSourceConfig.DoesNotExist:
            return {
                'exists': False,
                'is_stale': False
            }

        is_stale, stale_reasons, missing_keys = self.check_staleness(
            source_table_path, schema_config, config
        )

        return {
            'exists': True,
            'config': {
                'id': str(config.id),
                'source_table_path': config.source_table_path,
                'optimized_table_name': config.optimized_table_name,
                'target_project': config.target_project,
                'target_dataset': config.target_dataset,
                'composite_key_mappings': config.composite_key_mappings,
                'clustering': config.clustering,
                'partition_column': config.partition_column,
                'status': config.status,
                'last_refresh_at': config.last_refresh_at.isoformat() if config.last_refresh_at else None,
                'last_refresh_error': config.last_refresh_error,
                'row_count': config.row_count,
                'size_bytes': config.size_bytes,
                'created_at': config.created_at.isoformat(),
                'updated_at': config.updated_at.isoformat(),
            },
            'is_stale': is_stale,
            'stale_reasons': stale_reasons,
            'missing_keys': missing_keys,
            'optimized_table_path': config.optimized_table_path
        }

    def create_optimized_source(
        self,
        source_table_path: str,
        schema_config: SchemaConfig,
        auto_detect_clustering: bool = True,
        clustering_columns: Optional[List[str]] = None,
        target_project: Optional[str] = None,
        target_dataset: Optional[str] = None
    ) -> Dict:
        """Create the optimized source table in BigQuery."""
        try:
            # Analyze schema for composite keys
            composite_keys = self.analyze_schema_for_composite_keys(schema_config)

            if not composite_keys:
                return {
                    'success': False,
                    'message': "No composite keys found in schema. "
                               "Optimized source is only useful when you have multi-column COUNT_DISTINCT metrics."
                }

            # Determine clustering columns
            if clustering_columns:
                clustering_cols = clustering_columns[:4]
                auto_detected = False
            elif auto_detect_clustering:
                clustering_cols = self.auto_detect_clustering_columns(
                    schema_config, source_table_path
                )
                auto_detected = True
            else:
                clustering_cols = []
                auto_detected = False

            # Generate table name using table_id to avoid conflicts
            # Use first 8 chars of table_id for shorter name
            table_id_short = self.table_id.replace('-', '')[:8]
            optimized_table_name = f"optimized_source_{table_id_short}"

            source_project = source_table_path.split('.')[0]
            source_dataset = source_table_path.split('.')[1]

            # Create or update config
            config, created = OptimizedSourceConfig.objects.update_or_create(
                bigquery_table=self.bigquery_table,
                defaults={
                    'source_table_path': source_table_path,
                    'optimized_table_name': optimized_table_name,
                    'target_project': target_project or source_project,
                    'target_dataset': target_dataset or source_dataset,
                    'composite_key_mappings': composite_keys,
                    'clustering': {
                        'columns': clustering_cols,
                        'auto_detected': auto_detected
                    } if clustering_cols else None,
                    'status': OptimizedSourceStatus.BUILDING,
                }
            )

            # Drop existing table if it exists (needed when clustering/partitioning changes)
            target_path = config.optimized_table_path
            try:
                self.client.delete_table(target_path)
                logger.info(f"Dropped existing optimized table: {target_path}")
            except Exception as e:
                if "Not found" not in str(e):
                    logger.warning(f"Could not drop existing table: {e}")

            # Generate and execute SQL (use CREATE TABLE instead of CREATE OR REPLACE)
            sql = self.generate_create_sql(source_table_path, config)

            start_time = time.time()

            job = self.client.query(sql)
            job.result()  # Wait for completion

            execution_time_ms = int((time.time() - start_time) * 1000)

            # Get job stats
            bytes_processed = job.total_bytes_processed or 0

            # Get row count from created table
            target_path = config.optimized_table_path
            count_result = list(self.client.query(
                f"SELECT COUNT(*) as cnt FROM `{target_path}`"
            ).result())[0]
            row_count = count_result.cnt

            # Update config with results
            config.status = OptimizedSourceStatus.READY
            config.last_refresh_at = timezone.now()
            config.row_count = row_count
            config.size_bytes = bytes_processed
            config.last_refresh_error = None
            config.save()

            return {
                'success': True,
                'message': f"Created optimized source table with {len(composite_keys)} composite key(s)",
                'optimized_table_path': target_path,
                'composite_keys_created': [k['key_column_name'] for k in composite_keys],
                'clustering_columns': clustering_cols,
                'bytes_processed': bytes_processed,
                'row_count': row_count,
                'execution_time_ms': execution_time_ms
            }

        except Exception as e:
            # Update status to error
            try:
                config = self.bigquery_table.optimized_source_config
                config.status = OptimizedSourceStatus.ERROR
                config.last_refresh_error = str(e)
                config.save()
            except OptimizedSourceConfig.DoesNotExist:
                pass

            logger.exception(f"Failed to create optimized source: {e}")
            return {
                'success': False,
                'message': f"Failed to create optimized source: {str(e)}"
            }

    def refresh_optimized_source(
        self,
        source_table_path: str,
        schema_config: SchemaConfig,
        incremental: bool = True
    ) -> Dict:
        """
        Refresh the optimized source table.

        If incremental=True, only adds new dates.
        If incremental=False, recreates the entire table.
        """
        try:
            config = self.bigquery_table.optimized_source_config
        except OptimizedSourceConfig.DoesNotExist:
            return {
                'success': False,
                'message': "No optimized source configuration found. Create one first."
            }

        try:
            if not incremental:
                # Full refresh - regenerate everything
                new_keys = self.analyze_schema_for_composite_keys(schema_config)
                config.composite_key_mappings = new_keys
                config.status = OptimizedSourceStatus.BUILDING
                config.save()

                sql = self.generate_create_sql(source_table_path, config)
            else:
                # Incremental - find missing dates
                target_path = config.optimized_table_path

                query = f"""
                SELECT DISTINCT date
                FROM `{source_table_path}`
                WHERE date NOT IN (
                    SELECT DISTINCT date FROM `{target_path}`
                )
                ORDER BY date
                """

                result = self.client.query(query).result()
                missing_dates = [str(row.date) for row in result]

                if not missing_dates:
                    return {
                        'success': True,
                        'message': "Optimized source is already up to date",
                        'optimized_table_path': target_path
                    }

                sql = self.generate_incremental_insert_sql(
                    source_table_path, config, missing_dates
                )

            # Execute SQL
            start_time = time.time()

            job = self.client.query(sql)
            job.result()

            execution_time_ms = int((time.time() - start_time) * 1000)
            bytes_processed = job.total_bytes_processed or 0

            # Get updated row count
            target_path = config.optimized_table_path
            count_result = list(self.client.query(
                f"SELECT COUNT(*) as cnt FROM `{target_path}`"
            ).result())[0]
            row_count = count_result.cnt

            # Update config
            config.status = OptimizedSourceStatus.READY
            config.last_refresh_at = timezone.now()
            config.last_refresh_error = None
            config.row_count = row_count
            config.save()

            return {
                'success': True,
                'message': f"{'Full' if not incremental else 'Incremental'} refresh completed",
                'optimized_table_path': target_path,
                'composite_keys_created': [k['key_column_name'] for k in config.composite_key_mappings],
                'clustering_columns': config.clustering.get('columns', []) if config.clustering else [],
                'bytes_processed': bytes_processed,
                'row_count': row_count,
                'execution_time_ms': execution_time_ms
            }

        except Exception as e:
            config.status = OptimizedSourceStatus.ERROR
            config.last_refresh_error = str(e)
            config.save()

            logger.exception(f"Refresh failed: {e}")
            return {
                'success': False,
                'message': f"Refresh failed: {str(e)}"
            }

    def delete_optimized_source(self, drop_table: bool = False) -> Tuple[bool, str]:
        """
        Delete optimized source configuration and optionally the BigQuery table.

        Returns:
            Tuple of (success, message)
        """
        try:
            config = self.bigquery_table.optimized_source_config
        except OptimizedSourceConfig.DoesNotExist:
            return False, "No optimized source configuration found"

        try:
            if drop_table:
                target_path = config.optimized_table_path
                if target_path:
                    try:
                        self.client.delete_table(target_path)
                    except Exception as e:
                        if "Not found" not in str(e):
                            return False, f"Failed to drop table: {str(e)}"

            config.delete()
            return True, "Optimized source deleted successfully"

        except Exception as e:
            return False, f"Failed to delete: {str(e)}"

    def get_key_column_mapping(self) -> Dict[str, str]:
        """
        Get mapping of composite key columns for use by RollupService.

        Returns dict mapping original column combination to key column name.
        Example: {"query,visit_id": "_key_query_visit_id"}
        """
        try:
            config = self.bigquery_table.optimized_source_config
        except OptimizedSourceConfig.DoesNotExist:
            return {}

        mapping = {}
        for key in config.composite_key_mappings:
            sorted_cols = sorted(key['source_columns'])
            lookup_key = ",".join(sorted_cols)
            mapping[lookup_key] = key['key_column_name']

        return mapping
