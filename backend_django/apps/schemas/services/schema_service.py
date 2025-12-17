"""
Schema detection and management service.
Auto-detects BigQuery table schema and provides operations for schema configuration.
"""
import logging
from typing import List, Optional, Tuple
from google.cloud import bigquery

from apps.tables.models import BigQueryTable
from apps.schemas.models import (
    SchemaConfig, CalculatedMetric, Dimension, CalculatedDimension,
    FormatType, DataType, FilterType
)

logger = logging.getLogger(__name__)


class SchemaService:
    """Service for managing dynamic schema configuration."""

    def __init__(self, bigquery_table: BigQueryTable):
        self.bigquery_table = bigquery_table
        self._client = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy-load BigQuery client."""
        if self._client is None:
            # Get credentials from the table's credential
            if self.bigquery_table.gcp_credential:
                credentials = self.bigquery_table.gcp_credential.get_credentials_object()
                billing_project = (
                    self.bigquery_table.billing_project or
                    self.bigquery_table.project_id
                )
                self._client = bigquery.Client(
                    project=billing_project,
                    credentials=credentials
                )
            else:
                # Use Application Default Credentials
                billing_project = (
                    self.bigquery_table.billing_project or
                    self.bigquery_table.project_id
                )
                self._client = bigquery.Client(project=billing_project)
        return self._client

    def get_or_create_schema(self, auto_detect: bool = True) -> SchemaConfig:
        """Get existing schema or create a new one."""
        schema_config, created = SchemaConfig.objects.get_or_create(
            bigquery_table=self.bigquery_table
        )

        if created and auto_detect:
            self.detect_and_populate_schema(schema_config)

        return schema_config

    def detect_and_create_schema(self) -> SchemaConfig:
        """Create new schema by auto-detecting from BigQuery table."""
        schema_config, _ = SchemaConfig.objects.get_or_create(
            bigquery_table=self.bigquery_table
        )

        # Clear existing metrics/dimensions
        schema_config.calculated_metrics.all().delete()
        schema_config.dimensions.all().delete()
        schema_config.calculated_dimensions.all().delete()

        self.detect_and_populate_schema(schema_config)
        return schema_config

    def detect_and_populate_schema(self, schema_config: SchemaConfig) -> dict:
        """
        Auto-detect schema from BigQuery table and populate dimensions.
        Returns detection result with warnings.
        """
        table_ref = self.bigquery_table.full_table_path
        table_obj = self.client.get_table(table_ref)

        detected_dimensions = []
        warnings = []

        # Keywords for classification
        metric_keywords = [
            'queries', 'query', 'purchase', 'revenue', 'gross',
            'total', 'count', 'sum', 'amount', 'value', 'price',
            'cost', 'sales', 'orders', 'items', 'products',
            'clicks', 'impressions', 'views', 'sessions'
        ]
        boolean_prefixes = ['is_', 'has_', 'attr_']
        id_suffixes = ['_id', '_key']

        sort_order = 0
        for field in table_obj.schema:
            column_name = field.name
            field_type = field.field_type
            column_lower = column_name.lower()

            # Skip metadata columns
            if column_name in ['_PARTITIONTIME', '_PARTITIONDATE']:
                continue

            sort_order += 1

            # Handle DATE/TIMESTAMP columns - always dimensions
            if field_type in ['DATE', 'TIMESTAMP']:
                dimension = Dimension.objects.create(
                    schema_config=schema_config,
                    dimension_id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type=DataType.DATE,
                    is_filterable=True,
                    is_groupable=True,
                    sort_order=0,  # Date usually first
                    filter_type=FilterType.DATE_RANGE,
                    description=f"Date/time dimension from {column_name}"
                )
                detected_dimensions.append(dimension)
                continue

            # Handle BOOLEAN columns
            if field_type == 'BOOLEAN' or any(
                column_lower.startswith(prefix) for prefix in boolean_prefixes
            ):
                dimension = Dimension.objects.create(
                    schema_config=schema_config,
                    dimension_id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type=DataType.BOOLEAN,
                    is_filterable=True,
                    is_groupable=True,
                    sort_order=100 + sort_order,
                    filter_type=FilterType.BOOLEAN,
                    description=f"Boolean filter from {column_name}"
                )
                detected_dimensions.append(dimension)
                continue

            # Handle numeric types
            if field_type in ['INTEGER', 'INT64', 'FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC']:
                is_metric = any(keyword in column_lower for keyword in metric_keywords)
                is_dimension = (
                    any(column_lower.endswith(suffix) for suffix in id_suffixes) or
                    column_lower.startswith('n_') or
                    column_lower.startswith('num_')
                )

                if not is_metric or is_dimension:
                    # Treat as dimension
                    data_type = (
                        DataType.INTEGER if field_type in ['INTEGER', 'INT64']
                        else DataType.FLOAT
                    )
                    filter_type = FilterType.RANGE if is_dimension else FilterType.SINGLE

                    dimension = Dimension.objects.create(
                        schema_config=schema_config,
                        dimension_id=column_name,
                        column_name=column_name,
                        display_name=self._humanize_name(column_name),
                        data_type=data_type,
                        is_filterable=True,
                        is_groupable=True,
                        sort_order=50 + sort_order,
                        filter_type=filter_type,
                        description=f"Numeric dimension from {column_name}"
                    )
                    detected_dimensions.append(dimension)

            # Handle STRING columns
            elif field_type == 'STRING':
                is_search_term = 'search' in column_lower and 'term' in column_lower

                dimension = Dimension.objects.create(
                    schema_config=schema_config,
                    dimension_id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type=DataType.STRING,
                    is_filterable=not is_search_term,
                    is_groupable=True,
                    sort_order=50 + sort_order,
                    filter_type=None if is_search_term else FilterType.MULTI,
                    description=f"String dimension from {column_name}"
                )
                detected_dimensions.append(dimension)

            else:
                warnings.append(
                    f"Unknown field type {field_type} for column {column_name}"
                )

        return {
            'detected_dimensions': detected_dimensions,
            'column_count': len(table_obj.schema),
            'warnings': warnings
        }

    def _humanize_name(self, column_name: str) -> str:
        """Convert column_name to Human Readable Name."""
        if column_name == 'n_words_normalized':
            return '# Words'
        if column_name == 'n_attributes':
            return '# Attributes'
        if column_name.startswith('attr_'):
            return column_name[5:].replace('_', ' ').title()

        return column_name.replace('_', ' ').title()

    def create_default_metrics(self, schema_config: SchemaConfig) -> List[CalculatedMetric]:
        """Create default calculated metrics for e-commerce analytics."""
        default_metrics = [
            {
                'metric_id': 'ctr',
                'display_name': 'CTR',
                'formula': '{queries_pdp} / {queries}',
                'sql_expression': 'SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))',
                'depends_on': ['queries_pdp', 'queries'],
                'format_type': FormatType.PERCENT,
                'decimal_places': 2,
                'category': 'conversion',
                'sort_order': 10,
                'description': 'Click-through rate (queries with PDP / total queries)'
            },
            {
                'metric_id': 'a2c_rate',
                'display_name': 'A2C Rate',
                'formula': '{queries_a2c} / {queries}',
                'sql_expression': 'SAFE_DIVIDE(SUM(queries_a2c), SUM(queries))',
                'depends_on': ['queries_a2c', 'queries'],
                'format_type': FormatType.PERCENT,
                'decimal_places': 2,
                'category': 'conversion',
                'sort_order': 11,
                'description': 'Add-to-cart rate'
            },
            {
                'metric_id': 'conversion_rate',
                'display_name': 'Conv. Rate',
                'formula': '{purchases} / {queries}',
                'sql_expression': 'SAFE_DIVIDE(SUM(purchases), SUM(queries))',
                'depends_on': ['purchases', 'queries'],
                'format_type': FormatType.PERCENT,
                'decimal_places': 2,
                'category': 'conversion',
                'sort_order': 12,
                'description': 'Conversion rate (purchases / queries)'
            },
            {
                'metric_id': 'pdp_conversion',
                'display_name': 'PDP Conv.',
                'formula': '{purchases} / {queries_pdp}',
                'sql_expression': 'SAFE_DIVIDE(SUM(purchases), SUM(queries_pdp))',
                'depends_on': ['purchases', 'queries_pdp'],
                'format_type': FormatType.PERCENT,
                'decimal_places': 2,
                'category': 'conversion',
                'sort_order': 13,
                'description': 'PDP conversion rate'
            },
            {
                'metric_id': 'revenue_per_query',
                'display_name': 'Rev/Query',
                'formula': '{gross_purchase} / {queries}',
                'sql_expression': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(queries))',
                'depends_on': ['gross_purchase', 'queries'],
                'format_type': FormatType.CURRENCY,
                'decimal_places': 2,
                'category': 'revenue',
                'sort_order': 14,
                'description': 'Average revenue per query'
            },
            {
                'metric_id': 'aov',
                'display_name': 'AOV',
                'formula': '{gross_purchase} / {purchases}',
                'sql_expression': 'SAFE_DIVIDE(SUM(gross_purchase), SUM(purchases))',
                'depends_on': ['gross_purchase', 'purchases'],
                'format_type': FormatType.CURRENCY,
                'decimal_places': 2,
                'category': 'revenue',
                'sort_order': 15,
                'description': 'Average order value'
            },
        ]

        created_metrics = []
        for metric_data in default_metrics:
            metric = CalculatedMetric.objects.create(
                schema_config=schema_config,
                **metric_data
            )
            created_metrics.append(metric)

        return created_metrics

    def copy_schema_from(self, source_schema: SchemaConfig) -> SchemaConfig:
        """Copy schema configuration from another table."""
        target_schema = self.get_or_create_schema(auto_detect=False)

        # Clear existing
        target_schema.calculated_metrics.all().delete()
        target_schema.dimensions.all().delete()
        target_schema.calculated_dimensions.all().delete()

        # Copy settings
        target_schema.primary_sort_metric = source_schema.primary_sort_metric
        target_schema.avg_per_day_metric = source_schema.avg_per_day_metric
        target_schema.pagination_threshold = source_schema.pagination_threshold
        target_schema.save()

        # Copy metrics
        for metric in source_schema.calculated_metrics.all():
            CalculatedMetric.objects.create(
                schema_config=target_schema,
                metric_id=metric.metric_id,
                display_name=metric.display_name,
                formula=metric.formula,
                sql_expression=metric.sql_expression,
                depends_on=metric.depends_on,
                depends_on_base=metric.depends_on_base,
                depends_on_calculated=metric.depends_on_calculated,
                depends_on_dimensions=metric.depends_on_dimensions,
                format_type=metric.format_type,
                decimal_places=metric.decimal_places,
                category=metric.category,
                is_visible_by_default=metric.is_visible_by_default,
                sort_order=metric.sort_order,
                description=metric.description
            )

        # Copy dimensions
        for dimension in source_schema.dimensions.all():
            Dimension.objects.create(
                schema_config=target_schema,
                dimension_id=dimension.dimension_id,
                column_name=dimension.column_name,
                display_name=dimension.display_name,
                data_type=dimension.data_type,
                is_filterable=dimension.is_filterable,
                is_groupable=dimension.is_groupable,
                sort_order=dimension.sort_order,
                filter_type=dimension.filter_type,
                description=dimension.description
            )

        # Copy calculated dimensions
        for calc_dim in source_schema.calculated_dimensions.all():
            CalculatedDimension.objects.create(
                schema_config=target_schema,
                dimension_id=calc_dim.dimension_id,
                display_name=calc_dim.display_name,
                sql_expression=calc_dim.sql_expression,
                depends_on=calc_dim.depends_on,
                data_type=calc_dim.data_type,
                is_filterable=calc_dim.is_filterable,
                is_groupable=calc_dim.is_groupable,
                sort_order=calc_dim.sort_order,
                filter_type=calc_dim.filter_type,
                description=calc_dim.description
            )

        return target_schema

    def get_all_metrics(self) -> List[dict]:
        """Get all metrics as a unified list for API responses."""
        schema_config = self.get_or_create_schema()
        metrics = []

        for metric in schema_config.calculated_metrics.order_by('sort_order'):
            metrics.append({
                'id': str(metric.id),
                'metric_id': metric.metric_id,
                'display_name': metric.display_name,
                'type': 'calculated',
                'formula': metric.formula,
                'sql_expression': metric.sql_expression,
                'depends_on': metric.depends_on,
                'format_type': metric.format_type,
                'decimal_places': metric.decimal_places,
                'category': metric.category,
                'is_visible_by_default': metric.is_visible_by_default,
                'sort_order': metric.sort_order,
                'description': metric.description
            })

        return metrics

    def get_all_dimensions(self) -> List[dict]:
        """Get all dimensions (regular and calculated) for API responses."""
        schema_config = self.get_or_create_schema()
        dimensions = []

        # Regular dimensions
        for dim in schema_config.dimensions.order_by('sort_order'):
            dimensions.append({
                'id': str(dim.id),
                'dimension_id': dim.dimension_id,
                'column_name': dim.column_name,
                'display_name': dim.display_name,
                'type': 'regular',
                'data_type': dim.data_type,
                'is_filterable': dim.is_filterable,
                'is_groupable': dim.is_groupable,
                'filter_type': dim.filter_type,
                'sort_order': dim.sort_order
            })

        # Calculated dimensions
        for calc_dim in schema_config.calculated_dimensions.order_by('sort_order'):
            dimensions.append({
                'id': str(calc_dim.id),
                'dimension_id': calc_dim.dimension_id,
                'column_name': None,
                'display_name': calc_dim.display_name,
                'type': 'calculated',
                'sql_expression': calc_dim.sql_expression,
                'data_type': calc_dim.data_type,
                'is_filterable': calc_dim.is_filterable,
                'is_groupable': calc_dim.is_groupable,
                'filter_type': calc_dim.filter_type,
                'sort_order': calc_dim.sort_order
            })

        return sorted(dimensions, key=lambda x: x['sort_order'])
