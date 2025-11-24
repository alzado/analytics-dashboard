"""
Schema detection and management service.
Auto-detects BigQuery table schema and provides CRUD operations for schema configuration.
"""
import os
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from google.cloud import bigquery

from models.schemas import (
    BaseMetric, CalculatedMetric, DimensionDef, SchemaConfig,
    SchemaDetectionResult, MetricCreate, DimensionCreate
)
from config import table_registry


class SchemaService:
    """Service for managing dynamic schema configuration"""

    def __init__(self, bigquery_client: bigquery.Client, table_id: Optional[str] = None):
        self.client = bigquery_client
        self.table_id = table_id
        self._schema_cache: Optional[SchemaConfig] = None

    def _get_schema_file_path(self) -> str:
        """Get the schema file path for the current table."""
        if self.table_id:
            return table_registry.get_schema_path(self.table_id)
        # Fallback to legacy path if no table_id
        from config import LEGACY_SCHEMA_FILE
        return LEGACY_SCHEMA_FILE

    def detect_schema(
        self,
        project_id: str,
        dataset: str,
        table: str
    ) -> SchemaDetectionResult:
        """
        Auto-detect schema from BigQuery table.
        Classifies columns as base metrics or dimensions with smart defaults.
        """
        table_ref = f"{project_id}.{dataset}.{table}"
        table_obj = self.client.get_table(table_ref)

        detected_metrics: List[BaseMetric] = []
        detected_dimensions: List[DimensionDef] = []
        warnings: List[str] = []

        # Known metric keywords that indicate a column should be aggregated
        metric_keywords = [
            'queries', 'query', 'purchase', 'revenue', 'gross',
            'total', 'count', 'sum', 'amount', 'value', 'price',
            'cost', 'sales', 'orders', 'items', 'products',
            'clicks', 'impressions', 'views', 'sessions'
        ]

        # Known dimension keywords
        dimension_keywords = [
            'country', 'channel', 'category', 'type', 'name',
            'status', 'gender', 'marca', 'brand', 'color',
            'material', 'size', 'model', 'classification'
        ]

        # Column prefixes that indicate special handling
        boolean_prefixes = ['is_', 'has_', 'attr_']
        id_suffixes = ['_id', '_key']

        for field in table_obj.schema:
            column_name = field.name
            field_type = field.field_type
            column_lower = column_name.lower()

            # Skip certain metadata columns
            if column_name in ['_PARTITIONTIME', '_PARTITIONDATE']:
                continue

            # Handle DATE columns specially - always dimensions for time grouping
            if field_type == 'DATE' or field_type == 'TIMESTAMP':
                detected_dimensions.append(DimensionDef(
                    id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type='DATE',
                    is_filterable=True,
                    is_groupable=True,
                    sort_order=0,  # Date usually comes first
                    filter_type='date_range',
                    description=f"Date/time dimension from {column_name}"
                ))
                continue

            # Handle BOOLEAN columns - usually dimensions (filters)
            if field_type == 'BOOLEAN' or any(column_lower.startswith(prefix) for prefix in boolean_prefixes):
                detected_dimensions.append(DimensionDef(
                    id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type='BOOLEAN',
                    is_filterable=True,
                    is_groupable=True,
                    sort_order=100,
                    filter_type='boolean',
                    description=f"Boolean filter from {column_name}"
                ))
                continue

            # Handle numeric types - could be metrics or dimensions
            if field_type in ['INTEGER', 'INT64', 'FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC']:
                # Check if this looks like a metric
                is_metric = any(keyword in column_lower for keyword in metric_keywords)

                # Check if this looks like a dimension (IDs, counts of categories, etc.)
                is_dimension = (
                    any(column_lower.endswith(suffix) for suffix in id_suffixes) or
                    column_lower.startswith('n_') or
                    column_lower.startswith('num_')
                )

                if is_metric and not is_dimension:
                    # It's a metric column
                    data_type = 'INTEGER' if field_type in ['INTEGER', 'INT64'] else 'FLOAT'

                    # Determine aggregation and format
                    aggregation = 'SUM'
                    format_type = 'number'
                    decimal_places = 0
                    category = 'other'

                    if 'revenue' in column_lower or 'price' in column_lower or 'gross' in column_lower:
                        format_type = 'currency'
                        decimal_places = 2
                        category = 'revenue'
                    elif 'purchase' in column_lower or 'sale' in column_lower or 'order' in column_lower:
                        category = 'conversion'
                    elif 'query' in column_lower or 'click' in column_lower or 'view' in column_lower:
                        category = 'volume'

                    detected_metrics.append(BaseMetric(
                        id=column_name,
                        column_name=column_name,
                        display_name=self._humanize_name(column_name),
                        aggregation=aggregation,
                        data_type=data_type,
                        format_type=format_type,
                        decimal_places=decimal_places,
                        category=category,
                        is_visible_by_default=True,
                        sort_order=50,
                        description=f"Auto-detected metric from {column_name}"
                    ))
                else:
                    # It's a dimension (probably an integer category or count)
                    data_type = 'INTEGER' if field_type in ['INTEGER', 'INT64'] else 'FLOAT'
                    filter_type = 'range' if is_dimension else 'single'

                    detected_dimensions.append(DimensionDef(
                        id=column_name,
                        column_name=column_name,
                        display_name=self._humanize_name(column_name),
                        data_type=data_type,
                        is_filterable=True,
                        is_groupable=True,
                        sort_order=50,
                        filter_type=filter_type,
                        description=f"Numeric dimension from {column_name}"
                    ))

            # Handle STRING columns - usually dimensions
            elif field_type == 'STRING':
                # Check if it's the search term column
                is_search_term = 'search' in column_lower and 'term' in column_lower

                # Determine filter type
                filter_type = 'multi' if not is_search_term else None

                detected_dimensions.append(DimensionDef(
                    id=column_name,
                    column_name=column_name,
                    display_name=self._humanize_name(column_name),
                    data_type='STRING',
                    is_filterable=not is_search_term,  # Search term not filterable
                    is_groupable=True,
                    sort_order=50,
                    filter_type=filter_type,
                    description=f"String dimension from {column_name}"
                ))

            else:
                # Unknown type - log warning
                warnings.append(f"Unknown field type {field_type} for column {column_name}")

        # Add virtual days_in_range metric (system-generated, always available)
        days_in_range_metric = BaseMetric(
            id='days_in_range',
            column_name='date',  # Uses date column for calculation
            display_name='Days in Range',
            aggregation='COUNT',  # Special: Will be replaced with DATE_DIFF in SQL
            data_type='INTEGER',
            format_type='number',
            decimal_places=0,
            category='system',
            is_visible_by_default=False,  # Hidden by default, only used in formulas
            is_system=True,  # Mark as system metric
            sort_order=9999,  # Last in lists
            description='Virtual metric: Number of days in the queried date range (per dimension group)'
        )
        detected_metrics.append(days_in_range_metric)

        return SchemaDetectionResult(
            detected_base_metrics=detected_metrics,
            detected_dimensions=detected_dimensions,
            column_count=len(table_obj.schema),
            warnings=warnings
        )

    def _humanize_name(self, column_name: str) -> str:
        """Convert column_name to Human Readable Name"""
        # Handle special cases
        if column_name == 'n_words_normalized':
            return '# Words'
        if column_name == 'n_attributes':
            return '# Attributes'
        if column_name.startswith('attr_'):
            return column_name[5:].replace('_', ' ').title()

        # General case: replace underscores, capitalize
        return column_name.replace('_', ' ').title()

    def create_default_schema(self) -> SchemaConfig:
        """
        Create a default schema based on the original hardcoded configuration.
        Used for backward compatibility or when auto-detection fails.
        """
        now = datetime.utcnow().isoformat()

        # Default base metrics
        base_metrics = [
            BaseMetric(
                id='queries', column_name='queries', display_name='Queries',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=1, description='Total search queries'
            ),
            BaseMetric(
                id='queries_pdp', column_name='queries_pdp', display_name='Queries PDP',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=2, description='Queries with product detail page views'
            ),
            BaseMetric(
                id='queries_a2c', column_name='queries_a2c', display_name='Queries A2C',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=3, description='Queries with add-to-cart'
            ),
            BaseMetric(
                id='purchases', column_name='purchases', display_name='Purchases',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='conversion', is_visible_by_default=True,
                sort_order=4, description='Total purchases'
            ),
            BaseMetric(
                id='gross_purchase', column_name='gross_purchase', display_name='Revenue',
                aggregation='SUM', data_type='FLOAT', format_type='currency',
                decimal_places=2, category='revenue', is_visible_by_default=True,
                sort_order=5, description='Total revenue'
            ),
        ]

        # Default calculated metrics
        calculated_metrics = [
            CalculatedMetric(
                id='ctr', display_name='CTR',
                formula='{queries_pdp} / {queries}',
                sql_expression='SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))',
                depends_on=['queries_pdp', 'queries'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=10,
                description='Click-through rate (queries with PDP / total queries)'
            ),
            CalculatedMetric(
                id='a2c_rate', display_name='A2C Rate',
                formula='{queries_a2c} / {queries}',
                sql_expression='SAFE_DIVIDE(SUM(queries_a2c), SUM(queries))',
                depends_on=['queries_a2c', 'queries'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=11,
                description='Add-to-cart rate'
            ),
            CalculatedMetric(
                id='conversion_rate', display_name='Conv. Rate',
                formula='{purchases} / {queries}',
                sql_expression='SAFE_DIVIDE(SUM(purchases), SUM(queries))',
                depends_on=['purchases', 'queries'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=12,
                description='Conversion rate (purchases / queries)'
            ),
            CalculatedMetric(
                id='pdp_conversion', display_name='PDP Conv.',
                formula='{purchases} / {queries_pdp}',
                sql_expression='SAFE_DIVIDE(SUM(purchases), SUM(queries_pdp))',
                depends_on=['purchases', 'queries_pdp'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=13,
                description='PDP conversion rate'
            ),
            CalculatedMetric(
                id='revenue_per_query', display_name='Rev/Query',
                formula='{gross_purchase} / {queries}',
                sql_expression='SAFE_DIVIDE(SUM(gross_purchase), SUM(queries))',
                depends_on=['gross_purchase', 'queries'],
                format_type='currency', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=14,
                description='Average revenue per query'
            ),
            CalculatedMetric(
                id='aov', display_name='AOV',
                formula='{gross_purchase} / {purchases}',
                sql_expression='SAFE_DIVIDE(SUM(gross_purchase), SUM(purchases))',
                depends_on=['gross_purchase', 'purchases'],
                format_type='currency', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=15,
                description='Average order value'
            ),
        ]

        # Default dimensions
        dimensions = [
            DimensionDef(
                id='n_words_normalized', column_name='n_words_normalized',
                display_name='# Words', data_type='INTEGER',
                is_filterable=True, is_groupable=True, sort_order=1,
                filter_type='range', description='Number of words in search query'
            ),
            DimensionDef(
                id='n_attributes', column_name='n_attributes',
                display_name='# Attributes', data_type='INTEGER',
                is_filterable=True, is_groupable=True, sort_order=2,
                filter_type='range', description='Number of attributes in search'
            ),
            DimensionDef(
                id='channel', column_name='channel',
                display_name='Channel', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=3,
                filter_type='multi', description='Sales channel (App, Web, etc.)'
            ),
            DimensionDef(
                id='country', column_name='country',
                display_name='Country', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=4,
                filter_type='multi', description='Country'
            ),
        ]

        return SchemaConfig(
            base_metrics=base_metrics,
            calculated_metrics=calculated_metrics,
            dimensions=dimensions,
            version=1,
            created_at=now,
            updated_at=now
        )

    def load_schema(self) -> Optional[SchemaConfig]:
        """Load schema configuration from file"""
        if self._schema_cache:
            return self._schema_cache

        schema_file = self._get_schema_file_path()
        if not os.path.exists(schema_file):
            return None

        try:
            with open(schema_file, 'r') as f:
                data = json.load(f)
                schema = SchemaConfig(**data)

                # Migrate old schemas to populate new dependency fields
                needs_migration = self._migrate_calculated_metric_dependencies(schema)
                if needs_migration:
                    # Save the migrated schema
                    self.save_schema(schema)

                self._schema_cache = schema
                return schema
        except Exception as e:
            print(f"Failed to load schema config: {e}")
            return None

    def _migrate_calculated_metric_dependencies(self, schema: SchemaConfig) -> bool:
        """
        Migrate old calculated metrics to populate depends_on_base and depends_on_calculated fields.
        Returns True if migration was performed, False otherwise.
        """
        needs_migration = False
        base_metric_ids = {m.id for m in schema.base_metrics}
        calculated_metric_ids = {m.id for m in schema.calculated_metrics}

        for calc_metric in schema.calculated_metrics:
            # Check if this metric needs migration
            # Migrate if EITHER field is empty but depends_on has values
            if calc_metric.depends_on and (not calc_metric.depends_on_base or not calc_metric.depends_on_calculated):
                # Parse the depends_on field to separate base and calculated dependencies
                depends_on_base = []
                depends_on_calculated = []

                for dep_id in calc_metric.depends_on:
                    if dep_id in base_metric_ids:
                        depends_on_base.append(dep_id)
                    elif dep_id in calculated_metric_ids:
                        depends_on_calculated.append(dep_id)

                # Update the fields
                calc_metric.depends_on_base = depends_on_base
                calc_metric.depends_on_calculated = depends_on_calculated
                needs_migration = True

        return needs_migration

    def save_schema(self, schema: SchemaConfig) -> None:
        """Save schema configuration to file"""
        schema_file = self._get_schema_file_path()
        os.makedirs(os.path.dirname(schema_file), exist_ok=True)

        # Update timestamp
        schema.updated_at = datetime.utcnow().isoformat()

        with open(schema_file, 'w') as f:
            json.dump(schema.model_dump(), f, indent=2)

        # Update cache
        self._schema_cache = schema

    def get_or_create_schema(
        self,
        project_id: str,
        dataset: str,
        table: str,
        auto_detect: bool = True
    ) -> SchemaConfig:
        """
        Get existing schema or create a new one.
        If auto_detect=True and no schema exists, auto-detects from BigQuery table.
        Otherwise, uses default hardcoded schema.
        """
        existing = self.load_schema()
        if existing:
            return existing

        # No existing schema - create one
        if auto_detect:
            try:
                detection_result = self.detect_schema(project_id, dataset, table)

                now = datetime.utcnow().isoformat()
                schema = SchemaConfig(
                    base_metrics=detection_result.detected_base_metrics,
                    calculated_metrics=[],  # Start with no calculated metrics
                    dimensions=detection_result.detected_dimensions,
                    version=1,
                    created_at=now,
                    updated_at=now
                )

                self.save_schema(schema)
                return schema
            except Exception as e:
                print(f"Auto-detection failed: {e}. Using default schema.")
                schema = self.create_default_schema()
                self.save_schema(schema)
                return schema
        else:
            schema = self.create_default_schema()
            self.save_schema(schema)
            return schema

    def clear_cache(self):
        """Clear the schema cache"""
        self._schema_cache = None

    def get_all_metrics(self, schema: Optional[SchemaConfig] = None) -> List[Dict]:
        """Get all metrics (base + calculated) as a unified list for API responses"""
        if not schema:
            schema = self.load_schema()
        if not schema:
            return []

        all_metrics = []

        # Add base metrics
        for metric in schema.base_metrics:
            all_metrics.append({
                'id': metric.id,
                'display_name': metric.display_name,
                'type': 'base',
                'column_name': metric.column_name,
                'aggregation': metric.aggregation,
                'format_type': metric.format_type,
                'decimal_places': metric.decimal_places,
                'category': metric.category,
                'is_visible_by_default': metric.is_visible_by_default,
                'sort_order': metric.sort_order,
                'description': metric.description
            })

        # Add calculated metrics
        for metric in schema.calculated_metrics:
            all_metrics.append({
                'id': metric.id,
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

        # Sort by sort_order
        all_metrics.sort(key=lambda x: x['sort_order'])

        return all_metrics

    def get_metric_by_id(self, metric_id: str, schema: Optional[SchemaConfig] = None) -> Optional[Dict]:
        """Get a specific metric by ID"""
        if not schema:
            schema = self.load_schema()
        if not schema:
            return None

        # Check base metrics
        for metric in schema.base_metrics:
            if metric.id == metric_id:
                return metric.model_dump()

        # Check calculated metrics
        for metric in schema.calculated_metrics:
            if metric.id == metric_id:
                return metric.model_dump()

        return None

    def get_dimension_by_id(self, dimension_id: str, schema: Optional[SchemaConfig] = None) -> Optional[DimensionDef]:
        """Get a specific dimension by ID"""
        if not schema:
            schema = self.load_schema()
        if not schema:
            return None

        for dim in schema.dimensions:
            if dim.id == dimension_id:
                return dim

        return None

    def copy_schema(self, source_table_id: str, target_table_id: str) -> bool:
        """
        Copy schema from one table to another.
        Returns True on success, False on failure.
        """
        try:
            # Load source schema
            source_schema_path = table_registry.get_schema_path(source_table_id)
            if not os.path.exists(source_schema_path):
                print(f"Source schema not found: {source_schema_path}")
                return False

            with open(source_schema_path, 'r') as f:
                schema_data = json.load(f)

            # Update timestamps and version
            now = datetime.utcnow().isoformat()
            schema_data['created_at'] = now
            schema_data['updated_at'] = now

            # Save to target
            target_schema_path = table_registry.get_schema_path(target_table_id)
            os.makedirs(os.path.dirname(target_schema_path), exist_ok=True)

            with open(target_schema_path, 'w') as f:
                json.dump(schema_data, f, indent=2)

            return True

        except Exception as e:
            return False

    def apply_template(self, template_name: str) -> bool:
        """
        Apply a pre-built schema template.
        Templates: 'ecommerce', 'saas', 'marketing'
        Returns True on success, False on failure.
        """
        now = datetime.utcnow().isoformat()

        try:
            if template_name.lower() == 'ecommerce':
                schema = self._create_ecommerce_template()
            elif template_name.lower() == 'saas':
                schema = self._create_saas_template()
            elif template_name.lower() == 'marketing':
                schema = self._create_marketing_template()
            else:
                return False

            # Update timestamps
            schema.created_at = now
            schema.updated_at = now

            # Save schema
            self.save_schema(schema)
            return True

        except Exception as e:
            return False

    def _create_ecommerce_template(self) -> SchemaConfig:
        """E-commerce analytics template (default/existing schema)"""
        return self.create_default_schema()

    def _create_saas_template(self) -> SchemaConfig:
        """SaaS/subscription analytics template"""
        now = datetime.utcnow().isoformat()

        base_metrics = [
            BaseMetric(
                id='sessions', column_name='sessions', display_name='Sessions',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=1, description='Total user sessions'
            ),
            BaseMetric(
                id='signups', column_name='signups', display_name='Signups',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='conversion', is_visible_by_default=True,
                sort_order=2, description='New user signups'
            ),
            BaseMetric(
                id='trials', column_name='trials', display_name='Trial Starts',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='conversion', is_visible_by_default=True,
                sort_order=3, description='Trial subscriptions started'
            ),
            BaseMetric(
                id='conversions', column_name='conversions', display_name='Conversions',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='conversion', is_visible_by_default=True,
                sort_order=4, description='Trial to paid conversions'
            ),
            BaseMetric(
                id='mrr', column_name='mrr', display_name='MRR',
                aggregation='SUM', data_type='FLOAT', format_type='currency',
                decimal_places=2, category='revenue', is_visible_by_default=True,
                sort_order=5, description='Monthly recurring revenue'
            ),
        ]

        calculated_metrics = [
            CalculatedMetric(
                id='signup_rate', display_name='Signup Rate',
                formula='{signups} / {sessions}',
                sql_expression='SAFE_DIVIDE(SUM(signups), SUM(sessions))',
                depends_on=['signups', 'sessions'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=10,
                description='Signup rate (signups / sessions)'
            ),
            CalculatedMetric(
                id='trial_rate', display_name='Trial Rate',
                formula='{trials} / {signups}',
                sql_expression='SAFE_DIVIDE(SUM(trials), SUM(signups))',
                depends_on=['trials', 'signups'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=11,
                description='Trial start rate'
            ),
            CalculatedMetric(
                id='conversion_rate', display_name='Conv. Rate',
                formula='{conversions} / {trials}',
                sql_expression='SAFE_DIVIDE(SUM(conversions), SUM(trials))',
                depends_on=['conversions', 'trials'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=12,
                description='Trial to paid conversion rate'
            ),
            CalculatedMetric(
                id='arpu', display_name='ARPU',
                formula='{mrr} / {conversions}',
                sql_expression='SAFE_DIVIDE(SUM(mrr), SUM(conversions))',
                depends_on=['mrr', 'conversions'],
                format_type='currency', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=13,
                description='Average revenue per user'
            ),
        ]

        dimensions = [
            DimensionDef(
                id='plan_type', column_name='plan_type',
                display_name='Plan Type', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=1,
                filter_type='multi', description='Subscription plan tier'
            ),
            DimensionDef(
                id='channel', column_name='channel',
                display_name='Channel', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=2,
                filter_type='multi', description='Acquisition channel'
            ),
            DimensionDef(
                id='country', column_name='country',
                display_name='Country', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=3,
                filter_type='multi', description='User country'
            ),
        ]

        return SchemaConfig(
            base_metrics=base_metrics,
            calculated_metrics=calculated_metrics,
            dimensions=dimensions,
            version=1,
            created_at=now,
            updated_at=now
        )

    def _create_marketing_template(self) -> SchemaConfig:
        """Digital marketing/advertising template"""
        now = datetime.utcnow().isoformat()

        base_metrics = [
            BaseMetric(
                id='impressions', column_name='impressions', display_name='Impressions',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=1, description='Total ad impressions'
            ),
            BaseMetric(
                id='clicks', column_name='clicks', display_name='Clicks',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='volume', is_visible_by_default=True,
                sort_order=2, description='Total clicks'
            ),
            BaseMetric(
                id='conversions', column_name='conversions', display_name='Conversions',
                aggregation='SUM', data_type='INTEGER', format_type='number',
                decimal_places=0, category='conversion', is_visible_by_default=True,
                sort_order=3, description='Total conversions'
            ),
            BaseMetric(
                id='spend', column_name='spend', display_name='Spend',
                aggregation='SUM', data_type='FLOAT', format_type='currency',
                decimal_places=2, category='revenue', is_visible_by_default=True,
                sort_order=4, description='Total ad spend'
            ),
            BaseMetric(
                id='revenue', column_name='revenue', display_name='Revenue',
                aggregation='SUM', data_type='FLOAT', format_type='currency',
                decimal_places=2, category='revenue', is_visible_by_default=True,
                sort_order=5, description='Total revenue'
            ),
        ]

        calculated_metrics = [
            CalculatedMetric(
                id='ctr', display_name='CTR',
                formula='{clicks} / {impressions}',
                sql_expression='SAFE_DIVIDE(SUM(clicks), SUM(impressions))',
                depends_on=['clicks', 'impressions'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=10,
                description='Click-through rate'
            ),
            CalculatedMetric(
                id='cvr', display_name='CVR',
                formula='{conversions} / {clicks}',
                sql_expression='SAFE_DIVIDE(SUM(conversions), SUM(clicks))',
                depends_on=['conversions', 'clicks'],
                format_type='percent', decimal_places=2, category='conversion',
                is_visible_by_default=True, sort_order=11,
                description='Conversion rate'
            ),
            CalculatedMetric(
                id='cpc', display_name='CPC',
                formula='{spend} / {clicks}',
                sql_expression='SAFE_DIVIDE(SUM(spend), SUM(clicks))',
                depends_on=['spend', 'clicks'],
                format_type='currency', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=12,
                description='Cost per click'
            ),
            CalculatedMetric(
                id='cpa', display_name='CPA',
                formula='{spend} / {conversions}',
                sql_expression='SAFE_DIVIDE(SUM(spend), SUM(conversions))',
                depends_on=['spend', 'conversions'],
                format_type='currency', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=13,
                description='Cost per acquisition'
            ),
            CalculatedMetric(
                id='roas', display_name='ROAS',
                formula='{revenue} / {spend}',
                sql_expression='SAFE_DIVIDE(SUM(revenue), SUM(spend))',
                depends_on=['revenue', 'spend'],
                format_type='number', decimal_places=2, category='revenue',
                is_visible_by_default=True, sort_order=14,
                description='Return on ad spend'
            ),
        ]

        dimensions = [
            DimensionDef(
                id='campaign', column_name='campaign',
                display_name='Campaign', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=1,
                filter_type='multi', description='Campaign name'
            ),
            DimensionDef(
                id='ad_group', column_name='ad_group',
                display_name='Ad Group', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=2,
                filter_type='multi', description='Ad group name'
            ),
            DimensionDef(
                id='channel', column_name='channel',
                display_name='Channel', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=3,
                filter_type='multi', description='Marketing channel'
            ),
            DimensionDef(
                id='device', column_name='device',
                display_name='Device', data_type='STRING',
                is_filterable=True, is_groupable=True, sort_order=4,
                filter_type='multi', description='Device type'
            ),
        ]

        return SchemaConfig(
            base_metrics=base_metrics,
            calculated_metrics=calculated_metrics,
            dimensions=dimensions,
            version=1,
            created_at=now,
            updated_at=now
        )
