"""
Schema utility URLs at /api/tables/schema/*.
FastAPI compatibility for schema copy and template endpoints.
"""
from django.urls import path
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status

from .models import BigQueryTable
from apps.schemas.models import SchemaConfig, CalculatedMetric, Dimension


class SchemaCopyView(APIView):
    """Copy schema from one table to another."""
    permission_classes = []

    def post(self, request):
        """Copy schema configuration between tables."""
        source_table_id = request.data.get('source_table_id')
        target_table_id = request.data.get('target_table_id')

        if not source_table_id or not target_table_id:
            return Response(
                {'error': 'source_table_id and target_table_id are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        # Validate source table access
        try:
            source_table = BigQueryTable.objects.get(
                id=source_table_id,
                owner=user
            )
        except BigQueryTable.DoesNotExist:
            try:
                source_table = BigQueryTable.objects.get(
                    id=source_table_id,
                    organization_id__in=org_ids
                )
            except BigQueryTable.DoesNotExist:
                return Response(
                    {'error': 'Source table not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        # Validate target table access
        try:
            target_table = BigQueryTable.objects.get(
                id=target_table_id,
                owner=user
            )
        except BigQueryTable.DoesNotExist:
            try:
                target_table = BigQueryTable.objects.get(
                    id=target_table_id,
                    organization_id__in=org_ids
                )
            except BigQueryTable.DoesNotExist:
                return Response(
                    {'error': 'Target table not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        # Get or create target schema
        target_schema, _ = SchemaConfig.objects.get_or_create(
            bigquery_table=target_table
        )

        # Clear existing target schema items
        target_schema.calculated_metrics.all().delete()
        target_schema.dimensions.all().delete()
        target_schema.calculated_dimensions.all().delete()

        # Copy from source
        try:
            source_schema = SchemaConfig.objects.get(bigquery_table=source_table)

            # Copy metrics
            for metric in source_schema.calculated_metrics.all():
                CalculatedMetric.objects.create(
                    schema_config=target_schema,
                    metric_id=metric.metric_id,
                    display_name=metric.display_name,
                    formula=metric.formula,
                    sql_expression=metric.sql_expression,
                    format_type=metric.format_type,
                    decimal_places=metric.decimal_places,
                    category=metric.category,
                    is_visible_by_default=metric.is_visible_by_default,
                    sort_order=metric.sort_order
                )

            # Copy dimensions
            for dim in source_schema.dimensions.all():
                Dimension.objects.create(
                    schema_config=target_schema,
                    dimension_id=dim.dimension_id,
                    column_name=dim.column_name,
                    display_name=dim.display_name,
                    data_type=dim.data_type,
                    filter_type=dim.filter_type,
                    is_filterable=dim.is_filterable,
                    is_groupable=dim.is_groupable,
                    sort_order=dim.sort_order
                )

        except SchemaConfig.DoesNotExist:
            return Response(
                {'error': 'Source table has no schema configured'},
                status=status.HTTP_400_BAD_REQUEST
            )

        return Response({
            'success': True,
            'message': f'Schema copied from {source_table.name} to {target_table.name}'
        })


class SchemaTemplateView(APIView):
    """Apply a predefined schema template to a table."""
    permission_classes = []

    # Predefined templates
    TEMPLATES = {
        'ecommerce': {
            'metrics': [
                {'id': 'queries', 'name': 'Queries', 'formula': 'SUM({queries})', 'format': 'number'},
                {'id': 'revenue', 'name': 'Revenue', 'formula': 'SUM({revenue})', 'format': 'currency'},
                {'id': 'purchases', 'name': 'Purchases', 'formula': 'SUM({purchases})', 'format': 'number'},
                {'id': 'conversion_rate', 'name': 'Conversion Rate', 'formula': '{purchases} / {queries}', 'format': 'percent'},
                {'id': 'aov', 'name': 'AOV', 'formula': '{revenue} / {purchases}', 'format': 'currency'},
            ],
            'dimensions': [
                {'id': 'channel', 'name': 'Channel', 'column': 'channel'},
                {'id': 'country', 'name': 'Country', 'column': 'country'},
                {'id': 'search_term', 'name': 'Search Term', 'column': 'search_term'},
            ]
        },
        'saas': {
            'metrics': [
                {'id': 'users', 'name': 'Users', 'formula': 'SUM({users})', 'format': 'number'},
                {'id': 'sessions', 'name': 'Sessions', 'formula': 'SUM({sessions})', 'format': 'number'},
                {'id': 'signups', 'name': 'Signups', 'formula': 'SUM({signups})', 'format': 'number'},
                {'id': 'mrr', 'name': 'MRR', 'formula': 'SUM({mrr})', 'format': 'currency'},
                {'id': 'churn_rate', 'name': 'Churn Rate', 'formula': '{churned} / {users}', 'format': 'percent'},
            ],
            'dimensions': [
                {'id': 'plan', 'name': 'Plan', 'column': 'plan'},
                {'id': 'source', 'name': 'Source', 'column': 'source'},
            ]
        },
        'marketing': {
            'metrics': [
                {'id': 'impressions', 'name': 'Impressions', 'formula': 'SUM({impressions})', 'format': 'number'},
                {'id': 'clicks', 'name': 'Clicks', 'formula': 'SUM({clicks})', 'format': 'number'},
                {'id': 'ctr', 'name': 'CTR', 'formula': '{clicks} / {impressions}', 'format': 'percent'},
                {'id': 'spend', 'name': 'Spend', 'formula': 'SUM({spend})', 'format': 'currency'},
                {'id': 'cpc', 'name': 'CPC', 'formula': '{spend} / {clicks}', 'format': 'currency'},
            ],
            'dimensions': [
                {'id': 'campaign', 'name': 'Campaign', 'column': 'campaign'},
                {'id': 'ad_group', 'name': 'Ad Group', 'column': 'ad_group'},
                {'id': 'keyword', 'name': 'Keyword', 'column': 'keyword'},
            ]
        }
    }

    def post(self, request):
        """Apply a schema template to a table."""
        template_name = request.data.get('template_name')
        table_id = request.data.get('table_id')

        if not template_name:
            return Response(
                {'error': 'template_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if template_name not in self.TEMPLATES:
            return Response(
                {'error': f'Unknown template: {template_name}. Available: {list(self.TEMPLATES.keys())}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not table_id:
            return Response(
                {'error': 'table_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        user = request.user
        org_ids = user.memberships.values_list('organization_id', flat=True)

        # Validate table access
        try:
            table = BigQueryTable.objects.get(id=table_id, owner=user)
        except BigQueryTable.DoesNotExist:
            try:
                table = BigQueryTable.objects.get(id=table_id, organization_id__in=org_ids)
            except BigQueryTable.DoesNotExist:
                return Response(
                    {'error': 'Table not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        # Get or create schema
        schema, _ = SchemaConfig.objects.get_or_create(bigquery_table=table)

        # Apply template
        template = self.TEMPLATES[template_name]

        # Clear existing
        schema.calculated_metrics.all().delete()
        schema.dimensions.all().delete()

        # Create metrics from template
        for i, metric in enumerate(template['metrics']):
            format_mapping = {
                'number': 'number',
                'currency': 'currency',
                'percent': 'percent'
            }
            CalculatedMetric.objects.create(
                schema_config=schema,
                metric_id=metric['id'],
                display_name=metric['name'],
                formula=metric['formula'],
                sql_expression='',  # Will be computed from formula
                format_type=format_mapping.get(metric.get('format'), 'number'),
                decimal_places=2,
                category='general',
                is_visible_by_default=True,
                sort_order=i
            )

        # Create dimensions from template
        for i, dim in enumerate(template['dimensions']):
            Dimension.objects.create(
                schema_config=schema,
                dimension_id=dim['id'],
                column_name=dim['column'],
                display_name=dim['name'],
                data_type='STRING',
                filter_type='multi',
                is_filterable=True,
                is_groupable=True,
                sort_order=i
            )

        return Response({
            'success': True,
            'message': f'Applied {template_name} template to table'
        })


urlpatterns = [
    path('copy/', SchemaCopyView.as_view(), name='schema-copy-root'),
    path('template/', SchemaTemplateView.as_view(), name='schema-template-root'),
]
