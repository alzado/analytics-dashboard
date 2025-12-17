"""
Admin configuration for schemas app.
"""
from django.contrib import admin
from .models import (
    SchemaConfig, CalculatedMetric, Dimension,
    CalculatedDimension, CustomDimension, OptimizedSourceConfig
)


class CalculatedMetricInline(admin.TabularInline):
    """Inline for calculated metrics."""
    model = CalculatedMetric
    extra = 0
    fields = ('metric_id', 'display_name', 'formula', 'format_type', 'category', 'sort_order')
    readonly_fields = ('id',)


class DimensionInline(admin.TabularInline):
    """Inline for dimensions."""
    model = Dimension
    extra = 0
    fields = ('dimension_id', 'column_name', 'display_name', 'data_type', 'is_filterable', 'is_groupable')
    readonly_fields = ('id',)


@admin.register(SchemaConfig)
class SchemaConfigAdmin(admin.ModelAdmin):
    """Admin for SchemaConfig model."""

    list_display = ('bigquery_table', 'metric_count', 'dimension_count', 'version', 'updated_at')
    list_filter = ('created_at', 'updated_at')
    search_fields = ('bigquery_table__name', 'bigquery_table__table_name')
    autocomplete_fields = ['bigquery_table']
    readonly_fields = ('id', 'created_at', 'updated_at')
    inlines = [CalculatedMetricInline, DimensionInline]

    fieldsets = (
        (None, {'fields': ('id', 'bigquery_table', 'version')}),
        ('Pivot Settings', {'fields': ('primary_sort_metric', 'avg_per_day_metric', 'pagination_threshold')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )

    @admin.display(description='Metrics')
    def metric_count(self, obj):
        return obj.calculated_metrics.count()

    @admin.display(description='Dimensions')
    def dimension_count(self, obj):
        return obj.dimensions.count()


@admin.register(CalculatedMetric)
class CalculatedMetricAdmin(admin.ModelAdmin):
    """Admin for CalculatedMetric model."""

    list_display = ('metric_id', 'display_name', 'schema_config', 'format_type', 'category', 'sort_order')
    list_filter = ('format_type', 'category', 'is_visible_by_default')
    search_fields = ('metric_id', 'display_name', 'formula', 'schema_config__bigquery_table__name')
    autocomplete_fields = ['schema_config']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'schema_config', 'metric_id', 'display_name')}),
        ('Formula', {'fields': ('formula', 'sql_expression')}),
        ('Dependencies', {
            'fields': ('depends_on', 'depends_on_base', 'depends_on_calculated', 'depends_on_dimensions'),
            'classes': ('collapse',),
        }),
        ('Display', {'fields': ('format_type', 'decimal_places', 'category', 'is_visible_by_default', 'sort_order', 'description')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


@admin.register(Dimension)
class DimensionAdmin(admin.ModelAdmin):
    """Admin for Dimension model."""

    list_display = ('dimension_id', 'display_name', 'column_name', 'schema_config', 'data_type', 'is_filterable', 'is_groupable')
    list_filter = ('data_type', 'is_filterable', 'is_groupable', 'filter_type')
    search_fields = ('dimension_id', 'display_name', 'column_name', 'schema_config__bigquery_table__name')
    autocomplete_fields = ['schema_config']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'schema_config', 'dimension_id', 'column_name', 'display_name')}),
        ('Type', {'fields': ('data_type', 'filter_type')}),
        ('Capabilities', {'fields': ('is_filterable', 'is_groupable')}),
        ('Display', {'fields': ('sort_order', 'description')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


@admin.register(CalculatedDimension)
class CalculatedDimensionAdmin(admin.ModelAdmin):
    """Admin for CalculatedDimension model."""

    list_display = ('dimension_id', 'display_name', 'schema_config', 'data_type', 'is_filterable')
    list_filter = ('data_type', 'is_filterable', 'is_groupable')
    search_fields = ('dimension_id', 'display_name', 'schema_config__bigquery_table__name')
    autocomplete_fields = ['schema_config']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'schema_config', 'dimension_id', 'display_name')}),
        ('SQL', {'fields': ('sql_expression', 'depends_on')}),
        ('Type', {'fields': ('data_type', 'filter_type')}),
        ('Capabilities', {'fields': ('is_filterable', 'is_groupable')}),
        ('Display', {'fields': ('sort_order', 'description')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


@admin.register(CustomDimension)
class CustomDimensionAdmin(admin.ModelAdmin):
    """Admin for CustomDimension model."""

    list_display = ('name', 'schema_config', 'dimension_type', 'metric', 'updated_at')
    list_filter = ('dimension_type', 'created_at')
    search_fields = ('name', 'schema_config__bigquery_table__name')
    autocomplete_fields = ['schema_config']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'schema_config', 'name', 'dimension_type')}),
        ('Configuration', {'fields': ('metric', 'values_json')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


@admin.register(OptimizedSourceConfig)
class OptimizedSourceConfigAdmin(admin.ModelAdmin):
    """Admin for OptimizedSourceConfig model."""

    list_display = ('bigquery_table', 'optimized_table_name', 'status', 'row_count', 'last_refresh_at')
    list_filter = ('status', 'created_at')
    search_fields = ('bigquery_table__name', 'source_table_path', 'optimized_table_name')
    autocomplete_fields = ['bigquery_table']
    readonly_fields = ('id', 'created_at', 'updated_at', 'last_refresh_at', 'row_count', 'size_bytes')

    fieldsets = (
        (None, {'fields': ('id', 'bigquery_table')}),
        ('Source', {'fields': ('source_table_path',)}),
        ('Target', {'fields': ('optimized_table_name', 'target_project', 'target_dataset', 'partition_column')}),
        ('Configuration', {'fields': ('composite_key_mappings', 'clustering')}),
        ('Status', {'fields': ('status', 'last_refresh_at', 'last_refresh_error')}),
        ('Statistics', {'fields': ('row_count', 'size_bytes')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )
