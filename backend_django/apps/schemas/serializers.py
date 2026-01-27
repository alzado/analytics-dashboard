"""
Serializers for schema models.
"""
from rest_framework import serializers
from .models import (
    SchemaConfig, CalculatedMetric, Dimension,
    CalculatedDimension, CustomDimension,
    JoinedDimensionSource, JoinedDimensionColumn,
    FormatType, DataType, FilterType
)


# =============================================================================
# Schema Configuration Serializers
# =============================================================================

class SchemaConfigSerializer(serializers.ModelSerializer):
    """Read serializer for SchemaConfig with related metrics/dimensions."""
    metrics_count = serializers.SerializerMethodField()
    dimensions_count = serializers.SerializerMethodField()
    # Include nested calculated_metrics and dimensions arrays for frontend compatibility
    calculated_metrics = serializers.SerializerMethodField()
    dimensions = serializers.SerializerMethodField()
    # Include empty base_metrics for frontend compatibility (Django only has calculated_metrics)
    base_metrics = serializers.SerializerMethodField()

    class Meta:
        model = SchemaConfig
        fields = [
            'id', 'bigquery_table', 'primary_sort_metric', 'avg_per_day_metric',
            'pagination_threshold', 'version', 'metrics_count', 'dimensions_count',
            'base_metrics', 'calculated_metrics', 'dimensions',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

    def get_metrics_count(self, obj):
        return obj.calculated_metrics.count()

    def get_dimensions_count(self, obj):
        return obj.dimensions.count()

    def get_base_metrics(self, obj):
        # Django doesn't use base_metrics, return empty array for compatibility
        return []

    def get_calculated_metrics(self, obj):
        from .serializers import CalculatedMetricListSerializer
        return CalculatedMetricListSerializer(obj.calculated_metrics.all(), many=True).data

    def get_dimensions(self, obj):
        from .serializers import DimensionListSerializer
        from .models import JoinedDimensionStatus

        # Get regular dimensions
        regular_dims = DimensionListSerializer(obj.dimensions.all(), many=True).data

        # Get joined dimensions from ready sources
        joined_dims = []
        for source in obj.joined_dimension_sources.filter(status=JoinedDimensionStatus.READY):
            for col in source.columns.all():
                joined_dims.append({
                    'id': col.dimension_id,
                    'column_name': col.source_column_name,  # For compatibility
                    'display_name': col.display_name,
                    'data_type': col.data_type,
                    'is_filterable': col.is_filterable,
                    'is_groupable': col.is_groupable,
                    'filter_type': col.filter_type,
                    'is_joined': True,  # Mark as joined dimension
                    'source_id': str(source.id),
                    'source_name': source.name,
                })

        return regular_dims + joined_dims


class SchemaConfigUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for pivot table settings."""
    class Meta:
        model = SchemaConfig
        fields = ['primary_sort_metric', 'avg_per_day_metric', 'pagination_threshold']


# =============================================================================
# Calculated Metric Serializers
# =============================================================================

class CalculatedMetricSerializer(serializers.ModelSerializer):
    """Read serializer for CalculatedMetric."""
    # Return metric_id as 'id' for frontend compatibility
    id = serializers.CharField(source='metric_id', read_only=True)

    class Meta:
        model = CalculatedMetric
        fields = [
            'id', 'display_name', 'formula', 'sql_expression',
            'depends_on', 'depends_on_base', 'depends_on_calculated',
            'depends_on_dimensions', 'format_type', 'decimal_places',
            'category', 'is_visible_by_default', 'sort_order', 'description',
            'created_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'sql_expression', 'depends_on', 'depends_on_base',
            'depends_on_calculated', 'depends_on_dimensions',
            'created_at', 'updated_at'
        ]


class CalculatedMetricCreateSerializer(serializers.ModelSerializer):
    """Create serializer for CalculatedMetric."""
    metric_id = serializers.CharField(
        required=False, allow_blank=True,
        help_text='Optional - auto-generated from display_name if not provided'
    )

    class Meta:
        model = CalculatedMetric
        fields = [
            'metric_id', 'display_name', 'formula', 'format_type',
            'decimal_places', 'category', 'is_visible_by_default',
            'sort_order', 'description'
        ]

    def validate_formula(self, value):
        """Basic formula syntax validation."""
        if not value or not value.strip():
            raise serializers.ValidationError("Formula cannot be empty")

        # Check for balanced braces
        if value.count('{') != value.count('}'):
            raise serializers.ValidationError("Unbalanced braces in formula")

        # Check for balanced parentheses
        if value.count('(') != value.count(')'):
            raise serializers.ValidationError("Unbalanced parentheses in formula")

        # Check for empty braces
        if '{}' in value:
            raise serializers.ValidationError("Empty metric reference found")

        return value


class CalculatedMetricUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for CalculatedMetric (partial updates)."""
    class Meta:
        model = CalculatedMetric
        fields = [
            'display_name', 'formula', 'format_type', 'decimal_places',
            'category', 'is_visible_by_default', 'sort_order', 'description'
        ]


class CalculatedMetricListSerializer(serializers.ModelSerializer):
    """Serializer for listing metrics - includes formula fields for display."""
    # Return metric_id as 'id' for frontend compatibility
    id = serializers.CharField(source='metric_id', read_only=True)

    class Meta:
        model = CalculatedMetric
        fields = [
            'id', 'display_name', 'formula', 'sql_expression',
            'depends_on', 'depends_on_base', 'depends_on_calculated',
            'depends_on_dimensions', 'format_type', 'decimal_places',
            'category', 'is_visible_by_default', 'sort_order'
        ]


# =============================================================================
# Dimension Serializers
# =============================================================================

class DimensionSerializer(serializers.ModelSerializer):
    """Read serializer for Dimension."""
    # Return dimension_id as 'id' for frontend compatibility
    id = serializers.CharField(source='dimension_id', read_only=True)

    class Meta:
        model = Dimension
        fields = [
            'id', 'column_name', 'display_name',
            'data_type', 'is_filterable', 'is_groupable',
            'sort_order', 'filter_type', 'description',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class DimensionCreateSerializer(serializers.ModelSerializer):
    """Create serializer for Dimension."""
    # Accept 'id' from frontend and map to 'dimension_id'
    id = serializers.CharField(source='dimension_id', required=False, allow_blank=True)

    class Meta:
        model = Dimension
        fields = [
            'id', 'dimension_id', 'column_name', 'display_name',
            'data_type', 'is_filterable', 'is_groupable',
            'sort_order', 'filter_type', 'description'
        ]
        extra_kwargs = {
            'dimension_id': {'required': False},  # Not required - auto-generated from display_name
            'column_name': {'required': False, 'allow_blank': True},  # Not required - auto-generated
        }


class DimensionUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for Dimension (partial updates)."""
    class Meta:
        model = Dimension
        fields = [
            'column_name', 'display_name', 'data_type',
            'is_filterable', 'is_groupable', 'sort_order',
            'filter_type', 'description'
        ]


class DimensionListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for listing dimensions."""
    # Return dimension_id as 'id' for frontend compatibility
    id = serializers.CharField(source='dimension_id', read_only=True)

    class Meta:
        model = Dimension
        fields = [
            'id', 'column_name', 'display_name',
            'data_type', 'is_filterable', 'is_groupable', 'filter_type'
        ]


# =============================================================================
# Calculated Dimension Serializers
# =============================================================================

class CalculatedDimensionSerializer(serializers.ModelSerializer):
    """Read serializer for CalculatedDimension."""
    # Return dimension_id as 'id' for frontend compatibility
    id = serializers.CharField(source='dimension_id', read_only=True)

    class Meta:
        model = CalculatedDimension
        fields = [
            'id', 'display_name', 'sql_expression',
            'depends_on', 'data_type', 'is_filterable', 'is_groupable',
            'sort_order', 'filter_type', 'description',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'depends_on', 'created_at', 'updated_at']


class CalculatedDimensionCreateSerializer(serializers.ModelSerializer):
    """Create serializer for CalculatedDimension."""
    dimension_id = serializers.CharField(
        required=False, allow_blank=True,
        help_text='Optional - auto-generated from display_name if not provided'
    )

    class Meta:
        model = CalculatedDimension
        fields = [
            'dimension_id', 'display_name', 'sql_expression',
            'data_type', 'is_filterable', 'is_groupable',
            'sort_order', 'filter_type', 'description'
        ]

    def validate_sql_expression(self, value):
        """Basic SQL expression validation."""
        if not value or not value.strip():
            raise serializers.ValidationError("SQL expression cannot be empty")

        # Check for balanced parentheses
        if value.count('(') != value.count(')'):
            raise serializers.ValidationError("Unbalanced parentheses in SQL expression")

        # Check for dangerous keywords
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'EXEC']
        value_upper = value.upper()
        for keyword in dangerous_keywords:
            if keyword in value_upper:
                raise serializers.ValidationError(f"Forbidden keyword in SQL: {keyword}")

        return value


class CalculatedDimensionUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for CalculatedDimension (partial updates)."""
    class Meta:
        model = CalculatedDimension
        fields = [
            'display_name', 'sql_expression', 'data_type',
            'is_filterable', 'is_groupable', 'sort_order',
            'filter_type', 'description'
        ]


# =============================================================================
# Custom Dimension Serializers
# =============================================================================

class CustomDimensionValueSerializer(serializers.Serializer):
    """Serializer for custom dimension value (date range)."""
    label = serializers.CharField(max_length=255)
    start_date = serializers.CharField(max_length=10)
    end_date = serializers.CharField(max_length=10)
    date_range_type = serializers.ChoiceField(
        choices=['absolute', 'relative'],
        default='absolute'
    )
    relative_date_preset = serializers.CharField(required=False, allow_blank=True)


class MetricConditionSerializer(serializers.Serializer):
    """Serializer for metric condition."""
    operator = serializers.ChoiceField(
        choices=['>', '<', '>=', '<=', '=', 'between', 'is_null', 'is_not_null']
    )
    value = serializers.FloatField(required=False, allow_null=True)
    value_max = serializers.FloatField(required=False, allow_null=True)


class MetricDimensionValueSerializer(serializers.Serializer):
    """Serializer for metric-based dimension value."""
    label = serializers.CharField(max_length=255)
    conditions = MetricConditionSerializer(many=True)


class CustomDimensionSerializer(serializers.ModelSerializer):
    """Read serializer for CustomDimension."""
    # Return dimension_type as 'type' for frontend compatibility
    type = serializers.CharField(source='dimension_type', read_only=True)
    values = serializers.SerializerMethodField()
    metric_values = serializers.SerializerMethodField()

    class Meta:
        model = CustomDimension
        fields = [
            'id', 'name', 'type', 'metric',
            'values', 'metric_values',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'type', 'created_at', 'updated_at']

    def get_values(self, obj):
        if obj.dimension_type == 'date_range':
            return obj.values_json
        return None

    def get_metric_values(self, obj):
        if obj.dimension_type == 'metric_condition':
            return obj.values_json
        return None


class CustomDimensionCreateSerializer(serializers.ModelSerializer):
    """Create serializer for CustomDimension."""
    values = CustomDimensionValueSerializer(many=True, required=False)
    metric_values = MetricDimensionValueSerializer(many=True, required=False)
    table_id = serializers.UUIDField(write_only=True, required=False, allow_null=True)
    # Accept 'type' from frontend, map to 'dimension_type' model field
    type = serializers.ChoiceField(
        choices=[('date_range', 'Date Range'), ('metric_condition', 'Metric Condition')],
        write_only=True,
        source='dimension_type'
    )

    class Meta:
        model = CustomDimension
        fields = ['name', 'type', 'metric', 'values', 'metric_values', 'table_id']

    def validate(self, data):
        dimension_type = data.get('dimension_type', 'date_range')

        if dimension_type == 'date_range':
            if not data.get('values'):
                raise serializers.ValidationError(
                    {'values': 'Values are required for date_range type'}
                )
        elif dimension_type == 'metric_condition':
            if not data.get('metric'):
                raise serializers.ValidationError(
                    {'metric': 'Metric is required for metric_condition type'}
                )
            if not data.get('metric_values'):
                raise serializers.ValidationError(
                    {'metric_values': 'Metric values are required for metric_condition type'}
                )

        return data

    def create(self, validated_data):
        table_id = validated_data.pop('table_id', None)

        # Get schema_config from table_id or use the first available
        if table_id:
            from apps.tables.models import BigQueryTable
            try:
                table = BigQueryTable.objects.get(id=table_id)
                schema_config = table.schema_config
            except BigQueryTable.DoesNotExist:
                raise serializers.ValidationError({'table_id': 'Table not found'})
        else:
            schema_config = SchemaConfig.objects.first()
            if not schema_config:
                raise serializers.ValidationError(
                    {'table_id': 'No schema configuration found. Please specify a table_id.'}
                )

        validated_data['schema_config'] = schema_config

        dimension_type = validated_data.get('dimension_type', 'date_range')

        if dimension_type == 'date_range':
            validated_data['values_json'] = validated_data.pop('values', [])
            validated_data.pop('metric_values', None)
        else:
            validated_data['values_json'] = validated_data.pop('metric_values', [])
            validated_data.pop('values', None)

        return super().create(validated_data)


class CustomDimensionUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for CustomDimension - allows editing all fields."""
    values = CustomDimensionValueSerializer(many=True, required=False)
    metric_values = MetricDimensionValueSerializer(many=True, required=False)

    class Meta:
        model = CustomDimension
        fields = ['name', 'metric', 'values', 'metric_values']

    def update(self, instance, validated_data):
        # Update name if provided
        if 'name' in validated_data:
            instance.name = validated_data['name']

        # Update metric if provided (for metric_condition type)
        if 'metric' in validated_data:
            instance.metric = validated_data['metric']

        # Update values_json based on dimension type
        if instance.dimension_type == 'date_range':
            if 'values' in validated_data:
                instance.values_json = validated_data['values']
        elif instance.dimension_type == 'metric_condition':
            if 'metric_values' in validated_data:
                instance.values_json = validated_data['metric_values']

        instance.save()
        return instance


# =============================================================================
# Custom Metric Serializers
# =============================================================================

class CustomMetricSerializer(serializers.ModelSerializer):
    """Read serializer for CustomMetric."""

    class Meta:
        from .models import CustomMetric
        model = CustomMetric
        fields = [
            'id', 'name', 'metric_id', 'source_metric',
            'aggregation_type', 'exclude_dimensions',
            'format_type', 'decimal_places', 'description',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class CustomMetricCreateSerializer(serializers.ModelSerializer):
    """Create serializer for CustomMetric."""
    table_id = serializers.UUIDField(write_only=True, required=False, allow_null=True)

    class Meta:
        from .models import CustomMetric
        model = CustomMetric
        fields = [
            'name', 'metric_id', 'source_metric',
            'aggregation_type', 'exclude_dimensions',
            'format_type', 'decimal_places', 'description',
            'table_id'
        ]

    def validate_metric_id(self, value):
        """Ensure metric_id is unique within the schema."""
        schema_config = self.context.get('schema_config')
        if schema_config:
            from .models import CustomMetric
            existing = CustomMetric.objects.filter(
                schema_config=schema_config,
                metric_id=value
            )
            if self.instance:
                existing = existing.exclude(pk=self.instance.pk)
            if existing.exists():
                raise serializers.ValidationError(
                    f"Custom metric with ID '{value}' already exists"
                )
        return value

    def create(self, validated_data):
        """Create custom metric with schema_config from table_id."""
        table_id = validated_data.pop('table_id', None)

        # Get schema_config from table_id or use the first available
        if table_id:
            from apps.tables.models import BigQueryTable
            try:
                table = BigQueryTable.objects.get(id=table_id)
                schema_config = table.schema_config
            except BigQueryTable.DoesNotExist:
                raise serializers.ValidationError({'table_id': 'Table not found'})
        else:
            # Fallback: use the first available schema_config
            schema_config = SchemaConfig.objects.first()
            if not schema_config:
                raise serializers.ValidationError(
                    {'table_id': 'No schema configuration found. Please specify a table_id.'}
                )

        validated_data['schema_config'] = schema_config
        return super().create(validated_data)


class CustomMetricUpdateSerializer(serializers.ModelSerializer):
    """Update serializer for CustomMetric."""

    class Meta:
        from .models import CustomMetric
        model = CustomMetric
        fields = [
            'name', 'source_metric', 'aggregation_type',
            'exclude_dimensions', 'format_type', 'decimal_places', 'description'
        ]


# =============================================================================
# Formula Validation Serializers
# =============================================================================

class FormulaValidationRequestSerializer(serializers.Serializer):
    """Request serializer for formula validation."""
    formula = serializers.CharField()


class FormulaValidationResponseSerializer(serializers.Serializer):
    """Response serializer for formula validation."""
    valid = serializers.BooleanField()
    errors = serializers.ListField(child=serializers.CharField(), default=list)
    sql_expression = serializers.CharField(default='')
    depends_on = serializers.ListField(child=serializers.CharField(), default=list)
    depends_on_base = serializers.ListField(child=serializers.CharField(), default=list)
    depends_on_calculated = serializers.ListField(child=serializers.CharField(), default=list)
    depends_on_dimensions = serializers.ListField(child=serializers.CharField(), default=list)


# =============================================================================
# Schema Detection Serializers
# =============================================================================

class SchemaDetectionResponseSerializer(serializers.Serializer):
    """Response serializer for schema auto-detection."""
    detected_dimensions = DimensionSerializer(many=True)
    column_count = serializers.IntegerField()
    warnings = serializers.ListField(child=serializers.CharField(), default=list)


# =============================================================================
# Full Schema Serializers (for export/import)
# =============================================================================

class FullSchemaSerializer(serializers.Serializer):
    """Full schema export/import serializer."""
    schema_config = SchemaConfigSerializer()
    calculated_metrics = CalculatedMetricSerializer(many=True)
    dimensions = DimensionSerializer(many=True)
    calculated_dimensions = CalculatedDimensionSerializer(many=True)
    custom_dimensions = CustomDimensionSerializer(many=True)


# =============================================================================
# Joined Dimension Serializers
# =============================================================================

class JoinedDimensionColumnSerializer(serializers.ModelSerializer):
    """Read serializer for JoinedDimensionColumn."""
    id = serializers.CharField(source='dimension_id', read_only=True)

    class Meta:
        model = JoinedDimensionColumn
        fields = [
            'id', 'source_column_name', 'display_name', 'data_type',
            'is_filterable', 'is_groupable', 'filter_type',
            'sort_order', 'description'
        ]


class JoinedDimensionColumnCreateSerializer(serializers.Serializer):
    """Create serializer for defining columns during upload."""
    source_column_name = serializers.CharField(max_length=100)
    dimension_id = serializers.CharField(max_length=100, required=False, allow_blank=True)
    display_name = serializers.CharField(max_length=255)
    data_type = serializers.ChoiceField(
        choices=['STRING', 'INTEGER', 'FLOAT', 'BOOLEAN'],
        default='STRING'
    )
    is_filterable = serializers.BooleanField(default=True)
    is_groupable = serializers.BooleanField(default=True)
    filter_type = serializers.ChoiceField(
        choices=['single', 'multi', 'boolean'],
        default='multi'
    )


class JoinedDimensionSourceSerializer(serializers.ModelSerializer):
    """Read serializer for JoinedDimensionSource with columns."""
    columns = JoinedDimensionColumnSerializer(many=True, read_only=True)
    bq_table_path = serializers.CharField(read_only=True)

    class Meta:
        model = JoinedDimensionSource
        fields = [
            'id', 'name', 'original_filename', 'file_type',
            'join_key_column', 'target_dimension_id',
            'bq_project', 'bq_dataset', 'bq_table', 'bq_table_path',
            'status', 'error_message', 'row_count', 'uploaded_at',
            'columns', 'created_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'bq_table', 'bq_table_path', 'status', 'error_message',
            'row_count', 'uploaded_at', 'created_at', 'updated_at'
        ]


class JoinedDimensionSourceCreateSerializer(serializers.Serializer):
    """Create serializer for file upload and source creation."""
    name = serializers.CharField(max_length=255)
    join_key_column = serializers.CharField(max_length=100)
    target_dimension_id = serializers.CharField(max_length=100)
    columns = JoinedDimensionColumnCreateSerializer(many=True)
    bq_project = serializers.CharField(max_length=255)
    bq_dataset = serializers.CharField(max_length=255)

    def validate_columns(self, value):
        """Ensure at least one column is provided."""
        if not value:
            raise serializers.ValidationError(
                'At least one dimension column must be selected'
            )
        return value


class JoinedDimensionSourceUpdateSerializer(serializers.Serializer):
    """Update serializer for JoinedDimensionSource - allows editing name and columns."""
    name = serializers.CharField(max_length=255, required=False)
    columns = JoinedDimensionColumnCreateSerializer(many=True, required=False)


class FilePreviewColumnSerializer(serializers.Serializer):
    """Serializer for column preview data."""
    name = serializers.CharField()
    inferred_type = serializers.CharField()
    sample_values = serializers.ListField(child=serializers.JSONField())
    null_count = serializers.IntegerField()
    unique_count = serializers.IntegerField()


class FilePreviewSerializer(serializers.Serializer):
    """Response serializer for file preview."""
    row_count = serializers.IntegerField()
    columns = FilePreviewColumnSerializer(many=True)
    preview_rows = serializers.ListField(child=serializers.DictField())
