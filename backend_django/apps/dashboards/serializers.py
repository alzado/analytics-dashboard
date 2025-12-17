"""
Serializers for dashboard API.
"""
from rest_framework import serializers
from .models import Dashboard, Widget, WidgetType


class WidgetSerializer(serializers.ModelSerializer):
    """Serializer for dashboard widgets."""
    table_id = serializers.UUIDField(source='table.id', read_only=True, allow_null=True)
    parent_dashboard_id = serializers.UUIDField(source='dashboard.id', read_only=True, allow_null=True)
    last_edited_at = serializers.DateTimeField(source='updated_at', read_only=True)

    class Meta:
        model = Widget
        fields = [
            'id', 'name', 'widget_type', 'table_id', 'config',
            'position', 'sort_order', 'parent_dashboard_id', 'is_draft',
            'last_edited_at', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'last_edited_at']


class WidgetCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating widgets."""
    table_id = serializers.UUIDField(required=False, allow_null=True)

    class Meta:
        model = Widget
        fields = ['name', 'widget_type', 'table_id', 'config', 'position', 'sort_order']

    def create(self, validated_data):
        table_id = validated_data.pop('table_id', None)
        if table_id:
            from apps.tables.models import BigQueryTable
            validated_data['table'] = BigQueryTable.objects.get(id=table_id)
        return super().create(validated_data)


class WidgetUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating widgets."""
    table_id = serializers.UUIDField(required=False, allow_null=True)

    class Meta:
        model = Widget
        fields = ['name', 'widget_type', 'table_id', 'config', 'position', 'sort_order']

    def update(self, instance, validated_data):
        table_id = validated_data.pop('table_id', None)
        if table_id:
            from apps.tables.models import BigQueryTable
            validated_data['table'] = BigQueryTable.objects.get(id=table_id)
        elif table_id is None and 'table_id' in self.initial_data:
            validated_data['table'] = None
        return super().update(instance, validated_data)


class DashboardSerializer(serializers.ModelSerializer):
    """Serializer for dashboards with nested widgets."""
    widgets = WidgetSerializer(many=True, read_only=True)
    owner_name = serializers.CharField(source='owner.email', read_only=True)
    default_table_id = serializers.UUIDField(
        source='default_table.id',
        read_only=True,
        allow_null=True
    )

    class Meta:
        model = Dashboard
        fields = [
            'id', 'name', 'description', 'owner_name', 'default_table_id',
            'layout', 'widgets', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'owner_name', 'widgets', 'created_at', 'updated_at']


class DashboardCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating dashboards."""
    default_table_id = serializers.UUIDField(required=False, allow_null=True)
    organization_id = serializers.UUIDField(required=False, allow_null=True)
    description = serializers.CharField(required=False, allow_blank=True, allow_null=True, default='')

    class Meta:
        model = Dashboard
        fields = ['name', 'description', 'default_table_id', 'organization_id', 'layout']

    def validate_description(self, value):
        """Convert null to empty string."""
        return value if value is not None else ''

    def create(self, validated_data):
        table_id = validated_data.pop('default_table_id', None)
        org_id = validated_data.pop('organization_id', None)

        if table_id:
            from apps.tables.models import BigQueryTable
            validated_data['default_table'] = BigQueryTable.objects.get(id=table_id)

        if org_id:
            from apps.organizations.models import Organization
            validated_data['organization'] = Organization.objects.get(id=org_id)

        return super().create(validated_data)


class DashboardUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating dashboards."""
    default_table_id = serializers.UUIDField(required=False, allow_null=True)

    class Meta:
        model = Dashboard
        fields = ['name', 'description', 'default_table_id', 'layout']

    def update(self, instance, validated_data):
        table_id = validated_data.pop('default_table_id', None)
        if table_id:
            from apps.tables.models import BigQueryTable
            validated_data['default_table'] = BigQueryTable.objects.get(id=table_id)
        elif table_id is None and 'default_table_id' in self.initial_data:
            validated_data['default_table'] = None
        return super().update(instance, validated_data)


class DashboardListSerializer(serializers.ModelSerializer):
    """Serializer for dashboard list (without widgets)."""
    owner_name = serializers.CharField(source='owner.email', read_only=True)
    widget_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Dashboard
        fields = [
            'id', 'name', 'description', 'owner_name',
            'widget_count', 'created_at', 'updated_at'
        ]


class StandaloneWidgetCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating standalone widgets."""
    table_id = serializers.UUIDField(required=False, allow_null=True)

    class Meta:
        model = Widget
        fields = ['name', 'widget_type', 'table_id', 'config', 'position', 'sort_order', 'is_draft']

    def create(self, validated_data):
        table_id = validated_data.pop('table_id', None)
        if table_id:
            from apps.tables.models import BigQueryTable
            validated_data['table'] = BigQueryTable.objects.get(id=table_id)
        # Set owner from context
        validated_data['owner'] = self.context['request'].user
        return super().create(validated_data)
