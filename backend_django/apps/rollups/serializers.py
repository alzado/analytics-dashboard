"""
Serializers for rollups API.
"""
from rest_framework import serializers
from .models import Rollup, RollupConfig, RollupStatus


class RollupSerializer(serializers.ModelSerializer):
    """Serializer for rollup details."""
    full_rollup_path = serializers.ReadOnlyField()
    is_ready = serializers.ReadOnlyField()

    class Meta:
        model = Rollup
        fields = [
            'id', 'name', 'rollup_id', 'rollup_project', 'rollup_dataset',
            'rollup_table', 'full_rollup_path', 'dimensions', 'metrics',
            'is_searchable', 'status', 'error_message', 'row_count',
            'size_bytes', 'last_refresh_at', 'refresh_duration_seconds',
            'min_date', 'max_date', 'is_ready', 'created_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'full_rollup_path', 'status', 'error_message',
            'row_count', 'size_bytes', 'last_refresh_at',
            'refresh_duration_seconds', 'is_ready', 'created_at', 'updated_at'
        ]


class RollupCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating rollups."""
    # Accept frontend field names and map to model fields
    display_name = serializers.CharField(source='name', required=False, allow_blank=True)
    # description is accepted but ignored (model doesn't have it)
    description = serializers.CharField(required=False, allow_blank=True, allow_null=True, write_only=True)
    target_project = serializers.CharField(source='rollup_project', required=False, allow_blank=True, allow_null=True)
    target_dataset = serializers.CharField(source='rollup_dataset', required=False, allow_blank=True, allow_null=True)
    target_table_name = serializers.CharField(source='rollup_table', required=False, allow_blank=True, allow_null=True)

    class Meta:
        model = Rollup
        fields = [
            'display_name', 'description', 'target_project', 'target_dataset',
            'target_table_name', 'dimensions', 'is_searchable'
        ]

    def create(self, validated_data):
        # Remove description since model doesn't have it
        validated_data.pop('description', None)
        return super().create(validated_data)


class RollupListSerializer(serializers.ModelSerializer):
    """Serializer for rollup list - uses frontend field names."""
    is_ready = serializers.ReadOnlyField()
    # Map model fields to frontend expected field names
    display_name = serializers.CharField(source='name', read_only=True)
    target_project = serializers.CharField(source='rollup_project', read_only=True)
    target_dataset = serializers.CharField(source='rollup_dataset', read_only=True)
    target_table_name = serializers.CharField(source='rollup_table', read_only=True)
    last_refresh_error = serializers.CharField(source='error_message', read_only=True)

    class Meta:
        model = Rollup
        fields = [
            'id', 'display_name', 'dimensions', 'is_searchable',
            'status', 'row_count', 'size_bytes', 'last_refresh_at', 'is_ready',
            'target_project', 'target_dataset', 'target_table_name',
            'last_refresh_error', 'created_at', 'updated_at'
        ]


class RollupConfigSerializer(serializers.ModelSerializer):
    """Serializer for rollup configuration."""
    class Meta:
        model = RollupConfig
        fields = [
            'id', 'default_project', 'default_dataset',
            'auto_refresh_enabled', 'refresh_schedule_cron',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class RollupRefreshResponseSerializer(serializers.Serializer):
    """Serializer for rollup refresh response."""
    success = serializers.BooleanField()
    message = serializers.CharField()
    rollup_id = serializers.UUIDField()
    status = serializers.CharField()


class RollupPreviewSqlSerializer(serializers.Serializer):
    """Serializer for rollup SQL preview."""
    create_sql = serializers.CharField()
    refresh_sql = serializers.CharField()
    rollup_table_path = serializers.CharField()


class RollupStatusResponseSerializer(serializers.Serializer):
    """Serializer for rollup status response."""
    rollup_id = serializers.UUIDField()
    name = serializers.CharField()
    status = serializers.CharField()
    last_refresh_at = serializers.DateTimeField(allow_null=True)
    row_count = serializers.IntegerField()
    size_bytes = serializers.IntegerField()
    error_message = serializers.CharField(allow_blank=True)
