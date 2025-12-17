"""
BigQuery Table serializers.
"""
from rest_framework import serializers
from .models import BigQueryTable, Visibility


class BigQueryTableSerializer(serializers.ModelSerializer):
    """Serializer for BigQuery table."""
    full_table_path = serializers.ReadOnlyField()
    owner_email = serializers.EmailField(source='owner.email', read_only=True)

    class Meta:
        model = BigQueryTable
        fields = [
            'id', 'name', 'description', 'project_id', 'dataset',
            'table_name', 'billing_project', 'full_table_path',
            'visibility', 'allowed_min_date', 'allowed_max_date',
            'owner_email', 'organization',
            'created_at', 'updated_at', 'last_used_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'last_used_at']


class BigQueryTableCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating a BigQuery table."""
    credentials_json = serializers.CharField(write_only=True, required=False, allow_blank=True)
    credential_id = serializers.UUIDField(write_only=True, required=False)

    class Meta:
        model = BigQueryTable
        fields = [
            'name', 'description', 'project_id', 'dataset', 'table_name',
            'billing_project', 'organization', 'visibility',
            'allowed_min_date', 'allowed_max_date',
            'credentials_json', 'credential_id'
        ]


class BigQueryTableUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating a BigQuery table."""

    class Meta:
        model = BigQueryTable
        fields = [
            'name', 'description', 'billing_project',
            'visibility', 'allowed_min_date', 'allowed_max_date'
        ]


class BigQueryConfigUpdateSerializer(serializers.Serializer):
    """Serializer for updating BigQuery connection config."""
    project_id = serializers.CharField(max_length=255, required=False)
    dataset = serializers.CharField(max_length=255, required=False)
    table_name = serializers.CharField(max_length=255, required=False)
    billing_project = serializers.CharField(max_length=255, required=False, allow_blank=True)
    credentials_json = serializers.CharField(write_only=True, required=False, allow_blank=True)
    credential_id = serializers.UUIDField(write_only=True, required=False)
