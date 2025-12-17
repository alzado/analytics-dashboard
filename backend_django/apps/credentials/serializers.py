"""
Credentials serializers.
"""
from rest_framework import serializers
from .models import GCPCredential, CredentialType


class GCPCredentialSerializer(serializers.ModelSerializer):
    """Serializer for GCP credentials (without sensitive data)."""

    class Meta:
        model = GCPCredential
        fields = [
            'id', 'name', 'credential_type', 'project_id',
            'is_default', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class GCPCredentialCreateSerializer(serializers.Serializer):
    """Serializer for creating GCP credentials."""
    name = serializers.CharField(max_length=255)
    credentials_json = serializers.CharField()
    organization_id = serializers.UUIDField(required=False, allow_null=True)
    is_default = serializers.BooleanField(default=False)

    def validate_credentials_json(self, value):
        """Validate that credentials JSON is valid."""
        import json
        try:
            creds = json.loads(value)
            if 'project_id' not in creds:
                raise serializers.ValidationError("Credentials must contain 'project_id'")
            return value
        except json.JSONDecodeError:
            raise serializers.ValidationError("Invalid JSON")


class GCPCredentialUpdateSerializer(serializers.Serializer):
    """Serializer for updating GCP credentials."""
    name = serializers.CharField(max_length=255, required=False)
    is_default = serializers.BooleanField(required=False)
