"""
User serializers.
"""
from rest_framework import serializers
from .models import User


class UserSerializer(serializers.ModelSerializer):
    """Serializer for User model."""
    has_bigquery_access = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            'id', 'email', 'name', 'avatar_url',
            'is_active', 'created_at', 'updated_at', 'last_login_at',
            'has_bigquery_access'
        ]
        read_only_fields = ['id', 'email', 'google_id', 'created_at', 'updated_at', 'last_login_at', 'has_bigquery_access']

    def get_has_bigquery_access(self, obj):
        return obj.has_bigquery_access()


class UserProfileUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating user profile."""

    class Meta:
        model = User
        fields = ['name', 'avatar_url']


class GoogleAuthSerializer(serializers.Serializer):
    """Serializer for Google OAuth authentication."""
    # Accept both 'id_token' (frontend sends this) and 'token' (alias)
    id_token = serializers.CharField(required=True)


class RefreshTokenSerializer(serializers.Serializer):
    """Serializer for token refresh."""
    refresh_token = serializers.CharField(required=True)


class AuthTokenResponseSerializer(serializers.Serializer):
    """Serializer for authentication response."""
    access_token = serializers.CharField()
    refresh_token = serializers.CharField()
    token_type = serializers.CharField(default='Bearer')
    user = UserSerializer()


class GCPOAuthCallbackSerializer(serializers.Serializer):
    """Serializer for GCP OAuth callback."""
    code = serializers.CharField(required=True, help_text="Authorization code from Google")
    state = serializers.CharField(required=False, allow_blank=True)
    redirect_uri = serializers.CharField(required=False, allow_blank=True)


class GCPOAuthStatusSerializer(serializers.Serializer):
    """Serializer for GCP OAuth status response."""
    has_bigquery_access = serializers.BooleanField()
    scopes = serializers.ListField(child=serializers.CharField())
    token_expiry = serializers.DateTimeField(allow_null=True)


class GCPProjectSerializer(serializers.Serializer):
    """Serializer for GCP project."""
    project_id = serializers.CharField()
    name = serializers.CharField()
    project_number = serializers.CharField()
