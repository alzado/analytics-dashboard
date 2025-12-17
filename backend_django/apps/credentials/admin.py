"""
Admin configuration for credentials app.
"""
from django.contrib import admin
from .models import GCPCredential


@admin.register(GCPCredential)
class GCPCredentialAdmin(admin.ModelAdmin):
    """Admin for GCPCredential model."""

    list_display = ('name', 'project_id', 'credential_type', 'user', 'organization', 'is_default', 'created_at')
    list_filter = ('credential_type', 'is_default', 'created_at')
    search_fields = ('name', 'project_id', 'user__email', 'organization__name')
    autocomplete_fields = ['user', 'organization']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'name', 'project_id', 'credential_type', 'is_default')}),
        ('Ownership', {'fields': ('user', 'organization')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )

    # Don't show encrypted credentials in admin
    exclude = ('encrypted_credentials',)
