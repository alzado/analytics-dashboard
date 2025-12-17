"""
Admin configuration for users app.
"""
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import User


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """Admin for custom User model."""

    list_display = ('email', 'name', 'is_active', 'is_staff', 'has_bigquery', 'created_at')
    list_filter = ('is_active', 'is_staff', 'is_superuser', 'created_at')
    search_fields = ('email', 'name', 'google_id')
    ordering = ('-created_at',)

    fieldsets = (
        (None, {'fields': ('email', 'name')}),
        ('Google OAuth', {'fields': ('google_id', 'avatar_url')}),
        ('GCP Access', {
            'fields': ('gcp_token_expiry', 'gcp_scopes'),
            'classes': ('collapse',),
        }),
        ('Permissions', {'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at', 'last_login_at')}),
    )

    readonly_fields = ('id', 'google_id', 'created_at', 'updated_at', 'last_login_at',
                       'gcp_token_expiry', 'gcp_scopes')

    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'name', 'password1', 'password2'),
        }),
    )

    @admin.display(boolean=True, description='BigQuery Access')
    def has_bigquery(self, obj):
        return obj.has_bigquery_access()
