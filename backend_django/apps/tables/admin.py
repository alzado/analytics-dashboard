"""
Admin configuration for tables app.
"""
from django.contrib import admin
from .models import BigQueryTable, AppSettings


@admin.register(BigQueryTable)
class BigQueryTableAdmin(admin.ModelAdmin):
    """Admin for BigQueryTable model."""

    list_display = ('name', 'full_table_path', 'owner', 'organization', 'visibility', 'last_used_at')
    list_filter = ('visibility', 'created_at', 'organization')
    search_fields = ('name', 'project_id', 'dataset', 'table_name', 'owner__email')
    autocomplete_fields = ['owner', 'organization', 'gcp_credential']
    readonly_fields = ('id', 'created_at', 'updated_at', 'last_used_at')
    date_hierarchy = 'created_at'

    fieldsets = (
        (None, {'fields': ('id', 'name', 'description')}),
        ('BigQuery Location', {'fields': ('project_id', 'dataset', 'table_name', 'billing_project')}),
        ('Ownership', {'fields': ('owner', 'organization', 'gcp_credential')}),
        ('Access Control', {'fields': ('visibility', 'allowed_min_date', 'allowed_max_date')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at', 'last_used_at')}),
    )


@admin.register(AppSettings)
class AppSettingsAdmin(admin.ModelAdmin):
    """Admin for AppSettings model (singleton)."""

    list_display = ('id', 'default_billing_project', 'updated_at')
    readonly_fields = ('id', 'updated_at')

    def has_add_permission(self, request):
        # Only allow one instance
        return not AppSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False
