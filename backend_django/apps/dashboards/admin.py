"""
Admin configuration for dashboards app.
"""
from django.contrib import admin
from .models import Dashboard, Widget


class WidgetInline(admin.TabularInline):
    """Inline for dashboard widgets."""
    model = Widget
    extra = 0
    fields = ('name', 'widget_type', 'table', 'sort_order', 'is_draft')
    readonly_fields = ('id',)


@admin.register(Dashboard)
class DashboardAdmin(admin.ModelAdmin):
    """Admin for Dashboard model."""

    list_display = ('name', 'owner', 'organization', 'widget_count', 'default_table', 'updated_at')
    list_filter = ('created_at', 'organization')
    search_fields = ('name', 'description', 'owner__email', 'organization__name')
    autocomplete_fields = ['owner', 'organization', 'default_table']
    readonly_fields = ('id', 'created_at', 'updated_at')
    inlines = [WidgetInline]

    fieldsets = (
        (None, {'fields': ('id', 'name', 'description')}),
        ('Ownership', {'fields': ('owner', 'organization')}),
        ('Configuration', {'fields': ('default_table', 'layout')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )

    @admin.display(description='Widgets')
    def widget_count(self, obj):
        return obj.widgets.count()


@admin.register(Widget)
class WidgetAdmin(admin.ModelAdmin):
    """Admin for Widget model."""

    list_display = ('name', 'widget_type', 'dashboard', 'owner', 'table', 'is_draft', 'sort_order')
    list_filter = ('widget_type', 'is_draft', 'created_at')
    search_fields = ('name', 'dashboard__name', 'owner__email')
    autocomplete_fields = ['dashboard', 'owner', 'table']
    readonly_fields = ('id', 'created_at', 'updated_at')

    fieldsets = (
        (None, {'fields': ('id', 'name', 'widget_type', 'is_draft')}),
        ('Ownership', {'fields': ('dashboard', 'owner')}),
        ('Data Source', {'fields': ('table',)}),
        ('Configuration', {'fields': ('config', 'position', 'sort_order')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )
