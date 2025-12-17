"""
Admin configuration for organizations app.
"""
from django.contrib import admin
from .models import Organization, OrganizationMembership


class MembershipInline(admin.TabularInline):
    """Inline for organization memberships."""
    model = OrganizationMembership
    extra = 0
    autocomplete_fields = ['user']
    readonly_fields = ('id', 'joined_at')


@admin.register(Organization)
class OrganizationAdmin(admin.ModelAdmin):
    """Admin for Organization model."""

    list_display = ('name', 'slug', 'member_count', 'created_at')
    list_filter = ('created_at',)
    search_fields = ('name', 'slug', 'description')
    prepopulated_fields = {'slug': ('name',)}
    readonly_fields = ('id', 'created_at', 'updated_at')
    inlines = [MembershipInline]

    fieldsets = (
        (None, {'fields': ('id', 'name', 'slug', 'description', 'avatar_url')}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )

    @admin.display(description='Members')
    def member_count(self, obj):
        return obj.memberships.count()


@admin.register(OrganizationMembership)
class OrganizationMembershipAdmin(admin.ModelAdmin):
    """Admin for OrganizationMembership model."""

    list_display = ('user', 'organization', 'role', 'joined_at')
    list_filter = ('role', 'joined_at')
    search_fields = ('user__email', 'user__name', 'organization__name')
    autocomplete_fields = ['user', 'organization']
    readonly_fields = ('id', 'joined_at')
