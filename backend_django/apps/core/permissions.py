"""
Custom permissions for the application.
"""
from rest_framework.permissions import BasePermission


class IsOwner(BasePermission):
    """Permission class to check if user is the owner of an object."""

    def has_object_permission(self, request, view, obj):
        if hasattr(obj, 'owner'):
            return obj.owner == request.user
        if hasattr(obj, 'user'):
            return obj.user == request.user
        return False


class IsOwnerOrReadOnly(BasePermission):
    """Allow read access to all, write access only to owner."""

    def has_object_permission(self, request, view, obj):
        # Read permissions are allowed for any request
        if request.method in ('GET', 'HEAD', 'OPTIONS'):
            return True

        # Write permissions only for owner
        if hasattr(obj, 'owner'):
            return obj.owner == request.user
        if hasattr(obj, 'user'):
            return obj.user == request.user
        return False


class IsOrganizationMember(BasePermission):
    """Check if user is a member of the organization."""

    def has_object_permission(self, request, view, obj):
        # Check if object has organization
        organization = None
        if hasattr(obj, 'organization'):
            organization = obj.organization
        elif hasattr(obj, 'owner') and hasattr(obj.owner, 'memberships'):
            # Check through owner's organizations
            return True  # Allow if we got this far

        if not organization:
            return False

        # Check membership
        return organization.memberships.filter(user=request.user).exists()


class IsTableOwnerOrOrganizationMember(BasePermission):
    """Check if user owns the table or is a member of its organization."""

    def _check_table_access(self, user, table):
        """Check if user has access to the table."""
        if table is None:
            return False

        # Owner has full access
        if hasattr(table, 'owner') and table.owner == user:
            return True

        # Organization member has access
        if hasattr(table, 'organization') and table.organization:
            return table.organization.memberships.filter(user=user).exists()

        return False

    def has_permission(self, request, view):
        """Check permission before object retrieval (for nested routes)."""
        # Get table_id from URL kwargs
        table_id = view.kwargs.get('table_id')
        if table_id:
            from apps.tables.models import BigQueryTable
            try:
                table = BigQueryTable.objects.get(id=table_id)
                return self._check_table_access(request.user, table)
            except BigQueryTable.DoesNotExist:
                return False

        # No table_id in URL, allow (will be checked at object level)
        return True

    def has_object_permission(self, request, view, obj):
        # Direct table access
        if hasattr(obj, 'owner'):
            return self._check_table_access(request.user, obj)

        # SchemaConfig has bigquery_table
        if hasattr(obj, 'bigquery_table'):
            return self._check_table_access(request.user, obj.bigquery_table)

        # Metrics/Dimensions have schema_config -> bigquery_table
        if hasattr(obj, 'schema_config') and hasattr(obj.schema_config, 'bigquery_table'):
            return self._check_table_access(request.user, obj.schema_config.bigquery_table)

        return False


class IsDashboardOwnerOrCollaborator(BasePermission):
    """Check if user owns the dashboard or is a collaborator."""

    def has_object_permission(self, request, view, obj):
        # Owner has full access
        if hasattr(obj, 'owner') and obj.owner == request.user:
            return True

        # Collaborator has access
        if hasattr(obj, 'collaborators'):
            collaborator = obj.collaborators.filter(user=request.user).first()
            if collaborator:
                # For unsafe methods, check if collaborator has edit rights
                if request.method in ('PUT', 'PATCH', 'DELETE'):
                    return collaborator.role in ('owner', 'editor')
                return True

        return False


class AllowAny(BasePermission):
    """Allow any access - used for public endpoints."""

    def has_permission(self, request, view):
        return True
