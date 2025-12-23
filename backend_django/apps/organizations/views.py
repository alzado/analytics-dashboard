"""
Organization views.
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.db.models import Q

from .models import Organization, OrganizationMembership, OrgRole
from .serializers import (
    OrganizationSerializer,
    OrganizationCreateSerializer,
    OrganizationUpdateSerializer,
    MembershipSerializer,
    InviteMemberSerializer,
    UpdateMemberRoleSerializer
)
from apps.users.models import User


class OrganizationViewSet(viewsets.ModelViewSet):
    """ViewSet for organizations."""
    permission_classes = []
    lookup_field = 'id'

    def get_queryset(self):
        """Return organizations the user is a member of."""
        return Organization.objects.filter(
            memberships__user=self.request.user
        ).distinct()

    def get_serializer_class(self):
        if self.action == 'create':
            return OrganizationCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return OrganizationUpdateSerializer
        return OrganizationSerializer

    def perform_create(self, serializer):
        """Create organization and add creator as owner."""
        organization = serializer.save()
        OrganizationMembership.objects.create(
            user=self.request.user,
            organization=organization,
            role=OrgRole.OWNER
        )

    def perform_destroy(self, instance):
        """Only owner can delete organization."""
        membership = instance.memberships.filter(user=self.request.user).first()
        if not membership or membership.role != OrgRole.OWNER:
            return Response(
                {'error': 'Only owner can delete organization'},
                status=status.HTTP_403_FORBIDDEN
            )
        instance.delete()

    @action(detail=True, methods=['get', 'post'])
    def members(self, request, id=None):
        """List or add members to organization."""
        organization = self.get_object()

        if request.method == 'GET':
            memberships = organization.memberships.select_related('user').all()
            serializer = MembershipSerializer(memberships, many=True)
            return Response(serializer.data)

        # POST - add member
        serializer = InviteMemberSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Check if requester has permission to add members
        requester_membership = organization.memberships.filter(user=request.user).first()
        if not requester_membership or requester_membership.role not in [OrgRole.OWNER, OrgRole.ADMIN]:
            return Response(
                {'error': 'Only owner or admin can add members'},
                status=status.HTTP_403_FORBIDDEN
            )

        # Find user by email
        try:
            user = User.objects.get(email=serializer.validated_data['email'])
        except User.DoesNotExist:
            return Response(
                {'error': 'User not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Check if already a member
        if organization.memberships.filter(user=user).exists():
            return Response(
                {'error': 'User is already a member'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Create membership
        membership = OrganizationMembership.objects.create(
            user=user,
            organization=organization,
            role=serializer.validated_data['role']
        )

        return Response(
            MembershipSerializer(membership).data,
            status=status.HTTP_201_CREATED
        )

    @action(detail=True, methods=['put', 'delete'], url_path='members/(?P<user_id>[^/.]+)')
    def member_detail(self, request, id=None, user_id=None):
        """Update or remove a member."""
        organization = self.get_object()

        # Check if requester has permission
        requester_membership = organization.memberships.filter(user=request.user).first()
        if not requester_membership or requester_membership.role not in [OrgRole.OWNER, OrgRole.ADMIN]:
            return Response(
                {'error': 'Only owner or admin can manage members'},
                status=status.HTTP_403_FORBIDDEN
            )

        # Get target membership
        membership = get_object_or_404(
            organization.memberships,
            user_id=user_id
        )

        if request.method == 'PUT':
            serializer = UpdateMemberRoleSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            # Only owner can change roles
            if requester_membership.role != OrgRole.OWNER:
                return Response(
                    {'error': 'Only owner can change roles'},
                    status=status.HTTP_403_FORBIDDEN
                )

            # Cannot change owner's role
            if membership.role == OrgRole.OWNER:
                return Response(
                    {'error': 'Cannot change owner role'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            membership.role = serializer.validated_data['role']
            membership.save()
            return Response(MembershipSerializer(membership).data)

        # DELETE - remove member
        if membership.role == OrgRole.OWNER:
            return Response(
                {'error': 'Cannot remove owner'},
                status=status.HTTP_400_BAD_REQUEST
            )

        membership.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
