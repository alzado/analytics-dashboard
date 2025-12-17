"""
Organization serializers.
"""
from rest_framework import serializers
from django.utils.text import slugify
from .models import Organization, OrganizationMembership, OrgRole
from apps.users.serializers import UserSerializer


class OrganizationSerializer(serializers.ModelSerializer):
    """Serializer for Organization model."""
    member_count = serializers.SerializerMethodField()

    class Meta:
        model = Organization
        fields = ['id', 'name', 'slug', 'description', 'avatar_url', 'member_count', 'created_at', 'updated_at']
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']

    def get_member_count(self, obj):
        return obj.memberships.count()


class OrganizationCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating an organization."""

    class Meta:
        model = Organization
        fields = ['name', 'description', 'avatar_url']

    def create(self, validated_data):
        # Generate unique slug from name
        base_slug = slugify(validated_data['name'])
        slug = base_slug
        counter = 1
        while Organization.objects.filter(slug=slug).exists():
            slug = f"{base_slug}-{counter}"
            counter += 1

        validated_data['slug'] = slug
        return super().create(validated_data)


class OrganizationUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating an organization."""

    class Meta:
        model = Organization
        fields = ['name', 'description', 'avatar_url']


class MembershipSerializer(serializers.ModelSerializer):
    """Serializer for organization membership."""
    user = UserSerializer(read_only=True)

    class Meta:
        model = OrganizationMembership
        fields = ['id', 'user', 'role', 'joined_at']
        read_only_fields = ['id', 'joined_at']


class InviteMemberSerializer(serializers.Serializer):
    """Serializer for inviting a member to an organization."""
    email = serializers.EmailField()
    role = serializers.ChoiceField(choices=OrgRole.choices, default=OrgRole.MEMBER)


class UpdateMemberRoleSerializer(serializers.Serializer):
    """Serializer for updating a member's role."""
    role = serializers.ChoiceField(choices=OrgRole.choices)
