"""
Core views - health checks and application settings.
"""
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from django.conf import settings as django_settings


@api_view(['GET'])
@permission_classes([AllowAny])
def root_view(request):
    """Root endpoint - API information."""
    return Response({
        'name': 'Search Analytics API',
        'version': '2.0.0',
        'framework': 'Django REST Framework',
        'status': 'running'
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def health_check(request):
    """Health check endpoint."""
    return Response({
        'status': 'healthy',
        'database': 'connected'
    })


class SettingsView(APIView):
    """Application settings endpoint."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get application settings."""
        from apps.tables.models import AppSettings

        settings = AppSettings.get_instance()
        return Response({
            'default_billing_project': settings.default_billing_project,
        })

    def put(self, request):
        """Update application settings."""
        from apps.tables.models import AppSettings

        settings = AppSettings.get_instance()

        if 'default_billing_project' in request.data:
            settings.default_billing_project = request.data['default_billing_project']
            settings.save()

        return Response({
            'default_billing_project': settings.default_billing_project,
        })
