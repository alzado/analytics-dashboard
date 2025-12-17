"""
ASGI config for search_analytics project.
"""
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'search_analytics.settings.development')

application = get_asgi_application()
