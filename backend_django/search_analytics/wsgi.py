"""
WSGI config for search_analytics project.
"""
import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'search_analytics.settings.development')

application = get_wsgi_application()
