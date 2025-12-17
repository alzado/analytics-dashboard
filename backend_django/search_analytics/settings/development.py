"""
Django development settings.
"""
from .base import *

DEBUG = True

ALLOWED_HOSTS = ['*']

# CORS - Allow all in development
CORS_ALLOW_ALL_ORIGINS = True

# More verbose logging in development
LOGGING['root']['level'] = 'DEBUG'
LOGGING['loggers']['apps']['level'] = 'DEBUG'
