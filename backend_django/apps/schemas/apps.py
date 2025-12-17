"""
Schemas app configuration.
"""
from django.apps import AppConfig


class SchemasConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.schemas'
    verbose_name = 'Schema Management'
