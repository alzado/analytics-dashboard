"""
Core app URLs - health checks and settings.
"""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.root_view, name='root'),
    path('health/', views.health_check, name='health'),
    path('settings/', views.SettingsView.as_view(), name='settings'),
]
