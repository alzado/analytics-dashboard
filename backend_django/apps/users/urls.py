"""
User authentication URLs.
"""
from django.urls import path
from . import views

urlpatterns = [
    # User authentication
    path('google/', views.GoogleAuthView.as_view(), name='google-auth'),
    path('refresh/', views.RefreshTokenView.as_view(), name='refresh-token'),
    path('me/', views.CurrentUserView.as_view(), name='current-user'),

    # GCP/BigQuery OAuth
    path('gcp/authorize/', views.GCPAuthUrlView.as_view(), name='gcp-auth-url'),
    path('gcp/callback/', views.GCPAuthCallbackView.as_view(), name='gcp-auth-callback'),
    path('gcp/status/', views.GCPAuthStatusView.as_view(), name='gcp-auth-status'),
    path('gcp/revoke/', views.GCPAuthRevokeView.as_view(), name='gcp-auth-revoke'),
    path('gcp/projects/', views.GCPProjectsView.as_view(), name='gcp-projects'),
]
