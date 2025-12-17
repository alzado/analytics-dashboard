"""
URL configuration for optimized source endpoints.

These provide FastAPI-compatible routes for managing optimized source tables.
"""
from django.urls import path
from .views import (
    OptimizedSourceStatusView,
    OptimizedSourceAnalyzeView,
    OptimizedSourcePreviewSqlView,
    OptimizedSourceCreateView,
    OptimizedSourceRefreshView,
    OptimizedSourceDeleteView
)

urlpatterns = [
    path('status/', OptimizedSourceStatusView.as_view(), name='optimized-source-status'),
    path('analyze/', OptimizedSourceAnalyzeView.as_view(), name='optimized-source-analyze'),
    path('preview-sql/', OptimizedSourcePreviewSqlView.as_view(), name='optimized-source-preview-sql'),
    path('create/', OptimizedSourceCreateView.as_view(), name='optimized-source-create'),
    path('refresh/', OptimizedSourceRefreshView.as_view(), name='optimized-source-refresh'),
    path('delete/', OptimizedSourceDeleteView.as_view(), name='optimized-source-delete'),
]
