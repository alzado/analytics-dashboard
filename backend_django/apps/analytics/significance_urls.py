"""
Significance endpoint URL at root level (/api/significance/).
FastAPI compatibility alias for /api/analytics/significance/
"""
from django.urls import path
from .views import SignificanceView

urlpatterns = [
    path('', SignificanceView.as_view(), name='significance-root'),
]
