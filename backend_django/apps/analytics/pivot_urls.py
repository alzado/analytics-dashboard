"""
Pivot table URLs at /api/pivot/ level.

These are for FastAPI frontend compatibility - the frontend calls
/api/pivot directly instead of /api/analytics/pivot
"""
from django.urls import path
from .views import (
    PivotView,
    PivotChildrenView,
    DimensionValuesView
)

urlpatterns = [
    # /api/pivot/ - Main pivot table endpoint
    path('pivot/', PivotView.as_view(), name='pivot'),

    # /api/pivot/children/ - Get children for pivot rows
    path('pivot/children/', PivotChildrenView.as_view(), name='pivot-children-all'),

    # /api/pivot/<dimension>/<value>/children/ - Get children for specific dimension value
    path(
        'pivot/<str:dimension>/<str:value>/children/',
        PivotChildrenView.as_view(),
        name='pivot-children'
    ),

    # /api/pivot/dimension/<dimension>/values/ - Get distinct values for a dimension
    path(
        'pivot/dimension/<str:dimension>/values/',
        DimensionValuesView.as_view(),
        name='dimension-values'
    ),
]
