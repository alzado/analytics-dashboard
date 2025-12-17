"""
URL configuration for standalone widgets.
"""
from django.urls import path
from .views import StandaloneWidgetViewSet

urlpatterns = [
    # List/create standalone widgets
    path('', StandaloneWidgetViewSet.as_view({
        'get': 'list',
        'post': 'create'
    }), name='widgets-list'),

    # Get/delete specific widget
    path('<uuid:widget_id>/', StandaloneWidgetViewSet.as_view({
        'get': 'retrieve',
        'delete': 'destroy'
    }), name='widgets-detail'),

    # Attach widget to dashboard
    path('<uuid:widget_id>/attach/', StandaloneWidgetViewSet.as_view({
        'post': 'attach'
    }), name='widgets-attach'),

    # Clone widget
    path('<uuid:widget_id>/clone/', StandaloneWidgetViewSet.as_view({
        'post': 'clone'
    }), name='widgets-clone'),
]
