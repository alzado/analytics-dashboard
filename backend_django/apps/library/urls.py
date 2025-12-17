"""
URL configuration for library app.
"""
from django.urls import path
from .views import LibraryListView, LibrarySearchView, LibraryStatsView

urlpatterns = [
    path('', LibraryListView.as_view(), name='library-list'),
    path('search/', LibrarySearchView.as_view(), name='library-search'),
    path('stats/', LibraryStatsView.as_view(), name='library-stats'),
]
