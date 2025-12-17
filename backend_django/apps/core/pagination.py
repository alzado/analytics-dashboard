"""
Custom pagination classes for the application.
"""
from rest_framework.pagination import PageNumberPagination, LimitOffsetPagination


class StandardPagination(PageNumberPagination):
    """Standard pagination with page number and size."""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 1000


class LargePagination(PageNumberPagination):
    """Pagination for larger datasets."""
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 5000


class SmallPagination(PageNumberPagination):
    """Pagination for smaller lists."""
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100


class OffsetPagination(LimitOffsetPagination):
    """Offset-based pagination (limit/offset style)."""
    default_limit = 50
    max_limit = 1000
