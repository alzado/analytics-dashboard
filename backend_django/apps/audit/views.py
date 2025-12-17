"""
Audit views for query logging and usage statistics.
"""
from datetime import timedelta
from django.db.models import Sum, Avg, Count, F
from django.db.models.functions import TruncHour, TruncDay, TruncWeek
from django.utils import timezone
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from .models import QueryLog, CacheEntry
from .serializers import (
    QueryLogSerializer,
    QueryLogResponseSerializer,
    UsageStatsSerializer,
    UsageTimeSeriesSerializer,
    CacheStatsSerializer,
    CacheClearResponseSerializer,
    ClearLogsResponseSerializer
)


class QueryLogListView(APIView):
    """Get query logs with filtering and pagination."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Get query logs.

        Query params:
        - limit: Max entries (default 100)
        - offset: Skip entries (default 0)
        - start_date: Filter by date (YYYY-MM-DD)
        - end_date: Filter by date (YYYY-MM-DD)
        - query_type: Filter by query type
        - endpoint: Filter by endpoint
        """
        limit = int(request.query_params.get('limit', 100))
        offset = int(request.query_params.get('offset', 0))
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        query_type = request.query_params.get('query_type')
        endpoint = request.query_params.get('endpoint')

        queryset = QueryLog.objects.all()

        if start_date:
            queryset = queryset.filter(created_at__date__gte=start_date)
        if end_date:
            queryset = queryset.filter(created_at__date__lte=end_date)
        if query_type:
            queryset = queryset.filter(query_type=query_type)
        if endpoint:
            queryset = queryset.filter(endpoint__icontains=endpoint)

        total = queryset.count()
        logs = queryset[offset:offset + limit]

        serializer = QueryLogResponseSerializer({
            'logs': logs,
            'total': total,
            'limit': limit,
            'offset': offset
        })
        return Response(serializer.data)


class QueryLogClearView(APIView):
    """Clear query logs."""
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Clear all query logs."""
        count = QueryLog.objects.count()
        QueryLog.objects.all().delete()

        serializer = ClearLogsResponseSerializer({
            'success': True,
            'message': f'Successfully cleared {count} log entries',
            'logs_deleted': count
        })
        return Response(serializer.data)


class UsageStatsView(APIView):
    """Get aggregated usage statistics."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Get usage stats.

        Query params:
        - start_date: Filter by date (YYYY-MM-DD)
        - end_date: Filter by date (YYYY-MM-DD)
        """
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')

        queryset = QueryLog.objects.all()

        if start_date:
            queryset = queryset.filter(created_at__date__gte=start_date)
        if end_date:
            queryset = queryset.filter(created_at__date__lte=end_date)

        # Calculate statistics
        total_queries = queryset.count()
        if total_queries == 0:
            stats = {
                'total_queries': 0,
                'total_bytes_processed': 0,
                'total_bytes_billed': 0,
                'avg_execution_time_ms': 0,
                'cache_hit_rate': 0,
                'queries_by_type': {},
                'error_count': 0
            }
        else:
            aggregates = queryset.aggregate(
                total_bytes_processed=Sum('bytes_processed'),
                total_bytes_billed=Sum('bytes_billed'),
                avg_execution_time_ms=Avg('execution_time_ms')
            )

            cache_hits = queryset.filter(cache_hit=True).count()
            error_count = queryset.filter(is_success=False).count()

            # Get query counts by type
            queries_by_type = dict(
                queryset.values('query_type')
                .annotate(count=Count('id'))
                .values_list('query_type', 'count')
            )

            stats = {
                'total_queries': total_queries,
                'total_bytes_processed': aggregates['total_bytes_processed'] or 0,
                'total_bytes_billed': aggregates['total_bytes_billed'] or 0,
                'avg_execution_time_ms': aggregates['avg_execution_time_ms'] or 0,
                'cache_hit_rate': cache_hits / total_queries if total_queries > 0 else 0,
                'queries_by_type': queries_by_type,
                'error_count': error_count
            }

        serializer = UsageStatsSerializer(stats)
        return Response(serializer.data)


class TodayUsageStatsView(APIView):
    """Get usage statistics for today."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get today's usage stats."""
        today = timezone.now().date()
        queryset = QueryLog.objects.filter(created_at__date=today)

        total_queries = queryset.count()
        if total_queries == 0:
            stats = {
                'total_queries': 0,
                'total_bytes_processed': 0,
                'total_bytes_billed': 0,
                'avg_execution_time_ms': 0,
                'cache_hit_rate': 0,
                'queries_by_type': {},
                'error_count': 0
            }
        else:
            aggregates = queryset.aggregate(
                total_bytes_processed=Sum('bytes_processed'),
                total_bytes_billed=Sum('bytes_billed'),
                avg_execution_time_ms=Avg('execution_time_ms')
            )

            cache_hits = queryset.filter(cache_hit=True).count()
            error_count = queryset.filter(is_success=False).count()

            queries_by_type = dict(
                queryset.values('query_type')
                .annotate(count=Count('id'))
                .values_list('query_type', 'count')
            )

            stats = {
                'total_queries': total_queries,
                'total_bytes_processed': aggregates['total_bytes_processed'] or 0,
                'total_bytes_billed': aggregates['total_bytes_billed'] or 0,
                'avg_execution_time_ms': aggregates['avg_execution_time_ms'] or 0,
                'cache_hit_rate': cache_hits / total_queries if total_queries > 0 else 0,
                'queries_by_type': queries_by_type,
                'error_count': error_count
            }

        serializer = UsageStatsSerializer(stats)
        return Response(serializer.data)


class UsageTimeSeriesView(APIView):
    """Get usage statistics over time."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Get usage time series.

        Query params:
        - granularity: hourly, daily, weekly (default: daily)
        - start_date: Filter by date (YYYY-MM-DD)
        - end_date: Filter by date (YYYY-MM-DD)
        """
        granularity = request.query_params.get('granularity', 'daily')
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')

        queryset = QueryLog.objects.all()

        if start_date:
            queryset = queryset.filter(created_at__date__gte=start_date)
        if end_date:
            queryset = queryset.filter(created_at__date__lte=end_date)

        # Select truncation function based on granularity
        if granularity == 'hourly':
            trunc_func = TruncHour('created_at')
        elif granularity == 'weekly':
            trunc_func = TruncWeek('created_at')
        else:
            trunc_func = TruncDay('created_at')

        # Aggregate by time period
        data = (
            queryset
            .annotate(period=trunc_func)
            .values('period')
            .annotate(
                query_count=Count('id'),
                bytes_processed=Sum('bytes_processed'),
                bytes_billed=Sum('bytes_billed'),
                avg_execution_time_ms=Avg('execution_time_ms')
            )
            .order_by('period')
        )

        # Format results
        results = []
        for row in data:
            results.append({
                'period': row['period'].isoformat() if row['period'] else None,
                'query_count': row['query_count'],
                'bytes_processed': row['bytes_processed'] or 0,
                'bytes_billed': row['bytes_billed'] or 0,
                'avg_execution_time_ms': row['avg_execution_time_ms'] or 0
            })

        serializer = UsageTimeSeriesSerializer(results, many=True)
        return Response(serializer.data)


class CacheStatsView(APIView):
    """Get cache statistics."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get cache stats."""
        now = timezone.now()

        total_entries = CacheEntry.objects.count()
        total_hits = CacheEntry.objects.aggregate(
            total=Sum('hit_count')
        )['total'] or 0
        expired_entries = CacheEntry.objects.filter(expires_at__lt=now).count()

        entries_by_type = dict(
            CacheEntry.objects.values('query_type')
            .annotate(count=Count('id'))
            .values_list('query_type', 'count')
        )

        stats = {
            'total_entries': total_entries,
            'total_hits': total_hits,
            'expired_entries': expired_entries,
            'entries_by_type': entries_by_type
        }

        serializer = CacheStatsSerializer(stats)
        return Response(serializer.data)


class CacheClearView(APIView):
    """Clear cache entries."""
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Clear all cache entries."""
        count = CacheEntry.objects.count()
        CacheEntry.objects.all().delete()

        serializer = CacheClearResponseSerializer({
            'success': True,
            'message': 'Cleared all cache entries',
            'entries_deleted': count
        })
        return Response(serializer.data)


class CacheClearByTableView(APIView):
    """Clear cache for a specific table."""
    permission_classes = [IsAuthenticated]

    def post(self, request, table_id):
        """Clear cache entries for a specific table."""
        count = CacheEntry.objects.filter(bigquery_table_id=table_id).count()
        CacheEntry.objects.filter(bigquery_table_id=table_id).delete()

        serializer = CacheClearResponseSerializer({
            'success': True,
            'message': f'Cleared cache for table {table_id}',
            'entries_deleted': count
        })
        return Response(serializer.data)


class CacheClearByTypeView(APIView):
    """Clear cache by query type."""
    permission_classes = [IsAuthenticated]

    def post(self, request, query_type):
        """Clear cache entries for a specific query type."""
        count = CacheEntry.objects.filter(query_type=query_type).count()
        CacheEntry.objects.filter(query_type=query_type).delete()

        serializer = CacheClearResponseSerializer({
            'success': True,
            'message': f'Cleared cache for query type {query_type}',
            'entries_deleted': count
        })
        return Response(serializer.data)
