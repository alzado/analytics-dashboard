"""
Query cache service for storing and retrieving BigQuery query results.
Uses Django's cache framework for storage.
Manual invalidation only - no TTL/expiration.

KEY DESIGN: Cache at BigQuery execution layer
- Cache key = MD5 hash of the raw SQL query string
- Cache the raw results (as list of dicts)
- Same SQL query = same cache hit (maximum reuse)
"""

import json
import hashlib
import re
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any

from django.core.cache import cache

logger = logging.getLogger(__name__)


class QueryCacheService:
    """Service for caching BigQuery query results using Django's cache framework."""

    # Supported query types for caching
    QUERY_TYPES = [
        'pivot',            # Main pivot table queries
        'pivot_children',   # Drill-down search terms
        'pivot_totals',     # Pivot totals row
        'pivot_date_range', # Date range queries for avg/day calculations
        'pivot_count',      # Count queries for pagination
        'kpi',              # Overview KPI metrics
        'trends',           # Time-series data
        'dimension_values', # Dimension value lists for filters
        'filter_options',   # Filter dropdown options
        'significance',     # Statistical significance calculations
        'aggregated_totals', # Aggregated totals for significance
        'calculated_dimension_values'
    ]

    # Cache key prefixes
    CACHE_PREFIX = 'bq_cache:'
    METADATA_KEY = 'bq_cache_metadata'

    def __init__(self):
        """Initialize the cache service."""
        self._ensure_metadata_exists()

    def _ensure_metadata_exists(self):
        """Ensure the metadata tracking dict exists in cache."""
        if cache.get(self.METADATA_KEY) is None:
            cache.set(self.METADATA_KEY, {}, timeout=None)

    def _get_metadata(self) -> Dict:
        """Get cache metadata."""
        return cache.get(self.METADATA_KEY) or {}

    def _set_metadata(self, metadata: Dict):
        """Set cache metadata."""
        cache.set(self.METADATA_KEY, metadata, timeout=None)

    def _get_cache_key(self, key: str) -> str:
        """Get prefixed cache key."""
        return f"{self.CACHE_PREFIX}{key}"

    @staticmethod
    def sql_to_cache_key(sql_query: str) -> str:
        """
        Generate cache key from SQL query string.

        Normalizes whitespace so that semantically identical queries
        with different formatting produce the same cache key.
        """
        # Normalize whitespace: collapse multiple spaces/newlines to single space
        normalized = re.sub(r'\s+', ' ', sql_query.strip())
        return hashlib.md5(normalized.encode()).hexdigest()

    def get(self, cache_key: str) -> Optional[Any]:
        """
        Retrieve cached result by key.
        Updates access time and count on hit.
        Returns None on miss.
        """
        full_key = self._get_cache_key(cache_key)
        result = cache.get(full_key)

        if result is not None:
            # Update access stats in metadata
            try:
                metadata = self._get_metadata()
                if cache_key in metadata:
                    metadata[cache_key]['last_accessed_at'] = datetime.utcnow().isoformat()
                    metadata[cache_key]['access_count'] = metadata[cache_key].get('access_count', 0) + 1
                    self._set_metadata(metadata)
            except Exception as e:
                logger.warning(f"Failed to update cache stats: {e}")

            return result

        return None

    def set(
        self,
        cache_key: str,
        query_type: str,
        table_id: str,
        sql_query: str,
        result: Any,
        row_count: Optional[int] = None
    ) -> bool:
        """
        Store query result in cache.

        Args:
            cache_key: MD5 hash of the SQL query
            query_type: Type of query (pivot, kpi, trends, etc.)
            table_id: BigQuery table ID for per-table clearing
            sql_query: The raw SQL query (stored for debugging/inspection)
            result: Query result as list of dicts (DataFrame.to_dict('records'))
            row_count: Number of rows in result
        """
        try:
            # Calculate size
            result_json = json.dumps(result)
            result_size = len(result_json.encode('utf-8'))

            # Store in Django cache (no expiration)
            full_key = self._get_cache_key(cache_key)
            cache.set(full_key, result, timeout=None)

            # Track metadata
            metadata = self._get_metadata()
            metadata[cache_key] = {
                'query_type': query_type,
                'table_id': table_id or 'default',
                'result_size_bytes': result_size,
                'row_count': row_count,
                'created_at': datetime.utcnow().isoformat(),
                'last_accessed_at': datetime.utcnow().isoformat(),
                'access_count': 1
            }
            self._set_metadata(metadata)
            return True

        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False

    def clear_all(self) -> int:
        """Clear entire cache. Returns number of entries deleted."""
        try:
            metadata = self._get_metadata()
            count = len(metadata)

            # Clear all cached entries
            for cache_key in list(metadata.keys()):
                full_key = self._get_cache_key(cache_key)
                cache.delete(full_key)

            # Clear metadata
            self._set_metadata({})

            return count

        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return 0

    def clear_by_table(self, table_id: str) -> int:
        """Clear cache for specific table. Returns number of entries deleted."""
        try:
            metadata = self._get_metadata()
            count = 0

            # Find and delete entries for this table
            keys_to_delete = []
            for cache_key, entry in metadata.items():
                if entry.get('table_id') == table_id:
                    keys_to_delete.append(cache_key)

            for cache_key in keys_to_delete:
                full_key = self._get_cache_key(cache_key)
                cache.delete(full_key)
                del metadata[cache_key]
                count += 1

            self._set_metadata(metadata)
            return count

        except Exception as e:
            logger.error(f"Cache clear by table error: {e}")
            return 0

    def clear_by_query_type(self, query_type: str) -> int:
        """Clear cache for specific query type. Returns number of entries deleted."""
        try:
            metadata = self._get_metadata()
            count = 0

            # Find and delete entries for this query type
            keys_to_delete = []
            for cache_key, entry in metadata.items():
                if entry.get('query_type') == query_type:
                    keys_to_delete.append(cache_key)

            for cache_key in keys_to_delete:
                full_key = self._get_cache_key(cache_key)
                cache.delete(full_key)
                del metadata[cache_key]
                count += 1

            self._set_metadata(metadata)
            return count

        except Exception as e:
            logger.error(f"Cache clear by query type error: {e}")
            return 0

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        try:
            metadata = self._get_metadata()

            if not metadata:
                return {
                    'total_entries': 0,
                    'total_size_bytes': 0,
                    'total_size_mb': 0,
                    'total_hits': 0,
                    'avg_hits_per_entry': 0,
                    'oldest_entry': None,
                    'newest_entry': None,
                    'by_table': [],
                    'by_query_type': []
                }

            # Calculate stats
            total_entries = len(metadata)
            total_size_bytes = sum(e.get('result_size_bytes', 0) for e in metadata.values())
            total_hits = sum(e.get('access_count', 0) for e in metadata.values())
            avg_hits = total_hits / total_entries if total_entries > 0 else 0

            # Find oldest/newest
            created_dates = [e.get('created_at') for e in metadata.values() if e.get('created_at')]
            oldest_entry = min(created_dates) if created_dates else None
            newest_entry = max(created_dates) if created_dates else None

            # Group by table
            by_table = {}
            for entry in metadata.values():
                table_id = entry.get('table_id', 'default')
                if table_id not in by_table:
                    by_table[table_id] = {'table_id': table_id, 'entries': 0, 'size_bytes': 0, 'hits': 0}
                by_table[table_id]['entries'] += 1
                by_table[table_id]['size_bytes'] += entry.get('result_size_bytes', 0)
                by_table[table_id]['hits'] += entry.get('access_count', 0)

            # Group by query type
            by_query_type = {}
            for entry in metadata.values():
                qtype = entry.get('query_type', 'unknown')
                if qtype not in by_query_type:
                    by_query_type[qtype] = {'query_type': qtype, 'entries': 0, 'size_bytes': 0, 'hits': 0}
                by_query_type[qtype]['entries'] += 1
                by_query_type[qtype]['size_bytes'] += entry.get('result_size_bytes', 0)
                by_query_type[qtype]['hits'] += entry.get('access_count', 0)

            return {
                'total_entries': total_entries,
                'total_size_bytes': total_size_bytes,
                'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
                'total_hits': total_hits,
                'avg_hits_per_entry': round(avg_hits, 1),
                'oldest_entry': oldest_entry,
                'newest_entry': newest_entry,
                'by_table': sorted(by_table.values(), key=lambda x: x['entries'], reverse=True),
                'by_query_type': sorted(by_query_type.values(), key=lambda x: x['entries'], reverse=True)
            }

        except Exception as e:
            logger.error(f"Get stats error: {e}")
            return {
                'total_entries': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'total_hits': 0,
                'avg_hits_per_entry': 0,
                'oldest_entry': None,
                'newest_entry': None,
                'by_table': [],
                'by_query_type': []
            }


# Global instance
_query_cache: Optional[QueryCacheService] = None


def get_query_cache() -> QueryCacheService:
    """Get the query cache service instance (singleton)."""
    global _query_cache
    if _query_cache is None:
        _query_cache = QueryCacheService()
    return _query_cache
