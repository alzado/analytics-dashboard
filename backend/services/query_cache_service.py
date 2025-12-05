"""
Query cache service for storing and retrieving BigQuery query results.
Uses SQLite (same database as query_logger) for persistent storage.
Manual invalidation only - no TTL/expiration.

KEY DESIGN: Cache at BigQuery execution layer
- Cache key = MD5 hash of the raw SQL query string
- Cache the raw DataFrame results (as list of dicts)
- Caching happens inside BigQueryService._execute_and_log_query()
- Same SQL query = same cache hit (maximum reuse)
"""

import sqlite3
import json
import hashlib
import re
from datetime import datetime
from typing import Optional, Dict, List, Any
from contextlib import contextmanager


class QueryCacheService:
    """Service for caching BigQuery query results in SQLite."""

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
        'calculated_dimension_values'
    ]

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_database()

    @contextmanager
    def _get_connection(self):
        """Get a database connection context manager."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_database(self):
        """Initialize the cache table schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cache_key TEXT UNIQUE NOT NULL,
                    query_type TEXT NOT NULL,
                    table_id TEXT NOT NULL,
                    sql_query TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    result_size_bytes INTEGER NOT NULL,
                    row_count INTEGER,
                    created_at TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL,
                    access_count INTEGER DEFAULT 1
                )
            """)
            # Indexes for efficient lookups and clearing
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cache_key ON query_cache(cache_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cache_table_id ON query_cache(table_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cache_query_type ON query_cache(query_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cache_created ON query_cache(created_at)")
            conn.commit()

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
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT result_json FROM query_cache WHERE cache_key = ?",
                (cache_key,)
            )
            row = cursor.fetchone()
            if row:
                # Update access stats
                cursor.execute("""
                    UPDATE query_cache
                    SET last_accessed_at = ?, access_count = access_count + 1
                    WHERE cache_key = ?
                """, (datetime.utcnow().isoformat(), cache_key))
                conn.commit()
                return json.loads(row['result_json'])
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
        Uses INSERT OR REPLACE to handle duplicates.

        Args:
            cache_key: MD5 hash of the SQL query
            query_type: Type of query (pivot, kpi, trends, etc.)
            table_id: BigQuery table ID for per-table clearing
            sql_query: The raw SQL query (stored for debugging/inspection)
            result: Query result as list of dicts (DataFrame.to_dict('records'))
            row_count: Number of rows in result
        """
        try:
            result_json = json.dumps(result)
            result_size = len(result_json.encode('utf-8'))
            now = datetime.utcnow().isoformat()

            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO query_cache (
                        cache_key, query_type, table_id, sql_query,
                        result_json, result_size_bytes, row_count,
                        created_at, last_accessed_at, access_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    cache_key, query_type, table_id or 'default',
                    sql_query,
                    result_json, result_size, row_count,
                    now, now
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False

    def clear_all(self) -> int:
        """Clear entire cache. Returns number of entries deleted."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM query_cache")
            count = cursor.fetchone()['count']
            cursor.execute("DELETE FROM query_cache")
            conn.commit()

            # Reset auto-increment
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='query_cache'")
            conn.commit()

            return count

    def clear_by_table(self, table_id: str) -> int:
        """Clear cache for specific table. Returns number of entries deleted."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as count FROM query_cache WHERE table_id = ?",
                (table_id,)
            )
            count = cursor.fetchone()['count']
            cursor.execute("DELETE FROM query_cache WHERE table_id = ?", (table_id,))
            conn.commit()
            return count

    def clear_by_query_type(self, query_type: str) -> int:
        """Clear cache for specific query type. Returns number of entries deleted."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as count FROM query_cache WHERE query_type = ?",
                (query_type,)
            )
            count = cursor.fetchone()['count']
            cursor.execute("DELETE FROM query_cache WHERE query_type = ?", (query_type,))
            conn.commit()
            return count

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Overall stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_entries,
                    COALESCE(SUM(result_size_bytes), 0) as total_size_bytes,
                    COALESCE(SUM(access_count), 0) as total_hits,
                    COALESCE(AVG(access_count), 0) as avg_hits_per_entry,
                    MIN(created_at) as oldest_entry,
                    MAX(created_at) as newest_entry
                FROM query_cache
            """)
            overall = dict(cursor.fetchone())

            # Stats by table
            cursor.execute("""
                SELECT
                    table_id,
                    COUNT(*) as entries,
                    SUM(result_size_bytes) as size_bytes,
                    SUM(access_count) as hits
                FROM query_cache
                GROUP BY table_id
                ORDER BY entries DESC
            """)
            by_table = [dict(row) for row in cursor.fetchall()]

            # Stats by query type
            cursor.execute("""
                SELECT
                    query_type,
                    COUNT(*) as entries,
                    SUM(result_size_bytes) as size_bytes,
                    SUM(access_count) as hits
                FROM query_cache
                GROUP BY query_type
                ORDER BY entries DESC
            """)
            by_query_type = [dict(row) for row in cursor.fetchall()]

            total_size_bytes = overall['total_size_bytes'] or 0

            return {
                'total_entries': overall['total_entries'] or 0,
                'total_size_bytes': total_size_bytes,
                'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
                'total_hits': overall['total_hits'] or 0,
                'avg_hits_per_entry': round(overall['avg_hits_per_entry'] or 0, 1),
                'oldest_entry': overall['oldest_entry'],
                'newest_entry': overall['newest_entry'],
                'by_table': by_table,
                'by_query_type': by_query_type
            }


# Global instance
_query_cache: Optional[QueryCacheService] = None


def get_query_cache() -> Optional[QueryCacheService]:
    """Get the global query cache instance."""
    return _query_cache


def initialize_query_cache(db_path: str) -> QueryCacheService:
    """
    Initialize the global query cache service.

    Args:
        db_path: Path to SQLite database file

    Returns:
        QueryCacheService instance
    """
    global _query_cache
    _query_cache = QueryCacheService(db_path)
    return _query_cache
