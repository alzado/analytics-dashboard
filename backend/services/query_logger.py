"""
Service for logging BigQuery query usage and statistics.
"""
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from contextlib import contextmanager


class QueryLogger:
    """Service for logging and retrieving BigQuery query usage."""

    def __init__(self, db_path: str):
        """
        Initialize the query logger.

        Args:
            db_path: Path to SQLite database file
        """
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
        """Initialize the database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    query_type TEXT NOT NULL,
                    bytes_processed INTEGER NOT NULL,
                    bytes_billed INTEGER NOT NULL,
                    execution_time_ms INTEGER NOT NULL,
                    filters_json TEXT,
                    row_count INTEGER,
                    error TEXT
                )
            """)

            # Create indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp
                ON query_logs(timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_type
                ON query_logs(query_type)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_endpoint
                ON query_logs(endpoint)
            """)

            conn.commit()

    def log_query(
        self,
        endpoint: str,
        query_type: str,
        bytes_processed: int,
        bytes_billed: int,
        execution_time_ms: int,
        filters: Optional[Dict] = None,
        row_count: Optional[int] = None,
        error: Optional[str] = None
    ) -> int:
        """
        Log a BigQuery query execution.

        Args:
            endpoint: API endpoint that triggered the query
            query_type: Type of query (kpi, trends, pivot, search_terms, etc.)
            bytes_processed: Bytes processed by BigQuery
            bytes_billed: Bytes billed by BigQuery
            execution_time_ms: Query execution time in milliseconds
            filters: Applied filters as dictionary
            row_count: Number of rows returned
            error: Error message if query failed

        Returns:
            ID of the inserted log entry
        """
        timestamp = datetime.utcnow().isoformat()
        filters_json = json.dumps(filters) if filters else None

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO query_logs (
                    timestamp, endpoint, query_type, bytes_processed, bytes_billed,
                    execution_time_ms, filters_json, row_count, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp, endpoint, query_type, bytes_processed, bytes_billed,
                execution_time_ms, filters_json, row_count, error
            ))
            conn.commit()
            return cursor.lastrowid

    def get_logs(
        self,
        limit: int = 100,
        offset: int = 0,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        query_type: Optional[str] = None,
        endpoint: Optional[str] = None
    ) -> Tuple[List[Dict], int]:
        """
        Retrieve query logs with filtering and pagination.

        Args:
            limit: Maximum number of logs to return
            offset: Number of logs to skip
            start_date: Filter logs from this date (ISO format)
            end_date: Filter logs until this date (ISO format)
            query_type: Filter by query type
            endpoint: Filter by endpoint

        Returns:
            Tuple of (list of log entries, total count)
        """
        where_clauses = []
        params = []

        if start_date:
            where_clauses.append("timestamp >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("timestamp <= ?")
            params.append(end_date)

        if query_type:
            where_clauses.append("query_type = ?")
            params.append(query_type)

        if endpoint:
            where_clauses.append("endpoint = ?")
            params.append(endpoint)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM query_logs WHERE {where_clause}"
            cursor.execute(count_query, params)
            total = cursor.fetchone()['total']

            # Get logs
            query = f"""
                SELECT
                    id, timestamp, endpoint, query_type, bytes_processed, bytes_billed,
                    execution_time_ms, filters_json, row_count, error
                FROM query_logs
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """
            cursor.execute(query, params + [limit, offset])

            logs = []
            for row in cursor.fetchall():
                log = dict(row)
                # Parse filters_json back to dict
                if log['filters_json']:
                    try:
                        log['filters'] = json.loads(log['filters_json'])
                    except json.JSONDecodeError:
                        log['filters'] = None
                else:
                    log['filters'] = None
                del log['filters_json']
                logs.append(log)

            return logs, total

    def get_usage_stats(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict:
        """
        Get aggregated usage statistics.

        Args:
            start_date: Start date for stats (ISO format)
            end_date: End date for stats (ISO format)

        Returns:
            Dictionary with usage statistics
        """
        where_clauses = []
        params = []

        if start_date:
            where_clauses.append("timestamp >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("timestamp <= ?")
            params.append(end_date)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = f"""
                SELECT
                    COUNT(*) as total_queries,
                    SUM(bytes_processed) as total_bytes_processed,
                    SUM(bytes_billed) as total_bytes_billed,
                    AVG(execution_time_ms) as avg_execution_time,
                    MAX(execution_time_ms) as max_execution_time,
                    MIN(execution_time_ms) as min_execution_time,
                    SUM(row_count) as total_rows
                FROM query_logs
                WHERE {where_clause} AND error IS NULL
            """
            cursor.execute(query, params)
            row = cursor.fetchone()

            # Get stats by query type
            type_query = f"""
                SELECT
                    query_type,
                    COUNT(*) as count,
                    SUM(bytes_processed) as bytes_processed
                FROM query_logs
                WHERE {where_clause} AND error IS NULL
                GROUP BY query_type
            """
            cursor.execute(type_query, params)
            by_type = [dict(row) for row in cursor.fetchall()]

            # Calculate estimated cost ($5 per TB)
            total_bytes_billed = row['total_bytes_billed'] or 0
            estimated_cost = (total_bytes_billed / (1024 ** 4)) * 5.0

            return {
                'total_queries': row['total_queries'] or 0,
                'total_bytes_processed': row['total_bytes_processed'] or 0,
                'total_bytes_billed': row['total_bytes_billed'] or 0,
                'total_gb_processed': round((row['total_bytes_processed'] or 0) / (1024 ** 3), 2),
                'total_gb_billed': round((row['total_bytes_billed'] or 0) / (1024 ** 3), 2),
                'avg_execution_time_ms': round(row['avg_execution_time'] or 0, 2),
                'max_execution_time_ms': row['max_execution_time'] or 0,
                'min_execution_time_ms': row['min_execution_time'] or 0,
                'total_rows': row['total_rows'] or 0,
                'estimated_cost_usd': round(estimated_cost, 4),
                'by_query_type': by_type
            }

    def get_usage_by_date(
        self,
        granularity: str = 'daily',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get usage statistics grouped by date.

        Args:
            granularity: Time granularity ('hourly', 'daily', 'weekly')
            start_date: Start date (ISO format)
            end_date: End date (ISO format)

        Returns:
            List of usage statistics by date
        """
        where_clauses = []
        params = []

        if start_date:
            where_clauses.append("timestamp >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("timestamp <= ?")
            params.append(end_date)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # SQLite date format mapping
        date_format_map = {
            'hourly': '%Y-%m-%d %H:00:00',
            'daily': '%Y-%m-%d',
            'weekly': '%Y-W%W'
        }
        date_format = date_format_map.get(granularity, '%Y-%m-%d')

        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = f"""
                SELECT
                    strftime('{date_format}', timestamp) as date,
                    COUNT(*) as queries,
                    SUM(bytes_processed) as bytes_processed,
                    SUM(bytes_billed) as bytes_billed,
                    AVG(execution_time_ms) as avg_execution_time
                FROM query_logs
                WHERE {where_clause} AND error IS NULL
                GROUP BY date
                ORDER BY date
            """
            cursor.execute(query, params)

            results = []
            for row in cursor.fetchall():
                bytes_billed = row['bytes_billed'] or 0
                estimated_cost = (bytes_billed / (1024 ** 4)) * 5.0

                results.append({
                    'date': row['date'],
                    'queries': row['queries'],
                    'bytes_processed': row['bytes_processed'] or 0,
                    'bytes_billed': row['bytes_billed'] or 0,
                    'gb_processed': round((row['bytes_processed'] or 0) / (1024 ** 3), 2),
                    'gb_billed': round((row['bytes_billed'] or 0) / (1024 ** 3), 2),
                    'avg_execution_time_ms': round(row['avg_execution_time'] or 0, 2),
                    'estimated_cost_usd': round(estimated_cost, 4)
                })

            return results

    def get_today_stats(self) -> Dict:
        """Get usage statistics for today."""
        today = datetime.utcnow().date().isoformat()
        return self.get_usage_stats(start_date=today)

    def clear_logs(self) -> int:
        """
        Clear all query logs.

        Returns:
            Number of logs deleted
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total FROM query_logs")
            count = cursor.fetchone()['total']

            cursor.execute("DELETE FROM query_logs")
            conn.commit()

            # Reset auto-increment
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='query_logs'")
            conn.commit()

            return count


# Global instance
_query_logger: Optional[QueryLogger] = None


def get_query_logger() -> Optional[QueryLogger]:
    """Get the global query logger instance."""
    return _query_logger


def initialize_query_logger(db_path: str) -> QueryLogger:
    """
    Initialize the global query logger.

    Args:
        db_path: Path to SQLite database file

    Returns:
        QueryLogger instance
    """
    global _query_logger
    _query_logger = QueryLogger(db_path)
    return _query_logger
