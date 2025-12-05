"""
Dimension expression parser.
Handles {column} references and validates SQL expressions for calculated dimensions.
Unlike metric formulas, dimension expressions operate on row-level data and
cannot contain aggregation functions (SUM, AVG, COUNT, etc.).
"""
import re
from typing import List, Set, Tuple, Optional
from google.cloud import bigquery


class DimensionExpressionParser:
    """
    Parses dimension expressions with {column} references.

    Expression syntax:
    - {column_name} - reference to a table column
    - SQL functions: REGEXP_EXTRACT, COALESCE, CAST, CASE, UPPER, LOWER, TRIM, etc.
    - Operators: ||, +, -, *, /
    - NO aggregation functions (SUM, AVG, COUNT, MIN, MAX)

    Examples:
    - UPPER({country})
    - COALESCE(REGEXP_EXTRACT({recommendation_id}, r'pattern'), {fallback_col})
    - CASE WHEN {n_words} > 3 THEN 'long' ELSE 'short' END
    - CAST(COALESCE(REGEXP_EXTRACT({recommendation_id}, r'^(?:[^!]+!){5}([^!]+)'), REGEXP_EXTRACT({post_recommendation_id}, r'^(?:[^!]+!){5}([^!]+)')) AS STRING)
    """

    # Aggregation functions that are NOT allowed in dimension expressions
    FORBIDDEN_AGGREGATIONS = {
        'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'COUNT_DISTINCT',
        'APPROX_COUNT_DISTINCT', 'ANY_VALUE', 'ARRAY_AGG',
        'STRING_AGG', 'COUNTIF', 'LOGICAL_AND', 'LOGICAL_OR',
        'BIT_AND', 'BIT_OR', 'BIT_XOR'
    }

    # Dangerous SQL keywords that could indicate SQL injection attempts
    DANGEROUS_KEYWORDS = {
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
        'EXEC', 'EXECUTE', 'TRUNCATE', 'GRANT', 'REVOKE'
    }

    def __init__(self, bigquery_client: Optional[bigquery.Client] = None, table_path: Optional[str] = None):
        """
        Initialize the parser.

        Args:
            bigquery_client: Optional BigQuery client for validation
            table_path: Optional table path for column validation
        """
        self.client = bigquery_client
        self.table_path = table_path
        self._column_cache: Optional[Set[str]] = None

    def _get_table_columns(self) -> Set[str]:
        """Get all column names from the BigQuery table."""
        if self._column_cache is not None:
            return self._column_cache

        if not self.client or not self.table_path:
            return set()

        try:
            table = self.client.get_table(self.table_path)
            self._column_cache = {field.name for field in table.schema}
            return self._column_cache
        except Exception:
            return set()

    def clear_column_cache(self):
        """Clear the cached column list."""
        self._column_cache = None

    def parse_expression(self, expression: str, validate_columns: bool = True) -> Tuple[str, List[str], List[str]]:
        """
        Parse a dimension expression.

        Args:
            expression: SQL expression with {column_name} references
            validate_columns: Whether to validate that columns exist in the table

        Returns:
            Tuple of (sql_expression, depends_on_columns, errors)
        """
        errors = []
        depends_on = []

        # Check for empty expression
        if not expression or not expression.strip():
            errors.append("Expression cannot be empty")
            return "", depends_on, errors

        # Check for balanced braces
        if expression.count('{') != expression.count('}'):
            errors.append("Unbalanced braces in expression")
            return "", depends_on, errors

        # Check for balanced parentheses
        if expression.count('(') != expression.count(')'):
            errors.append("Unbalanced parentheses in expression")
            return "", depends_on, errors

        # Extract {column_name} references
        column_refs = re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', expression)

        # Check for empty braces
        if '{}' in expression:
            errors.append("Empty column reference found")
            return "", depends_on, errors

        # Validate column references if requested
        if validate_columns:
            valid_columns = self._get_table_columns()

            for col_ref in column_refs:
                if valid_columns and col_ref not in valid_columns:
                    errors.append(f"Unknown column reference: {col_ref}")
                elif col_ref not in depends_on:
                    depends_on.append(col_ref)
        else:
            # Just collect unique column references
            depends_on = list(dict.fromkeys(column_refs))

        # Check for forbidden aggregation functions
        expression_upper = expression.upper()
        for agg_func in self.FORBIDDEN_AGGREGATIONS:
            # Look for function call pattern: FUNCTION_NAME(
            pattern = rf'\b{agg_func}\s*\('
            if re.search(pattern, expression_upper):
                errors.append(f"Aggregation function '{agg_func}' is not allowed in dimension expressions")

        # Check for dangerous SQL keywords
        for keyword in self.DANGEROUS_KEYWORDS:
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, expression_upper):
                errors.append(f"Dangerous SQL keyword '{keyword}' is not allowed")

        if errors:
            return "", depends_on, errors

        # Convert {column} to actual column names
        sql_expression = expression
        for col_ref in set(column_refs):
            sql_expression = sql_expression.replace(f'{{{col_ref}}}', col_ref)

        return sql_expression, depends_on, errors

    def validate_expression(
        self,
        expression: str,
        validate_columns: bool = True,
        dry_run: bool = True
    ) -> Tuple[bool, str, List[str], List[str], List[str]]:
        """
        Fully validate a dimension expression including BigQuery dry run.

        Args:
            expression: SQL expression with {column_name} references
            validate_columns: Whether to validate that columns exist
            dry_run: Whether to perform BigQuery dry run validation

        Returns:
            Tuple of (is_valid, sql_expression, depends_on, errors, warnings)
        """
        warnings = []

        # Parse the expression
        sql_expression, depends_on, parse_errors = self.parse_expression(expression, validate_columns)

        if parse_errors:
            return False, "", depends_on, parse_errors, warnings

        # BigQuery dry run validation
        if dry_run and self.client and self.table_path:
            try:
                test_query = f"""
                    SELECT {sql_expression} AS test_dimension
                    FROM `{self.table_path}`
                    LIMIT 0
                """
                job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
                self.client.query(test_query, job_config=job_config)
            except Exception as e:
                error_msg = str(e)
                # Extract just the relevant part of the error message
                if 'message' in error_msg.lower():
                    # Try to extract just the SQL error message
                    parse_errors.append(f"SQL validation failed: {error_msg}")
                else:
                    parse_errors.append(f"SQL validation failed: {error_msg}")
                return False, sql_expression, depends_on, parse_errors, warnings

        # Generate warnings for common issues
        expression_upper = expression.upper()

        # Warning: Consider NULL handling
        if 'NULL' not in expression_upper and 'COALESCE' not in expression_upper and 'IFNULL' not in expression_upper:
            warnings.append("Consider using COALESCE or IFNULL to handle NULL values")

        # Warning: REGEXP_EXTRACT without COALESCE
        if 'REGEXP_EXTRACT' in expression_upper and 'COALESCE' not in expression_upper:
            warnings.append("REGEXP_EXTRACT returns NULL if no match; consider wrapping with COALESCE")

        return True, sql_expression, depends_on, [], warnings

    def validate_syntax_only(self, expression: str) -> List[str]:
        """
        Validate expression syntax without schema context.
        Useful for quick client-side validation.

        Args:
            expression: SQL expression to validate

        Returns:
            List of syntax errors (empty if valid)
        """
        errors = []

        # Check for empty expression
        if not expression or not expression.strip():
            errors.append("Expression cannot be empty")
            return errors

        # Check for balanced braces
        if expression.count('{') != expression.count('}'):
            errors.append("Unbalanced braces in expression")

        # Check for balanced parentheses
        if expression.count('(') != expression.count(')'):
            errors.append("Unbalanced parentheses in expression")

        # Check for empty braces
        if '{}' in expression:
            errors.append("Empty column reference found")

        # Check for invalid column reference format
        invalid_refs = re.findall(r'\{([^a-zA-Z_][^}]*)\}', expression)
        if invalid_refs:
            errors.append(f"Invalid column reference format (must start with letter or underscore): {invalid_refs}")

        # Check for forbidden aggregation functions
        expression_upper = expression.upper()
        for agg_func in self.FORBIDDEN_AGGREGATIONS:
            pattern = rf'\b{agg_func}\s*\('
            if re.search(pattern, expression_upper):
                errors.append(f"Aggregation function '{agg_func}' is not allowed in dimension expressions")

        # Check for dangerous SQL keywords
        for keyword in self.DANGEROUS_KEYWORDS:
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, expression_upper):
                errors.append(f"Dangerous SQL keyword '{keyword}' is not allowed")

        return errors

    def extract_column_references(self, expression: str) -> List[str]:
        """
        Extract all column references from an expression.

        Args:
            expression: SQL expression with {column_name} references

        Returns:
            List of unique column names referenced
        """
        column_refs = re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', expression)
        # Return unique, order-preserved
        return list(dict.fromkeys(column_refs))
