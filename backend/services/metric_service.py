"""
Metric management service with formula parsing.
Handles CRUD operations for base and calculated metrics, and converts
user-friendly formulas to SQL expressions.
"""
import re
from typing import List, Set, Tuple, Optional
from datetime import datetime

from models.schemas import (
    BaseMetric, CalculatedMetric, SchemaConfig,
    MetricCreate, CalculatedMetricCreate,
    MetricUpdate, CalculatedMetricUpdate
)
from services.schema_service import SchemaService


class FormulaParser:
    """
    Parses user-friendly metric formulas and converts them to SQL expressions.

    Formula syntax:
    - {metric_id} - reference to a base metric
    - Operators: +, -, *, /
    - Parentheses: ( )
    - Functions: SUM, AVG, COUNT, COUNT_DISTINCT, MIN, MAX

    Examples:
    - {queries_pdp} / {queries} → SAFE_DIVIDE(SUM(queries_pdp), SUM(queries))
    - ({purchases} + {returns}) / {queries} → SAFE_DIVIDE((SUM(purchases) + SUM(returns)), SUM(queries))
    """

    def __init__(self, schema_service: SchemaService):
        self.schema_service = schema_service

    def parse_formula(self, formula: str, schema: SchemaConfig) -> Tuple[str, List[str], List[str]]:
        """
        Parse a formula and return (sql_expression, depends_on, errors).

        Args:
            formula: User formula like "{queries_pdp} / {queries}"
            schema: Schema configuration to look up metric definitions

        Returns:
            Tuple of (sql_expression, depends_on_metric_ids, errors)
        """
        errors = []
        depends_on = []

        # Extract all metric references {metric_id}
        metric_refs = re.findall(r'\{([a-zA-Z0-9_]+)\}', formula)

        # Validate that all referenced metrics exist (both base and calculated)
        base_metric_ids = {m.id for m in schema.base_metrics}
        calculated_metric_ids = {m.id for m in schema.calculated_metrics}
        all_metric_ids = base_metric_ids | calculated_metric_ids

        for metric_ref in metric_refs:
            if metric_ref not in all_metric_ids:
                errors.append(f"Unknown metric reference: {metric_ref}")
            else:
                if metric_ref not in depends_on:
                    depends_on.append(metric_ref)

        if errors:
            return "", depends_on, errors

        # Replace base metric references with their aggregated forms
        # Note: Formulas should not contain {calculated_metric} references because
        # the formula builder UI expands calculated metrics inline
        sql_expression = formula
        for metric_ref in metric_refs:
            base_metric = next((m for m in schema.base_metrics if m.id == metric_ref), None)
            if base_metric:
                agg_func = base_metric.aggregation
                # Replace {metric_id} with AGG(column_name)
                if agg_func == 'COUNT_DISTINCT':
                    replacement = f"COUNT(DISTINCT {base_metric.column_name})"
                else:
                    replacement = f"{agg_func}({base_metric.column_name})"
                sql_expression = sql_expression.replace(f"{{{metric_ref}}}", replacement)

        # Handle division specially - wrap with SAFE_DIVIDE
        sql_expression = self._convert_division_to_safe_divide(sql_expression)

        # Validate SQL expression (basic check)
        if not self._is_valid_sql_expression(sql_expression):
            errors.append("Generated SQL expression appears invalid")

        return sql_expression, depends_on, errors

    def _convert_division_to_safe_divide(self, expression: str) -> str:
        """
        Convert division operations to SAFE_DIVIDE.

        Simple approach: Find top-level divisions (not inside parentheses or functions)
        and wrap them with SAFE_DIVIDE.
        """
        # This is a simplified implementation
        # For a production system, you'd want a proper expression parser

        # Check if expression contains division at top level
        if '/' not in expression:
            return expression

        # For simple case: "A / B" → "SAFE_DIVIDE(A, B)"
        # For complex case with multiple operations, we need to handle operator precedence

        # Simple regex for "numerator / denominator" pattern
        # This handles cases like "SUM(x) / SUM(y)" or "(A + B) / (C + D)"
        division_pattern = r'(.+?)\s*/\s*(.+)'

        match = re.match(division_pattern, expression)
        if match:
            numerator = match.group(1).strip()
            denominator = match.group(2).strip()

            # Handle nested divisions
            if '/' in denominator:
                denominator = self._convert_division_to_safe_divide(denominator)

            return f"SAFE_DIVIDE({numerator}, {denominator})"

        return expression

    def _is_valid_sql_expression(self, expression: str) -> bool:
        """Basic validation of SQL expression"""
        # Check for balanced parentheses
        if expression.count('(') != expression.count(')'):
            return False

        # Check for dangerous keywords (SQL injection prevention)
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'EXEC']
        expression_upper = expression.upper()
        for keyword in dangerous_keywords:
            if keyword in expression_upper:
                return False

        return True

    def validate_formula_syntax(self, formula: str) -> List[str]:
        """Validate formula syntax without schema context"""
        errors = []

        # Check for balanced braces
        if formula.count('{') != formula.count('}'):
            errors.append("Unbalanced braces in formula")

        # Check for balanced parentheses
        if formula.count('(') != formula.count(')'):
            errors.append("Unbalanced parentheses in formula")

        # Check for valid metric reference format
        invalid_refs = re.findall(r'\{([^a-zA-Z0-9_{}]+)\}', formula)
        if invalid_refs:
            errors.append(f"Invalid metric reference format: {invalid_refs}")

        # Check for empty braces
        if '{}' in formula:
            errors.append("Empty metric reference found")

        return errors


class MetricService:
    """Service for managing metrics (base and calculated)"""

    def __init__(self, schema_service: SchemaService):
        self.schema_service = schema_service
        self.formula_parser = FormulaParser(schema_service)

    def create_base_metric(self, metric_data: MetricCreate) -> BaseMetric:
        """Create a new base metric"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found. Please initialize schema first.")

        # Check for duplicate ID
        if any(m.id == metric_data.id for m in schema.base_metrics):
            raise ValueError(f"Metric with ID '{metric_data.id}' already exists")

        # Create metric
        metric = BaseMetric(**metric_data.model_dump())

        # Add to schema
        schema.base_metrics.append(metric)
        self.schema_service.save_schema(schema)

        return metric

    def create_calculated_metric(self, metric_data: CalculatedMetricCreate) -> CalculatedMetric:
        """Create a new calculated metric with formula"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found. Please initialize schema first.")

        # Check for duplicate ID
        if any(m.id == metric_data.id for m in schema.calculated_metrics):
            raise ValueError(f"Calculated metric with ID '{metric_data.id}' already exists")

        # Validate and parse formula
        syntax_errors = self.formula_parser.validate_formula_syntax(metric_data.formula)
        if syntax_errors:
            raise ValueError(f"Formula syntax errors: {', '.join(syntax_errors)}")

        sql_expression, depends_on, parse_errors = self.formula_parser.parse_formula(
            metric_data.formula,
            schema
        )

        if parse_errors:
            raise ValueError(f"Formula parse errors: {', '.join(parse_errors)}")

        # Create metric
        metric = CalculatedMetric(
            id=metric_data.id,
            display_name=metric_data.display_name,
            formula=metric_data.formula,
            sql_expression=sql_expression,
            depends_on=depends_on,
            format_type=metric_data.format_type,
            decimal_places=metric_data.decimal_places,
            category=metric_data.category,
            is_visible_by_default=metric_data.is_visible_by_default,
            sort_order=metric_data.sort_order,
            description=metric_data.description
        )

        # Add to schema
        schema.calculated_metrics.append(metric)
        self.schema_service.save_schema(schema)

        return metric

    def update_base_metric(self, metric_id: str, update_data: MetricUpdate) -> BaseMetric:
        """Update an existing base metric"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Find metric
        metric = next((m for m in schema.base_metrics if m.id == metric_id), None)
        if not metric:
            raise ValueError(f"Base metric '{metric_id}' not found")

        # Update fields
        update_dict = update_data.model_dump(exclude_unset=True)
        for field, value in update_dict.items():
            setattr(metric, field, value)

        self.schema_service.save_schema(schema)

        return metric

    def update_calculated_metric(self, metric_id: str, update_data: CalculatedMetricUpdate) -> CalculatedMetric:
        """Update an existing calculated metric"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Find metric
        metric = next((m for m in schema.calculated_metrics if m.id == metric_id), None)
        if not metric:
            raise ValueError(f"Calculated metric '{metric_id}' not found")

        # If formula is being updated, re-parse it
        if update_data.formula:
            syntax_errors = self.formula_parser.validate_formula_syntax(update_data.formula)
            if syntax_errors:
                raise ValueError(f"Formula syntax errors: {', '.join(syntax_errors)}")

            sql_expression, depends_on, parse_errors = self.formula_parser.parse_formula(
                update_data.formula,
                schema
            )

            if parse_errors:
                raise ValueError(f"Formula parse errors: {', '.join(parse_errors)}")

            metric.formula = update_data.formula
            metric.sql_expression = sql_expression
            metric.depends_on = depends_on

        # Update other fields
        update_dict = update_data.model_dump(exclude_unset=True, exclude={'formula'})
        for field, value in update_dict.items():
            setattr(metric, field, value)

        self.schema_service.save_schema(schema)

        return metric

    def delete_base_metric(self, metric_id: str) -> None:
        """Delete a base metric"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Check if any calculated metrics depend on this metric
        dependent_metrics = [
            m.id for m in schema.calculated_metrics
            if metric_id in m.depends_on
        ]

        if dependent_metrics:
            raise ValueError(
                f"Cannot delete metric '{metric_id}' because it is used by calculated metrics: "
                f"{', '.join(dependent_metrics)}"
            )

        # Remove metric
        schema.base_metrics = [m for m in schema.base_metrics if m.id != metric_id]
        self.schema_service.save_schema(schema)

    def delete_calculated_metric(self, metric_id: str) -> None:
        """Delete a calculated metric"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Remove metric
        schema.calculated_metrics = [m for m in schema.calculated_metrics if m.id != metric_id]
        self.schema_service.save_schema(schema)

    def get_base_metric(self, metric_id: str) -> Optional[BaseMetric]:
        """Get a base metric by ID"""
        schema = self.schema_service.load_schema()
        if not schema:
            return None

        return next((m for m in schema.base_metrics if m.id == metric_id), None)

    def get_calculated_metric(self, metric_id: str) -> Optional[CalculatedMetric]:
        """Get a calculated metric by ID"""
        schema = self.schema_service.load_schema()
        if not schema:
            return None

        return next((m for m in schema.calculated_metrics if m.id == metric_id), None)

    def list_base_metrics(self) -> List[BaseMetric]:
        """List all base metrics"""
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        return sorted(schema.base_metrics, key=lambda m: m.sort_order)

    def list_calculated_metrics(self) -> List[CalculatedMetric]:
        """List all calculated metrics"""
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        return sorted(schema.calculated_metrics, key=lambda m: m.sort_order)

    def validate_formula(self, formula: str) -> dict:
        """
        Validate a formula without saving it.
        Returns dict with: {valid: bool, sql_expression: str, depends_on: list, errors: list}
        """
        schema = self.schema_service.load_schema()
        if not schema:
            return {
                'valid': False,
                'errors': ['Schema not loaded'],
                'sql_expression': '',
                'depends_on': []
            }

        # Check syntax
        syntax_errors = self.formula_parser.validate_formula_syntax(formula)
        if syntax_errors:
            return {
                'valid': False,
                'errors': syntax_errors,
                'sql_expression': '',
                'depends_on': []
            }

        # Parse formula
        sql_expression, depends_on, parse_errors = self.formula_parser.parse_formula(formula, schema)

        if parse_errors:
            return {
                'valid': False,
                'errors': parse_errors,
                'sql_expression': sql_expression,
                'depends_on': depends_on
            }

        return {
            'valid': True,
            'errors': [],
            'sql_expression': sql_expression,
            'depends_on': depends_on
        }
