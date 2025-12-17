"""
Metric management service with formula parsing.
Handles CRUD operations for calculated metrics and converts
user-friendly formulas to SQL expressions.
"""
import re
import logging
from typing import List, Set, Tuple, Optional

from apps.schemas.models import SchemaConfig, CalculatedMetric, FormatType

logger = logging.getLogger(__name__)


class FormulaParser:
    """
    Parses user-friendly metric formulas and converts them to SQL expressions.

    Formula syntax:
    - {metric_id} - reference to a calculated metric
    - Operators: +, -, *, /
    - Parentheses: ( )
    - Comparison operators: >, <, >=, <=, =, !=, <>
    - Logical operators: AND, OR, NOT
    - CASE/WHEN expressions
    - Functions: SUM, AVG, COUNT, COUNT_DISTINCT, MIN, MAX, SAFE_DIVIDE
    """

    # SQL keywords that are allowed in formulas
    ALLOWED_KEYWORDS = {
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
        'IS', 'NULL', 'TRUE', 'FALSE',
        'LIKE', 'ILIKE',
        'SAFE_DIVIDE', 'COALESCE', 'IFNULL', 'NULLIF',
        'CAST', 'AS', 'STRING', 'INT64', 'FLOAT64', 'BOOL',
        'ABS', 'ROUND', 'FLOOR', 'CEIL', 'CEILING',
        'GREATEST', 'LEAST', 'IF',
        'SUM', 'AVG', 'COUNT', 'MIN', 'MAX',
        'COUNT_DISTINCT', 'DISTINCT',
        'APPROX_COUNT_DISTINCT', 'FARM_FINGERPRINT', 'CONCAT'
    }

    def __init__(self, schema_config: SchemaConfig):
        self.schema_config = schema_config

    def parse_formula(
        self,
        formula: str,
        current_metric_id: Optional[str] = None
    ) -> Tuple[str, List[str], List[str], List[str], List[str], List[str]]:
        """
        Parse a formula and return:
        (sql_expression, depends_on, depends_on_base, depends_on_calculated,
         depends_on_dimensions, errors)
        """
        errors = []
        depends_on_base = []  # Always empty - kept for backward compatibility
        depends_on_calculated = []
        depends_on_dimensions = []

        # Extract all metric references {metric_id}
        metric_refs = re.findall(r'\{([a-zA-Z0-9_]+)\}', formula)

        # Get existing calculated metric IDs
        calculated_metric_ids = set(
            self.schema_config.calculated_metrics.values_list('metric_id', flat=True)
        )

        # Add system metrics
        system_metrics = {'days_in_range'}
        all_metric_ids = calculated_metric_ids | system_metrics

        for metric_ref in metric_refs:
            if metric_ref not in all_metric_ids:
                errors.append(f"Unknown metric reference: {metric_ref}")
            else:
                if metric_ref in system_metrics:
                    depends_on_base.append(metric_ref)
                elif metric_ref in calculated_metric_ids:
                    if metric_ref not in depends_on_calculated:
                        depends_on_calculated.append(metric_ref)

        # Check for circular dependencies
        if current_metric_id and current_metric_id in depends_on_calculated:
            errors.append(
                f"Circular dependency detected: metric '{current_metric_id}' "
                "references itself"
            )

        # Check for deeper circular dependencies
        if current_metric_id and depends_on_calculated:
            circular_deps = self._detect_circular_dependencies(
                current_metric_id,
                depends_on_calculated,
                set()
            )
            if circular_deps:
                errors.append(
                    f"Circular dependency chain detected: {' -> '.join(circular_deps)}"
                )

        if errors:
            depends_on_all = depends_on_base + depends_on_calculated
            return "", depends_on_all, depends_on_base, depends_on_calculated, depends_on_dimensions, errors

        # Generate SQL by resolving metric references
        sql_expression = self._resolve_formula_to_sql(formula, current_metric_id)

        # Handle division - wrap with SAFE_DIVIDE
        sql_expression = self._convert_division_to_safe_divide(sql_expression)

        # Validate SQL expression
        if not self._is_valid_sql_expression(sql_expression):
            errors.append("Generated SQL expression appears invalid")

        # Extract dimension dependencies from SQL
        dimension_column_to_id = {
            d.column_name: d.dimension_id
            for d in self.schema_config.dimensions.all()
        }
        for column_name, dim_id in dimension_column_to_id.items():
            if re.search(rf'\b{re.escape(column_name)}\b', sql_expression):
                if dim_id not in depends_on_dimensions:
                    depends_on_dimensions.append(dim_id)

        depends_on_all = depends_on_base + depends_on_calculated
        return (
            sql_expression, depends_on_all, depends_on_base,
            depends_on_calculated, depends_on_dimensions, errors
        )

    def _detect_circular_dependencies(
        self,
        metric_id: str,
        depends_on: List[str],
        visited: Set[str]
    ) -> Optional[List[str]]:
        """Detect circular dependencies in calculated metrics."""
        for dep_id in depends_on:
            if dep_id == metric_id:
                return [metric_id, dep_id]

            if dep_id in visited:
                return [metric_id, dep_id]

            # Check if this dependency is a calculated metric
            try:
                dep_metric = self.schema_config.calculated_metrics.get(
                    metric_id=dep_id
                )
                if dep_metric.depends_on_calculated:
                    new_visited = visited | {dep_id}
                    chain = self._detect_circular_dependencies(
                        metric_id,
                        dep_metric.depends_on_calculated,
                        new_visited
                    )
                    if chain:
                        return [dep_id] + chain
            except CalculatedMetric.DoesNotExist:
                pass

        return None

    def _resolve_formula_to_sql(
        self,
        formula: str,
        current_metric_id: Optional[str] = None,
        visited: Optional[Set[str]] = None
    ) -> str:
        """
        Recursively resolve a formula to SQL by replacing metric references.
        """
        if visited is None:
            visited = set()
        if current_metric_id:
            visited.add(current_metric_id)

        metric_refs = re.findall(r'\{([a-zA-Z0-9_]+)\}', formula)

        sql_expression = formula
        for metric_ref in metric_refs:
            if metric_ref in visited:
                continue

            # Handle system metrics
            if metric_ref == 'days_in_range':
                replacement = "DATE_DIFF(MAX(date), MIN(date), DAY) + 1"
                sql_expression = sql_expression.replace(
                    f"{{{metric_ref}}}", replacement
                )
                continue

            # Find the calculated metric
            try:
                calc_metric = self.schema_config.calculated_metrics.get(
                    metric_id=metric_ref
                )
                # Recursively resolve
                resolved_sql = self._resolve_formula_to_sql(
                    calc_metric.formula,
                    metric_ref,
                    visited.copy()
                )
                sql_expression = sql_expression.replace(
                    f"{{{metric_ref}}}", f"({resolved_sql})"
                )
            except CalculatedMetric.DoesNotExist:
                pass

        return sql_expression

    def _convert_division_to_safe_divide(self, expression: str) -> str:
        """Convert division operations to SAFE_DIVIDE."""
        if '/' not in expression:
            return expression

        # Simple regex for "numerator / denominator" pattern
        division_pattern = r'(.+?)\s*/\s*(.+)'

        match = re.match(division_pattern, expression)
        if match:
            numerator = match.group(1).strip()
            denominator = match.group(2).strip()

            if '/' in denominator:
                denominator = self._convert_division_to_safe_divide(denominator)

            return f"SAFE_DIVIDE({numerator}, {denominator})"

        return expression

    def _is_valid_sql_expression(self, expression: str) -> bool:
        """Basic validation of SQL expression."""
        # Check for balanced parentheses
        if expression.count('(') != expression.count(')'):
            return False

        # Check for dangerous keywords
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'EXEC'
        ]
        expression_upper = expression.upper()
        for keyword in dangerous_keywords:
            if keyword in expression_upper:
                return False

        return True

    def validate_formula_syntax(self, formula: str) -> List[str]:
        """Validate formula syntax without schema context."""
        errors = []

        if formula.count('{') != formula.count('}'):
            errors.append("Unbalanced braces in formula")

        if formula.count('(') != formula.count(')'):
            errors.append("Unbalanced parentheses in formula")

        invalid_refs = re.findall(r'\{([^a-zA-Z0-9_{}]+)\}', formula)
        if invalid_refs:
            errors.append(f"Invalid metric reference format: {invalid_refs}")

        if '{}' in formula:
            errors.append("Empty metric reference found")

        # Validate CASE/WHEN syntax
        formula_upper = formula.upper()
        case_count = len(re.findall(r'\bCASE\b', formula_upper))
        end_count = len(re.findall(r'\bEND\b', formula_upper))
        if case_count != end_count:
            errors.append(
                f"Unbalanced CASE/END: found {case_count} CASE and {end_count} END"
            )

        if case_count > 0:
            when_count = len(re.findall(r'\bWHEN\b', formula_upper))
            then_count = len(re.findall(r'\bTHEN\b', formula_upper))

            if when_count == 0:
                errors.append("CASE expression requires at least one WHEN clause")

            if when_count != then_count:
                errors.append(
                    f"Unbalanced WHEN/THEN: found {when_count} WHEN "
                    f"and {then_count} THEN"
                )

        # Check for balanced quotes
        if formula.count("'") % 2 != 0:
            errors.append("Unbalanced single quotes in string literals")

        return errors


class MetricService:
    """Service for managing calculated metrics."""

    def __init__(self, schema_config: SchemaConfig):
        self.schema_config = schema_config
        self.formula_parser = FormulaParser(schema_config)

    def create_metric(
        self,
        display_name: str,
        formula: str,
        metric_id: Optional[str] = None,
        format_type: str = FormatType.NUMBER,
        decimal_places: int = 2,
        category: str = 'other',
        is_visible_by_default: bool = True,
        sort_order: int = 999,
        description: Optional[str] = None
    ) -> CalculatedMetric:
        """Create a new calculated metric with formula."""
        # Auto-generate ID from display name if not provided
        if not metric_id:
            metric_id = re.sub(r'[^\w\s-]', '', display_name.lower())
            metric_id = re.sub(r'[-\s]+', '_', metric_id)

            # Ensure uniqueness
            base_id = metric_id
            counter = 1
            while self.schema_config.calculated_metrics.filter(
                metric_id=metric_id
            ).exists():
                metric_id = f"{base_id}_{counter}"
                counter += 1

        # Check for duplicate
        if self.schema_config.calculated_metrics.filter(
            metric_id=metric_id
        ).exists():
            raise ValueError(
                f"Calculated metric with ID '{metric_id}' already exists"
            )

        # Validate formula syntax
        syntax_errors = self.formula_parser.validate_formula_syntax(formula)
        if syntax_errors:
            raise ValueError(f"Formula syntax errors: {', '.join(syntax_errors)}")

        # Parse formula
        (
            sql_expression, depends_on, depends_on_base,
            depends_on_calculated, depends_on_dimensions, parse_errors
        ) = self.formula_parser.parse_formula(formula, metric_id)

        if parse_errors:
            raise ValueError(f"Formula parse errors: {', '.join(parse_errors)}")

        # Create metric
        metric = CalculatedMetric.objects.create(
            schema_config=self.schema_config,
            metric_id=metric_id,
            display_name=display_name,
            formula=formula,
            sql_expression=sql_expression,
            depends_on=depends_on,
            depends_on_base=depends_on_base,
            depends_on_calculated=depends_on_calculated,
            depends_on_dimensions=depends_on_dimensions,
            format_type=format_type,
            decimal_places=decimal_places,
            category=category,
            is_visible_by_default=is_visible_by_default,
            sort_order=sort_order,
            description=description
        )

        return metric

    def update_metric(
        self,
        metric_id: str,
        **update_data
    ) -> CalculatedMetric:
        """Update an existing calculated metric."""
        try:
            metric = self.schema_config.calculated_metrics.get(metric_id=metric_id)
        except CalculatedMetric.DoesNotExist:
            raise ValueError(f"Calculated metric '{metric_id}' not found")

        # If formula is being updated, re-parse it
        if 'formula' in update_data:
            formula = update_data['formula']

            syntax_errors = self.formula_parser.validate_formula_syntax(formula)
            if syntax_errors:
                raise ValueError(
                    f"Formula syntax errors: {', '.join(syntax_errors)}"
                )

            (
                sql_expression, depends_on, depends_on_base,
                depends_on_calculated, depends_on_dimensions, parse_errors
            ) = self.formula_parser.parse_formula(formula, metric_id)

            if parse_errors:
                raise ValueError(
                    f"Formula parse errors: {', '.join(parse_errors)}"
                )

            metric.formula = formula
            metric.sql_expression = sql_expression
            metric.depends_on = depends_on
            metric.depends_on_base = depends_on_base
            metric.depends_on_calculated = depends_on_calculated
            metric.depends_on_dimensions = depends_on_dimensions

        # Update other fields
        for field in [
            'display_name', 'format_type', 'decimal_places',
            'category', 'is_visible_by_default', 'sort_order', 'description'
        ]:
            if field in update_data:
                setattr(metric, field, update_data[field])

        metric.save()
        return metric

    def delete_metric(self, metric_id: str) -> None:
        """Delete a calculated metric."""
        deleted_count, _ = self.schema_config.calculated_metrics.filter(
            metric_id=metric_id
        ).delete()

        if deleted_count == 0:
            raise ValueError(f"Calculated metric '{metric_id}' not found")

    def get_metric(self, metric_id: str) -> Optional[CalculatedMetric]:
        """Get a calculated metric by ID."""
        try:
            return self.schema_config.calculated_metrics.get(metric_id=metric_id)
        except CalculatedMetric.DoesNotExist:
            return None

    def list_metrics(self) -> List[CalculatedMetric]:
        """List all calculated metrics."""
        return list(
            self.schema_config.calculated_metrics.order_by('sort_order')
        )

    def validate_formula(self, formula: str) -> dict:
        """
        Validate a formula without saving it.
        Returns dict with validation results.
        """
        syntax_errors = self.formula_parser.validate_formula_syntax(formula)
        if syntax_errors:
            return {
                'valid': False,
                'errors': syntax_errors,
                'sql_expression': '',
                'depends_on': [],
                'depends_on_base': [],
                'depends_on_calculated': [],
                'depends_on_dimensions': []
            }

        (
            sql_expression, depends_on, depends_on_base,
            depends_on_calculated, depends_on_dimensions, parse_errors
        ) = self.formula_parser.parse_formula(formula)

        if parse_errors:
            return {
                'valid': False,
                'errors': parse_errors,
                'sql_expression': sql_expression,
                'depends_on': depends_on,
                'depends_on_base': depends_on_base,
                'depends_on_calculated': depends_on_calculated,
                'depends_on_dimensions': depends_on_dimensions
            }

        return {
            'valid': True,
            'errors': [],
            'sql_expression': sql_expression,
            'depends_on': depends_on,
            'depends_on_base': depends_on_base,
            'depends_on_calculated': depends_on_calculated,
            'depends_on_dimensions': depends_on_dimensions
        }

    def get_dependents(self, metric_id: str) -> List[str]:
        """Get all metrics that depend on the given metric."""
        dependents = []
        for metric in self.schema_config.calculated_metrics.all():
            if metric_id in metric.depends_on_calculated:
                dependents.append(metric.metric_id)
        return dependents

    def cascade_update_dependents(self, metric_id: str) -> dict:
        """
        Cascade update all metrics that depend on the given metric.
        Re-parses formulas to regenerate SQL expressions.
        """
        dependents = self.get_dependents(metric_id)
        if not dependents:
            return {'updated_count': 0, 'updated_metrics': []}

        updated_metrics = []
        for dep_id in dependents:
            try:
                metric = self.schema_config.calculated_metrics.get(
                    metric_id=dep_id
                )
                (
                    sql_expression, depends_on, depends_on_base,
                    depends_on_calculated, depends_on_dimensions, errors
                ) = self.formula_parser.parse_formula(metric.formula, dep_id)

                if not errors:
                    metric.sql_expression = sql_expression
                    metric.depends_on = depends_on
                    metric.depends_on_base = depends_on_base
                    metric.depends_on_calculated = depends_on_calculated
                    metric.depends_on_dimensions = depends_on_dimensions
                    metric.save()
                    updated_metrics.append(dep_id)

            except CalculatedMetric.DoesNotExist:
                continue

        return {
            'updated_count': len(updated_metrics),
            'updated_metrics': updated_metrics
        }

    def extract_formula_components(self, metric_id: str) -> Optional[dict]:
        """
        Extract numerator and denominator metric IDs from a calculated metric formula.
        Only works for simple ratio formulas like {A} / {B}.

        Args:
            metric_id: ID of the calculated metric

        Returns:
            Dict with:
                - numerator_metric_id: str (e.g., 'queries_pdp')
                - denominator_metric_id: str (e.g., 'queries')
                - is_simple_ratio: bool (True if formula is simple A/B)
            Returns None if metric not found or not a calculated metric.
        """
        try:
            metric = self.schema_config.calculated_metrics.get(metric_id=metric_id)
        except CalculatedMetric.DoesNotExist:
            return None

        # Check if it's a percent format metric
        if metric.format_type != FormatType.PERCENT:
            return {
                'numerator_metric_id': None,
                'denominator_metric_id': None,
                'is_simple_ratio': False,
                'reason': 'Not a percent format metric'
            }

        formula = metric.formula.strip()

        # Pattern for simple ratio: {metric_a} / {metric_b}
        # Allows optional whitespace around the division operator
        simple_ratio_pattern = r'^\{([a-zA-Z0-9_]+)\}\s*/\s*\{([a-zA-Z0-9_]+)\}$'
        match = re.match(simple_ratio_pattern, formula)

        if match:
            numerator_id = match.group(1)
            denominator_id = match.group(2)

            # All metrics are calculated metrics now
            calculated_metric_ids = set(
                self.schema_config.calculated_metrics.values_list('metric_id', flat=True)
            )

            # Verify numerator exists as a metric
            if numerator_id not in calculated_metric_ids:
                return {
                    'numerator_metric_id': numerator_id,
                    'denominator_metric_id': denominator_id,
                    'is_simple_ratio': False,
                    'reason': f'Numerator {numerator_id} is not a valid metric'
                }

            # Verify denominator exists as a metric
            if denominator_id not in calculated_metric_ids:
                return {
                    'numerator_metric_id': numerator_id,
                    'denominator_metric_id': denominator_id,
                    'is_simple_ratio': False,
                    'reason': f'Denominator {denominator_id} is not a valid metric'
                }

            return {
                'numerator_metric_id': numerator_id,
                'denominator_metric_id': denominator_id,
                'is_simple_ratio': True,
                'reason': None
            }
        else:
            return {
                'numerator_metric_id': None,
                'denominator_metric_id': None,
                'is_simple_ratio': False,
                'reason': 'Formula is not a simple {A}/{B} ratio'
            }

    def get_eligible_significance_metrics(self) -> List[dict]:
        """
        Get all calculated metrics that are eligible for significance testing.
        Only percent-format metrics with simple {A}/{B} formulas are eligible.

        Returns:
            List of dicts with:
                - metric_id: str
                - display_name: str
                - numerator_metric_id: str
                - denominator_metric_id: str
        """
        eligible = []
        for metric in self.schema_config.calculated_metrics.all():
            if metric.format_type != FormatType.PERCENT:
                continue

            components = self.extract_formula_components(metric.metric_id)
            if components and components.get('is_simple_ratio'):
                eligible.append({
                    'metric_id': metric.metric_id,
                    'display_name': metric.display_name,
                    'numerator_metric_id': components['numerator_metric_id'],
                    'denominator_metric_id': components['denominator_metric_id']
                })

        return eligible
