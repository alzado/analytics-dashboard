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

    def parse_formula(self, formula: str, schema: SchemaConfig, current_metric_id: Optional[str] = None) -> Tuple[str, List[str], List[str], List[str], List[str]]:
        """
        Parse a formula and return (sql_expression, depends_on, depends_on_base, depends_on_calculated, errors).

        Args:
            formula: User formula like "{queries_pdp} / {queries}"
            schema: Schema configuration to look up metric definitions
            current_metric_id: ID of the metric being created/updated (for circular dependency detection)

        Returns:
            Tuple of (sql_expression, depends_on_all, depends_on_base, depends_on_calculated, errors)
        """
        errors = []
        depends_on_base = []
        depends_on_calculated = []

        # Extract all metric references {metric_id}
        metric_refs = re.findall(r'\{([a-zA-Z0-9_]+)\}', formula)

        # Validate that all referenced metrics exist (both base and calculated)
        base_metric_ids = {m.id for m in schema.base_metrics}
        calculated_metric_ids = {m.id for m in schema.calculated_metrics}

        # Add system metrics that are always available
        system_metrics = {'days_in_range'}
        all_metric_ids = base_metric_ids | calculated_metric_ids | system_metrics

        for metric_ref in metric_refs:
            print(f"DEBUG parse_formula: Processing metric_ref '{metric_ref}'")
            if metric_ref not in all_metric_ids:
                errors.append(f"Unknown metric reference: {metric_ref}")
            else:
                # Track which type of metric is referenced
                if metric_ref in system_metrics and metric_ref not in depends_on_base:
                    # System metrics are treated as base metrics
                    print(f"DEBUG parse_formula: Adding system metric '{metric_ref}' to depends_on_base")
                    depends_on_base.append(metric_ref)
                elif metric_ref in base_metric_ids and metric_ref not in depends_on_base:
                    print(f"DEBUG parse_formula: Adding base metric '{metric_ref}' to depends_on_base")
                    depends_on_base.append(metric_ref)
                elif metric_ref in calculated_metric_ids and metric_ref not in depends_on_calculated:
                    print(f"DEBUG parse_formula: Adding calculated metric '{metric_ref}' to depends_on_calculated")
                    depends_on_calculated.append(metric_ref)

        # Check for circular dependencies
        if current_metric_id and current_metric_id in depends_on_calculated:
            errors.append(f"Circular dependency detected: metric '{current_metric_id}' references itself")

        # Check for deeper circular dependencies
        if current_metric_id and depends_on_calculated:
            circular_deps = self._detect_circular_dependencies(
                current_metric_id,
                depends_on_calculated,
                schema
            )
            if circular_deps:
                errors.append(f"Circular dependency chain detected: {' -> '.join(circular_deps)}")

        if errors:
            depends_on_all = depends_on_base + depends_on_calculated
            return "", depends_on_all, depends_on_base, depends_on_calculated, errors

        # Generate SQL by recursively resolving metric references
        sql_expression = self._resolve_formula_to_sql(formula, schema, current_metric_id)

        # Handle division specially - wrap with SAFE_DIVIDE
        sql_expression = self._convert_division_to_safe_divide(sql_expression)

        # Validate SQL expression (basic check)
        if not self._is_valid_sql_expression(sql_expression):
            errors.append("Generated SQL expression appears invalid")

        depends_on_all = depends_on_base + depends_on_calculated
        return sql_expression, depends_on_all, depends_on_base, depends_on_calculated, errors

    def _detect_circular_dependencies(self, metric_id: str, depends_on: List[str], schema: SchemaConfig, visited: Optional[Set[str]] = None) -> Optional[List[str]]:
        """
        Detect circular dependencies in calculated metrics.
        Returns the circular dependency chain if found, None otherwise.
        """
        if visited is None:
            visited = {metric_id}

        for dep_id in depends_on:
            if dep_id == metric_id:
                return [metric_id, dep_id]

            if dep_id in visited:
                return [metric_id, dep_id]

            # Check if this dependency is a calculated metric
            dep_metric = next((m for m in schema.calculated_metrics if m.id == dep_id), None)
            if dep_metric and dep_metric.depends_on_calculated:
                new_visited = visited | {dep_id}
                chain = self._detect_circular_dependencies(
                    metric_id,
                    dep_metric.depends_on_calculated,
                    schema,
                    new_visited
                )
                if chain:
                    return [dep_id] + chain

        return None

    def _resolve_formula_to_sql(self, formula: str, schema: SchemaConfig, current_metric_id: Optional[str] = None, visited: Optional[Set[str]] = None, resolve_for_subquery: bool = False) -> str:
        """
        Recursively resolve a formula to SQL by replacing metric references with their SQL expressions.
        This allows calculated metrics to reference other calculated metrics, and changes to base metrics
        propagate through all dependent calculated metrics.

        Args:
            formula: Formula with {metric_id} references
            schema: Schema configuration
            current_metric_id: ID of current metric being resolved (for cycle detection)
            visited: Set of metric IDs already visited (for cycle detection)
            resolve_for_subquery: If True, resolve for use in outer SELECT referencing subquery aliases.
                                   System metrics like days_in_range will be treated as aliases.

        Returns:
            SQL expression with all metric references resolved
        """
        import re

        if visited is None:
            visited = set()
        if current_metric_id:
            visited.add(current_metric_id)

        # Find all metric references in the formula
        metric_refs = re.findall(r'\{([a-zA-Z0-9_]+)\}', formula)

        # Check if formula contains days_in_range - if so, we need subquery mode
        has_days_in_range = 'days_in_range' in metric_refs

        # If this formula has days_in_range and references other calculated metrics,
        # we need to use alias mode to avoid nested aggregations
        if has_days_in_range:
            has_calculated_deps = any(
                metric_ref != 'days_in_range' and
                any(m.id == metric_ref for m in schema.calculated_metrics)
                for metric_ref in metric_refs
            )
            if has_calculated_deps:
                resolve_for_subquery = True

        sql_expression = formula
        for metric_ref in metric_refs:
            # Check for circular reference
            if metric_ref in visited:
                # Skip circular reference - this will be caught by validation
                continue

            # Special handling for system metrics
            if metric_ref == 'days_in_range':
                if resolve_for_subquery:
                    # In subquery mode, reference days_in_range as an alias from the inner query
                    replacement = "days_in_range"
                else:
                    # Virtual metric: days_in_range computes DATE_DIFF between min and max dates
                    replacement = "DATE_DIFF(MAX(date), MIN(date), DAY) + 1"
                sql_expression = sql_expression.replace(f"{{{metric_ref}}}", replacement)
                continue

            # Check if it's a base metric
            base_metric = next((m for m in schema.base_metrics if m.id == metric_ref), None)
            if base_metric:
                if resolve_for_subquery:
                    # In subquery mode, reference base metric by alias
                    replacement = metric_ref
                else:
                    # Regular base metric: Replace with aggregated column
                    agg_func = base_metric.aggregation
                    if agg_func == 'COUNT_DISTINCT':
                        replacement = f"COUNT(DISTINCT {base_metric.column_name})"
                    else:
                        replacement = f"{agg_func}({base_metric.column_name})"
                sql_expression = sql_expression.replace(f"{{{metric_ref}}}", replacement)
            else:
                # It's a calculated metric - recursively resolve its formula
                calc_metric = next((m for m in schema.calculated_metrics if m.id == metric_ref), None)
                if calc_metric:
                    if resolve_for_subquery:
                        # In subquery mode, reference calculated metric by alias
                        # This assumes the calculated metric is computed in an inner subquery
                        replacement = metric_ref
                        sql_expression = sql_expression.replace(f"{{{metric_ref}}}", replacement)
                    else:
                        # Recursively resolve the calculated metric's formula
                        resolved_sql = self._resolve_formula_to_sql(
                            calc_metric.formula,
                            schema,
                            metric_ref,
                            visited.copy(),
                            resolve_for_subquery
                        )
                        # Wrap in parentheses to preserve order of operations
                        sql_expression = sql_expression.replace(f"{{{metric_ref}}}", f"({resolved_sql})")

        return sql_expression

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

        # Auto-generate ID from display name if not provided
        if not metric_data.id:
            import re
            # Convert display name to snake_case ID
            metric_id = re.sub(r'[^\w\s-]', '', metric_data.display_name.lower())
            metric_id = re.sub(r'[-\s]+', '_', metric_id)

            # Ensure uniqueness by appending number if needed
            base_id = metric_id
            counter = 1
            while any(m.id == metric_id for m in schema.calculated_metrics):
                metric_id = f"{base_id}_{counter}"
                counter += 1

            metric_data.id = metric_id

        # Check for duplicate ID
        if any(m.id == metric_data.id for m in schema.calculated_metrics):
            raise ValueError(f"Calculated metric with ID '{metric_data.id}' already exists")

        # Validate and parse formula
        syntax_errors = self.formula_parser.validate_formula_syntax(metric_data.formula)
        if syntax_errors:
            raise ValueError(f"Formula syntax errors: {', '.join(syntax_errors)}")

        sql_expression, depends_on, depends_on_base, depends_on_calculated, parse_errors = self.formula_parser.parse_formula(
            metric_data.formula,
            schema,
            current_metric_id=metric_data.id
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
            depends_on_base=depends_on_base,
            depends_on_calculated=depends_on_calculated,
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

    def create_daily_average_metric(self, source_metric_id: str, table_id: Optional[str] = None) -> CalculatedMetric:
        """
        Create a daily average metric (per-day version) from a source metric.

        Args:
            source_metric_id: ID of the source metric to create daily average for
            table_id: Optional table ID (for multi-table support)

        Returns:
            Created CalculatedMetric with formula: {source_metric} / {days_in_range}

        Raises:
            ValueError: If source metric not found, not volume category, or daily version already exists
        """
        schema = self.schema_service.load_schema()

        # Find source metric (check both base and calculated metrics)
        source_metric = next((m for m in schema.base_metrics if m.id == source_metric_id), None)
        is_base_metric = source_metric is not None

        if not source_metric:
            source_metric = next((m for m in schema.calculated_metrics if m.id == source_metric_id), None)

        if not source_metric:
            raise ValueError(f"Source metric '{source_metric_id}' not found")

        # Validate source metric is volume category
        if source_metric.category != 'volume':
            raise ValueError(f"Only volume metrics can have daily averages. Metric '{source_metric_id}' has category '{source_metric.category}'")

        # Generate daily average metric ID
        daily_avg_id = f"{source_metric_id}_per_day"

        # Check if daily average metric already exists
        existing = next((m for m in schema.calculated_metrics if m.id == daily_avg_id), None)
        if existing:
            raise ValueError(f"Daily average metric '{daily_avg_id}' already exists")

        # Create formula: {source_metric} / {days_in_range}
        formula = f"{{{source_metric_id}}} / {{days_in_range}}"

        # Parse and generate SQL expression
        sql_expression, depends_on_all, depends_on_base, depends_on_calculated, errors = self.formula_parser.parse_formula(formula, schema)

        if errors:
            raise ValueError(f"Failed to parse formula: {', '.join(errors)}")

        # Create calculated metric with inherited properties
        metric = CalculatedMetric(
            id=daily_avg_id,
            display_name=f"{source_metric.display_name} per Day",
            formula=formula,
            sql_expression=sql_expression,
            depends_on=depends_on_all,
            depends_on_base=depends_on_base,
            depends_on_calculated=depends_on_calculated,
            format_type=source_metric.format_type,  # Inherit format from source
            decimal_places=2,  # Daily averages typically need more precision
            category='volume_daily',  # New category for daily averages
            is_visible_by_default=True,
            sort_order=source_metric.sort_order + 500,  # Place after source metric
            description=f"Daily average of {source_metric.display_name} (divided by days in filtered date range)"
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

            sql_expression, depends_on, depends_on_base, depends_on_calculated, parse_errors = self.formula_parser.parse_formula(
                update_data.formula,
                schema,
                current_metric_id=metric_id
            )

            if parse_errors:
                raise ValueError(f"Formula parse errors: {', '.join(parse_errors)}")

            metric.formula = update_data.formula
            metric.sql_expression = sql_expression
            metric.depends_on = depends_on
            metric.depends_on_base = depends_on_base
            metric.depends_on_calculated = depends_on_calculated

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
        Returns dict with: {valid: bool, sql_expression: str, depends_on: list, depends_on_base: list, depends_on_calculated: list, errors: list}
        """
        schema = self.schema_service.load_schema()
        if not schema:
            return {
                'valid': False,
                'errors': ['Schema not loaded'],
                'sql_expression': '',
                'depends_on': [],
                'depends_on_base': [],
                'depends_on_calculated': []
            }

        # Check syntax
        syntax_errors = self.formula_parser.validate_formula_syntax(formula)
        if syntax_errors:
            return {
                'valid': False,
                'errors': syntax_errors,
                'sql_expression': '',
                'depends_on': [],
                'depends_on_base': [],
                'depends_on_calculated': []
            }

        # Parse formula
        sql_expression, depends_on, depends_on_base, depends_on_calculated, parse_errors = self.formula_parser.parse_formula(formula, schema)

        if parse_errors:
            return {
                'valid': False,
                'errors': parse_errors,
                'sql_expression': sql_expression,
                'depends_on': depends_on,
                'depends_on_base': depends_on_base,
                'depends_on_calculated': depends_on_calculated
            }

        return {
            'valid': True,
            'errors': [],
            'sql_expression': sql_expression,
            'depends_on': depends_on,
            'depends_on_base': depends_on_base,
            'depends_on_calculated': depends_on_calculated
        }

    def get_all_dependents(self, metric_id: str, metric_type: str = 'base') -> List[str]:
        """
        Get all calculated metrics that depend on the given metric (directly or indirectly).

        Args:
            metric_id: ID of the metric to find dependents for
            metric_type: 'base' or 'calculated'

        Returns:
            List of calculated metric IDs that depend on this metric
        """
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        dependents = set()

        # Find direct dependents
        if metric_type == 'base':
            direct_dependents = [
                m.id for m in schema.calculated_metrics
                if metric_id in m.depends_on_base
            ]
        else:  # calculated
            direct_dependents = [
                m.id for m in schema.calculated_metrics
                if metric_id in m.depends_on_calculated
            ]

        # Recursively find indirect dependents
        for dep_id in direct_dependents:
            dependents.add(dep_id)
            # Find metrics that depend on this dependent
            indirect_dependents = self.get_all_dependents(dep_id, 'calculated')
            dependents.update(indirect_dependents)

        return sorted(list(dependents))

    def _topological_sort_metrics(self, metric_ids: List[str], schema: SchemaConfig) -> List[str]:
        """
        Sort calculated metrics in dependency order (metrics with no dependencies first).
        This ensures that when we re-parse formulas, dependencies are updated before dependents.

        Args:
            metric_ids: List of calculated metric IDs to sort
            schema: Current schema

        Returns:
            Sorted list of metric IDs
        """
        # Build dependency graph
        in_degree = {mid: 0 for mid in metric_ids}
        adj_list = {mid: [] for mid in metric_ids}

        for metric_id in metric_ids:
            metric = next((m for m in schema.calculated_metrics if m.id == metric_id), None)
            if not metric:
                continue

            # Count dependencies that are in our update set
            deps_in_set = [d for d in metric.depends_on_calculated if d in metric_ids]
            in_degree[metric_id] = len(deps_in_set)

            # Build adjacency list (reverse direction: dep -> dependent)
            for dep_id in deps_in_set:
                if dep_id in adj_list:
                    adj_list[dep_id].append(metric_id)

        # Kahn's algorithm for topological sort
        queue = [mid for mid in metric_ids if in_degree[mid] == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(current)

            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If result doesn't contain all metrics, there's a cycle (shouldn't happen with our validation)
        if len(result) != len(metric_ids):
            # Fall back to original order
            return metric_ids

        return result

    def cascade_update_dependents(self, metric_id: str, metric_type: str = 'base') -> dict:
        """
        Cascade update all calculated metrics that depend on the given metric.
        Re-parses formulas to regenerate SQL expressions.

        Args:
            metric_id: ID of the metric that was changed
            metric_type: 'base' or 'calculated'

        Returns:
            Dict with: {updated_count: int, updated_metrics: List[str]}
        """
        schema = self.schema_service.load_schema()
        if not schema:
            return {'updated_count': 0, 'updated_metrics': []}

        # Find all metrics that need updating
        dependent_ids = self.get_all_dependents(metric_id, metric_type)

        if not dependent_ids:
            return {'updated_count': 0, 'updated_metrics': []}

        # Sort in dependency order
        sorted_ids = self._topological_sort_metrics(dependent_ids, schema)

        updated_metrics = []

        # Re-parse each metric's formula in dependency order
        for dep_id in sorted_ids:
            metric = next((m for m in schema.calculated_metrics if m.id == dep_id), None)
            if not metric:
                continue

            try:
                # Re-parse the formula with current schema (which has updated dependencies)
                sql_expression, depends_on, depends_on_base, depends_on_calculated, parse_errors = self.formula_parser.parse_formula(
                    metric.formula,
                    schema,
                    current_metric_id=dep_id
                )

                if parse_errors:
                    # Log error but continue with other metrics
                    continue

                # Update the metric
                metric.sql_expression = sql_expression
                metric.depends_on = depends_on
                metric.depends_on_base = depends_on_base
                metric.depends_on_calculated = depends_on_calculated

                updated_metrics.append(dep_id)

            except Exception as e:
                continue

        # Save updated schema
        if updated_metrics:
            self.schema_service.save_schema(schema)

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
        schema = self.schema_service.load_schema()
        if not schema:
            return None

        # Find the calculated metric
        metric = next((m for m in schema.calculated_metrics if m.id == metric_id), None)
        if not metric:
            return None

        # Check if it's a percent format metric
        if metric.format_type != 'percent':
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

            # Collect all valid metric IDs (both base and calculated)
            base_metric_ids = {m.id for m in schema.base_metrics}
            calculated_metric_ids = {m.id for m in schema.calculated_metrics}
            all_metric_ids = base_metric_ids | calculated_metric_ids

            # Verify numerator exists as a metric
            if numerator_id not in all_metric_ids:
                return {
                    'numerator_metric_id': numerator_id,
                    'denominator_metric_id': denominator_id,
                    'is_simple_ratio': False,
                    'reason': f'Numerator {numerator_id} is not a valid metric'
                }

            # Verify denominator exists as a metric
            if denominator_id not in all_metric_ids:
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
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        eligible = []
        for metric in schema.calculated_metrics:
            if metric.format_type != 'percent':
                continue

            components = self.extract_formula_components(metric.id)
            if components and components.get('is_simple_ratio'):
                eligible.append({
                    'metric_id': metric.id,
                    'display_name': metric.display_name,
                    'numerator_metric_id': components['numerator_metric_id'],
                    'denominator_metric_id': components['denominator_metric_id']
                })

        return eligible
