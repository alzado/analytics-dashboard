"""
Dimension management service.
Handles CRUD operations for dimensions and calculated dimensions.
"""
import re
import logging
from typing import List, Optional

from apps.schemas.models import (
    SchemaConfig, Dimension, CalculatedDimension,
    DataType, FilterType
)

logger = logging.getLogger(__name__)


class DimensionService:
    """Service for managing dimensions."""

    def __init__(self, schema_config: SchemaConfig):
        self.schema_config = schema_config

    # =========================================================================
    # Regular Dimension Operations
    # =========================================================================

    def create_dimension(
        self,
        dimension_id: str,
        column_name: str,
        display_name: str,
        data_type: str = DataType.STRING,
        is_filterable: bool = True,
        is_groupable: bool = True,
        sort_order: int = 999,
        filter_type: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dimension:
        """Create a new dimension."""
        # Check for duplicate
        if self.schema_config.dimensions.filter(
            dimension_id=dimension_id
        ).exists():
            raise ValueError(
                f"Dimension with ID '{dimension_id}' already exists"
            )

        dimension = Dimension.objects.create(
            schema_config=self.schema_config,
            dimension_id=dimension_id,
            column_name=column_name,
            display_name=display_name,
            data_type=data_type,
            is_filterable=is_filterable,
            is_groupable=is_groupable,
            sort_order=sort_order,
            filter_type=filter_type,
            description=description
        )

        return dimension

    def update_dimension(
        self,
        dimension_id: str,
        **update_data
    ) -> Dimension:
        """Update an existing dimension."""
        try:
            dimension = self.schema_config.dimensions.get(
                dimension_id=dimension_id
            )
        except Dimension.DoesNotExist:
            raise ValueError(f"Dimension '{dimension_id}' not found")

        # Update fields
        for field in [
            'column_name', 'display_name', 'data_type',
            'is_filterable', 'is_groupable', 'sort_order',
            'filter_type', 'description'
        ]:
            if field in update_data:
                setattr(dimension, field, update_data[field])

        dimension.save()
        return dimension

    def delete_dimension(self, dimension_id: str) -> None:
        """Delete a dimension."""
        deleted_count, _ = self.schema_config.dimensions.filter(
            dimension_id=dimension_id
        ).delete()

        if deleted_count == 0:
            raise ValueError(f"Dimension '{dimension_id}' not found")

    def get_dimension(self, dimension_id: str) -> Optional[Dimension]:
        """Get a dimension by ID."""
        try:
            return self.schema_config.dimensions.get(dimension_id=dimension_id)
        except Dimension.DoesNotExist:
            return None

    def list_dimensions(self) -> List[Dimension]:
        """List all dimensions."""
        return list(self.schema_config.dimensions.order_by('sort_order'))

    def list_filterable_dimensions(self) -> List[Dimension]:
        """List only filterable dimensions."""
        return list(
            self.schema_config.dimensions.filter(
                is_filterable=True
            ).order_by('sort_order')
        )

    def list_groupable_dimensions(self) -> List[Dimension]:
        """List only groupable dimensions."""
        return list(
            self.schema_config.dimensions.filter(
                is_groupable=True
            ).order_by('sort_order')
        )

    # =========================================================================
    # Calculated Dimension Operations
    # =========================================================================

    def create_calculated_dimension(
        self,
        display_name: str,
        sql_expression: str,
        dimension_id: Optional[str] = None,
        data_type: str = DataType.STRING,
        is_filterable: bool = True,
        is_groupable: bool = True,
        sort_order: int = 999,
        filter_type: str = FilterType.MULTI,
        description: Optional[str] = None
    ) -> CalculatedDimension:
        """Create a new calculated dimension with SQL expression."""
        # Auto-generate ID from display name if not provided
        if not dimension_id:
            dimension_id = re.sub(r'[^\w\s-]', '', display_name.lower())
            dimension_id = re.sub(r'[-\s]+', '_', dimension_id)

            # Ensure uniqueness
            base_id = dimension_id
            counter = 1
            while self.schema_config.calculated_dimensions.filter(
                dimension_id=dimension_id
            ).exists():
                dimension_id = f"{base_id}_{counter}"
                counter += 1

        # Check for duplicate
        if self.schema_config.calculated_dimensions.filter(
            dimension_id=dimension_id
        ).exists():
            raise ValueError(
                f"Calculated dimension with ID '{dimension_id}' already exists"
            )

        # Parse expression to find dependencies
        depends_on = self._extract_column_references(sql_expression)

        # Validate expression
        errors = self._validate_sql_expression(sql_expression)
        if errors:
            raise ValueError(f"SQL expression errors: {', '.join(errors)}")

        calc_dimension = CalculatedDimension.objects.create(
            schema_config=self.schema_config,
            dimension_id=dimension_id,
            display_name=display_name,
            sql_expression=sql_expression,
            depends_on=depends_on,
            data_type=data_type,
            is_filterable=is_filterable,
            is_groupable=is_groupable,
            sort_order=sort_order,
            filter_type=filter_type,
            description=description
        )

        return calc_dimension

    def update_calculated_dimension(
        self,
        dimension_id: str,
        **update_data
    ) -> CalculatedDimension:
        """Update an existing calculated dimension."""
        try:
            calc_dim = self.schema_config.calculated_dimensions.get(
                dimension_id=dimension_id
            )
        except CalculatedDimension.DoesNotExist:
            raise ValueError(
                f"Calculated dimension '{dimension_id}' not found"
            )

        # If sql_expression is being updated, re-parse dependencies
        if 'sql_expression' in update_data:
            sql_expression = update_data['sql_expression']

            errors = self._validate_sql_expression(sql_expression)
            if errors:
                raise ValueError(f"SQL expression errors: {', '.join(errors)}")

            calc_dim.sql_expression = sql_expression
            calc_dim.depends_on = self._extract_column_references(sql_expression)

        # Update other fields
        for field in [
            'display_name', 'data_type', 'is_filterable',
            'is_groupable', 'sort_order', 'filter_type', 'description'
        ]:
            if field in update_data:
                setattr(calc_dim, field, update_data[field])

        calc_dim.save()
        return calc_dim

    def delete_calculated_dimension(self, dimension_id: str) -> None:
        """Delete a calculated dimension."""
        deleted_count, _ = self.schema_config.calculated_dimensions.filter(
            dimension_id=dimension_id
        ).delete()

        if deleted_count == 0:
            raise ValueError(
                f"Calculated dimension '{dimension_id}' not found"
            )

    def get_calculated_dimension(
        self,
        dimension_id: str
    ) -> Optional[CalculatedDimension]:
        """Get a calculated dimension by ID."""
        try:
            return self.schema_config.calculated_dimensions.get(
                dimension_id=dimension_id
            )
        except CalculatedDimension.DoesNotExist:
            return None

    def list_calculated_dimensions(self) -> List[CalculatedDimension]:
        """List all calculated dimensions."""
        return list(
            self.schema_config.calculated_dimensions.order_by('sort_order')
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _extract_column_references(self, sql_expression: str) -> List[str]:
        """Extract column references {column_name} from SQL expression."""
        return re.findall(r'\{([a-zA-Z0-9_]+)\}', sql_expression)

    def _validate_sql_expression(self, sql_expression: str) -> List[str]:
        """Validate SQL expression for calculated dimensions."""
        errors = []

        if not sql_expression or not sql_expression.strip():
            errors.append("SQL expression cannot be empty")
            return errors

        # Check for balanced parentheses
        if sql_expression.count('(') != sql_expression.count(')'):
            errors.append("Unbalanced parentheses in SQL expression")

        # Check for dangerous keywords
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'EXEC'
        ]
        sql_upper = sql_expression.upper()
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                errors.append(f"Forbidden keyword in SQL: {keyword}")

        return errors

    def validate_expression(self, sql_expression: str) -> dict:
        """
        Validate a SQL expression without saving.
        Returns validation result with compiled expression.
        """
        errors = self._validate_sql_expression(sql_expression)
        depends_on = self._extract_column_references(sql_expression)

        # Compile expression by replacing {column} with actual column references
        compiled_expression = sql_expression
        for column in depends_on:
            compiled_expression = compiled_expression.replace(
                f"{{{column}}}", column
            )

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'sql_expression': compiled_expression,
            'depends_on': depends_on,
            'warnings': []
        }

    def get_all_dimensions(self) -> List[dict]:
        """Get all dimensions (regular and calculated) as a unified list."""
        dimensions = []

        # Regular dimensions
        for dim in self.list_dimensions():
            dimensions.append({
                'id': str(dim.id),
                'dimension_id': dim.dimension_id,
                'display_name': dim.display_name,
                'type': 'regular',
                'column_name': dim.column_name,
                'data_type': dim.data_type,
                'is_filterable': dim.is_filterable,
                'is_groupable': dim.is_groupable,
                'filter_type': dim.filter_type,
                'sort_order': dim.sort_order
            })

        # Calculated dimensions
        for calc_dim in self.list_calculated_dimensions():
            dimensions.append({
                'id': str(calc_dim.id),
                'dimension_id': calc_dim.dimension_id,
                'display_name': calc_dim.display_name,
                'type': 'calculated',
                'sql_expression': calc_dim.sql_expression,
                'depends_on': calc_dim.depends_on,
                'data_type': calc_dim.data_type,
                'is_filterable': calc_dim.is_filterable,
                'is_groupable': calc_dim.is_groupable,
                'filter_type': calc_dim.filter_type,
                'sort_order': calc_dim.sort_order
            })

        return sorted(dimensions, key=lambda x: x['sort_order'])
