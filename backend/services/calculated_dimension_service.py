"""
Calculated dimension management service.
Handles CRUD operations for calculated dimensions with SQL expressions.
"""
import re
from typing import List, Optional

from models.schemas import (
    CalculatedDimensionDef, CalculatedDimensionCreate, CalculatedDimensionUpdate,
    ExpressionValidationResult, SchemaConfig
)
from services.schema_service import SchemaService
from services.dimension_expression_parser import DimensionExpressionParser


class CalculatedDimensionService:
    """Service for managing calculated dimensions with SQL expressions."""

    def __init__(
        self,
        schema_service: SchemaService,
        expression_parser: Optional[DimensionExpressionParser] = None
    ):
        """
        Initialize the service.

        Args:
            schema_service: Schema service for loading/saving schema
            expression_parser: Optional expression parser (created on demand if not provided)
        """
        self.schema_service = schema_service
        self.expression_parser = expression_parser

    def _get_parser(self) -> DimensionExpressionParser:
        """Get or create the expression parser."""
        if self.expression_parser:
            return self.expression_parser
        # Create a parser without BigQuery client (will skip dry-run validation)
        return DimensionExpressionParser()

    def _generate_id_from_display_name(self, display_name: str, schema: SchemaConfig) -> str:
        """
        Generate a unique ID from display name.

        Args:
            display_name: Human-readable display name
            schema: Current schema to check for duplicates

        Returns:
            Unique snake_case ID
        """
        # Convert display name to snake_case ID
        dim_id = re.sub(r'[^\w\s-]', '', display_name.lower())
        dim_id = re.sub(r'[-\s]+', '_', dim_id)

        # Ensure uniqueness
        base_id = dim_id
        counter = 1
        existing_ids = self._get_all_dimension_ids(schema)

        while dim_id in existing_ids:
            dim_id = f"{base_id}_{counter}"
            counter += 1

        return dim_id

    def _get_all_dimension_ids(self, schema: SchemaConfig) -> set:
        """Get all dimension IDs (regular and calculated)."""
        ids = {d.id for d in schema.dimensions}
        if hasattr(schema, 'calculated_dimensions'):
            ids |= {d.id for d in schema.calculated_dimensions}
        return ids

    def create_calculated_dimension(
        self,
        data: CalculatedDimensionCreate,
        validate_with_bigquery: bool = True
    ) -> CalculatedDimensionDef:
        """
        Create a new calculated dimension.

        Args:
            data: Dimension creation data
            validate_with_bigquery: Whether to validate expression with BigQuery dry run

        Returns:
            Created CalculatedDimensionDef

        Raises:
            ValueError: If validation fails or dimension already exists
        """
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found. Please initialize schema first.")

        # Ensure calculated_dimensions list exists
        if not hasattr(schema, 'calculated_dimensions') or schema.calculated_dimensions is None:
            schema.calculated_dimensions = []

        # Auto-generate ID if not provided
        dim_id = data.id
        if not dim_id:
            dim_id = self._generate_id_from_display_name(data.display_name, schema)

        # Check for duplicate ID across both dimension types
        all_dim_ids = self._get_all_dimension_ids(schema)
        if dim_id in all_dim_ids:
            raise ValueError(f"Dimension ID '{dim_id}' already exists")

        # Validate expression
        parser = self._get_parser()
        is_valid, sql_expr, depends_on, errors, warnings = parser.validate_expression(
            data.sql_expression,
            validate_columns=validate_with_bigquery,
            dry_run=validate_with_bigquery
        )

        if not is_valid:
            raise ValueError(f"Expression validation failed: {'; '.join(errors)}")

        # Create the dimension
        dimension = CalculatedDimensionDef(
            id=dim_id,
            display_name=data.display_name,
            sql_expression=sql_expr,
            data_type=data.data_type,
            is_filterable=data.is_filterable,
            is_groupable=data.is_groupable,
            sort_order=data.sort_order,
            filter_type=data.filter_type,
            depends_on=depends_on,
            description=data.description
        )

        # Add to schema
        schema.calculated_dimensions.append(dimension)
        self.schema_service.save_schema(schema)

        return dimension

    def update_calculated_dimension(
        self,
        dimension_id: str,
        data: CalculatedDimensionUpdate,
        validate_with_bigquery: bool = True
    ) -> CalculatedDimensionDef:
        """
        Update an existing calculated dimension.

        Args:
            dimension_id: ID of dimension to update
            data: Update data (partial update)
            validate_with_bigquery: Whether to validate expression with BigQuery dry run

        Returns:
            Updated CalculatedDimensionDef

        Raises:
            ValueError: If dimension not found or validation fails
        """
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Find the dimension
        dimension = self.get_calculated_dimension(dimension_id)
        if not dimension:
            raise ValueError(f"Calculated dimension '{dimension_id}' not found")

        # If expression is being updated, validate it
        if data.sql_expression is not None:
            parser = self._get_parser()
            is_valid, sql_expr, depends_on, errors, warnings = parser.validate_expression(
                data.sql_expression,
                validate_columns=validate_with_bigquery,
                dry_run=validate_with_bigquery
            )

            if not is_valid:
                raise ValueError(f"Expression validation failed: {'; '.join(errors)}")

            # Update expression-related fields
            dimension.sql_expression = sql_expr
            dimension.depends_on = depends_on

        # Update other fields from the update data
        update_dict = data.model_dump(exclude_unset=True, exclude={'sql_expression'})
        for field, value in update_dict.items():
            if value is not None:
                setattr(dimension, field, value)

        self.schema_service.save_schema(schema)

        return dimension

    def delete_calculated_dimension(self, dimension_id: str) -> bool:
        """
        Delete a calculated dimension.

        Args:
            dimension_id: ID of dimension to delete

        Returns:
            True if deleted successfully

        Raises:
            ValueError: If dimension not found
        """
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        if not hasattr(schema, 'calculated_dimensions'):
            raise ValueError(f"Calculated dimension '{dimension_id}' not found")

        # Check if dimension exists
        original_count = len(schema.calculated_dimensions)
        schema.calculated_dimensions = [
            d for d in schema.calculated_dimensions if d.id != dimension_id
        ]

        if len(schema.calculated_dimensions) == original_count:
            raise ValueError(f"Calculated dimension '{dimension_id}' not found")

        self.schema_service.save_schema(schema)
        return True

    def get_calculated_dimension(self, dimension_id: str) -> Optional[CalculatedDimensionDef]:
        """
        Get a calculated dimension by ID.

        Args:
            dimension_id: ID of dimension to get

        Returns:
            CalculatedDimensionDef or None if not found
        """
        schema = self.schema_service.load_schema()
        if not schema or not hasattr(schema, 'calculated_dimensions'):
            return None

        return next(
            (d for d in schema.calculated_dimensions if d.id == dimension_id),
            None
        )

    def list_calculated_dimensions(self) -> List[CalculatedDimensionDef]:
        """
        List all calculated dimensions.

        Returns:
            List of CalculatedDimensionDef sorted by sort_order
        """
        schema = self.schema_service.load_schema()
        if not schema or not hasattr(schema, 'calculated_dimensions'):
            return []

        return sorted(schema.calculated_dimensions, key=lambda d: d.sort_order)

    def list_all_dimensions(self) -> List[dict]:
        """
        List all dimensions (regular and calculated) with type indicator.

        Returns:
            List of dicts with dimension info and 'type' field ('regular' or 'calculated')
        """
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        all_dims = []

        # Add regular dimensions
        for dim in schema.dimensions:
            all_dims.append({
                'id': dim.id,
                'display_name': dim.display_name,
                'data_type': dim.data_type,
                'is_filterable': dim.is_filterable,
                'is_groupable': dim.is_groupable,
                'sort_order': dim.sort_order,
                'type': 'regular',
                'column_name': dim.column_name
            })

        # Add calculated dimensions
        if hasattr(schema, 'calculated_dimensions') and schema.calculated_dimensions:
            for dim in schema.calculated_dimensions:
                all_dims.append({
                    'id': dim.id,
                    'display_name': dim.display_name,
                    'data_type': dim.data_type,
                    'is_filterable': dim.is_filterable,
                    'is_groupable': dim.is_groupable,
                    'sort_order': dim.sort_order,
                    'type': 'calculated',
                    'sql_expression': dim.sql_expression,
                    'depends_on': dim.depends_on
                })

        return sorted(all_dims, key=lambda d: d['sort_order'])

    def list_groupable_dimensions(self) -> List[dict]:
        """
        List all dimensions that can be used for GROUP BY.

        Returns:
            List of dimension dicts with is_groupable=True
        """
        return [d for d in self.list_all_dimensions() if d.get('is_groupable', True)]

    def list_filterable_dimensions(self) -> List[dict]:
        """
        List all dimensions that can be used for filtering.

        Returns:
            List of dimension dicts with is_filterable=True
        """
        return [d for d in self.list_all_dimensions() if d.get('is_filterable', True)]

    def validate_expression(
        self,
        expression: str,
        validate_with_bigquery: bool = True
    ) -> ExpressionValidationResult:
        """
        Validate an expression without saving it.

        Args:
            expression: SQL expression to validate
            validate_with_bigquery: Whether to perform BigQuery dry run

        Returns:
            ExpressionValidationResult with validation details
        """
        parser = self._get_parser()

        # First check syntax
        syntax_errors = parser.validate_syntax_only(expression)
        if syntax_errors:
            return ExpressionValidationResult(
                valid=False,
                errors=syntax_errors,
                sql_expression="",
                depends_on=[],
                warnings=[]
            )

        # Full validation
        is_valid, sql_expr, depends_on, errors, warnings = parser.validate_expression(
            expression,
            validate_columns=validate_with_bigquery,
            dry_run=validate_with_bigquery
        )

        return ExpressionValidationResult(
            valid=is_valid,
            errors=errors,
            sql_expression=sql_expr,
            depends_on=depends_on,
            warnings=warnings
        )

    def is_calculated_dimension(self, dimension_id: str) -> bool:
        """
        Check if a dimension ID refers to a calculated dimension.

        Args:
            dimension_id: Dimension ID to check

        Returns:
            True if it's a calculated dimension
        """
        return self.get_calculated_dimension(dimension_id) is not None
