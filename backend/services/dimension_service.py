"""
Dimension management service.
Handles CRUD operations for dimensions.
"""
from typing import List, Optional

from models.schemas import DimensionDef, DimensionCreate, DimensionUpdate
from services.schema_service import SchemaService


class DimensionService:
    """Service for managing dimensions"""

    def __init__(self, schema_service: SchemaService):
        self.schema_service = schema_service

    def create_dimension(self, dimension_data: DimensionCreate) -> DimensionDef:
        """Create a new dimension"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found. Please initialize schema first.")

        # Check for duplicate ID
        if any(d.id == dimension_data.id for d in schema.dimensions):
            raise ValueError(f"Dimension with ID '{dimension_data.id}' already exists")

        # Create dimension
        dimension = DimensionDef(**dimension_data.model_dump())

        # Add to schema
        schema.dimensions.append(dimension)
        self.schema_service.save_schema(schema)

        return dimension

    def update_dimension(self, dimension_id: str, update_data: DimensionUpdate) -> DimensionDef:
        """Update an existing dimension"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Find dimension
        dimension = next((d for d in schema.dimensions if d.id == dimension_id), None)
        if not dimension:
            raise ValueError(f"Dimension '{dimension_id}' not found")

        # Update fields
        update_dict = update_data.model_dump(exclude_unset=True)
        for field, value in update_dict.items():
            setattr(dimension, field, value)

        self.schema_service.save_schema(schema)

        return dimension

    def delete_dimension(self, dimension_id: str) -> None:
        """Delete a dimension"""
        schema = self.schema_service.load_schema()
        if not schema:
            raise ValueError("Schema not found")

        # Remove dimension
        schema.dimensions = [d for d in schema.dimensions if d.id != dimension_id]
        self.schema_service.save_schema(schema)

    def get_dimension(self, dimension_id: str) -> Optional[DimensionDef]:
        """Get a dimension by ID"""
        schema = self.schema_service.load_schema()
        if not schema:
            return None

        return next((d for d in schema.dimensions if d.id == dimension_id), None)

    def list_dimensions(self) -> List[DimensionDef]:
        """List all dimensions"""
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        return sorted(schema.dimensions, key=lambda d: d.sort_order)

    def list_filterable_dimensions(self) -> List[DimensionDef]:
        """List only filterable dimensions"""
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        filterable = [d for d in schema.dimensions if d.is_filterable]
        return sorted(filterable, key=lambda d: d.sort_order)

    def list_groupable_dimensions(self) -> List[DimensionDef]:
        """List only groupable dimensions"""
        schema = self.schema_service.load_schema()
        if not schema:
            return []

        groupable = [d for d in schema.dimensions if d.is_groupable]
        return sorted(groupable, key=lambda d: d.sort_order)
