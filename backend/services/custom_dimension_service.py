"""
Custom Dimension Service - Manages user-defined custom dimensions
"""
import json
import os
import uuid
from datetime import datetime
from typing import List, Optional, Dict
from models.schemas import CustomDimension, CustomDimensionValue, CustomDimensionCreate, CustomDimensionUpdate


class CustomDimensionService:
    """Service for managing custom dimensions"""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._ensure_config_dir()

    def _ensure_config_dir(self):
        """Ensure the config directory exists"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)

    def _load_dimensions(self) -> Dict[str, List[dict]]:
        """Load dimensions from JSON file"""
        if not os.path.exists(self.config_path):
            return {"dimensions": []}

        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"dimensions": []}

    def _save_dimensions(self, data: Dict[str, List[dict]]):
        """Save dimensions to JSON file"""
        self._ensure_config_dir()
        with open(self.config_path, 'w') as f:
            json.dump(data, f, indent=2)

    def get_all(self) -> List[CustomDimension]:
        """Get all custom dimensions"""
        data = self._load_dimensions()
        return [CustomDimension(**dim) for dim in data.get("dimensions", [])]

    def get_by_id(self, dimension_id: str) -> Optional[CustomDimension]:
        """Get a custom dimension by ID"""
        dimensions = self.get_all()
        for dim in dimensions:
            if dim.id == dimension_id:
                return dim
        return None

    def create(self, dimension_data: CustomDimensionCreate) -> CustomDimension:
        """Create a new custom dimension"""
        data = self._load_dimensions()

        # Generate new dimension
        new_dimension = CustomDimension(
            id=str(uuid.uuid4()),
            name=dimension_data.name,
            type=dimension_data.type,
            values=dimension_data.values,
            metric=dimension_data.metric,
            metric_values=dimension_data.metric_values,
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat()
        )

        # Add to list
        data["dimensions"].append(new_dimension.dict())
        self._save_dimensions(data)

        return new_dimension

    def update(self, dimension_id: str, update_data: CustomDimensionUpdate) -> Optional[CustomDimension]:
        """Update an existing custom dimension"""
        data = self._load_dimensions()
        dimensions = data.get("dimensions", [])

        # Find and update dimension
        for i, dim in enumerate(dimensions):
            if dim["id"] == dimension_id:
                # Update fields
                if update_data.name is not None:
                    dim["name"] = update_data.name
                if update_data.values is not None:
                    dim["values"] = [v.dict() for v in update_data.values]
                if update_data.metric is not None:
                    dim["metric"] = update_data.metric
                if update_data.metric_values is not None:
                    dim["metric_values"] = [v.dict() for v in update_data.metric_values]
                dim["updated_at"] = datetime.now().isoformat()

                # Save
                self._save_dimensions(data)
                return CustomDimension(**dim)

        return None

    def delete(self, dimension_id: str) -> bool:
        """Delete a custom dimension"""
        data = self._load_dimensions()
        dimensions = data.get("dimensions", [])

        # Filter out the dimension to delete
        original_length = len(dimensions)
        data["dimensions"] = [dim for dim in dimensions if dim["id"] != dimension_id]

        if len(data["dimensions"]) < original_length:
            self._save_dimensions(data)
            return True

        return False

    def duplicate(self, dimension_id: str) -> Optional[CustomDimension]:
        """Duplicate an existing custom dimension"""
        original = self.get_by_id(dimension_id)
        if not original:
            return None

        data = self._load_dimensions()

        # Create new dimension with copied data
        new_dimension = CustomDimension(
            id=str(uuid.uuid4()),
            name=f"{original.name} (Copy)",
            type=original.type,
            values=original.values,
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat()
        )

        # Add to list
        data["dimensions"].append(new_dimension.dict())
        self._save_dimensions(data)

        return new_dimension


# Singleton instance
_custom_dimension_service: Optional[CustomDimensionService] = None


def get_custom_dimension_service(config_path: str = "/app/config/custom_dimensions.json") -> CustomDimensionService:
    """Get or create the custom dimension service singleton"""
    global _custom_dimension_service
    if _custom_dimension_service is None:
        _custom_dimension_service = CustomDimensionService(config_path)
    return _custom_dimension_service
