"""
Configuration for backend services - Multi-table BigQuery support.
"""
import os
import json
import uuid
from typing import Optional, Dict, List
from datetime import datetime


# File paths for multi-table configuration
TABLES_REGISTRY_FILE = "/app/config/tables_registry.json"
ACTIVE_TABLE_FILE = "/app/config/active_table.json"
TABLE_CONFIGS_DIR = "/app/config/table_configs"
SCHEMAS_DIR = "/app/config/schemas"  # Per-table schema directory
CUSTOM_DIMENSIONS_FILE = "/app/config/custom_dimensions.json"
QUERY_LOGS_DB_PATH = "/app/config/query_logs.db"

# Legacy paths for migration
LEGACY_CONFIG_FILE = "/app/config/bigquery_config.json"
LEGACY_SCHEMA_FILE = "/app/config/schema_config.json"


class TableInfo:
    """Information about a configured BigQuery table."""

    def __init__(
        self,
        table_id: str,
        name: str,
        project_id: str,
        dataset: str,
        table: str,
        credentials_json: str = "",
        allowed_min_date: Optional[str] = None,
        allowed_max_date: Optional[str] = None,
        created_at: Optional[str] = None,
        last_used_at: Optional[str] = None
    ):
        self.table_id = table_id
        self.name = name
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.credentials_json = credentials_json
        self.allowed_min_date = allowed_min_date
        self.allowed_max_date = allowed_max_date
        self.created_at = created_at or datetime.utcnow().isoformat()
        self.last_used_at = last_used_at or datetime.utcnow().isoformat()

    def to_dict(self) -> Dict:
        return {
            'table_id': self.table_id,
            'name': self.name,
            'project_id': self.project_id,
            'dataset': self.dataset,
            'table': self.table,
            'credentials_json': self.credentials_json,
            'allowed_min_date': self.allowed_min_date,
            'allowed_max_date': self.allowed_max_date,
            'created_at': self.created_at,
            'last_used_at': self.last_used_at
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'TableInfo':
        return cls(**data)


class TableRegistry:
    """Registry for managing multiple BigQuery table configurations."""

    def __init__(self):
        """Initialize table registry and perform migration if needed."""
        self._ensure_directories()
        self._migrate_legacy_config()
        self._load_registry()

    def _ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        os.makedirs(TABLE_CONFIGS_DIR, exist_ok=True)
        os.makedirs(SCHEMAS_DIR, exist_ok=True)

    def _migrate_legacy_config(self) -> None:
        """Migrate old single-table config to new multi-table structure."""
        if os.path.exists(LEGACY_CONFIG_FILE) and not os.path.exists(TABLES_REGISTRY_FILE):
            print("Migrating legacy config to multi-table structure...")
            try:
                # Load legacy config
                with open(LEGACY_CONFIG_FILE, 'r') as f:
                    legacy_data = json.load(f)

                # Create default table
                default_id = "default"
                table_info = TableInfo(
                    table_id=default_id,
                    name="Default Table",
                    project_id=legacy_data.get('project_id', ''),
                    dataset=legacy_data.get('dataset', ''),
                    table=legacy_data.get('table', '')
                )

                # Save to new structure
                self._save_table_config(default_id, legacy_data)
                self._save_registry({'tables': [table_info.to_dict()]})
                self._set_active_table(default_id)

                # Migrate schema if exists
                if os.path.exists(LEGACY_SCHEMA_FILE):
                    schema_target = os.path.join(SCHEMAS_DIR, f"schema_{default_id}.json")
                    with open(LEGACY_SCHEMA_FILE, 'r') as f:
                        schema_data = json.load(f)
                    with open(schema_target, 'w') as f:
                        json.dump(schema_data, f, indent=2)

                print("Migration completed successfully")
            except Exception as e:
                print(f"Migration failed: {e}")

    def _load_registry(self) -> None:
        """Load tables registry from file."""
        self.tables: Dict[str, TableInfo] = {}
        if os.path.exists(TABLES_REGISTRY_FILE):
            try:
                with open(TABLES_REGISTRY_FILE, 'r') as f:
                    data = json.load(f)
                    for table_data in data.get('tables', []):
                        table_info = TableInfo.from_dict(table_data)
                        self.tables[table_info.table_id] = table_info
            except Exception as e:
                print(f"Failed to load registry: {e}")

    def _save_registry(self, data: Dict) -> None:
        """Save tables registry to file."""
        with open(TABLES_REGISTRY_FILE, 'w') as f:
            json.dump(data, f, indent=2)

    def _save_table_config(self, table_id: str, config_data: Dict) -> None:
        """Save individual table configuration."""
        config_path = os.path.join(TABLE_CONFIGS_DIR, f"table_{table_id}.json")
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)

    def _get_table_config_path(self, table_id: str) -> str:
        """Get path to table configuration file."""
        return os.path.join(TABLE_CONFIGS_DIR, f"table_{table_id}.json")

    def get_schema_path(self, table_id: str) -> str:
        """Get path to schema configuration file for a table."""
        return os.path.join(SCHEMAS_DIR, f"schema_{table_id}.json")

    def list_tables(self) -> List[TableInfo]:
        """Get list of all configured tables."""
        return list(self.tables.values())

    def get_table(self, table_id: str) -> Optional[TableInfo]:
        """Get table info by ID."""
        return self.tables.get(table_id)

    def create_table(
        self,
        name: str,
        project_id: str,
        dataset: str,
        table: str,
        credentials_json: str = "",
        allowed_min_date: Optional[str] = None,
        allowed_max_date: Optional[str] = None
    ) -> TableInfo:
        """Create a new table configuration."""
        table_id = str(uuid.uuid4())[:8]  # Short UUID
        table_info = TableInfo(
            table_id=table_id,
            name=name,
            project_id=project_id,
            dataset=dataset,
            table=table
        )

        # Save table config
        config_data = {
            'project_id': project_id,
            'dataset': dataset,
            'table': table,
            'credentials_json': credentials_json,
            'allowed_min_date': allowed_min_date,
            'allowed_max_date': allowed_max_date
        }
        self._save_table_config(table_id, config_data)

        # Update registry
        self.tables[table_id] = table_info
        self._save_registry({'tables': [t.to_dict() for t in self.tables.values()]})

        # If this is the first table, make it active
        if len(self.tables) == 1:
            self._set_active_table(table_id)

        return table_info

    def update_table(self, table_id: str, name: str) -> bool:
        """Update table metadata (name only)."""
        if table_id not in self.tables:
            return False

        self.tables[table_id].name = name
        self._save_registry({'tables': [t.to_dict() for t in self.tables.values()]})
        return True

    def delete_table(self, table_id: str) -> bool:
        """Delete a table configuration."""
        if table_id not in self.tables:
            return False

        # Remove from registry
        del self.tables[table_id]
        self._save_registry({'tables': [t.to_dict() for t in self.tables.values()]})

        # Remove config file
        config_path = self._get_table_config_path(table_id)
        if os.path.exists(config_path):
            os.remove(config_path)

        # Remove schema file for this table
        schema_path = self.get_schema_path(table_id)
        if os.path.exists(schema_path):
            os.remove(schema_path)

        # If this was the active table, switch to first available
        if self.get_active_table_id() == table_id and self.tables:
            self._set_active_table(list(self.tables.keys())[0])

        return True

    def get_table_config(self, table_id: str) -> Optional[Dict]:
        """Load BigQuery configuration for a specific table."""
        config_path = self._get_table_config_path(table_id)
        if not os.path.exists(config_path):
            return None

        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load table config {table_id}: {e}")
            return None

    def update_table_config(
        self,
        table_id: str,
        project_id: str,
        dataset: str,
        table: str,
        credentials_json: str = "",
        allowed_min_date: Optional[str] = None,
        allowed_max_date: Optional[str] = None
    ) -> bool:
        """Update BigQuery configuration for a table."""
        if table_id not in self.tables:
            return False

        config_data = {
            'project_id': project_id,
            'dataset': dataset,
            'table': table,
            'credentials_json': credentials_json,
            'allowed_min_date': allowed_min_date,
            'allowed_max_date': allowed_max_date
        }
        self._save_table_config(table_id, config_data)

        # Update table info
        self.tables[table_id].project_id = project_id
        self.tables[table_id].dataset = dataset
        self.tables[table_id].table = table
        self._save_registry({'tables': [t.to_dict() for t in self.tables.values()]})

        return True

    def get_active_table_id(self) -> Optional[str]:
        """Get the currently active table ID."""
        if not os.path.exists(ACTIVE_TABLE_FILE):
            # If no active table set, use first available
            if self.tables:
                first_id = list(self.tables.keys())[0]
                self._set_active_table(first_id)
                return first_id
            return None

        try:
            with open(ACTIVE_TABLE_FILE, 'r') as f:
                data = json.load(f)
                return data.get('active_table_id')
        except Exception:
            return None

    def _set_active_table(self, table_id: str) -> None:
        """Set the active table (internal use)."""
        with open(ACTIVE_TABLE_FILE, 'w') as f:
            json.dump({'active_table_id': table_id}, f)

    def activate_table(self, table_id: str) -> bool:
        """Activate a table for use."""
        if table_id not in self.tables:
            return False

        self._set_active_table(table_id)

        # Update last used timestamp
        self.tables[table_id].last_used_at = datetime.utcnow().isoformat()
        self._save_registry({'tables': [t.to_dict() for t in self.tables.values()]})

        return True

    def get_active_table(self) -> Optional[TableInfo]:
        """Get the currently active table info."""
        active_id = self.get_active_table_id()
        if active_id:
            return self.tables.get(active_id)
        return None


# Global table registry instance
table_registry = TableRegistry()
