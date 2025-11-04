"""
Configuration for backend services - BigQuery only.
"""
import os
import json
from typing import Optional, Dict


CONFIG_FILE = "/app/config/bigquery_config.json"
CUSTOM_DIMENSIONS_FILE = "/app/config/custom_dimensions.json"
QUERY_LOGS_DB_PATH = "/app/config/query_logs.db"


class Config:
    """Application configuration - BigQuery with UI config support."""

    # BigQuery settings - try env vars first, then saved config
    BIGQUERY_PROJECT_ID: str = ""
    BIGQUERY_DATASET: str = ""
    BIGQUERY_TABLE: str = ""
    BIGQUERY_CREDENTIALS_JSON: str = ""  # Stored as JSON string

    # Date limits for BigQuery access (optional)
    ALLOWED_MIN_DATE: Optional[str] = None  # Format: YYYY-MM-DD
    ALLOWED_MAX_DATE: Optional[str] = None  # Format: YYYY-MM-DD

    def __init__(self):
        """Initialize config from env vars or saved config."""
        # Try environment variables first
        self.BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "")
        self.BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "")
        self.BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "")

        # If env vars are not set, try loading from saved config
        if not (self.BIGQUERY_PROJECT_ID and self.BIGQUERY_DATASET and self.BIGQUERY_TABLE):
            self.load_from_file()

    def load_from_file(self) -> bool:
        """Load configuration from file."""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    data = json.load(f)
                    self.BIGQUERY_PROJECT_ID = data.get('project_id', '')
                    self.BIGQUERY_DATASET = data.get('dataset', '')
                    self.BIGQUERY_TABLE = data.get('table', '')
                    self.BIGQUERY_CREDENTIALS_JSON = data.get('credentials_json', '')
                    self.ALLOWED_MIN_DATE = data.get('allowed_min_date')
                    self.ALLOWED_MAX_DATE = data.get('allowed_max_date')
                return True
        except Exception as e:
            print(f"Failed to load config from file: {e}")
        return False

    def save_to_file(
        self,
        project_id: str,
        dataset: str,
        table: str,
        credentials_json: str,
        allowed_min_date: Optional[str] = None,
        allowed_max_date: Optional[str] = None
    ) -> None:
        """Save configuration to file."""
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        data = {
            'project_id': project_id,
            'dataset': dataset,
            'table': table,
            'credentials_json': credentials_json,
            'allowed_min_date': allowed_min_date,
            'allowed_max_date': allowed_max_date
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(data, f)

        # Update current config
        self.BIGQUERY_PROJECT_ID = project_id
        self.BIGQUERY_DATASET = dataset
        self.BIGQUERY_TABLE = table
        self.BIGQUERY_CREDENTIALS_JSON = credentials_json
        self.ALLOWED_MIN_DATE = allowed_min_date
        self.ALLOWED_MAX_DATE = allowed_max_date

    def is_configured(self) -> bool:
        """Check if BigQuery is configured."""
        return bool(self.BIGQUERY_PROJECT_ID and self.BIGQUERY_DATASET and self.BIGQUERY_TABLE)

    def clear_configuration(self) -> None:
        """Clear credentials and date limits but preserve connection details."""
        # Save partial config with just connection details (no credentials)
        # This allows users to easily reconnect without re-entering project/dataset/table
        if self.BIGQUERY_PROJECT_ID or self.BIGQUERY_DATASET or self.BIGQUERY_TABLE:
            os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
            data = {
                'project_id': self.BIGQUERY_PROJECT_ID,
                'dataset': self.BIGQUERY_DATASET,
                'table': self.BIGQUERY_TABLE,
                'credentials_json': '',  # Clear credentials
                'allowed_min_date': None,  # Clear date limits
                'allowed_max_date': None
            }
            with open(CONFIG_FILE, 'w') as f:
                json.dump(data, f)
        elif os.path.exists(CONFIG_FILE):
            # If no connection details, remove the file entirely
            os.remove(CONFIG_FILE)

        # Clear sensitive data from current config
        self.BIGQUERY_CREDENTIALS_JSON = ""
        self.ALLOWED_MIN_DATE = None
        self.ALLOWED_MAX_DATE = None

    def validate_bigquery_config(self) -> None:
        """Validate that BigQuery is properly configured."""
        if not self.is_configured():
            raise ValueError(
                "BigQuery not configured. Please configure via the UI or environment variables."
            )


config = Config()
