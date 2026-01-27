"""
Joined Dimension Service for managing file uploads and BigQuery lookup tables.

This service handles:
1. Parsing CSV/Excel files for preview
2. Uploading data to BigQuery as lookup tables
3. Managing JoinedDimensionSource/Column models
4. Deleting BigQuery tables when sources are removed
"""
import re
import io
import uuid
import logging
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING

import pandas as pd
from google.cloud import bigquery
from django.utils import timezone

from apps.schemas.models import (
    JoinedDimensionSource, JoinedDimensionColumn, JoinedDimensionStatus,
    SchemaConfig, DataType, FilterType
)

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

logger = logging.getLogger(__name__)


class JoinedDimensionService:
    """Service for managing joined dimension sources and BigQuery lookup tables."""

    def __init__(
        self,
        bigquery_client: Optional[bigquery.Client] = None,
        schema_config: Optional[SchemaConfig] = None
    ):
        """
        Initialize the service.

        Args:
            bigquery_client: BigQuery client for table operations (required for uploads, optional for preview)
            schema_config: Optional SchemaConfig for context (required for uploads)
        """
        self.client = bigquery_client
        self.schema_config = schema_config

    def parse_file_preview(self, file: 'UploadedFile') -> Dict:
        """
        Parse file and return column info for preview.

        Args:
            file: Uploaded file (CSV or Excel)

        Returns:
            Dict with row_count, columns info, and preview_rows
        """
        df = self._read_file(file)

        columns = []
        for col in df.columns:
            # Infer data type
            dtype = str(df[col].dtype)
            if 'int' in dtype:
                data_type = 'INTEGER'
            elif 'float' in dtype:
                data_type = 'FLOAT'
            elif 'bool' in dtype:
                data_type = 'BOOLEAN'
            else:
                data_type = 'STRING'

            # Sample values (convert to string for JSON serialization)
            sample_values = df[col].dropna().head(5).tolist()
            sample_values = [
                str(v) if not isinstance(v, (bool, int, float, str, type(None))) else v
                for v in sample_values
            ]

            columns.append({
                'name': col,
                'inferred_type': data_type,
                'sample_values': sample_values,
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique())
            })

        # Convert preview rows to serializable format
        preview_rows = df.head(10).fillna('').to_dict('records')
        for row in preview_rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif not isinstance(value, (bool, int, float, str, type(None))):
                    row[key] = str(value)

        return {
            'row_count': len(df),
            'columns': columns,
            'preview_rows': preview_rows
        }

    def _read_file(self, file: 'UploadedFile') -> pd.DataFrame:
        """Read CSV or Excel file into DataFrame."""
        filename = file.name.lower()
        content = file.read()
        file.seek(0)  # Reset for potential re-read

        if filename.endswith('.csv'):
            return pd.read_csv(io.BytesIO(content))
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            return pd.read_excel(io.BytesIO(content))
        else:
            raise ValueError(f'Unsupported file type: {filename}')

    def process_upload(
        self,
        file: 'UploadedFile',
        name: str,
        join_key_column: str,
        target_dimension_id: str,
        columns: List[Dict],
        bq_project: str,
        bq_dataset: str
    ) -> JoinedDimensionSource:
        """
        Process file upload and create BigQuery lookup table.

        Args:
            file: Uploaded file (CSV or Excel)
            name: Display name for this source
            join_key_column: Column in file used as join key
            target_dimension_id: Dimension ID in schema to join against
            columns: List of column definitions to import as dimensions
            bq_project: BigQuery project for lookup table
            bq_dataset: BigQuery dataset for lookup table

        Returns:
            Created JoinedDimensionSource instance
        """
        if not self.client:
            raise ValueError("bigquery_client is required for upload")
        if not self.schema_config:
            raise ValueError("schema_config is required for upload")

        # Read file
        df = self._read_file(file)

        # Validate join key column exists
        if join_key_column not in df.columns:
            raise ValueError(f"Join key column '{join_key_column}' not found in file")

        # Validate dimension columns exist
        column_names = [c['source_column_name'] for c in columns]
        missing = set(column_names) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found in file: {missing}")

        # Generate unique table name
        short_uuid = str(uuid.uuid4())[:8]
        sanitized_name = self._sanitize_name(name)
        bq_table_name = f"_lookup_{sanitized_name}_{short_uuid}"

        # Create source record
        source = JoinedDimensionSource.objects.create(
            schema_config=self.schema_config,
            name=name,
            original_filename=file.name,
            file_type='xlsx' if file.name.lower().endswith(('.xlsx', '.xls')) else 'csv',
            join_key_column=join_key_column,
            target_dimension_id=target_dimension_id,
            bq_project=bq_project,
            bq_dataset=bq_dataset,
            bq_table=bq_table_name,
            status=JoinedDimensionStatus.PROCESSING
        )

        try:
            # Create column definitions
            for col_def in columns:
                dim_id = col_def.get('dimension_id') or self._generate_dimension_id(col_def['display_name'])
                JoinedDimensionColumn.objects.create(
                    source=source,
                    dimension_id=dim_id,
                    source_column_name=col_def['source_column_name'],
                    display_name=col_def['display_name'],
                    data_type=col_def.get('data_type', DataType.STRING),
                    is_filterable=col_def.get('is_filterable', True),
                    is_groupable=col_def.get('is_groupable', True),
                    filter_type=col_def.get('filter_type', FilterType.MULTI)
                )

            # Select only the columns we need for BigQuery
            columns_to_upload = [join_key_column] + column_names
            df_upload = df[columns_to_upload].copy()

            # Upload to BigQuery
            self._upload_to_bigquery(source, df_upload)

            # Update status
            source.status = JoinedDimensionStatus.READY
            source.row_count = len(df_upload)
            source.uploaded_at = timezone.now()
            source.save()

        except Exception as e:
            logger.exception(f"Error processing joined dimension upload: {e}")
            source.status = JoinedDimensionStatus.ERROR
            source.error_message = str(e)
            source.save()
            raise

        return source

    def _upload_to_bigquery(
        self,
        source: JoinedDimensionSource,
        df: pd.DataFrame
    ) -> None:
        """
        Upload DataFrame to BigQuery as lookup table.

        Converts the join key column to match the source dimension's data type
        for efficient joins without casting.

        Args:
            source: JoinedDimensionSource with BigQuery location
            df: DataFrame to upload
        """
        table_ref = source.bq_table_path
        logger.info(f"Uploading lookup table to {table_ref} ({len(df)} rows)")

        # Get target dimension's data type to match join key column type
        target_data_type = 'STRING'  # Default
        try:
            target_dim = source.schema_config.dimensions.get(
                dimension_id=source.target_dimension_id
            )
            target_data_type = target_dim.data_type
            logger.info(f"Target dimension {source.target_dimension_id} has type {target_data_type}")
        except Exception as e:
            logger.warning(f"Could not get target dimension type: {e}")

        # Convert join key column to match source type for efficient joins
        join_col = source.join_key_column
        if join_col in df.columns:
            if target_data_type in ('INT64', 'INTEGER'):
                # Convert to int64 - handle nulls by using Int64 nullable type
                df[join_col] = pd.to_numeric(df[join_col], errors='coerce').astype('Int64')
                logger.info(f"Converted join key '{join_col}' to INT64")
            elif target_data_type == 'FLOAT64':
                df[join_col] = pd.to_numeric(df[join_col], errors='coerce').astype('float64')
                logger.info(f"Converted join key '{join_col}' to FLOAT64")
            else:
                # Default to string
                df[join_col] = df[join_col].astype(str).replace('nan', None)
                logger.info(f"Converted join key '{join_col}' to STRING")

        # Configure job to overwrite existing table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True  # Let BigQuery infer schema from converted DataFrame
        )

        # Upload DataFrame
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion

        logger.info(f"Successfully uploaded lookup table {table_ref}")

    def reupload(
        self,
        source: JoinedDimensionSource,
        file: 'UploadedFile'
    ) -> JoinedDimensionSource:
        """
        Re-upload file data, replacing the BigQuery table.

        Args:
            source: Existing JoinedDimensionSource
            file: New file to upload

        Returns:
            Updated JoinedDimensionSource
        """
        if not self.client:
            raise ValueError("bigquery_client is required for reupload")

        # Read file
        df = self._read_file(file)

        # Validate join key column exists
        if source.join_key_column not in df.columns:
            raise ValueError(
                f"Join key column '{source.join_key_column}' not found in file"
            )

        # Validate dimension columns exist
        column_names = [col.source_column_name for col in source.columns.all()]
        missing = set(column_names) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found in file: {missing}")

        # Update source status
        source.status = JoinedDimensionStatus.PROCESSING
        source.original_filename = file.name
        source.error_message = None
        source.save()

        try:
            # Select only the columns we need
            columns_to_upload = [source.join_key_column] + column_names
            df_upload = df[columns_to_upload].copy()

            # Upload to BigQuery (overwrites existing)
            self._upload_to_bigquery(source, df_upload)

            # Update status
            source.status = JoinedDimensionStatus.READY
            source.row_count = len(df_upload)
            source.uploaded_at = timezone.now()
            source.save()

        except Exception as e:
            logger.exception(f"Error re-uploading joined dimension: {e}")
            source.status = JoinedDimensionStatus.ERROR
            source.error_message = str(e)
            source.save()
            raise

        return source

    def delete_source(self, source: JoinedDimensionSource) -> None:
        """
        Delete BigQuery table and source record.

        Args:
            source: JoinedDimensionSource to delete
        """
        if not self.client:
            raise ValueError("bigquery_client is required for delete")

        # Try to delete BigQuery table
        try:
            table_ref = source.bq_table_path
            logger.info(f"Deleting lookup table {table_ref}")
            self.client.delete_table(table_ref, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Error deleting BigQuery table: {e}")
            # Continue with deletion even if BQ table deletion fails

        # Delete Django model (cascades to columns)
        source.delete()

    def get_preview_data(
        self,
        source: JoinedDimensionSource,
        limit: int = 10
    ) -> Dict:
        """
        Get preview data from BigQuery lookup table.

        Args:
            source: JoinedDimensionSource
            limit: Max rows to return

        Returns:
            Dict with total_rows and preview data
        """
        if not self.client:
            raise ValueError("bigquery_client is required for preview")

        query = f"SELECT * FROM `{source.bq_table_path}` LIMIT {limit}"

        try:
            result = self.client.query(query).result()
            rows = [dict(row) for row in result]

            return {
                'total_rows': source.row_count,
                'preview': rows
            }
        except Exception as e:
            logger.exception(f"Error fetching preview data: {e}")
            return {
                'total_rows': source.row_count,
                'preview': [],
                'error': str(e)
            }

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in BigQuery table name."""
        # Remove non-alphanumeric characters and replace spaces with underscores
        sanitized = re.sub(r'[^\w\s-]', '', name.lower())
        sanitized = re.sub(r'[-\s]+', '_', sanitized)
        # Limit length
        return sanitized[:50]

    def _generate_dimension_id(self, display_name: str) -> str:
        """Generate dimension ID from display name."""
        sanitized = re.sub(r'[^\w\s-]', '', display_name.lower())
        sanitized = re.sub(r'[-\s]+', '_', sanitized)
        return f"joined_{sanitized}"
