from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict
from datetime import datetime, date
import sys
import os
from urllib.parse import parse_qs

# Add services directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))

from services import data_service
import services.data_service as data_svc
from services.bigquery_service import (
    initialize_bigquery_service,
    initialize_bigquery_with_json,
    get_bigquery_info,
    clear_bigquery_service
)
from services.custom_dimension_service import get_custom_dimension_service
from services.query_logger import initialize_query_logger
from config import CUSTOM_DIMENSIONS_FILE, QUERY_LOGS_DB_PATH, table_registry
from models.schemas import (
    FilterParams,
    PivotResponse,
    PivotChildRow,
    BigQueryInfo,
    BigQueryConfig,
    BigQueryConfigResponse,
    CustomDimension,
    CustomDimensionCreate,
    CustomDimensionUpdate,
    QueryLogResponse,
    QueryLogEntry,
    UsageStats,
    UsageTimeSeries,
    ClearLogsResponse,
    # Schema management models
    SchemaConfig,
    SchemaDetectionResult,
    BaseMetric,
    CalculatedMetric,
    DimensionDef,
    MetricCreate,
    CalculatedMetricCreate,
    DimensionCreate,
    MetricUpdate,
    CalculatedMetricUpdate,
    DimensionUpdate,
    PivotConfigUpdate,
    # Multi-table models
    TableInfoResponse,
    TableListResponse,
    TableCreateRequest,
    TableUpdateRequest,
    TableConfigUpdateRequest,
    TableActivateRequest,
    SchemaCopyRequest,
    SchemaTemplateRequest
)

app = FastAPI(title="Search Analytics API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to parse dimension filters from query parameters
def parse_dimension_filters(request: Request) -> Dict[str, List[str]]:
    """
    Parse dimension filters from query parameters.

    Supports multi-value query parameters like:
    ?country=USA&country=Canada&channel=Web

    Returns:
        Dictionary mapping dimension IDs to lists of values
        Example: {"country": ["USA", "Canada"], "channel": ["Web"]}
    """
    # Reserved parameters that are not dimension filters
    reserved_params = {
        'start_date', 'end_date', 'dimensions', 'dimension_values',
        'limit', 'offset', 'sort_by', 'granularity'
    }

    dimension_filters = {}
    query_string = str(request.url.query)

    if not query_string:
        return dimension_filters

    # Parse query string to handle multi-value parameters
    parsed = parse_qs(query_string)

    for key, values in parsed.items():
        # Skip reserved parameters and empty values
        if key in reserved_params or not values:
            continue

        # Filter out empty strings
        non_empty_values = [v for v in values if v]
        if non_empty_values:
            dimension_filters[key] = non_empty_values

    return dimension_filters

# Initialize data on startup
@app.on_event("startup")
async def startup_event():
    """Initialize BigQuery connection and query logger on application startup"""
    # Initialize query logger
    try:
        print(f"Initializing query logger at {QUERY_LOGS_DB_PATH}...")
        initialize_query_logger(QUERY_LOGS_DB_PATH)
        print("Query logger initialized successfully")
    except Exception as e:
        print(f"Failed to initialize query logger: {e}")

    # Initialize BigQuery for all configured tables
    try:
        tables = table_registry.list_tables()
        if tables:
            print(f"Found {len(tables)} configured table(s). Initializing connections...")
            for table_info in tables:
                try:
                    print(f"Initializing table: {table_info.name} (ID: {table_info.table_id})")
                    print(f"  Project: {table_info.project_id}")
                    print(f"  Dataset: {table_info.dataset}")
                    print(f"  Table: {table_info.table}")

                    # Initialize BigQuery service for this table
                    if table_info.credentials_json:
                        bq_service = initialize_bigquery_with_json(
                            project_id=table_info.project_id,
                            dataset=table_info.dataset,
                            table=table_info.table,
                            credentials_json=table_info.credentials_json,
                            table_id=table_info.table_id
                        )
                    else:
                        bq_service = initialize_bigquery_service(
                            project_id=table_info.project_id,
                            dataset=table_info.dataset,
                            table=table_info.table,
                            credentials_path=None,
                            table_id=table_info.table_id
                        )

                    # Set date limits
                    if bq_service and (table_info.allowed_min_date or table_info.allowed_max_date):
                        bq_service.set_date_limits(
                            min_date=table_info.allowed_min_date,
                            max_date=table_info.allowed_max_date
                        )
                        print(f"  Date limits: {table_info.allowed_min_date} to {table_info.allowed_max_date}")

                    print(f"  ✓ Table initialized successfully")
                except Exception as table_error:
                    print(f"  ✗ Failed to initialize table {table_info.name}: {table_error}")

            active_id = table_registry.get_active_table_id()
            if active_id:
                active_table = table_registry.get_table(active_id)
                print(f"\nActive table: {active_table.name} (ID: {active_id})")
            else:
                print("\nNo active table selected")
        else:
            print("No tables configured. Please configure via the UI at /info tab.")
    except Exception as e:
        print(f"Failed to initialize BigQuery tables: {e}")
        print("Please configure tables via the UI at /info tab.")

@app.get("/")
async def root():
    return {"message": "Search Analytics API", "version": "1.0.0"}

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/bigquery/info", response_model=BigQueryInfo)
async def get_bigquery_information():
    """Get BigQuery connection status and table information"""
    try:
        return get_bigquery_info()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bigquery/configure", response_model=BigQueryConfigResponse)
async def configure_bigquery(bq_config: BigQueryConfig):
    """Legacy endpoint - Configure BigQuery connection from UI. Creates or updates the default table."""
    try:
        # Check if there's already a "default" table
        tables = table_registry.list_tables()
        default_table = next((t for t in tables if t.name == "default"), None)

        if default_table:
            # Update existing default table
            table_registry.update_table(
                table_id=default_table.table_id,
                name="default",
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_json=bq_config.credentials_json if not bq_config.use_adc else "",
                allowed_min_date=bq_config.allowed_min_date,
                allowed_max_date=bq_config.allowed_max_date
            )
            table_id = default_table.table_id
        else:
            # Create new default table
            table_info = table_registry.create_table(
                name="default",
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_json=bq_config.credentials_json if not bq_config.use_adc else "",
                allowed_min_date=bq_config.allowed_min_date,
                allowed_max_date=bq_config.allowed_max_date
            )
            table_id = table_info.table_id

        # Initialize BigQuery with new configuration
        if bq_config.use_adc:
            # Use Application Default Credentials (user's gcloud auth)
            bq_service = initialize_bigquery_service(
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_path=None,  # Will use ADC
                table_id=table_id
            )
            message = "BigQuery configured successfully using your Google Cloud credentials"
        else:
            # Use Service Account JSON
            if not bq_config.credentials_json:
                raise ValueError("Service account JSON required when not using Application Default Credentials")
            bq_service = initialize_bigquery_with_json(
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_json=bq_config.credentials_json,
                table_id=table_id
            )
            message = "BigQuery configured successfully using service account credentials"

        # Set date limits on the service
        if bq_service:
            bq_service.set_date_limits(
                min_date=bq_config.allowed_min_date,
                max_date=bq_config.allowed_max_date
            )

        # Activate this table
        table_registry.activate_table(table_id)

        return BigQueryConfigResponse(
            success=True,
            message=message,
            connection_status="connected"
        )
    except Exception as e:
        return BigQueryConfigResponse(
            success=False,
            message=f"Failed to configure BigQuery: {str(e)}",
            connection_status="error"
        )

@app.post("/api/bigquery/disconnect", response_model=BigQueryConfigResponse)
async def disconnect_bigquery():
    """Legacy endpoint - Disconnect BigQuery and clear the 'default' table configuration"""
    try:
        # Find and delete the default table if it exists
        tables = table_registry.list_tables()
        default_table = next((t for t in tables if t.name == "default"), None)

        if default_table:
            table_registry.delete_table(default_table.table_id)
            # Clear the BigQuery service for this table
            from services.bigquery_service import _bq_services
            if default_table.table_id in _bq_services:
                del _bq_services[default_table.table_id]
            message = "BigQuery disconnected successfully"
        else:
            message = "No default table found to disconnect"

        return BigQueryConfigResponse(
            success=True,
            message=message,
            connection_status="not configured"
        )
    except Exception as e:
        return BigQueryConfigResponse(
            success=False,
            message=f"Failed to disconnect BigQuery: {str(e)}",
            connection_status="error"
        )

# Multi-Table Management Endpoints

@app.get("/api/tables", response_model=TableListResponse)
async def list_tables():
    """List all configured BigQuery table connections"""
    try:
        tables = table_registry.list_tables()
        active_table_id = table_registry.get_active_table_id()

        table_responses = [
            TableInfoResponse(
                table_id=t.table_id,
                name=t.name,
                project_id=t.project_id,
                dataset=t.dataset,
                table=t.table,
                created_at=t.created_at,
                last_used_at=t.last_used_at,
                is_active=(t.table_id == active_table_id)
            )
            for t in tables
        ]

        return TableListResponse(
            tables=table_responses,
            active_table_id=active_table_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tables", response_model=TableInfoResponse)
async def create_table(request: TableCreateRequest):
    """Create a new BigQuery table configuration"""
    try:
        from services.bigquery_service import initialize_bigquery_service, initialize_bigquery_with_json

        # Create table entry
        table_info = table_registry.create_table(
            name=request.name,
            project_id=request.project_id,
            dataset=request.dataset,
            table=request.table,
            credentials_json=request.credentials_json,
            allowed_min_date=request.allowed_min_date,
            allowed_max_date=request.allowed_max_date
        )

        # Initialize BigQuery service for this table
        if request.credentials_json:
            bq_service = initialize_bigquery_with_json(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_json=request.credentials_json,
                table_id=table_info.table_id
            )
        else:
            bq_service = initialize_bigquery_service(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_path=None,
                table_id=table_info.table_id
            )

        # Set date limits
        if bq_service:
            bq_service.set_date_limits(
                min_date=request.allowed_min_date,
                max_date=request.allowed_max_date
            )

        # Auto-detect schema for new table
        if bq_service and bq_service.schema_service:
            try:
                bq_service.schema_service.get_or_create_schema(
                    project_id=request.project_id,
                    dataset=request.dataset,
                    table=request.table,
                    auto_detect=True
                )
            except Exception as e:
                print(f"Schema auto-detection failed: {e}")

        return TableInfoResponse(
            table_id=table_info.table_id,
            name=table_info.name,
            project_id=table_info.project_id,
            dataset=table_info.dataset,
            table=table_info.table,
            created_at=table_info.created_at,
            last_used_at=table_info.last_used_at,
            is_active=False
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables/{table_id}", response_model=TableInfoResponse)
async def get_table(table_id: str):
    """Get information about a specific table"""
    try:
        table_info = table_registry.get_table(table_id)
        if not table_info:
            raise HTTPException(status_code=404, detail="Table not found")

        active_table_id = table_registry.get_active_table_id()

        return TableInfoResponse(
            table_id=table_info.table_id,
            name=table_info.name,
            project_id=table_info.project_id,
            dataset=table_info.dataset,
            table=table_info.table,
            created_at=table_info.created_at,
            last_used_at=table_info.last_used_at,
            is_active=(table_info.table_id == active_table_id)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/tables/{table_id}", response_model=TableInfoResponse)
async def update_table(table_id: str, request: TableUpdateRequest):
    """Update table metadata (name)"""
    try:
        success = table_registry.update_table(table_id, request.name)
        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

        table_info = table_registry.get_table(table_id)
        active_table_id = table_registry.get_active_table_id()

        return TableInfoResponse(
            table_id=table_info.table_id,
            name=table_info.name,
            project_id=table_info.project_id,
            dataset=table_info.dataset,
            table=table_info.table,
            created_at=table_info.created_at,
            last_used_at=table_info.last_used_at,
            is_active=(table_info.table_id == active_table_id)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/tables/{table_id}/config")
async def update_table_config(table_id: str, request: TableConfigUpdateRequest):
    """Update table BigQuery configuration"""
    try:
        from services.bigquery_service import initialize_bigquery_service, initialize_bigquery_with_json

        success = table_registry.update_table_config(
            table_id=table_id,
            project_id=request.project_id,
            dataset=request.dataset,
            table=request.table,
            credentials_json=request.credentials_json,
            allowed_min_date=request.allowed_min_date,
            allowed_max_date=request.allowed_max_date
        )

        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

        # Re-initialize BigQuery service for this table
        if request.credentials_json:
            bq_service = initialize_bigquery_with_json(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_json=request.credentials_json,
                table_id=table_id
            )
        else:
            bq_service = initialize_bigquery_service(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_path=None,
                table_id=table_id
            )

        # Set date limits
        if bq_service:
            bq_service.set_date_limits(
                min_date=request.allowed_min_date,
                max_date=request.allowed_max_date
            )

        return {"success": True, "message": "Table configuration updated"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/tables/{table_id}")
async def delete_table(table_id: str):
    """Delete a table configuration"""
    try:
        success = table_registry.delete_table(table_id)
        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

        # Clear BigQuery service for this table
        from services.bigquery_service import _bq_services
        if table_id in _bq_services:
            del _bq_services[table_id]

        return {"success": True, "message": "Table deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tables/{table_id}/activate")
async def activate_table(table_id: str):
    """Activate a table for use"""
    try:
        success = table_registry.activate_table(table_id)
        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

        return {"success": True, "message": "Table activated", "table_id": table_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tables/schema/copy")
async def copy_schema(request: SchemaCopyRequest):
    """Copy schema from one table to another"""
    try:
        from services.schema_service import SchemaService
        from services.bigquery_service import get_bigquery_service

        # Get BigQuery service for target table (to access schema service)
        bq_service = get_bigquery_service(request.target_table_id)
        if not bq_service or not bq_service.schema_service:
            raise HTTPException(status_code=400, detail="Target table BigQuery service not initialized")

        success = bq_service.schema_service.copy_schema(
            source_table_id=request.source_table_id,
            target_table_id=request.target_table_id
        )

        if not success:
            raise HTTPException(status_code=500, detail="Failed to copy schema")

        return {"success": True, "message": "Schema copied successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tables/schema/template")
async def apply_schema_template(request: SchemaTemplateRequest):
    """Apply a schema template to a table"""
    try:
        from services.bigquery_service import get_bigquery_service

        # Determine target table
        table_id = request.table_id
        if not table_id:
            table_id = table_registry.get_active_table_id()
            if not table_id:
                raise HTTPException(status_code=400, detail="No active table")

        # Get BigQuery service for table
        bq_service = get_bigquery_service(table_id)
        if not bq_service or not bq_service.schema_service:
            raise HTTPException(status_code=400, detail="BigQuery service not initialized for table")

        success = bq_service.schema_service.apply_template(request.template_name)

        if not success:
            raise HTTPException(status_code=500, detail="Failed to apply template")

        return {"success": True, "message": f"Template '{request.template_name}' applied successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/tables")
async def list_bigquery_tables():
    """List all tables in the configured BigQuery dataset"""
    try:
        from services.bigquery_service import get_bigquery_service
        bq_service = get_bigquery_service()

        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        tables = bq_service.list_tables_in_dataset()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/tables/{table_name}/dates")
async def get_table_dates(table_name: str):
    """Get date range for a specific table"""
    try:
        from services.bigquery_service import get_bigquery_service
        bq_service = get_bigquery_service()

        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        date_info = bq_service.get_table_date_range(table_name)
        return date_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Schema Management Endpoints

def get_schema_services():
    """Helper to get schema, metric, and dimension services for the active table"""
    from services.bigquery_service import get_bigquery_service
    from services.metric_service import MetricService
    from services.dimension_service import DimensionService

    bq_service = get_bigquery_service()
    if bq_service is None:
        raise HTTPException(status_code=400, detail="BigQuery not configured")

    # Use the schema service from bq_service which has the correct table_id
    if not bq_service.schema_service:
        raise HTTPException(status_code=500, detail="Schema service not initialized")

    schema_service = bq_service.schema_service
    metric_service = MetricService(schema_service)
    dimension_service = DimensionService(schema_service)

    return schema_service, metric_service, dimension_service

@app.get("/api/schema", response_model=SchemaConfig)
async def get_schema():
    """Get current schema configuration"""
    try:
        schema_service, _, _ = get_schema_services()
        schema = schema_service.load_schema()

        if schema is None:
            # Auto-create schema if it doesn't exist
            schema = schema_service.get_or_create_schema(
                project_id=config.BIGQUERY_PROJECT_ID,
                dataset=config.BIGQUERY_DATASET,
                table=config.BIGQUERY_TABLE,
                auto_detect=True
            )

        return schema
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema/detect", response_model=SchemaDetectionResult)
async def detect_schema():
    """Auto-detect schema from BigQuery table and save it"""
    try:
        from services.bigquery_service import get_bigquery_service

        bq_service = get_bigquery_service()
        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        schema_service = bq_service.schema_service
        if not schema_service:
            raise HTTPException(status_code=500, detail="Schema service not initialized")

        result = schema_service.detect_schema(
            project_id=bq_service.project_id,
            dataset=bq_service.dataset,
            table=bq_service.table
        )

        # Create and save schema from detected results
        from datetime import datetime
        now = datetime.utcnow().isoformat()
        detected_schema = SchemaConfig(
            base_metrics=result.detected_base_metrics,
            calculated_metrics=[],  # No calculated metrics from auto-detect
            dimensions=result.detected_dimensions,
            created_at=now,
            updated_at=now
        )
        schema_service.save_schema(detected_schema)

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema/reset")
async def reset_schema():
    """Reset schema to default configuration"""
    try:
        schema_service, _, _ = get_schema_services()

        default_schema = schema_service.create_default_schema()
        schema_service.save_schema(default_schema)

        return {"success": True, "message": "Schema reset to default configuration"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/schema/pivot-config", response_model=SchemaConfig)
async def update_pivot_config(update: PivotConfigUpdate):
    """Update pivot table configuration settings"""
    try:
        schema_service, _, _ = get_schema_services()

        # Load existing schema
        schema = schema_service.load_schema()
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found. Please detect or reset schema first.")

        # Apply updates (only update fields that were provided)
        if update.primary_sort_metric is not None:
            # Validate that metric exists
            metric = schema_service.get_metric_by_id(update.primary_sort_metric, schema)
            if not metric:
                raise HTTPException(
                    status_code=400,
                    detail=f"Metric '{update.primary_sort_metric}' not found in schema"
                )
            schema.primary_sort_metric = update.primary_sort_metric

        if update.avg_per_day_metric is not None:
            # Validate that metric exists
            metric = schema_service.get_metric_by_id(update.avg_per_day_metric, schema)
            if not metric:
                raise HTTPException(
                    status_code=400,
                    detail=f"Metric '{update.avg_per_day_metric}' not found in schema"
                )
            schema.avg_per_day_metric = update.avg_per_day_metric

        if update.pagination_threshold is not None:
            schema.pagination_threshold = update.pagination_threshold

        # Save updated schema
        schema_service.save_schema(schema)

        return schema
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Base Metrics Endpoints

@app.get("/api/metrics/base", response_model=List[BaseMetric])
async def list_base_metrics():
    """List all base metrics"""
    try:
        _, metric_service, _ = get_schema_services()
        return metric_service.list_base_metrics()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/base", response_model=BaseMetric)
async def create_base_metric(metric: MetricCreate):
    """Create a new base metric"""
    try:
        _, metric_service, _ = get_schema_services()
        return metric_service.create_base_metric(metric)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/base/{metric_id}", response_model=BaseMetric)
async def get_base_metric(metric_id: str):
    """Get a specific base metric"""
    try:
        _, metric_service, _ = get_schema_services()
        metric = metric_service.get_base_metric(metric_id)

        if metric is None:
            raise HTTPException(status_code=404, detail=f"Base metric '{metric_id}' not found")

        return metric
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/metrics/base/{metric_id}")
async def update_base_metric(metric_id: str, update: MetricUpdate):
    """Update a base metric and cascade update dependents"""
    try:
        _, metric_service, _ = get_schema_services()

        # Update the base metric
        updated_metric = metric_service.update_base_metric(metric_id, update)

        # Cascade update all dependent calculated metrics
        cascade_result = metric_service.cascade_update_dependents(metric_id, metric_type='base')

        return {
            "metric": updated_metric,
            "cascade_updated_count": cascade_result['updated_count'],
            "cascade_updated_metrics": cascade_result['updated_metrics']
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/metrics/base/{metric_id}")
async def delete_base_metric(metric_id: str):
    """Delete a base metric"""
    try:
        _, metric_service, _ = get_schema_services()
        metric_service.delete_base_metric(metric_id)
        return {"success": True, "message": f"Base metric '{metric_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Calculated Metrics Endpoints

@app.get("/api/metrics/calculated", response_model=List[CalculatedMetric])
async def list_calculated_metrics():
    """List all calculated metrics"""
    try:
        _, metric_service, _ = get_schema_services()
        return metric_service.list_calculated_metrics()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/calculated", response_model=CalculatedMetric)
async def create_calculated_metric(metric: CalculatedMetricCreate):
    """Create a new calculated metric"""
    try:
        _, metric_service, _ = get_schema_services()
        return metric_service.create_calculated_metric(metric)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/calculated/{metric_id}", response_model=CalculatedMetric)
async def get_calculated_metric(metric_id: str):
    """Get a specific calculated metric"""
    try:
        _, metric_service, _ = get_schema_services()
        metric = metric_service.get_calculated_metric(metric_id)

        if metric is None:
            raise HTTPException(status_code=404, detail=f"Calculated metric '{metric_id}' not found")

        return metric
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/metrics/calculated/{metric_id}")
async def update_calculated_metric(metric_id: str, update: CalculatedMetricUpdate):
    """Update a calculated metric and cascade update dependents"""
    try:
        _, metric_service, _ = get_schema_services()

        # Update the calculated metric
        updated_metric = metric_service.update_calculated_metric(metric_id, update)

        # Cascade update all dependent calculated metrics (if any)
        cascade_result = metric_service.cascade_update_dependents(metric_id, metric_type='calculated')

        return {
            "metric": updated_metric,
            "cascade_updated_count": cascade_result['updated_count'],
            "cascade_updated_metrics": cascade_result['updated_metrics']
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/metrics/calculated/{metric_id}")
async def delete_calculated_metric(metric_id: str):
    """Delete a calculated metric"""
    try:
        _, metric_service, _ = get_schema_services()
        metric_service.delete_calculated_metric(metric_id)
        return {"success": True, "message": f"Calculated metric '{metric_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/validate-formula")
async def validate_formula(formula: dict):
    """Validate a metric formula"""
    try:
        _, metric_service, _ = get_schema_services()
        result = metric_service.validate_formula(formula.get('formula', ''))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Dimensions Endpoints

@app.get("/api/dimensions", response_model=List[DimensionDef])
async def list_dimensions():
    """List all dimensions"""
    try:
        _, _, dimension_service = get_schema_services()
        return dimension_service.list_dimensions()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/dimensions", response_model=DimensionDef)
async def create_dimension(dimension: DimensionCreate):
    """Create a new dimension"""
    try:
        _, _, dimension_service = get_schema_services()
        return dimension_service.create_dimension(dimension)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/filterable", response_model=List[DimensionDef])
async def list_filterable_dimensions():
    """Get all filterable dimensions"""
    try:
        _, _, dimension_service = get_schema_services()
        return dimension_service.list_filterable_dimensions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/groupable", response_model=List[DimensionDef])
async def list_groupable_dimensions():
    """Get all groupable dimensions"""
    try:
        _, _, dimension_service = get_schema_services()
        return dimension_service.list_groupable_dimensions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/{dimension_id}", response_model=DimensionDef)
async def get_dimension(dimension_id: str):
    """Get a specific dimension"""
    try:
        _, _, dimension_service = get_schema_services()
        dimension = dimension_service.get_dimension(dimension_id)

        if dimension is None:
            raise HTTPException(status_code=404, detail=f"Dimension '{dimension_id}' not found")

        return dimension
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/dimensions/{dimension_id}", response_model=DimensionDef)
async def update_dimension(dimension_id: str, update: DimensionUpdate):
    """Update a dimension"""
    try:
        _, _, dimension_service = get_schema_services()
        return dimension_service.update_dimension(dimension_id, update)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/dimensions/{dimension_id}")
async def delete_dimension(dimension_id: str):
    """Delete a dimension"""
    try:
        _, _, dimension_service = get_schema_services()
        dimension_service.delete_dimension(dimension_id)
        return {"success": True, "message": f"Dimension '{dimension_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Custom Dimension Endpoints

@app.get("/api/custom-dimensions", response_model=List[CustomDimension])
async def list_custom_dimensions():
    """Get all custom dimensions"""
    try:
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        return cd_service.get_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/custom-dimensions", response_model=CustomDimension)
async def create_custom_dimension(dimension_data: CustomDimensionCreate):
    """Create a new custom dimension"""
    try:
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        return cd_service.create(dimension_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/custom-dimensions/{dimension_id}", response_model=CustomDimension)
async def update_custom_dimension(dimension_id: str, update_data: CustomDimensionUpdate):
    """Update an existing custom dimension"""
    try:
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        result = cd_service.update(dimension_id, update_data)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Custom dimension {dimension_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/custom-dimensions/{dimension_id}")
async def delete_custom_dimension(dimension_id: str):
    """Delete a custom dimension"""
    try:
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        success = cd_service.delete(dimension_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Custom dimension {dimension_id} not found")
        return {"success": True, "message": "Custom dimension deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/custom-dimensions/{dimension_id}/duplicate", response_model=CustomDimension)
async def duplicate_custom_dimension(dimension_id: str):
    """Duplicate an existing custom dimension"""
    try:
        cd_service = get_custom_dimension_service(CUSTOM_DIMENSIONS_FILE)
        result = cd_service.duplicate(dimension_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Custom dimension {dimension_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Pivot Table Endpoints

@app.get("/api/pivot", response_model=PivotResponse)
async def get_pivot_table(
    request: Request,
    dimensions: List[str] = Query([], description="Dimensions to pivot by (e.g., country, channel, n_words_normalized)"),
    dimension_values: Optional[List[str]] = Query(None, description="Specific dimension values to fetch (for multi-table matching)"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """
    Get pivot table data grouped by specified dimension(s).

    Supports dynamic dimension filtering via query parameters:
    - Multi-select: ?country=USA&country=Canada&channel=Web
    - Single-select: ?channel=App
    """
    try:
        # Parse dynamic dimension filters from query parameters
        dimension_filters = parse_dimension_filters(request)

        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters
        )
        return data_service.get_pivot_data(dimensions, filters, limit, offset, dimension_values)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/children", response_model=List[PivotChildRow])
async def get_all_pivot_children(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Get all search terms without dimension filtering"""
    try:
        # Parse dynamic dimension filters from query parameters
        dimension_filters = parse_dimension_filters(request)

        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters
        )
        return data_service.get_pivot_children('', '', filters, limit, offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/{dimension}/{value}/children", response_model=List[PivotChildRow])
async def get_pivot_children(
    request: Request,
    dimension: str,
    value: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Get child rows (search terms) for a specific dimension value"""
    try:
        # Parse dynamic dimension filters from query parameters
        dimension_filters = parse_dimension_filters(request)

        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters
        )
        return data_service.get_pivot_children(dimension, value, filters, limit, offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/dimension/{dimension}/values", response_model=List[str])
async def get_dimension_values(
    request: Request,
    dimension: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    Get distinct values for a given dimension.

    Supports filtering via query parameters to get values that exist
    with the current filters applied.
    """
    try:
        # Parse dynamic dimension filters from query parameters
        dimension_filters = parse_dimension_filters(request)

        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            dimension_filters=dimension_filters
        )
        return data_svc.get_dimension_values(dimension, filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Query Logging Endpoints

@app.get("/api/bigquery/logs", response_model=QueryLogResponse)
async def get_query_logs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    query_type: Optional[str] = None,
    endpoint: Optional[str] = None
):
    """Get query logs with filtering and pagination"""
    try:
        from services.query_logger import get_query_logger
        logger = get_query_logger()

        if logger is None:
            raise HTTPException(status_code=500, detail="Query logger not initialized")

        logs, total = logger.get_logs(
            limit=limit,
            offset=offset,
            start_date=start_date,
            end_date=end_date,
            query_type=query_type,
            endpoint=endpoint
        )

        return QueryLogResponse(
            logs=[QueryLogEntry(**log) for log in logs],
            total=total,
            limit=limit,
            offset=offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/usage/stats", response_model=UsageStats)
async def get_usage_stats(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get aggregated usage statistics"""
    try:
        from services.query_logger import get_query_logger
        logger = get_query_logger()

        if logger is None:
            raise HTTPException(status_code=500, detail="Query logger not initialized")

        stats = logger.get_usage_stats(start_date=start_date, end_date=end_date)
        return UsageStats(**stats)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/usage/stats/today", response_model=UsageStats)
async def get_today_usage_stats():
    """Get usage statistics for today"""
    try:
        from services.query_logger import get_query_logger
        logger = get_query_logger()

        if logger is None:
            raise HTTPException(status_code=500, detail="Query logger not initialized")

        stats = logger.get_today_stats()
        return UsageStats(**stats)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/usage/timeseries", response_model=List[UsageTimeSeries])
async def get_usage_timeseries(
    granularity: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get usage statistics by date"""
    try:
        from services.query_logger import get_query_logger
        logger = get_query_logger()

        if logger is None:
            raise HTTPException(status_code=500, detail="Query logger not initialized")

        data = logger.get_usage_by_date(
            granularity=granularity,
            start_date=start_date,
            end_date=end_date
        )
        return [UsageTimeSeries(**item) for item in data]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bigquery/logs/clear", response_model=ClearLogsResponse)
async def clear_query_logs():
    """Clear all query logs"""
    try:
        from services.query_logger import get_query_logger
        logger = get_query_logger()

        if logger is None:
            raise HTTPException(status_code=500, detail="Query logger not initialized")

        deleted_count = logger.clear_logs()

        return ClearLogsResponse(
            success=True,
            message=f"Successfully cleared {deleted_count} log entries",
            logs_deleted=deleted_count
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
