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
from services.query_cache_service import initialize_query_cache, get_query_cache, QueryCacheService
from config import CUSTOM_DIMENSIONS_FILE, QUERY_LOGS_DB_PATH, table_registry, dashboard_registry, app_settings
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
    # Calculated dimension models
    CalculatedDimensionDef,
    CalculatedDimensionCreate,
    CalculatedDimensionUpdate,
    ExpressionValidationResult,
    # Multi-table models
    TableInfoResponse,
    TableListResponse,
    TableCreateRequest,
    TableUpdateRequest,
    TableConfigUpdateRequest,
    TableActivateRequest,
    SchemaCopyRequest,
    SchemaTemplateRequest,
    # Dashboard models
    DashboardConfig,
    DashboardListResponse,
    DashboardCreateRequest,
    DashboardUpdateRequest,
    WidgetConfig,
    WidgetCreateRequest,
    WidgetUpdateRequest,
    # Significance testing models
    SignificanceRequest,
    SignificanceResponse,
    SignificanceResultItem,
    ColumnDefinition,
    # Cache management models
    CacheStats,
    CacheClearResponse,
    # Rollup models
    RollupDef,
    RollupConfig,
    RollupCreate,
    RollupUpdate,
    RollupRefreshResponse,
    RollupListResponse,
    RollupPreviewSqlResponse,
    RollupMetricDef,
    RollupStatusResponse,
    # Optimized source models
    OptimizedSourceConfig,
    OptimizedSourceCreate,
    OptimizedSourceResponse,
    OptimizedSourceStatusResponse,
    OptimizedSourceAnalysis,
    OptimizedSourcePreviewSql,
    # App settings models
    AppSettingsResponse,
    AppSettingsUpdate,
)
from services.statistical_service import StatisticalService

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
        'limit', 'offset', 'sort_by', 'granularity', 'table_id', 'skip_count', 'metrics',
        'date_range_type', 'relative_date_preset', 'require_rollup'
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
    """Initialize BigQuery connection, query logger, and query cache on application startup"""
    # Initialize query logger
    try:
        print(f"Initializing query logger at {QUERY_LOGS_DB_PATH}...")
        initialize_query_logger(QUERY_LOGS_DB_PATH)
        print("Query logger initialized successfully")
    except Exception as e:
        print(f"Failed to initialize query logger: {e}")

    # Initialize query cache (same DB as query logger)
    try:
        print(f"Initializing query cache at {QUERY_LOGS_DB_PATH}...")
        initialize_query_cache(QUERY_LOGS_DB_PATH)
        print("Query cache initialized successfully")
    except Exception as e:
        print(f"Failed to initialize query cache: {e}")

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
                            table_id=table_info.table_id,
                            billing_project=table_info.billing_project
                        )
                    else:
                        bq_service = initialize_bigquery_service(
                            project_id=table_info.project_id,
                            dataset=table_info.dataset,
                            table=table_info.table,
                            credentials_path=None,
                            table_id=table_info.table_id,
                            billing_project=table_info.billing_project
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


# =============================================================================
# Global App Settings Endpoints
# =============================================================================

@app.get("/api/settings", response_model=AppSettingsResponse)
async def get_app_settings():
    """Get global application settings"""
    return AppSettingsResponse(**app_settings.to_dict())


@app.put("/api/settings", response_model=AppSettingsResponse)
async def update_app_settings(settings_update: AppSettingsUpdate):
    """Update global application settings"""
    if settings_update.default_billing_project is not None:
        app_settings.set_default_billing_project(settings_update.default_billing_project)

    return AppSettingsResponse(**app_settings.to_dict())


@app.get("/api/bigquery/info", response_model=BigQueryInfo)
async def get_bigquery_information(table_id: Optional[str] = Query(None, description="Optional table ID to get info for")):
    """Get BigQuery connection status and table information"""
    try:
        return get_bigquery_info(table_id)
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
            # Update existing default table config
            table_registry.update_table_config(
                table_id=default_table.table_id,
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_json=bq_config.credentials_json if not bq_config.use_adc else "",
                billing_project=bq_config.billing_project,
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
                billing_project=bq_config.billing_project,
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
                table_id=table_id,
                billing_project=bq_config.billing_project
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
                table_id=table_id,
                billing_project=bq_config.billing_project
            )
            message = "BigQuery configured successfully using service account credentials"

        # Set date limits on the service
        if bq_service:
            bq_service.set_date_limits(
                min_date=bq_config.allowed_min_date,
                max_date=bq_config.allowed_max_date
            )

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
async def disconnect_bigquery(table_id: Optional[str] = Query(None, description="Table ID to disconnect")):
    """Disconnect and delete a BigQuery table configuration"""
    try:
        if not table_id:
            raise HTTPException(status_code=400, detail="table_id is required")

        # Delete the table
        success = table_registry.delete_table(table_id)

        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

        # Clear the BigQuery service for this table
        from services.bigquery_service import _bq_services
        if table_id in _bq_services:
            del _bq_services[table_id]

        return BigQueryConfigResponse(
            success=True,
            message="Table disconnected and deleted successfully",
            connection_status="not configured"
        )
    except HTTPException:
        raise
    except Exception as e:
        return BigQueryConfigResponse(
            success=False,
            message=f"Failed to disconnect BigQuery: {str(e)}",
            connection_status="error"
        )


# Multi-Table Management Endpoints

@app.get("/api/tables", response_model=TableListResponse)
async def list_tables(active_table_id: Optional[str] = None):
    """List all configured BigQuery table connections"""
    try:
        tables = table_registry.list_tables()

        table_responses = [
            TableInfoResponse(
                table_id=t.table_id,
                name=t.name,
                project_id=t.project_id,
                dataset=t.dataset,
                table=t.table,
                created_at=t.created_at,
                last_used_at=t.last_used_at,
                is_active=(t.table_id == active_table_id) if active_table_id else False
            )
            for t in tables
        ]

        return TableListResponse(tables=table_responses)
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
            billing_project=request.billing_project,
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
                table_id=table_info.table_id,
                billing_project=request.billing_project
            )
        else:
            bq_service = initialize_bigquery_service(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_path=None,
                table_id=table_info.table_id,
                billing_project=request.billing_project
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
                pass

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

        return TableInfoResponse(
            table_id=table_info.table_id,
            name=table_info.name,
            project_id=table_info.project_id,
            dataset=table_info.dataset,
            table=table_info.table,
            created_at=table_info.created_at,
            last_used_at=table_info.last_used_at
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

        return TableInfoResponse(
            table_id=table_info.table_id,
            name=table_info.name,
            project_id=table_info.project_id,
            dataset=table_info.dataset,
            table=table_info.table,
            created_at=table_info.created_at,
            last_used_at=table_info.last_used_at
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
            billing_project=request.billing_project,
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
                table_id=table_id,
                billing_project=request.billing_project
            )
        else:
            bq_service = initialize_bigquery_service(
                project_id=request.project_id,
                dataset=request.dataset,
                table=request.table,
                credentials_path=None,
                table_id=table_id,
                billing_project=request.billing_project
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
async def list_bigquery_tables(table_id: Optional[str] = Query(None, description="Table ID to use")):
    """List all tables in the configured BigQuery dataset"""
    try:
        from services.bigquery_service import get_bigquery_service
        bq_service = get_bigquery_service(table_id)

        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        tables = bq_service.list_tables_in_dataset()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bigquery/tables/{table_name}/dates")
async def get_table_dates(table_name: str, table_id: Optional[str] = Query(None, description="Table ID to use")):
    """Get date range for a specific table"""
    try:
        from services.bigquery_service import get_bigquery_service
        bq_service = get_bigquery_service(table_id)

        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        # Use the table from the bq_service which corresponds to the table_id
        date_info = bq_service.get_table_date_range()
        return date_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Schema Management Endpoints

def get_schema_services(table_id: Optional[str] = None):
    """Helper to get schema, metric, and dimension services for a table"""
    from services.bigquery_service import get_bigquery_service
    from services.metric_service import MetricService
    from services.dimension_service import DimensionService

    bq_service = get_bigquery_service(table_id)
    if bq_service is None:
        return None  # No BigQuery configured - return None instead of raising exception

    # Use the schema service from bq_service which has the correct table_id
    if not bq_service.schema_service:
        raise HTTPException(status_code=500, detail="Schema service not initialized")

    schema_service = bq_service.schema_service
    metric_service = MetricService(schema_service)
    dimension_service = DimensionService(schema_service)

    return schema_service, metric_service, dimension_service

@app.get("/api/schema", response_model=SchemaConfig)
async def get_schema(table_id: Optional[str] = Query(None, description="Table ID to get schema for")):
    """Get current schema configuration"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            # No tables configured - return empty schema
            from datetime import datetime
            now = datetime.utcnow().isoformat()
            return SchemaConfig(
                base_metrics=[],
                calculated_metrics=[],
                dimensions=[],
                created_at=now,
                updated_at=now
            )

        schema_service, _, _ = services
        schema = schema_service.load_schema()

        if schema is None:
            # Return empty schema if it doesn't exist - user must explicitly detect or create schema
            from datetime import datetime
            now = datetime.utcnow().isoformat()
            return SchemaConfig(
                base_metrics=[],
                calculated_metrics=[],
                dimensions=[],
                created_at=now,
                updated_at=now
            )

        return schema
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema/detect", response_model=SchemaDetectionResult)
async def detect_schema(table_id: Optional[str] = Query(None, description="Table ID to detect schema for")):
    """Auto-detect schema from BigQuery table and save it"""
    try:
        from services.bigquery_service import get_bigquery_service

        bq_service = get_bigquery_service(table_id)
        if bq_service is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured or table not found")

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
async def reset_schema(table_id: Optional[str] = Query(None, description="Table ID to reset schema for")):
    """Reset schema to empty configuration"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            raise HTTPException(status_code=400, detail="No table selected or BigQuery not configured")

        schema_service, _, _ = services

        # Create empty schema instead of default with hardcoded metrics
        from datetime import datetime
        now = datetime.utcnow().isoformat()
        empty_schema = SchemaConfig(
            base_metrics=[],
            calculated_metrics=[],
            dimensions=[],
            created_at=now,
            updated_at=now
        )
        schema_service.save_schema(empty_schema)

        return {"success": True, "message": "Schema reset to empty configuration"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/schema/pivot-config", response_model=SchemaConfig)
async def update_pivot_config(update: PivotConfigUpdate, table_id: Optional[str] = Query(None, description="Table ID to update pivot config for")):
    """Update pivot table configuration settings"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            raise HTTPException(status_code=400, detail="No table selected or BigQuery not configured")

        schema_service, _, _ = services

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

# Base Metrics Endpoints - DEPRECATED
# Base metrics are no longer used. All metrics should be calculated metrics.
# These endpoints are kept for backward compatibility but return empty lists.

@app.get("/api/metrics/base", response_model=List[BaseMetric])
async def list_base_metrics(table_id: Optional[str] = Query(None, description="Table ID to get metrics for")):
    """DEPRECATED: List all base metrics. Returns empty list - use calculated metrics instead."""
    return []  # Base metrics are deprecated, always return empty

@app.post("/api/metrics/base", response_model=BaseMetric)
async def create_base_metric(metric: MetricCreate, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """DEPRECATED: Create a new base metric. Use calculated metrics instead."""
    raise HTTPException(
        status_code=410,  # Gone
        detail="Base metrics are deprecated. Please use calculated metrics instead (POST /api/metrics/calculated)"
    )

@app.get("/api/metrics/base/{metric_id}", response_model=BaseMetric)
async def get_base_metric(metric_id: str, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """DEPRECATED: Get a specific base metric."""
    raise HTTPException(
        status_code=410,  # Gone
        detail="Base metrics are deprecated. Please use calculated metrics instead"
    )

@app.put("/api/metrics/base/{metric_id}")
async def update_base_metric(metric_id: str, update: MetricUpdate, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """DEPRECATED: Update a base metric."""
    raise HTTPException(
        status_code=410,  # Gone
        detail="Base metrics are deprecated. Please use calculated metrics instead (PUT /api/metrics/calculated/{metric_id})"
    )

@app.delete("/api/metrics/base/{metric_id}")
async def delete_base_metric(metric_id: str, table_id: Optional[str] = Query(None, description="Table ID")):
    """DEPRECATED: Delete a base metric."""
    raise HTTPException(
        status_code=410,  # Gone
        detail="Base metrics are deprecated. Please use calculated metrics instead"
    )

# Calculated Metrics Endpoints

@app.get("/api/metrics/calculated", response_model=List[CalculatedMetric])
async def list_calculated_metrics(table_id: Optional[str] = Query(None, description="Table ID to get metrics for")):
    """List all calculated metrics"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            return []  # No tables configured
        _, metric_service, _ = services
        return metric_service.list_calculated_metrics()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/calculated", response_model=CalculatedMetric)
async def create_calculated_metric(metric: CalculatedMetricCreate, table_id: Optional[str] = Query(None, description="Table ID")):
    """Create a new calculated metric"""
    try:
        _, metric_service, _ = get_schema_services(table_id)
        return metric_service.create_calculated_metric(metric)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/base/{metric_id}/daily-average", response_model=CalculatedMetric)
async def create_daily_average_metric(metric_id: str, table_id: Optional[str] = Query(None, description="Table ID")):
    """
    Create a daily average metric for a base or calculated metric.
    Automatically creates a calculated metric with formula: {metric_id} / {days_in_range}
    Only works for metrics with category='volume'.
    """
    try:
        _, metric_service, _ = get_schema_services(table_id)
        return metric_service.create_daily_average_metric(metric_id, table_id)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full traceback to console
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/calculated/{metric_id}", response_model=CalculatedMetric)
async def get_calculated_metric(metric_id: str, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """Get a specific calculated metric"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")
        _, metric_service, _ = services
        metric = metric_service.get_calculated_metric(metric_id)

        if metric is None:
            raise HTTPException(status_code=404, detail=f"Calculated metric '{metric_id}' not found")

        return metric
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/metrics/calculated/{metric_id}")
async def update_calculated_metric(metric_id: str, update: CalculatedMetricUpdate, table_id: Optional[str] = Query(None)):
    """Update a calculated metric and cascade update dependents"""
    try:
        _, metric_service, _ = get_schema_services(table_id)

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
async def delete_calculated_metric(metric_id: str, table_id: Optional[str] = Query(None, description="Table ID")):
    """Delete a calculated metric"""
    try:
        _, metric_service, _ = get_schema_services(table_id)
        metric_service.delete_calculated_metric(metric_id)
        return {"success": True, "message": f"Calculated metric '{metric_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/metrics/validate-formula")
async def validate_formula(formula: dict, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """Validate a metric formula"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")
        _, metric_service, _ = services
        result = metric_service.validate_formula(formula.get('formula', ''))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Dimensions Endpoints

@app.get("/api/dimensions", response_model=List[DimensionDef])
async def list_dimensions(table_id: Optional[str] = Query(None, description="Table ID to get dimensions for")):
    """List all dimensions"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            return []  # No tables configured
        _, _, dimension_service = services
        return dimension_service.list_dimensions()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/dimensions", response_model=DimensionDef)
async def create_dimension(dimension: DimensionCreate, table_id: Optional[str] = Query(None)):
    """Create a new dimension"""
    try:
        _, _, dimension_service = get_schema_services(table_id)
        return dimension_service.create_dimension(dimension)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/filterable", response_model=List[DimensionDef])
async def list_filterable_dimensions(table_id: Optional[str] = Query(None, description="Table ID to get dimensions for")):
    """Get all filterable dimensions"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            return []  # No tables configured
        _, _, dimension_service = services
        return dimension_service.list_filterable_dimensions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/groupable", response_model=List[DimensionDef])
async def list_groupable_dimensions(table_id: Optional[str] = Query(None, description="Table ID to get dimensions for")):
    """Get all groupable dimensions"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            return []  # No tables configured
        _, _, dimension_service = services
        return dimension_service.list_groupable_dimensions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dimensions/{dimension_id}", response_model=DimensionDef)
async def get_dimension(dimension_id: str, table_id: Optional[str] = Query(None, description="Optional table ID")):
    """Get a specific dimension"""
    try:
        services = get_schema_services(table_id)
        if services is None:
            raise HTTPException(status_code=400, detail="BigQuery not configured")
        _, _, dimension_service = services
        dimension = dimension_service.get_dimension(dimension_id)

        if dimension is None:
            raise HTTPException(status_code=404, detail=f"Dimension '{dimension_id}' not found")

        return dimension
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/dimensions/{dimension_id}", response_model=DimensionDef)
async def update_dimension(dimension_id: str, update: DimensionUpdate, table_id: Optional[str] = Query(None)):
    """Update a dimension"""
    try:
        _, _, dimension_service = get_schema_services(table_id)
        return dimension_service.update_dimension(dimension_id, update)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/dimensions/{dimension_id}")
async def delete_dimension(dimension_id: str, table_id: Optional[str] = Query(None, description="Table ID")):
    """Delete a dimension"""
    try:
        _, _, dimension_service = get_schema_services(table_id)
        dimension_service.delete_dimension(dimension_id)
        return {"success": True, "message": f"Dimension '{dimension_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Calculated Dimension Endpoints

def get_calculated_dimension_service(table_id: Optional[str] = None):
    """Get the calculated dimension service for a table."""
    from services.calculated_dimension_service import CalculatedDimensionService
    from services.dimension_expression_parser import DimensionExpressionParser
    from services.bigquery_service import get_bigquery_service

    schema_service, _, _ = get_schema_services(table_id)

    # Get BigQuery service for expression validation
    bq_service = get_bigquery_service(table_id)
    if bq_service:
        parser = DimensionExpressionParser(bq_service.client, bq_service.table_path)
    else:
        parser = DimensionExpressionParser()

    return CalculatedDimensionService(schema_service, parser)


@app.get("/api/dimensions/calculated", response_model=List[CalculatedDimensionDef])
async def list_calculated_dimensions(table_id: Optional[str] = Query(None, description="Table ID")):
    """List all calculated dimensions"""
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        return calc_dim_service.list_calculated_dimensions()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/dimensions/calculated", response_model=CalculatedDimensionDef)
async def create_calculated_dimension(
    dimension: CalculatedDimensionCreate,
    table_id: Optional[str] = Query(None, description="Table ID")
):
    """Create a new calculated dimension with SQL expression"""
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        return calc_dim_service.create_calculated_dimension(dimension)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dimensions/calculated/{dimension_id}", response_model=CalculatedDimensionDef)
async def get_calculated_dimension(
    dimension_id: str,
    table_id: Optional[str] = Query(None, description="Table ID")
):
    """Get a specific calculated dimension by ID"""
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        dimension = calc_dim_service.get_calculated_dimension(dimension_id)
        if not dimension:
            raise HTTPException(status_code=404, detail=f"Calculated dimension '{dimension_id}' not found")
        return dimension
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/dimensions/calculated/{dimension_id}", response_model=CalculatedDimensionDef)
async def update_calculated_dimension(
    dimension_id: str,
    update: CalculatedDimensionUpdate,
    table_id: Optional[str] = Query(None, description="Table ID")
):
    """Update an existing calculated dimension"""
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        return calc_dim_service.update_calculated_dimension(dimension_id, update)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/dimensions/calculated/{dimension_id}")
async def delete_calculated_dimension(
    dimension_id: str,
    table_id: Optional[str] = Query(None, description="Table ID")
):
    """Delete a calculated dimension"""
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        calc_dim_service.delete_calculated_dimension(dimension_id)
        return {"success": True, "message": f"Calculated dimension '{dimension_id}' deleted"}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/dimensions/validate-expression", response_model=ExpressionValidationResult)
async def validate_dimension_expression(
    request: dict,
    table_id: Optional[str] = Query(None, description="Table ID")
):
    """
    Validate a calculated dimension expression without saving.

    Request body:
        {"expression": "COALESCE(REGEXP_EXTRACT({col1}, r'pattern'), {col2})"}

    Returns validation result including parsed SQL, dependencies, errors, and warnings.
    """
    try:
        expression = request.get("expression", "")
        if not expression:
            raise HTTPException(status_code=400, detail="Expression is required")

        calc_dim_service = get_calculated_dimension_service(table_id)
        return calc_dim_service.validate_expression(expression)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dimensions/all", response_model=List[dict])
async def list_all_dimensions(table_id: Optional[str] = Query(None, description="Table ID")):
    """
    List all dimensions (regular and calculated) with type indicator.

    Returns list of dicts with dimension info and 'type' field ('regular' or 'calculated').
    """
    try:
        calc_dim_service = get_calculated_dimension_service(table_id)
        return calc_dim_service.list_all_dimensions()
    except HTTPException:
        raise
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
    date_range_type: Optional[str] = "absolute",
    relative_date_preset: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    table_id: Optional[str] = Query(None, description="Optional table ID for multi-table widget support"),
    skip_count: bool = Query(False, description="Skip count query for initial load (saves 1 BigQuery query)"),
    metrics: Optional[List[str]] = Query(None, description="Optional list of metric IDs to calculate (default: all metrics)"),
    require_rollup: bool = Query(True, description="If true (default), return error when no suitable rollup exists. Set to false to fall back to raw table."),
):
    """
    Get pivot table data grouped by specified dimension(s).

    Supports dynamic dimension filtering via query parameters:
    - Multi-select: ?country=USA&country=Canada&channel=Web
    - Single-select: ?channel=App
    - Table selection: ?table_id=abc123 (for widget multi-table support)
    - Performance: ?skip_count=true (skip count query on initial load)
    - Performance: ?metrics=queries&metrics=revenue (only calculate specified metrics)
    - Relative dates: ?date_range_type=relative&relative_date_preset=last_7_days
    - Rollup routing: ?require_rollup=true (error if no rollup, otherwise use raw table)
    """
    try:
        # Parse dynamic dimension filters from query parameters
        dimension_filters = parse_dimension_filters(request)

        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset,
            dimension_filters=dimension_filters
        )
        return data_service.get_pivot_data(dimensions, filters, limit, offset, dimension_values, table_id, skip_count, metrics, require_rollup)
    except ValueError as e:
        # ValueError for other validation errors (not rollup - those are returned inline now)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full traceback to console
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/children", response_model=List[PivotChildRow])
async def get_all_pivot_children(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_range_type: Optional[str] = "absolute",
    relative_date_preset: Optional[str] = None,
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
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset,
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
    date_range_type: Optional[str] = "absolute",
    relative_date_preset: Optional[str] = None,
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
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset,
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
    date_range_type: Optional[str] = "absolute",
    relative_date_preset: Optional[str] = None,
    table_id: Optional[str] = Query(None),
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
            date_range_type=date_range_type,
            relative_date_preset=relative_date_preset,
            dimension_filters=dimension_filters
        )
        return data_svc.get_dimension_values(dimension, filters, table_id)
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


# ============================================================================
# CACHE MANAGEMENT ENDPOINTS
# ============================================================================

@app.get("/api/cache/stats", response_model=CacheStats)
async def get_cache_stats():
    """Get cache statistics"""
    try:
        cache = get_query_cache()
        if cache is None:
            raise HTTPException(status_code=503, detail="Cache service not initialized")

        stats = cache.get_stats()
        return CacheStats(**stats)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear", response_model=CacheClearResponse)
async def clear_all_cache():
    """Clear entire cache"""
    try:
        cache = get_query_cache()
        if cache is None:
            raise HTTPException(status_code=503, detail="Cache service not initialized")

        count = cache.clear_all()
        return CacheClearResponse(
            success=True,
            message=f"Cleared all cache entries",
            entries_deleted=count
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear/table/{table_id}", response_model=CacheClearResponse)
async def clear_cache_by_table(table_id: str):
    """Clear cache for specific table"""
    try:
        cache = get_query_cache()
        if cache is None:
            raise HTTPException(status_code=503, detail="Cache service not initialized")

        count = cache.clear_by_table(table_id)
        return CacheClearResponse(
            success=True,
            message=f"Cleared cache for table {table_id}",
            entries_deleted=count
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear/type/{query_type}", response_model=CacheClearResponse)
async def clear_cache_by_query_type(query_type: str):
    """Clear cache for specific query type"""
    try:
        cache = get_query_cache()
        if cache is None:
            raise HTTPException(status_code=503, detail="Cache service not initialized")

        valid_types = QueryCacheService.QUERY_TYPES
        if query_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query type. Must be one of: {', '.join(valid_types)}"
            )

        count = cache.clear_by_query_type(query_type)
        return CacheClearResponse(
            success=True,
            message=f"Cleared cache for query type {query_type}",
            entries_deleted=count
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ROLLUP (PRE-AGGREGATION) ENDPOINTS
# ============================================================================

def get_rollup_service(table_id: Optional[str] = None):
    """Get rollup service for a table."""
    from services.rollup_service import RollupService
    from services.bigquery_service import get_bigquery_service

    bq_service = get_bigquery_service(table_id)
    if not bq_service:
        return None, None

    rollup_service = RollupService(bq_service.client, bq_service.table_id or table_id)
    return rollup_service, bq_service


@app.get("/api/rollups", response_model=RollupListResponse)
async def list_rollups(table_id: Optional[str] = None):
    """List all rollup definitions for a table"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        return rollup_service.list_rollups()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/rollups/{rollup_id}", response_model=RollupDef)
async def get_rollup(rollup_id: str, table_id: Optional[str] = None):
    """Get a specific rollup by ID"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        rollup = rollup_service.get_rollup(rollup_id)
        if not rollup:
            raise HTTPException(status_code=404, detail=f"Rollup '{rollup_id}' not found")

        return rollup
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rollups", response_model=List[RollupDef])
async def create_rollup(data: RollupCreate, table_id: Optional[str] = None):
    """Create rollup definitions for ALL dimension combinations.

    When given dimensions [A, B, C], creates 2^n rollups (8 total):
    - Baseline (no dimensions - pure totals for metric comparison)
    - Single dimensions: A, B, C
    - Pairs: A+B, A+C, B+C
    - Full: A+B+C

    Only dimensions need to be specified - all metrics are auto-included from schema.

    Returns:
        List of all created RollupDef objects
    """
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        # Validate dimensions exist in schema
        valid_dims = {d.id for d in bq_service.schema_config.dimensions}
        for dim in data.dimensions:
            if dim not in valid_dims:
                raise HTTPException(status_code=400, detail=f"Unknown dimension: {dim}")

        # Note: Metrics are auto-included from schema - no validation needed

        rollups = rollup_service.create_rollup(data, bq_service.schema_config)
        return rollups
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/rollups/{rollup_id}", response_model=RollupDef)
async def update_rollup(rollup_id: str, data: RollupUpdate, table_id: Optional[str] = None):
    """Update a rollup definition"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        rollup = rollup_service.update_rollup(rollup_id, data, bq_service.schema_config)
        return rollup
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/rollups/{rollup_id}")
async def delete_rollup(
    rollup_id: str,
    drop_table: bool = False,
    table_id: Optional[str] = None
):
    """Delete a rollup definition (optionally drop BigQuery table)"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        rollup_service.delete_rollup(
            rollup_id,
            drop_table=drop_table,
            source_project_id=bq_service.project_id,
            source_dataset=bq_service.dataset
        )

        return {"success": True, "message": f"Rollup '{rollup_id}' deleted"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rollups/{rollup_id}/refresh", response_model=RollupRefreshResponse)
async def refresh_rollup(
    rollup_id: str,
    incremental: bool = Query(False, description="If true, only add missing dates and metrics"),
    force: bool = Query(False, description="Force refresh even if status is ready"),
    use_batched: bool = Query(True, description="Use batched inserts for partition pruning (default: true)"),
    batch_size: int = Query(7, description="Number of dates per batch (default: 7 for balanced partition pruning)"),
    table_id: Optional[str] = None
):
    """Refresh a rollup table in BigQuery.

    - incremental=false (default): Full rebuild
    - incremental=true: Only INSERT missing dates and ADD missing metric columns
    - use_batched=true (default): Use batched inserts to leverage partition pruning on source table.
      This is more efficient when source table is partitioned by date.
    - use_batched=false: Use single CREATE TABLE AS SELECT (full table scan)
    """
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        result = rollup_service.refresh_rollup(
            rollup_id=rollup_id,
            source_table_path=bq_service.table_path,
            schema_config=bq_service.schema_config,
            source_project_id=bq_service.project_id,
            source_dataset=bq_service.dataset,
            incremental=incremental,
            force=force,
            use_batched=use_batched,
            batch_size=batch_size
        )

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/rollups/{rollup_id}/preview-sql", response_model=RollupPreviewSqlResponse)
async def preview_rollup_sql(rollup_id: str, table_id: Optional[str] = None):
    """Preview the SQL that would be generated for a rollup"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        result = rollup_service.preview_sql(
            rollup_id=rollup_id,
            source_table_path=bq_service.table_path,
            schema_config=bq_service.schema_config,
            source_project_id=bq_service.project_id,
            source_dataset=bq_service.dataset
        )

        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/rollups/{rollup_id}/status", response_model=RollupStatusResponse)
async def get_rollup_status(rollup_id: str, table_id: Optional[str] = None):
    """Get detailed status of a rollup including what's missing.

    Returns information about:
    - Whether the rollup table exists
    - Missing dates (dates in source but not in rollup)
    - Missing metrics (metrics in schema but not in rollup table)
    - Whether the rollup is up to date
    """
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        result = rollup_service.get_rollup_status(
            rollup_id=rollup_id,
            source_table_path=bq_service.table_path,
            schema_config=bq_service.schema_config,
            source_project_id=bq_service.project_id,
            source_dataset=bq_service.dataset
        )

        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rollups/refresh-all")
async def refresh_all_rollups(
    only_pending_or_stale: bool = True,
    table_id: Optional[str] = None
):
    """Refresh all rollups (or only pending/stale ones)"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        results = rollup_service.refresh_all(
            source_table_path=bq_service.table_path,
            schema_config=bq_service.schema_config,
            source_project_id=bq_service.project_id,
            source_dataset=bq_service.dataset,
            only_pending_or_stale=only_pending_or_stale
        )

        return {
            "success": True,
            "total": len(results),
            "successful": sum(1 for r in results if r.success),
            "failed": sum(1 for r in results if not r.success),
            "results": [r.model_dump() for r in results]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/rollups/config/default-project")
async def set_default_rollup_project(
    project: Optional[str] = None,
    table_id: Optional[str] = None
):
    """Set the default target project for rollups"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        config = rollup_service.set_default_project(project)
        return {
            "success": True,
            "default_target_project": config.default_target_project
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/rollups/config/default-dataset")
async def set_default_rollup_dataset(
    dataset: Optional[str] = None,
    table_id: Optional[str] = None
):
    """Set the default target dataset for rollups"""
    try:
        rollup_service, bq_service = get_rollup_service(table_id)
        if not rollup_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        config = rollup_service.set_default_dataset(dataset)
        return {
            "success": True,
            "default_target_dataset": config.default_target_dataset
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# OPTIMIZED SOURCE ENDPOINTS (Precomputed Composite Keys)
# ============================================================================

def get_optimized_source_service(table_id: Optional[str] = None):
    """Get optimized source service for a table."""
    from services.optimized_source_service import OptimizedSourceService
    from services.bigquery_service import get_bigquery_service

    bq_service = get_bigquery_service(table_id)
    if not bq_service:
        return None, None

    service = OptimizedSourceService(bq_service.client, bq_service.table_id or table_id)
    return service, bq_service


@app.get("/api/optimized-source/status", response_model=OptimizedSourceStatusResponse)
async def get_optimized_source_status(table_id: Optional[str] = None):
    """Get status of optimized source table for a BigQuery table."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        source_table_path = f"{bq_service.project_id}.{bq_service.dataset}.{bq_service.table}"

        return service.get_status(source_table_path, bq_service.schema_config)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/optimized-source/analyze", response_model=OptimizedSourceAnalysis)
async def analyze_optimized_source(table_id: Optional[str] = None):
    """Analyze schema to show what composite keys would be created."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        source_table_path = f"{bq_service.project_id}.{bq_service.dataset}.{bq_service.table}"

        return service.analyze(source_table_path, bq_service.schema_config)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/optimized-source/preview-sql", response_model=OptimizedSourcePreviewSql)
async def preview_optimized_source_sql(
    table_id: Optional[str] = None,
    auto_detect_clustering: bool = Query(True, description="Auto-detect clustering columns"),
    target_project: Optional[str] = Query(None, description="Target project for optimized table"),
    target_dataset: Optional[str] = Query(None, description="Target dataset for optimized table"),
):
    """Preview the SQL that would be generated for optimized source table."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        source_table_path = f"{bq_service.project_id}.{bq_service.dataset}.{bq_service.table}"

        # Get rollup defaults if target_project/target_dataset not provided
        rollup_service, _ = get_rollup_service(table_id)
        if rollup_service:
            rollup_config = rollup_service.load_config()
            if not target_project and rollup_config.default_target_project:
                target_project = rollup_config.default_target_project
            if not target_dataset and rollup_config.default_target_dataset:
                target_dataset = rollup_config.default_target_dataset

        data = OptimizedSourceCreate(
            auto_detect_clustering=auto_detect_clustering,
            target_project=target_project,
            target_dataset=target_dataset,
        )

        return service.preview_sql(
            source_table_path,
            bq_service.schema_config,
            data,
            bq_service.project_id,
            bq_service.dataset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/optimized-source/create", response_model=OptimizedSourceResponse)
async def create_optimized_source(
    data: OptimizedSourceCreate,
    table_id: Optional[str] = None
):
    """Create optimized source table with precomputed composite keys."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        source_table_path = f"{bq_service.project_id}.{bq_service.dataset}.{bq_service.table}"

        # Get rollup defaults if target_project/target_dataset not provided in data
        rollup_service, _ = get_rollup_service(table_id)
        if rollup_service:
            rollup_config = rollup_service.load_config()
            if not data.target_project and rollup_config.default_target_project:
                data.target_project = rollup_config.default_target_project
            if not data.target_dataset and rollup_config.default_target_dataset:
                data.target_dataset = rollup_config.default_target_dataset

        return service.create_optimized_source(
            source_table_path,
            bq_service.schema_config,
            data,
            bq_service.project_id,
            bq_service.dataset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/optimized-source/refresh", response_model=OptimizedSourceResponse)
async def refresh_optimized_source(
    incremental: bool = Query(True, description="If true, only add new dates"),
    table_id: Optional[str] = None
):
    """Refresh the optimized source table."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        if not bq_service.schema_config:
            raise HTTPException(status_code=400, detail="Schema not configured")

        source_table_path = f"{bq_service.project_id}.{bq_service.dataset}.{bq_service.table}"

        return service.refresh_optimized_source(
            source_table_path,
            bq_service.schema_config,
            incremental
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/optimized-source")
async def delete_optimized_source(
    drop_table: bool = Query(False, description="Also drop the BigQuery table"),
    table_id: Optional[str] = None
):
    """Delete optimized source configuration and optionally the BigQuery table."""
    try:
        service, bq_service = get_optimized_source_service(table_id)
        if not service or not bq_service:
            raise HTTPException(status_code=400, detail="BigQuery not configured")

        success, message = service.delete_optimized_source(drop_table)

        if not success:
            raise HTTPException(status_code=400, detail=message)

        return {"success": success, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DASHBOARD ENDPOINTS
# ============================================================================

@app.get("/api/dashboards", response_model=DashboardListResponse)
async def list_dashboards():
    """Get list of all dashboards"""
    try:
        dashboards = dashboard_registry.list_dashboards()
        return DashboardListResponse(dashboards=dashboards)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/dashboards", response_model=DashboardConfig)
async def create_dashboard(request: DashboardCreateRequest):
    """Create a new dashboard"""
    try:
        dashboard = dashboard_registry.create_dashboard(
            name=request.name,
            description=request.description
        )
        return dashboard
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboards/{dashboard_id}", response_model=DashboardConfig)
async def get_dashboard(dashboard_id: str):
    """Get a specific dashboard by ID"""
    try:
        dashboard = dashboard_registry.get_dashboard(dashboard_id)
        if not dashboard:
            raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
        return dashboard
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/dashboards/{dashboard_id}", response_model=DashboardConfig)
async def update_dashboard(dashboard_id: str, request: DashboardUpdateRequest):
    """Update a dashboard (name, description, or complete widget list)"""
    try:
        dashboard = dashboard_registry.update_dashboard(
            dashboard_id=dashboard_id,
            name=request.name,
            description=request.description,
            widgets=[w.dict() for w in request.widgets] if request.widgets else None
        )
        if not dashboard:
            raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
        return dashboard
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/dashboards/{dashboard_id}")
async def delete_dashboard(dashboard_id: str):
    """Delete a dashboard"""
    try:
        success = dashboard_registry.delete_dashboard(dashboard_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
        return {"success": True, "message": f"Dashboard {dashboard_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/dashboards/{dashboard_id}/widgets", response_model=DashboardConfig)
async def add_widget(dashboard_id: str, request: WidgetCreateRequest):
    """Add a widget to a dashboard"""
    try:
        widget_config = request.dict()
        dashboard = dashboard_registry.add_widget(dashboard_id, widget_config)
        if not dashboard:
            raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
        return dashboard
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/dashboards/{dashboard_id}/widgets/{widget_id}", response_model=DashboardConfig)
async def update_widget(dashboard_id: str, widget_id: str, request: WidgetUpdateRequest):
    """Update a widget in a dashboard"""
    try:
        widget_updates = {k: v for k, v in request.dict().items() if v is not None}
        dashboard = dashboard_registry.update_widget(dashboard_id, widget_id, widget_updates)
        if not dashboard:
            raise HTTPException(
                status_code=404,
                detail=f"Dashboard {dashboard_id} or widget {widget_id} not found"
            )
        return dashboard
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/dashboards/{dashboard_id}/widgets/{widget_id}", response_model=DashboardConfig)
async def delete_widget(dashboard_id: str, widget_id: str):
    """Delete a widget from a dashboard"""
    try:
        dashboard = dashboard_registry.delete_widget(dashboard_id, widget_id)
        if not dashboard:
            raise HTTPException(
                status_code=404,
                detail=f"Dashboard {dashboard_id} or widget {widget_id} not found"
            )
        return dashboard
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Significance Testing Endpoint

@app.post("/api/significance", response_model=SignificanceResponse)
async def calculate_significance(
    request: SignificanceRequest,
    table_id: Optional[str] = Query(None, description="Table ID (for multi-table support)")
):
    """
    Calculate statistical significance for rate metrics using two-proportion z-test.

    Only percent-format calculated metrics with simple {A}/{B} formulas are eligible.
    Uses event counts (e.g., queries, clicks) as the sample size, not days.

    Request body:
    - control_column: Reference column definition with dimension filters
    - treatment_columns: List of treatment columns to compare against control
    - metric_ids: List of metric IDs to analyze (only eligible percent metrics will be tested)
    - filters: Base filters (date range, etc.)
    - rows: Optional list of rows to test (for per-row significance)
    """
    try:
        # Initialize services
        stat_service = StatisticalService()
        bq_service = data_service.get_bigquery_service(table_id)
        if bq_service is None:
            raise ValueError("BigQuery not initialized. Please configure BigQuery connection.")

        # Get metric service to extract formula components
        from services.metric_service import MetricService
        from services.schema_service import SchemaService
        schema_service = SchemaService(bq_service.client, table_id=bq_service.table_id)
        metric_service = MetricService(schema_service)

        # Filter metrics to only eligible ones:
        # 1. Percent format calculated metrics with simple A/B formula
        # 2. _pct metrics (percentage of total for volume metrics)
        eligible_metrics = {}
        pct_metrics = {}  # Separate handling for _pct metrics

        for metric_id in request.metric_ids:
            # Check if it's a _pct metric (e.g., queries_pct)
            if metric_id.endswith('_pct'):
                base_metric_id = metric_id[:-4]  # Remove '_pct' suffix
                # Verify the base metric exists
                schema = schema_service.load_schema()
                base_exists = any(m.id == base_metric_id for m in schema.base_metrics)
                calc_exists = any(m.id == base_metric_id for m in schema.calculated_metrics)
                if base_exists or calc_exists:
                    pct_metrics[metric_id] = {
                        'base_metric_id': base_metric_id,
                        'is_pct_metric': True
                    }
            else:
                # Check if it's a calculated metric with simple A/B formula
                components = metric_service.extract_formula_components(metric_id)
                if components and components.get('is_simple_ratio'):
                    eligible_metrics[metric_id] = components

        if not eligible_metrics and not pct_metrics:
            # No eligible metrics - return empty results
            return SignificanceResponse(
                control_column_index=request.control_column.column_index,
                results={}
            )

        # Collect all base metrics needed (numerators and denominators)
        base_metrics_needed = set()
        for metric_id, components in eligible_metrics.items():
            base_metrics_needed.add(components['numerator_metric_id'])
            base_metrics_needed.add(components['denominator_metric_id'])
        for metric_id, pct_info in pct_metrics.items():
            base_metrics_needed.add(pct_info['base_metric_id'])

        # Merge dimension filters safely (handle None cases)
        base_dim_filters = request.filters.dimension_filters or {}
        control_dim_filters = request.control_column.dimension_filters or {}

        # === ROLLUP ROUTING ===
        # Collect ALL dimensions used in any filters (table dims + row dims)
        # A rollup must have ALL these dimensions to be usable
        filter_dimensions = set()
        # From base filters
        filter_dimensions.update(base_dim_filters.keys())
        # From control column filters (table dimensions)
        filter_dimensions.update(control_dim_filters.keys())
        # From treatment column filters
        for treatment in request.treatment_columns:
            if treatment.dimension_filters:
                filter_dimensions.update(treatment.dimension_filters.keys())
        # From row dimension filters
        if request.rows:
            for row in request.rows:
                if row.dimension_filters:
                    filter_dimensions.update(row.dimension_filters.keys())

        # Get route decision - need rollup with ALL filter dimensions
        # Create a dummy filter dict with just the keys (values don't matter for routing)
        routing_filters = FilterParams(
            dimension_filters={d: [] for d in filter_dimensions}
        ) if filter_dimensions else None

        route_decision = bq_service.get_route_decision(
            dimensions=[],  # No GROUP BY needed (we're aggregating totals)
            metrics=list(base_metrics_needed),
            filters=routing_filters,
            require_rollup=False
        )

        use_rollup = route_decision.use_rollup
        rollup_table_path = route_decision.rollup_table_path if use_rollup else None

        # Log routing decision
        print(f"Significance test routing: use_rollup={use_rollup}, reason={route_decision.reason}")

        # Helper function to fetch aggregated totals with combined filters
        def fetch_aggregated_totals(extra_filters: Dict[str, List[str]] = None) -> Dict[str, float]:
            combined_filters = {**base_dim_filters}
            if extra_filters:
                combined_filters.update(extra_filters)

            if use_rollup and rollup_table_path:
                # Use rollup table with re-aggregation (SUM metrics)
                return bq_service.query_rollup_aggregates(
                    rollup_table_path=rollup_table_path,
                    metric_ids=list(base_metrics_needed),
                    start_date=request.filters.start_date,
                    end_date=request.filters.end_date,
                    dimension_filters=combined_filters,
                    date_range_type=request.filters.date_range_type,
                    relative_date_preset=request.filters.relative_date_preset
                )
            else:
                # Fallback to raw table
                filters = FilterParams(
                    start_date=request.filters.start_date,
                    end_date=request.filters.end_date,
                    date_range_type=request.filters.date_range_type,
                    relative_date_preset=request.filters.relative_date_preset,
                    dimension_filters=combined_filters
                )
                return bq_service.query_kpi_metrics(filters=filters)

        # Helper function to run proportion test for a specific row
        def run_test_for_row(row_filters: Dict[str, List[str]] = None, row_id: str = None) -> Dict[str, List]:
            # Fetch control aggregated totals
            control_combined = {**control_dim_filters}
            if row_filters:
                control_combined.update(row_filters)
            control_totals = fetch_aggregated_totals(control_combined)

            # Build results for each treatment column
            all_metric_results = {}

            for treatment_col in request.treatment_columns:
                # Fetch treatment aggregated totals
                treatment_combined = {**(treatment_col.dimension_filters or {})}
                if row_filters:
                    treatment_combined.update(row_filters)
                treatment_totals = fetch_aggregated_totals(treatment_combined)

                # Run proportion test for each eligible metric
                for metric_id, components in eligible_metrics.items():
                    numerator_id = components['numerator_metric_id']
                    denominator_id = components['denominator_metric_id']

                    # Get counts
                    control_successes = int(control_totals.get(numerator_id, 0))
                    control_trials = int(control_totals.get(denominator_id, 0))
                    treatment_successes = int(treatment_totals.get(numerator_id, 0))
                    treatment_trials = int(treatment_totals.get(denominator_id, 0))

                    # Skip if no trials (avoid division by zero)
                    if control_trials == 0 and treatment_trials == 0:
                        continue

                    # Get direction preference
                    higher_is_better = stat_service.get_higher_is_better(metric_id)

                    # Run proportion significance test
                    result = stat_service.analyze_proportion_metric(
                        metric_id=metric_id,
                        control_successes=control_successes,
                        control_trials=control_trials,
                        treatment_successes=treatment_successes,
                        treatment_trials=treatment_trials,
                        column_index=treatment_col.column_index,
                        higher_is_better=higher_is_better
                    )

                    # Convert to SignificanceResultItem
                    result_item = SignificanceResultItem(
                        metric_id=result.metric_id,
                        column_index=result.column_index,
                        row_id=row_id,
                        prob_beat_control=result.prob_beat_control,
                        credible_interval_lower=result.credible_interval_lower,
                        credible_interval_upper=result.credible_interval_upper,
                        mean_difference=result.mean_difference,
                        relative_difference=result.relative_difference,
                        is_significant=result.is_significant,
                        direction=result.direction,
                        control_mean=result.control_mean,
                        treatment_mean=result.treatment_mean,
                        n_control_events=result.n_control_events,
                        n_treatment_events=result.n_treatment_events,
                        control_successes=result.control_successes,
                        treatment_successes=result.treatment_successes,
                        warning=result.warning
                    )

                    if metric_id not in all_metric_results:
                        all_metric_results[metric_id] = []
                    all_metric_results[metric_id].append(result_item)

                # Run proportion test for _pct metrics
                # For _pct metrics: numerator = row value, denominator = column total (without row filters)
                for metric_id, pct_info in pct_metrics.items():
                    base_metric_id = pct_info['base_metric_id']

                    # For _pct, we need column totals (without row filters) as denominator
                    # Numerator is the row-specific value (control_totals/treatment_totals already have row filters)
                    control_column_totals = fetch_aggregated_totals(control_dim_filters)  # No row filters
                    treatment_column_totals = fetch_aggregated_totals(treatment_col.dimension_filters or {})  # No row filters

                    # Get counts
                    # Successes = row value (with row filters applied)
                    # Trials = column total (without row filters)
                    control_successes = int(control_totals.get(base_metric_id, 0))
                    control_trials = int(control_column_totals.get(base_metric_id, 0))
                    treatment_successes = int(treatment_totals.get(base_metric_id, 0))
                    treatment_trials = int(treatment_column_totals.get(base_metric_id, 0))

                    # Skip if no trials (avoid division by zero)
                    if control_trials == 0 and treatment_trials == 0:
                        continue

                    # For _pct metrics, higher percentage is typically better (more market share)
                    higher_is_better = True

                    # Run proportion significance test
                    result = stat_service.analyze_proportion_metric(
                        metric_id=metric_id,
                        control_successes=control_successes,
                        control_trials=control_trials,
                        treatment_successes=treatment_successes,
                        treatment_trials=treatment_trials,
                        column_index=treatment_col.column_index,
                        higher_is_better=higher_is_better
                    )

                    # Convert to SignificanceResultItem
                    result_item = SignificanceResultItem(
                        metric_id=result.metric_id,
                        column_index=result.column_index,
                        row_id=row_id,
                        prob_beat_control=result.prob_beat_control,
                        credible_interval_lower=result.credible_interval_lower,
                        credible_interval_upper=result.credible_interval_upper,
                        mean_difference=result.mean_difference,
                        relative_difference=result.relative_difference,
                        is_significant=result.is_significant,
                        direction=result.direction,
                        control_mean=result.control_mean,
                        treatment_mean=result.treatment_mean,
                        n_control_events=result.n_control_events,
                        n_treatment_events=result.n_treatment_events,
                        control_successes=result.control_successes,
                        treatment_successes=result.treatment_successes,
                        warning=result.warning
                    )

                    if metric_id not in all_metric_results:
                        all_metric_results[metric_id] = []
                    all_metric_results[metric_id].append(result_item)

            return all_metric_results

        # Check if per-row testing is requested
        if request.rows:
            # Run per-row significance tests
            all_results = {}
            for row in request.rows:
                row_results = run_test_for_row(row.dimension_filters, row.row_id)
                # Merge results
                for metric_id, metric_results in row_results.items():
                    if metric_id not in all_results:
                        all_results[metric_id] = []
                    all_results[metric_id].extend(metric_results)
        else:
            # Run totals-only significance test
            all_results = run_test_for_row(None, None)

        return SignificanceResponse(
            control_column_index=request.control_column.column_index,
            results=all_results
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating significance: {str(e)}")
