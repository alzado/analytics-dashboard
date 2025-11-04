from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import datetime, date
import sys
import os

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
from config import config, CUSTOM_DIMENSIONS_FILE, QUERY_LOGS_DB_PATH
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
    ClearLogsResponse
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

    # Initialize BigQuery if configured
    try:
        if config.is_configured():
            print("Initializing BigQuery connection...")
            print(f"Project: {config.BIGQUERY_PROJECT_ID}")
            print(f"Dataset: {config.BIGQUERY_DATASET}")
            print(f"Table: {config.BIGQUERY_TABLE}")

            # Check if we have credentials JSON from saved config
            if config.BIGQUERY_CREDENTIALS_JSON:
                bq_service = initialize_bigquery_with_json(
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dataset=config.BIGQUERY_DATASET,
                    table=config.BIGQUERY_TABLE,
                    credentials_json=config.BIGQUERY_CREDENTIALS_JSON
                )
            else:
                bq_service = initialize_bigquery_service(
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dataset=config.BIGQUERY_DATASET,
                    table=config.BIGQUERY_TABLE,
                    credentials_path=None
                )

            # Set date limits from config
            if bq_service:
                bq_service.set_date_limits(
                    min_date=config.ALLOWED_MIN_DATE,
                    max_date=config.ALLOWED_MAX_DATE
                )
                if config.ALLOWED_MIN_DATE or config.ALLOWED_MAX_DATE:
                    print(f"Date limits configured: {config.ALLOWED_MIN_DATE} to {config.ALLOWED_MAX_DATE}")

            print("BigQuery connection initialized successfully")
        else:
            print("BigQuery not configured. Please configure via the UI at /info tab.")
    except Exception as e:
        print(f"Failed to initialize BigQuery: {e}")
        print("BigQuery not configured. Please configure via the UI at /info tab.")

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
    """Configure BigQuery connection from UI"""
    try:
        # Save configuration
        config.save_to_file(
            project_id=bq_config.project_id,
            dataset=bq_config.dataset,
            table=bq_config.table,
            credentials_json=bq_config.credentials_json if not bq_config.use_adc else "",
            allowed_min_date=bq_config.allowed_min_date,
            allowed_max_date=bq_config.allowed_max_date
        )

        # Initialize BigQuery with new configuration
        if bq_config.use_adc:
            # Use Application Default Credentials (user's gcloud auth)
            bq_service = initialize_bigquery_service(
                project_id=bq_config.project_id,
                dataset=bq_config.dataset,
                table=bq_config.table,
                credentials_path=None  # Will use ADC
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
                credentials_json=bq_config.credentials_json
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
async def disconnect_bigquery():
    """Disconnect BigQuery and clear configuration"""
    try:
        # Clear the configuration
        config.clear_configuration()

        # Clear the BigQuery service
        clear_bigquery_service()

        return BigQueryConfigResponse(
            success=True,
            message="BigQuery disconnected successfully",
            connection_status="not configured"
        )
    except Exception as e:
        return BigQueryConfigResponse(
            success=False,
            message=f"Failed to disconnect BigQuery: {str(e)}",
            connection_status="error"
        )

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
    dimensions: List[str] = Query([], description="Dimensions to pivot by (n_words_normalized, n_attributes, channel, country)"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    channel: Optional[str] = None,
    gcategory: Optional[str] = None,
    query_intent_classification: Optional[str] = None,
    n_words_normalized: Optional[int] = None,
    n_attributes: Optional[int] = None,
    n_attributes_min: Optional[int] = None,
    n_attributes_max: Optional[int] = None,
    attr_categoria: Optional[bool] = None,
    attr_tipo: Optional[bool] = None,
    attr_genero: Optional[bool] = None,
    attr_marca: Optional[bool] = None,
    attr_color: Optional[bool] = None,
    attr_material: Optional[bool] = None,
    attr_talla: Optional[bool] = None,
    attr_modelo: Optional[bool] = None,
    limit: int = 50,
):
    """Get pivot table data grouped by specified dimension"""
    try:
        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            country=country,
            channel=channel,
            gcategory=gcategory,
            query_intent_classification=query_intent_classification,
            n_words_normalized=n_words_normalized,
            n_attributes=n_attributes,
            n_attributes_min=n_attributes_min,
            n_attributes_max=n_attributes_max,
            attr_categoria=attr_categoria,
            attr_tipo=attr_tipo,
            attr_genero=attr_genero,
            attr_marca=attr_marca,
            attr_color=attr_color,
            attr_material=attr_material,
            attr_talla=attr_talla,
            attr_modelo=attr_modelo,
        )
        return data_service.get_pivot_data(dimensions, filters, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/children", response_model=List[PivotChildRow])
async def get_all_pivot_children(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    channel: Optional[str] = None,
    gcategory: Optional[str] = None,
    query_intent_classification: Optional[str] = None,
    n_attributes_min: Optional[int] = None,
    n_attributes_max: Optional[int] = None,
    attr_categoria: Optional[bool] = None,
    attr_tipo: Optional[bool] = None,
    attr_genero: Optional[bool] = None,
    attr_marca: Optional[bool] = None,
    attr_color: Optional[bool] = None,
    attr_material: Optional[bool] = None,
    attr_talla: Optional[bool] = None,
    attr_modelo: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Get all search terms without dimension filtering"""
    try:
        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            country=country,
            channel=channel,
            gcategory=gcategory,
            query_intent_classification=query_intent_classification,
            n_attributes_min=n_attributes_min,
            n_attributes_max=n_attributes_max,
            attr_categoria=attr_categoria,
            attr_tipo=attr_tipo,
            attr_genero=attr_genero,
            attr_marca=attr_marca,
            attr_color=attr_color,
            attr_material=attr_material,
            attr_talla=attr_talla,
            attr_modelo=attr_modelo,
        )
        return data_service.get_pivot_children('', '', filters, limit, offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/{dimension}/{value}/children", response_model=List[PivotChildRow])
async def get_pivot_children(
    dimension: str,
    value: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    channel: Optional[str] = None,
    gcategory: Optional[str] = None,
    query_intent_classification: Optional[str] = None,
    n_words_normalized: Optional[int] = None,
    n_attributes: Optional[int] = None,
    n_attributes_min: Optional[int] = None,
    n_attributes_max: Optional[int] = None,
    attr_categoria: Optional[bool] = None,
    attr_tipo: Optional[bool] = None,
    attr_genero: Optional[bool] = None,
    attr_marca: Optional[bool] = None,
    attr_color: Optional[bool] = None,
    attr_material: Optional[bool] = None,
    attr_talla: Optional[bool] = None,
    attr_modelo: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Get child rows (search terms) for a specific dimension value"""
    try:
        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            country=country,
            channel=channel,
            gcategory=gcategory,
            query_intent_classification=query_intent_classification,
            n_words_normalized=n_words_normalized,
            n_attributes=n_attributes,
            n_attributes_min=n_attributes_min,
            n_attributes_max=n_attributes_max,
            attr_categoria=attr_categoria,
            attr_tipo=attr_tipo,
            attr_genero=attr_genero,
            attr_marca=attr_marca,
            attr_color=attr_color,
            attr_material=attr_material,
            attr_talla=attr_talla,
            attr_modelo=attr_modelo,
        )
        return data_service.get_pivot_children(dimension, value, filters, limit, offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pivot/dimension/{dimension}/values", response_model=List[str])
async def get_dimension_values(
    dimension: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    channel: Optional[str] = None,
    gcategory: Optional[str] = None,
    query_intent_classification: Optional[str] = None,
    n_attributes_min: Optional[int] = None,
    n_attributes_max: Optional[int] = None,
):
    """Get distinct values for a given dimension"""
    try:
        filters = FilterParams(
            start_date=start_date,
            end_date=end_date,
            country=country,
            channel=channel,
            gcategory=gcategory,
            query_intent_classification=query_intent_classification,
            n_attributes_min=n_attributes_min,
            n_attributes_max=n_attributes_max,
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
