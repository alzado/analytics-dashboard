from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import datetime, date
import sys
import os

# Add services directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))

from services import data_service
from services.bigquery_service import (
    initialize_bigquery_service,
    initialize_bigquery_with_json,
    get_bigquery_info,
    clear_bigquery_service
)
from config import config
from models.schemas import (
    FilterParams,
    PivotResponse,
    PivotChildRow,
    BigQueryInfo,
    BigQueryConfig,
    BigQueryConfigResponse
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
    """Initialize BigQuery connection on application startup if configured"""
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

@app.get("/api/pivot", response_model=PivotResponse)
async def get_pivot_table(
    dimensions: List[str] = Query([], description="Dimensions to pivot by (n_words, n_attributes, channel, country)"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    channel: Optional[str] = None,
    gcategory: Optional[str] = None,
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
