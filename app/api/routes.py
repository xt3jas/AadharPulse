from datetime import datetime
from typing import Optional

import polars as pl
from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from ..api.schemas import (
    DistrictInsight,
    HealthResponse,
    IngestResponse,
    MetricsResponse,
    NationalSummary,
    PincodeInsight,
    SchemaType,
)
from ..core.config import get_settings
from ..services.ingestion import get_ingestion_service
from ..utils.delta_ops import get_delta_ops

router = APIRouter()

@router.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest CSV file",
    description="Upload a CSV file for processing"
)
async def ingest_file(
    file: UploadFile = File(..., description="CSV file to ingest"),
    force_schema: Optional[SchemaType] = Query(
        None,
        description="Force a specific schema type (skip auto-detection)"
    )
) -> IngestResponse:
    
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No filename provided"
        )
    
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are supported"
        )
    
    service = get_ingestion_service()
    result = await service.ingest_file(file, force_schema)
    
    if result.success:
        return IngestResponse(
            success=True,
            schema_detected=result.schema_type,
            total_rows=result.total_rows,
            valid_rows=result.valid_rows,
            rejected_rows=result.rejected_rows,
            validation_errors=result.validation_errors,
            message=result.message
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "success": False,
                "schema_detected": result.schema_type.value,
                "total_rows": result.total_rows,
                "valid_rows": result.valid_rows,
                "rejected_rows": result.rejected_rows,
                "validation_errors": [e.model_dump() for e in result.validation_errors],
                "message": result.message
            }
        )

@router.post(
    "/transform/silver",
    summary="Transform Bronze to Silver layer",
    description="Trigger transformation of Bronze data to Silver layer (deduplication, cleaning)"
)
async def transform_to_silver(
    schema_type: SchemaType = Query(..., description="Schema type to transform")
) -> dict:
    
    service = get_ingestion_service()
    rows = service.transform_to_silver(schema_type)
    
    return {
        "success": True,
        "schema_type": schema_type.value,
        "rows_in_silver": rows,
        "message": f"Transformed {rows} rows to Silver layer"
    }

@router.get(
    "/metrics/summary",
    response_model=MetricsResponse,
    summary="Get national summary metrics",
    description="Returns aggregated KPIs for the entire dataset"
)
async def get_national_summary() -> MetricsResponse:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    try:
        summary = generator.generate_national_summary()
        return MetricsResponse(
            success=True,
            data=summary,
            generated_at=datetime.now().isoformat()
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate summary: {e}"
        )

@router.get(
    "/metrics/district/{district}",
    response_model=MetricsResponse,
    summary="Get district insights",
    description="Returns detailed insights for a specific district"
)
async def get_district_metrics(
    district: str,
    state: Optional[str] = Query(None, description="State name for disambiguation")
) -> MetricsResponse:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    try:
        insight = generator.generate_district_insights(
            district=district.upper(),
            state=state.upper() if state else None
        )
        
        if insight is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"District '{district}' not found"
            )
        
        return MetricsResponse(
            success=True,
            data=insight,
            generated_at=datetime.now().isoformat()
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate district metrics: {e}"
        )

@router.get(
    "/metrics/pincode/{pincode}",
    response_model=MetricsResponse,
    summary="Get pincode insights",
    description="Returns detailed insights for a specific pincode"
)
async def get_pincode_metrics(pincode: str) -> MetricsResponse:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    if not pincode.isdigit() or len(pincode) != 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid pincode format. Must be 6 digits."
        )
    
    try:
        insight = generator.generate_pincode_insights(pincode)
        
        if insight is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pincode '{pincode}' not found"
            )
        
        return MetricsResponse(
            success=True,
            data=insight,
            generated_at=datetime.now().isoformat()
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate pincode metrics: {e}"
        )

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Returns the health status of the API and data layers"
)
async def health_check() -> HealthResponse:
    
    settings = get_settings()
    delta_ops = get_delta_ops()
    
    bronze_tables = len(delta_ops.list_tables("bronze"))
    silver_tables = len(delta_ops.list_tables("silver"))
    gold_tables = len(delta_ops.list_tables("gold"))
    
    return HealthResponse(
        status="healthy",
        version=settings.APP_VERSION,
        bronze_tables=bronze_tables,
        silver_tables=silver_tables,
        gold_tables=gold_tables
    )

@router.get(
    "/stats",
    summary="Get ingestion statistics",
    description="Returns detailed statistics about ingested data"
)
async def get_ingestion_stats() -> dict:
    
    service = get_ingestion_service()
    return {
        "success": True,
        "stats": service.get_ingestion_stats(),
        "generated_at": datetime.now().isoformat()
    }

@router.get(
    "/v3/intel/synthesis",
    summary="Synthesis View - Executive Dashboard",
    description="Returns high-level synthesis metrics for the command center"
)
async def get_synthesis_intel() -> dict:
    
    from ..services.analytics import get_insight_generator
    from ..services.clustering import get_maturity_classifier
    
    generator = get_insight_generator()
    
    try:
        district_df = generator.delta_ops.read_delta_as_polars("gold", "district_insights")
        
        cluster_distribution = {"Emerging": 0, "Mature": 0, "High Churn": 0}
        if not district_df.is_empty() and "sml_label" in district_df.columns:
            for row in district_df.group_by("sml_label").len().iter_rows(named=True):
                label = row["sml_label"]
                if label in cluster_distribution:
                    cluster_distribution[label] = row["len"]
        
        ghost_df = generator.detect_service_shadows()
        ghost_districts = ghost_df.head(10).to_dicts() if not ghost_df.is_empty() else []
        
        util_df = generator.calculate_utilization_rate()
        high_utilization = util_df.to_dicts() if not util_df.is_empty() else []
        
        return {
            "success": True,
            "context": "synthesis",
            "data": {
                "cluster_distribution": cluster_distribution,
                "total_districts": sum(cluster_distribution.values()),
                "ghost_districts": ghost_districts,
                "ghost_district_count": len(ghost_districts),
                "high_utilization_pincodes": high_utilization
            },
            "generated_at": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate synthesis intel: {e}"
        )

@router.get(
    "/v3/pillars/growth",
    summary="Growth Pillar - Enrolment Intelligence",
    description="Returns insights related to enrolment growth and coverage"
)
async def get_growth_pillar() -> dict:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    try:
        age_ladder_df = generator.get_age_ladder_data()
        age_ladder = age_ladder_df.to_dicts() if not age_ladder_df.is_empty() else []
        
        migration_df = generator.get_migration_hotspots()
        migration_hotspots = migration_df.head(20).to_dicts() if not migration_df.is_empty() else []
        
        zero_growth_df = generator.get_zero_growth_districts()
        zero_growth = zero_growth_df.head(20).to_dicts() if not zero_growth_df.is_empty() else []
        
        return {
            "success": True,
            "context": "growth",
            "data": {
                "age_ladder": age_ladder,
                "anomaly_states": [s for s in age_ladder if s.get("is_anomaly", False)],
                "migration_hotspots": migration_hotspots,
                "hotspot_count": len(migration_hotspots),
                "zero_growth_districts": zero_growth,
                "saturated_count": len(zero_growth)
            },
            "generated_at": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate growth intel: {e}"
        )

@router.get(
    "/v3/pillars/compliance",
    summary="Compliance Pillar - Biometric Intelligence",
    description="Returns insights related to biometric updates and compliance"
)
async def get_compliance_pillar() -> dict:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    try:
        student_surge = generator.predict_student_surge()
        
        digital_maturity = generator.calculate_digital_maturity()
        
        pincode_df = generator.delta_ops.read_delta_as_polars("gold", "pincode_insights")
        camp_grid = []
        if not pincode_df.is_empty() and "ovs" in pincode_df.columns:
            camp_grid = (
                pincode_df
                .select(["pincode", "district", "ovs", "total_enrolment", "ovs_classification"])
                .sort("ovs", descending=True)
                .head(50)
                .to_dicts()
            )
        
        return {
            "success": True,
            "context": "compliance",
            "data": {
                "student_surge": student_surge,
                "digital_maturity": digital_maturity,
                "camp_vs_center": {
                    "data": camp_grid,
                    "camp_count": len([p for p in camp_grid if p.get("ovs_classification") == "Temporary Camp"]),
                    "center_count": len([p for p in camp_grid if p.get("ovs_classification") == "Permanent Center"])
                }
            },
            "generated_at": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate compliance intel: {e}"
        )

@router.get(
    "/v3/pillars/hygiene",
    summary="Hygiene Pillar - Demographic Intelligence",
    description="Returns insights related to demographic updates and data hygiene anomalies"
)
async def get_hygiene_pillar() -> dict:
    
    from ..services.analytics import get_insight_generator
    
    generator = get_insight_generator()
    
    try:
        district_df = generator.delta_ops.read_delta_as_polars("gold", "district_insights")
        red_list = []
        churn_map = []
        
        if not district_df.is_empty():
            if "avg_dhr" in district_df.columns:
                red_list = (
                    district_df
                    .filter(pl.col("avg_dhr") > 1.5)
                    .sort("avg_dhr", descending=True)
                    .head(20)
                    .to_dicts()
                )
            
            if "total_demographic" in district_df.columns:
                churn_map = (
                    district_df
                    .select(["state", "district", "total_demographic"])
                    .sort("total_demographic", descending=True)
                    .head(50)
                    .to_dicts()
                )
        
        spike_df = generator.detect_synchronized_spikes()
        spikes = spike_df.to_dicts() if not spike_df.is_empty() else []
        
        return {
            "success": True,
            "context": "hygiene",
            "data": {
                "red_list": red_list,
                "fraud_risk_count": len(red_list),
                "churn_map": churn_map,
                "synchronized_spikes": spikes,
                "spike_count": len(spikes)
            },
            "generated_at": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate hygiene intel: {e}"
        )
