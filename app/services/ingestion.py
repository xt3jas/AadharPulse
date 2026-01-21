import io
from dataclasses import dataclass
from datetime import date
from typing import Optional

import polars as pl
from fastapi import UploadFile

from ..api.schemas import (
    IngestResponse,
    SchemaType,
    ValidationError,
)
from ..core.config import get_settings
from ..core.constants import (
    DELTA_TABLE_BIOMETRIC,
    DELTA_TABLE_DEMOGRAPHIC,
    DELTA_TABLE_ENROLMENT,
    NUMERICAL_COLUMNS,
    SCHEMA_BIOMETRIC,
    SCHEMA_COLUMN_MAP,
    SCHEMA_DEMOGRAPHIC,
    SCHEMA_ENROLMENT,
)
from ..utils.date_parser import DateParseError, standardize_date
from ..utils.delta_ops import get_delta_ops

@dataclass
class IngestResult:
    
    success: bool
    schema_type: SchemaType
    total_rows: int
    valid_rows: int
    rejected_rows: int
    validation_errors: list[ValidationError]
    message: str

class SchemaDetectionError(Exception):
    
    pass

class IngestionService:
    
    
    def __init__(self):
        
        self.settings = get_settings()
        self.delta_ops = get_delta_ops()
        self._max_validation_errors = 100
    
    def detect_schema(self, headers: list[str]) -> SchemaType:
        
        normalized = [h.lower().strip() for h in headers]
        normalized_set = set(normalized)
        
        for schema_name, required_columns in SCHEMA_COLUMN_MAP.items():
            if set(required_columns).issubset(normalized_set):
                return SchemaType(schema_name)
        
        raise SchemaDetectionError(
            f"Cannot detect schema from headers: {headers}. "
            f"Expected one of: Enrolment {list(SCHEMA_COLUMN_MAP[SCHEMA_ENROLMENT])}, "
            f"Biometric {list(SCHEMA_COLUMN_MAP[SCHEMA_BIOMETRIC])}, "
            f"Demographic {list(SCHEMA_COLUMN_MAP[SCHEMA_DEMOGRAPHIC])}"
        )
    
    def validate_and_transform(
        self,
        df: pl.DataFrame,
        schema_type: SchemaType
    ) -> tuple[pl.DataFrame, list[ValidationError]]:
        
        errors: list[ValidationError] = []
        schema_key = schema_type.value
        
        df = df.rename({col: col.lower().strip() for col in df.columns})
        
        if "pincode" in df.columns:
            df = df.with_columns(
                pl.col("pincode")
                .cast(pl.Utf8)
                .str.zfill(6)
                .alias("pincode")
            )
        
        if "date" in df.columns:
            date_results = []
            for row_idx, date_val in enumerate(df["date"].to_list()):
                try:
                    standardized = standardize_date(date_val, raise_on_error=True)
                    date_results.append(standardized)
                except DateParseError as e:
                    date_results.append(None)
                    if len(errors) < self._max_validation_errors:
                        errors.append(ValidationError(
                            row_number=row_idx + 2,
                            column="date",
                            value=str(date_val),
                            error=str(e)
                        ))
            
            df = df.with_columns(
                pl.Series("date", date_results).cast(pl.Date)
            )
        
        numerical_cols = NUMERICAL_COLUMNS.get(schema_key, ())
        for col_name in numerical_cols:
            if col_name in df.columns:
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Int64)
                )
                
                negative_mask = pl.col(col_name) < 0
                negative_rows = df.filter(negative_mask)
                
                for row_idx, row in enumerate(negative_rows.iter_rows(named=True)):
                    if len(errors) < self._max_validation_errors:
                        errors.append(ValidationError(
                            row_number=row_idx + 2,
                            column=col_name,
                            value=str(row.get(col_name, "")),
                            error="Negative value not allowed"
                        ))
        
        for col in ["state", "district"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).str.to_uppercase().str.strip_chars().alias(col)
                )
        
        valid_mask = pl.col("date").is_not_null()
        for col_name in numerical_cols:
            if col_name in df.columns:
                valid_mask = valid_mask & (pl.col(col_name) >= 0)
        
        valid_df = df.filter(valid_mask)
        
        return valid_df, errors
    
    async def ingest_file(
        self,
        file: UploadFile,
        force_schema: Optional[SchemaType] = None
    ) -> IngestResult:
        
        try:
            contents = await file.read()
            
            df = pl.read_csv(io.BytesIO(contents), infer_schema_length=0)
            total_rows = len(df)
            
            if total_rows == 0:
                return IngestResult(
                    success=False,
                    schema_type=SchemaType.ENROLMENT,
                    total_rows=0,
                    valid_rows=0,
                    rejected_rows=0,
                    validation_errors=[],
                    message="Empty file uploaded"
                )
            
            if force_schema:
                schema_type = force_schema
            else:
                try:
                    schema_type = self.detect_schema(df.columns)
                except SchemaDetectionError as e:
                    return IngestResult(
                        success=False,
                        schema_type=SchemaType.ENROLMENT,
                        total_rows=total_rows,
                        valid_rows=0,
                        rejected_rows=total_rows,
                        validation_errors=[ValidationError(
                            row_number=1,
                            column="header",
                            value=",".join(df.columns),
                            error=str(e)
                        )],
                        message=f"Schema detection failed: {e}"
                    )
            
            valid_df, errors = self.validate_and_transform(df, schema_type)
            valid_rows = len(valid_df)
            rejected_rows = total_rows - valid_rows
            
            if valid_rows == 0:
                return IngestResult(
                    success=False,
                    schema_type=schema_type,
                    total_rows=total_rows,
                    valid_rows=0,
                    rejected_rows=rejected_rows,
                    validation_errors=errors[:self._max_validation_errors],
                    message="All rows failed validation"
                )
            
            table_name = self._get_table_name(schema_type)
            rows_written = self.delta_ops.write_to_delta(
                valid_df,
                layer="bronze",
                table_name=table_name,
                mode="append"
            )
            
            return IngestResult(
                success=True,
                schema_type=schema_type,
                total_rows=total_rows,
                valid_rows=valid_rows,
                rejected_rows=rejected_rows,
                validation_errors=errors[:self._max_validation_errors],
                message=f"Successfully ingested {valid_rows} rows to Bronze/{table_name}"
            )
            
        except Exception as e:
            return IngestResult(
                success=False,
                schema_type=SchemaType.ENROLMENT,
                total_rows=0,
                valid_rows=0,
                rejected_rows=0,
                validation_errors=[ValidationError(
                    row_number=0,
                    column="file",
                    value=file.filename or "unknown",
                    error=str(e)
                )],
                message=f"Ingestion failed: {e}"
            )
    
    def ingest_csv_bytes(
        self,
        content: bytes,
        filename: str = "upload.csv",
        force_schema: Optional[SchemaType] = None
    ) -> dict:
        
        try:
            df = pl.read_csv(io.BytesIO(content), infer_schema_length=0)
            total_rows = len(df)
            
            if total_rows == 0:
                return {
                    "success": False,
                    "total_rows": 0,
                    "valid_rows": 0,
                    "message": "Empty file uploaded"
                }
            
            if force_schema:
                schema_type = force_schema
            else:
                try:
                    schema_type = self.detect_schema(df.columns)
                except SchemaDetectionError as e:
                    return {
                        "success": False,
                        "total_rows": total_rows,
                        "valid_rows": 0,
                        "message": f"Schema detection failed: {e}"
                    }
            
            valid_df, errors = self.validate_and_transform(df, schema_type)
            valid_rows = len(valid_df)
            rejected_rows = total_rows - valid_rows
            
            if valid_rows == 0:
                return {
                    "success": False,
                    "schema_type": schema_type.value,
                    "total_rows": total_rows,
                    "valid_rows": 0,
                    "rejected_rows": rejected_rows,
                    "message": "All rows failed validation"
                }
            
            table_name = self._get_table_name(schema_type)
            self.delta_ops.write_to_delta(
                valid_df,
                layer="bronze",
                table_name=table_name,
                mode="append"
            )
            
            return {
                "success": True,
                "schema_type": schema_type.value,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "rejected_rows": rejected_rows,
                "table_name": table_name,
                "message": f"Ingested {valid_rows} rows to Bronze/{table_name}"
            }
            
        except Exception as e:
            return {
                "success": False,
                "total_rows": 0,
                "valid_rows": 0,
                "message": f"Ingestion failed: {e}"
            }
    
    def _get_table_name(self, schema_type: SchemaType) -> str:
        
        mapping = {
            SchemaType.ENROLMENT: DELTA_TABLE_ENROLMENT,
            SchemaType.BIOMETRIC: DELTA_TABLE_BIOMETRIC,
            SchemaType.DEMOGRAPHIC: DELTA_TABLE_DEMOGRAPHIC,
        }
        return mapping[schema_type]
    
    def transform_to_silver(self, schema_type: SchemaType) -> int:
        
        table_name = self._get_table_name(schema_type)
        
        bronze_df = self.delta_ops.read_delta_as_polars("bronze", table_name)
        
        if bronze_df.is_empty():
            return 0
        
        silver_df = bronze_df.unique(
            subset=["date", "pincode", "district"],
            keep="last"
        )
        
        silver_df = silver_df.sort(["date", "state", "district", "pincode"])
        
        self.delta_ops.write_to_delta(
            silver_df,
            layer="silver",
            table_name=table_name,
            mode="overwrite"
        )
        
        return len(silver_df)
    
    def get_ingestion_stats(self) -> dict:
        
        stats = {
            "bronze": {},
            "silver": {},
        }
        
        for table_name in [DELTA_TABLE_ENROLMENT, DELTA_TABLE_BIOMETRIC, DELTA_TABLE_DEMOGRAPHIC]:
            for layer in ["bronze", "silver"]:
                metadata = self.delta_ops.get_table_metadata(layer, table_name)
                stats[layer][table_name] = {
                    "exists": metadata["exists"],
                    "row_count": metadata["row_count"],
                    "last_modified": metadata.get("last_modified"),
                }
        
        return stats

_ingestion_service: Optional[IngestionService] = None

def get_ingestion_service() -> IngestionService:
    
    global _ingestion_service
    if _ingestion_service is None:
        _ingestion_service = IngestionService()
    return _ingestion_service
