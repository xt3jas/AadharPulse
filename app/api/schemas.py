from datetime import date as DateType
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator

class SchemaType(str, Enum):
    
    ENROLMENT = "enrolment"
    BIOMETRIC = "biometric"
    DEMOGRAPHIC = "demographic"

class OVSClassification(str, Enum):
    
    TEMPORARY_CAMP = "Temporary Camp"
    PERMANENT_CENTER = "Permanent Center"
    NORMAL_ACTIVITY = "Normal Activity"

class MIIClassification(str, Enum):
    
    MIGRATION_HOTSPOT = "Migration Hotspot"
    BIRTH_RATE_DRIVEN = "Birth-Rate Driven"
    MIXED_POPULATION = "Mixed Population"

class DHRClassification(str, Enum):
    
    HIGH_FRAUD_RISK = "High Fraud Risk"
    NORMAL_MAINTENANCE = "Normal Maintenance"
    OVER_VERIFIED = "Over-Verified"

class TLPClassification(str, Enum):
    
    WEEKEND_WARRIOR = "Weekend Warrior Zone"
    SCHOOL_DRIVE = "School Drive Zone"
    BALANCED_LOAD = "Balanced Load"

class SMLCluster(str, Enum):
    
    EMERGING = "Emerging"
    SATURATED = "Saturated"
    HIGH_CHURN = "High Churn"

class BaseRow(BaseModel):
    
    date: DateType = Field(..., description="Transaction date in ISO-8601 format")
    state: str = Field(..., min_length=1, description="State name")
    district: str = Field(..., min_length=1, description="District name")
    pincode: str = Field(
        ...,
        min_length=6,
        max_length=6,
        pattern=r"^\d{6}$",
        description="6-digit pincode (stored as string to preserve leading zeros)"
    )

    @field_validator("state", "district", mode="before")
    @classmethod
    def normalize_string_fields(cls, v: str) -> str:
        
        if isinstance(v, str):
            return v.strip().upper()
        return v

    @field_validator("pincode", mode="before")
    @classmethod
    def ensure_pincode_string(cls, v) -> str:
        
        if isinstance(v, (int, float)):
            return str(int(v)).zfill(6)
        return str(v).strip().zfill(6)

class EnrolmentRow(BaseRow):
    
    age_0_5: int = Field(..., ge=0, description="Enrolments for age 0-5 years")
    age_5_17: int = Field(..., ge=0, description="Enrolments for age 5-17 years")
    age_18_greater: int = Field(..., ge=0, description="Enrolments for age 18+ years")

    @property
    def total_enrolment(self) -> int:
        
        return self.age_0_5 + self.age_5_17 + self.age_18_greater

    @property
    def adult_ratio(self) -> float:
        
        total = self.total_enrolment
        if total == 0:
            return 0.0
        return self.age_18_greater / total

class BiometricRow(BaseRow):
    
    bio_age_5_17: int = Field(..., ge=0, description="Biometric updates for age 5-17")
    bio_age_17_: int = Field(..., ge=0, description="Biometric updates for age 17+")

    @property
    def total_biometric(self) -> int:
        
        return self.bio_age_5_17 + self.bio_age_17_

class DemographicRow(BaseRow):
    
    demo_age_5_17: int = Field(..., ge=0, description="Demographic updates for age 5-17")
    demo_age_17_: int = Field(..., ge=0, description="Demographic updates for age 17+")

    @property
    def total_demographic(self) -> int:
        
        return self.demo_age_5_17 + self.demo_age_17_

class IngestRequest(BaseModel):
    
    force_schema: Optional[SchemaType] = Field(
        None,
        description="Optionally force a specific schema type instead of auto-detection"
    )

class ValidationError(BaseModel):
    
    row_number: int = Field(..., description="1-indexed row number in CSV")
    column: str = Field(..., description="Column name with error")
    value: str = Field(..., description="Invalid value")
    error: str = Field(..., description="Error description")

class IngestResponse(BaseModel):
    
    success: bool = Field(..., description="Whether ingestion was successful")
    schema_detected: SchemaType = Field(..., description="Auto-detected schema type")
    total_rows: int = Field(..., description="Total rows in uploaded file")
    valid_rows: int = Field(..., description="Rows that passed validation")
    rejected_rows: int = Field(..., description="Rows rejected due to validation errors")
    validation_errors: list[ValidationError] = Field(
        default_factory=list,
        description="List of validation errors (limited to first 100)"
    )
    message: str = Field(..., description="Human-readable status message")

class HealthResponse(BaseModel):
    
    status: str = Field(default="healthy")
    version: str
    bronze_tables: int = Field(..., description="Number of Bronze layer tables")
    silver_tables: int = Field(..., description="Number of Silver layer tables")
    gold_tables: int = Field(..., description="Number of Gold layer tables")

class TemporalLoadProfile(BaseModel):
    
    monday: float = Field(..., ge=0, le=1, description="Monday volume percentage")
    tuesday: float = Field(..., ge=0, le=1, description="Tuesday volume percentage")
    wednesday: float = Field(..., ge=0, le=1, description="Wednesday volume percentage")
    thursday: float = Field(..., ge=0, le=1, description="Thursday volume percentage")
    friday: float = Field(..., ge=0, le=1, description="Friday volume percentage")
    saturday: float = Field(..., ge=0, le=1, description="Saturday volume percentage")
    sunday: float = Field(..., ge=0, le=1, description="Sunday volume percentage")
    classification: TLPClassification = Field(..., description="TLP classification")
    recommendation: str = Field(..., description="Staffing recommendation")

class PincodeInsight(BaseModel):
    
    pincode: str = Field(..., description="6-digit pincode")
    state: str = Field(..., description="State name")
    district: str = Field(..., description="District name")
    
    total_enrolment: int = Field(..., ge=0, description="Total enrolments in period")
    total_biometric: int = Field(..., ge=0, description="Total biometric updates")
    total_demographic: int = Field(..., ge=0, description="Total demographic updates")
    
    ovs: float = Field(..., ge=0, description="Operational Volatility Score")
    ovs_classification: OVSClassification
    
    mii: float = Field(..., ge=0, le=1, description="Migration Impact Index")
    mii_classification: MIIClassification
    
    dhr: float = Field(..., ge=0, description="Data Hygiene Ratio")
    dhr_classification: DHRClassification
    
    tlp: TemporalLoadProfile = Field(..., description="Temporal Load Profile")
    
    is_volatile_camp: bool = Field(..., description="Flagged as temporary camp")
    is_migration_hotspot: bool = Field(..., description="Flagged as migration hotspot")
    is_fraud_risk: bool = Field(..., description="Flagged as high fraud risk")

class DistrictInsight(BaseModel):
    
    state: str = Field(..., description="State name")
    district: str = Field(..., description="District name")
    pincode_count: int = Field(..., ge=0, description="Number of pincodes")
    
    total_enrolment: int = Field(..., ge=0)
    total_biometric: int = Field(..., ge=0)
    total_demographic: int = Field(..., ge=0)
    
    avg_ovs: float = Field(..., ge=0, description="Average OVS across pincodes")
    avg_mii: float = Field(..., ge=0, le=1, description="Average MII")
    avg_dhr: float = Field(..., ge=0, description="Average DHR")
    
    sml_cluster: SMLCluster = Field(..., description="Saturation Maturity Level")
    sml_description: str = Field(..., description="Cluster interpretation")
    
    normalized_enrolment_rate: float = Field(..., ge=0, le=1)
    normalized_update_rate: float = Field(..., ge=0, le=1)
    normalized_adult_ratio: float = Field(..., ge=0, le=1)
    
    volatile_camp_count: int = Field(..., ge=0)
    migration_hotspot_count: int = Field(..., ge=0)
    fraud_risk_count: int = Field(..., ge=0)

class NationalSummary(BaseModel):
    
    total_states: int = Field(..., ge=0)
    total_districts: int = Field(..., ge=0)
    total_pincodes: int = Field(..., ge=0)
    
    total_enrolment: int = Field(..., ge=0)
    total_biometric: int = Field(..., ge=0)
    total_demographic: int = Field(..., ge=0)
    
    avg_saturation_rate: float = Field(..., ge=0, le=1)
    emerging_districts: int = Field(..., ge=0)
    saturated_districts: int = Field(..., ge=0)
    high_churn_districts: int = Field(..., ge=0)
    
    volatile_camp_count: int = Field(..., ge=0)
    migration_hotspot_count: int = Field(..., ge=0)
    high_fraud_risk_count: int = Field(..., ge=0)
    
    top_volatile_pincodes: list[str] = Field(default_factory=list)
    top_migration_districts: list[str] = Field(default_factory=list)
    top_fraud_risk_districts: list[str] = Field(default_factory=list)

class MetricsResponse(BaseModel):
    
    success: bool = True
    data: NationalSummary | DistrictInsight | PincodeInsight
    generated_at: str = Field(..., description="ISO-8601 timestamp of generation")
