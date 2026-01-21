from typing import Final

ENROLMENT_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "state",
    "district",
    "pincode",
    "age_0_5",
    "age_5_17",
    "age_18_greater",
)

BIOMETRIC_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "state",
    "district",
    "pincode",
    "bio_age_5_17",
    "bio_age_17_",
)

DEMOGRAPHIC_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "state",
    "district",
    "pincode",
    "demo_age_5_17",
    "demo_age_17_",
)

SCHEMA_ENROLMENT: Final[str] = "enrolment"
SCHEMA_BIOMETRIC: Final[str] = "biometric"
SCHEMA_DEMOGRAPHIC: Final[str] = "demographic"

SCHEMA_COLUMN_MAP: Final[dict[str, tuple[str, ...]]] = {
    SCHEMA_ENROLMENT: ENROLMENT_COLUMNS,
    SCHEMA_BIOMETRIC: BIOMETRIC_COLUMNS,
    SCHEMA_DEMOGRAPHIC: DEMOGRAPHIC_COLUMNS,
}

NUMERICAL_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    SCHEMA_ENROLMENT: ("age_0_5", "age_5_17", "age_18_greater"),
    SCHEMA_BIOMETRIC: ("bio_age_5_17", "bio_age_17_"),
    SCHEMA_DEMOGRAPHIC: ("demo_age_5_17", "demo_age_17_"),
}

OVS_ROLLING_WINDOW_DAYS: Final[int] = 30
OVS_CAMP_THRESHOLD: Final[float] = 4.0
OVS_CENTER_THRESHOLD: Final[float] = 0.5

OVS_LABEL_CAMP: Final[str] = "Temporary Camp"
OVS_LABEL_CENTER: Final[str] = "Permanent Center"
OVS_LABEL_NORMAL: Final[str] = "Normal Activity"

MII_HOTSPOT_THRESHOLD: Final[float] = 0.40
MII_NORMAL_THRESHOLD: Final[float] = 0.05

MII_LABEL_HOTSPOT: Final[str] = "Migration Hotspot"
MII_LABEL_NORMAL: Final[str] = "Birth-Rate Driven"
MII_LABEL_MIXED: Final[str] = "Mixed Population"

DHR_FRAUD_THRESHOLD: Final[float] = 1.5
DHR_NORMAL_VALUE: Final[float] = 0.5

DHR_LABEL_HIGH_RISK: Final[str] = "High Fraud Risk"
DHR_LABEL_NORMAL: Final[str] = "Normal Maintenance"
DHR_LABEL_OVER_VERIFIED: Final[str] = "Over-Verified"

DAY_NAMES: Final[dict[int, str]] = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}

WEEKEND_DAYS: Final[tuple[int, ...]] = (5, 6)
WEEKDAY_DAYS: Final[tuple[int, ...]] = (0, 1, 2, 3, 4)
SCHOOL_DRIVE_DAYS: Final[tuple[int, ...]] = (1, 2)

TLP_WEEKEND_WARRIOR_THRESHOLD: Final[float] = 0.60
TLP_SCHOOL_DRIVE_THRESHOLD: Final[float] = 0.60

TLP_LABEL_WEEKEND_WARRIOR: Final[str] = "Weekend Warrior Zone"
TLP_LABEL_SCHOOL_DRIVE: Final[str] = "School Drive Zone"
TLP_LABEL_BALANCED: Final[str] = "Balanced Load"

SML_N_CLUSTERS: Final[int] = 3
SML_RANDOM_STATE: Final[int] = 42

SML_CLUSTER_LABELS: Final[dict[int, str]] = {
    0: "Emerging",
    1: "Saturated",
    2: "High Churn",
}

SML_CLUSTER_DESCRIPTIONS: Final[dict[int, str]] = {
    0: "High new enrolment activity, expanding coverage",
    1: "Mature region with primarily update activity",
    2: "Significant demographic changes, requires monitoring",
}

MIN_VOLUME_FOR_CAMP_FLAG: Final[int] = 500
MIN_ENROLMENT_FOR_MIGRATION_FLAG: Final[int] = 100
MIN_TRANSACTIONS_FOR_FRAUD_FLAG: Final[int] = 1000
MIN_DAYS_FOR_OVS_CALCULATION: Final[int] = 7

DELTA_TABLE_ENROLMENT: Final[str] = "enrolment"
DELTA_TABLE_BIOMETRIC: Final[str] = "biometric"
DELTA_TABLE_DEMOGRAPHIC: Final[str] = "demographic"
DELTA_TABLE_INSIGHTS: Final[str] = "insights"
DELTA_TABLE_CLUSTERS: Final[str] = "clusters"
