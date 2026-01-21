from datetime import date
from typing import Optional

import polars as pl

from ..api.schemas import (
    DHRClassification,
    DistrictInsight,
    MIIClassification,
    NationalSummary,
    OVSClassification,
    PincodeInsight,
    SMLCluster,
    TemporalLoadProfile,
    TLPClassification,
)
from ..core.constants import (
    DAY_NAMES,
    DELTA_TABLE_BIOMETRIC,
    DELTA_TABLE_DEMOGRAPHIC,
    DELTA_TABLE_ENROLMENT,
    DHR_FRAUD_THRESHOLD,
    DHR_LABEL_HIGH_RISK,
    DHR_LABEL_NORMAL,
    DHR_LABEL_OVER_VERIFIED,
    MIN_DAYS_FOR_OVS_CALCULATION,
    MIN_ENROLMENT_FOR_MIGRATION_FLAG,
    MIN_TRANSACTIONS_FOR_FRAUD_FLAG,
    MIN_VOLUME_FOR_CAMP_FLAG,
    MII_HOTSPOT_THRESHOLD,
    MII_LABEL_HOTSPOT,
    MII_LABEL_MIXED,
    MII_LABEL_NORMAL,
    MII_NORMAL_THRESHOLD,
    OVS_CAMP_THRESHOLD,
    OVS_CENTER_THRESHOLD,
    OVS_LABEL_CAMP,
    OVS_LABEL_CENTER,
    OVS_LABEL_NORMAL,
    SCHOOL_DRIVE_DAYS,
    SML_CLUSTER_DESCRIPTIONS,
    SML_CLUSTER_LABELS,
    TLP_LABEL_BALANCED,
    TLP_LABEL_SCHOOL_DRIVE,
    TLP_LABEL_WEEKEND_WARRIOR,
    TLP_SCHOOL_DRIVE_THRESHOLD,
    TLP_WEEKEND_WARRIOR_THRESHOLD,
    WEEKEND_DAYS,
)
from ..utils.delta_ops import get_delta_ops
from .clustering import get_maturity_classifier

class InsightGenerator:
    
    
    def __init__(self):
        
        self.delta_ops = get_delta_ops()
    
    
    def calculate_ovs(self, daily_volumes: pl.Series) -> float:
        
        if daily_volumes.is_empty() or len(daily_volumes) < MIN_DAYS_FOR_OVS_CALCULATION:
            return 0.0
        
        volumes = daily_volumes.drop_nulls()
        
        if volumes.is_empty():
            return 0.0
        
        mean_val = volumes.mean()
        
        if mean_val is None or mean_val == 0:
            return 0.0
        
        std_val = volumes.std()
        
        if std_val is None:
            return 0.0
        
        ovs = float(std_val) / float(mean_val)
        
        return round(ovs, 4)
    
    def classify_ovs(self, ovs: float, total_volume: int = 0) -> OVSClassification:
        
        if total_volume < MIN_VOLUME_FOR_CAMP_FLAG:
            return OVSClassification.NORMAL_ACTIVITY
        
        if ovs > OVS_CAMP_THRESHOLD:
            return OVSClassification.TEMPORARY_CAMP
        elif ovs < OVS_CENTER_THRESHOLD:
            return OVSClassification.PERMANENT_CENTER
        else:
            return OVSClassification.NORMAL_ACTIVITY
    
    
    def calculate_mii(self, enrolment_18_plus: int, total_enrolment: int) -> float:
        
        if total_enrolment <= 0:
            return 0.0
        
        mii = enrolment_18_plus / total_enrolment
        return round(min(max(mii, 0.0), 1.0), 4)
    
    def classify_mii(self, mii: float, total_enrolment: int = 0) -> MIIClassification:
        
        if total_enrolment < MIN_ENROLMENT_FOR_MIGRATION_FLAG:
            return MIIClassification.MIXED_POPULATION
        
        if mii > MII_HOTSPOT_THRESHOLD:
            return MIIClassification.MIGRATION_HOTSPOT
        elif mii < MII_NORMAL_THRESHOLD:
            return MIIClassification.BIRTH_RATE_DRIVEN
        else:
            return MIIClassification.MIXED_POPULATION
    
    
    def calculate_dhr(self, demographic_updates: int, biometric_updates: int) -> float:
        
        if biometric_updates <= 0:
            return float(demographic_updates) if demographic_updates > 0 else 0.0
        
        dhr = demographic_updates / biometric_updates
        return round(dhr, 4)
    
    def classify_dhr(self, dhr: float, total_transactions: int = 0) -> DHRClassification:
        
        if total_transactions < MIN_TRANSACTIONS_FOR_FRAUD_FLAG:
            return DHRClassification.NORMAL_MAINTENANCE
        
        if dhr > DHR_FRAUD_THRESHOLD:
            return DHRClassification.HIGH_FRAUD_RISK
        elif dhr < 0.3:
            return DHRClassification.OVER_VERIFIED
        else:
            return DHRClassification.NORMAL_MAINTENANCE
    
    
    def calculate_tlp(self, transactions_df: pl.DataFrame) -> TemporalLoadProfile:
        
        if transactions_df.is_empty() or "date" not in transactions_df.columns:
            return self._empty_tlp()
        
        if "volume" not in transactions_df.columns:
            numeric_cols = [
                c for c in transactions_df.columns 
                if transactions_df[c].dtype in [pl.Int64, pl.Int32, pl.Float64]
            ]
            if not numeric_cols:
                return self._empty_tlp()
            transactions_df = transactions_df.with_columns(
                sum(pl.col(c) for c in numeric_cols).alias("volume")
            )
        
        df = transactions_df.with_columns(
            pl.col("date").cast(pl.Date).dt.weekday().alias("day_of_week")
        )
        
        daily_totals = (
            df.group_by("day_of_week")
            .agg(pl.col("volume").sum().alias("total"))
            .sort("day_of_week")
        )
        
        grand_total = daily_totals["total"].sum()
        
        if grand_total == 0:
            return self._empty_tlp()
        
        day_percentages = {i: 0.0 for i in range(7)}
        for row in daily_totals.iter_rows(named=True):
            day_idx = row["day_of_week"]
            if day_idx is not None and 0 <= day_idx <= 6:
                day_percentages[day_idx] = row["total"] / grand_total
        
        weekend_pct = sum(day_percentages.get(d, 0) for d in WEEKEND_DAYS)
        school_drive_pct = sum(day_percentages.get(d, 0) for d in SCHOOL_DRIVE_DAYS)
        
        if weekend_pct > TLP_WEEKEND_WARRIOR_THRESHOLD:
            classification = TLPClassification.WEEKEND_WARRIOR
            recommendation = "Deploy Mobile Van on Saturdays and Sundays"
        elif school_drive_pct > TLP_SCHOOL_DRIVE_THRESHOLD:
            classification = TLPClassification.SCHOOL_DRIVE
            recommendation = "Coordinate with schools for Tuesday/Wednesday drives"
        else:
            classification = TLPClassification.BALANCED_LOAD
            recommendation = "Standard staffing schedule recommended"
        
        return TemporalLoadProfile(
            monday=round(day_percentages[0], 4),
            tuesday=round(day_percentages[1], 4),
            wednesday=round(day_percentages[2], 4),
            thursday=round(day_percentages[3], 4),
            friday=round(day_percentages[4], 4),
            saturday=round(day_percentages[5], 4),
            sunday=round(day_percentages[6], 4),
            classification=classification,
            recommendation=recommendation
        )
    
    def _empty_tlp(self) -> TemporalLoadProfile:
        
        return TemporalLoadProfile(
            monday=0.0,
            tuesday=0.0,
            wednesday=0.0,
            thursday=0.0,
            friday=0.0,
            saturday=0.0,
            sunday=0.0,
            classification=TLPClassification.BALANCED_LOAD,
            recommendation="Insufficient data for scheduling recommendation"
        )
    
    
    def _load_silver_data(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        
        enrolment = self.delta_ops.read_delta_as_polars("silver", DELTA_TABLE_ENROLMENT)
        biometric = self.delta_ops.read_delta_as_polars("silver", DELTA_TABLE_BIOMETRIC)
        demographic = self.delta_ops.read_delta_as_polars("silver", DELTA_TABLE_DEMOGRAPHIC)
        return enrolment, biometric, demographic
    
    def _load_bronze_data(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        
        enrolment = self.delta_ops.read_delta_as_polars("bronze", DELTA_TABLE_ENROLMENT)
        biometric = self.delta_ops.read_delta_as_polars("bronze", DELTA_TABLE_BIOMETRIC)
        demographic = self.delta_ops.read_delta_as_polars("bronze", DELTA_TABLE_DEMOGRAPHIC)
        return enrolment, biometric, demographic
    
    def _get_data(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        
        enrolment, biometric, demographic = self._load_silver_data()
        
        if enrolment.is_empty():
            enrolment, biometric, demographic = self._load_bronze_data()
        
        return enrolment, biometric, demographic
    
    
    def generate_pincode_insights(self, pincode: str) -> Optional[PincodeInsight]:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        pincode_enrolment = enrolment_df.filter(pl.col("pincode") == pincode)
        pincode_biometric = biometric_df.filter(pl.col("pincode") == pincode)
        pincode_demographic = demographic_df.filter(pl.col("pincode") == pincode)
        
        if pincode_enrolment.is_empty():
            return None
        
        state = pincode_enrolment[0, "state"]
        district = pincode_enrolment[0, "district"]
        
        total_enrolment = 0
        enrolment_18_plus = 0
        
        if "age_0_5" in pincode_enrolment.columns:
            total_enrolment = int(
                pincode_enrolment.select([
                    pl.col("age_0_5").sum(),
                    pl.col("age_5_17").sum(),
                    pl.col("age_18_greater").sum()
                ]).sum_horizontal().item()
            )
            enrolment_18_plus = int(pincode_enrolment["age_18_greater"].sum())
        
        total_biometric = 0
        if not pincode_biometric.is_empty() and "bio_age_5_17" in pincode_biometric.columns:
            total_biometric = int(
                pincode_biometric.select([
                    pl.col("bio_age_5_17").sum(),
                    pl.col("bio_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        total_demographic = 0
        if not pincode_demographic.is_empty() and "demo_age_5_17" in pincode_demographic.columns:
            total_demographic = int(
                pincode_demographic.select([
                    pl.col("demo_age_5_17").sum(),
                    pl.col("demo_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        daily_volumes = (
            pincode_enrolment
            .with_columns(
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater"))
                .alias("daily_total")
            )
            .group_by("date")
            .agg(pl.col("daily_total").sum())
        )["daily_total"]
        
        ovs = self.calculate_ovs(daily_volumes)
        ovs_classification = self.classify_ovs(ovs, total_enrolment)
        
        mii = self.calculate_mii(enrolment_18_plus, total_enrolment)
        mii_classification = self.classify_mii(mii, total_enrolment)
        
        dhr = self.calculate_dhr(total_demographic, total_biometric)
        dhr_classification = self.classify_dhr(dhr, total_biometric + total_demographic)
        
        tlp_df = (
            pincode_enrolment
            .with_columns(
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater"))
                .alias("volume")
            )
            .select(["date", "volume"])
        )
        tlp = self.calculate_tlp(tlp_df)
        
        return PincodeInsight(
            pincode=pincode,
            state=state,
            district=district,
            total_enrolment=total_enrolment,
            total_biometric=total_biometric,
            total_demographic=total_demographic,
            ovs=ovs,
            ovs_classification=ovs_classification,
            mii=mii,
            mii_classification=mii_classification,
            dhr=dhr,
            dhr_classification=dhr_classification,
            tlp=tlp,
            is_volatile_camp=ovs_classification == OVSClassification.TEMPORARY_CAMP,
            is_migration_hotspot=mii_classification == MIIClassification.MIGRATION_HOTSPOT,
            is_fraud_risk=dhr_classification == DHRClassification.HIGH_FRAUD_RISK
        )
    
    
    def generate_district_insights(
        self,
        district: str,
        state: Optional[str] = None
    ) -> Optional[DistrictInsight]:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        district_filter = pl.col("district") == district.upper()
        if state:
            district_filter = district_filter & (pl.col("state") == state.upper())
        
        district_enrolment = enrolment_df.filter(district_filter)
        district_biometric = biometric_df.filter(district_filter)
        district_demographic = demographic_df.filter(district_filter)
        
        if district_enrolment.is_empty():
            return None
        
        state_name = district_enrolment[0, "state"]
        
        pincode_count = district_enrolment["pincode"].n_unique()
        
        total_enrolment = int(
            district_enrolment.select([
                pl.col("age_0_5").sum(),
                pl.col("age_5_17").sum(),
                pl.col("age_18_greater").sum()
            ]).sum_horizontal().item()
        )
        enrolment_18_plus = int(district_enrolment["age_18_greater"].sum())
        
        total_biometric = 0
        if not district_biometric.is_empty():
            total_biometric = int(
                district_biometric.select([
                    pl.col("bio_age_5_17").sum(),
                    pl.col("bio_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        total_demographic = 0
        if not district_demographic.is_empty():
            total_demographic = int(
                district_demographic.select([
                    pl.col("demo_age_5_17").sum(),
                    pl.col("demo_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        pincodes = district_enrolment["pincode"].unique().to_list()
        ovs_values = []
        volatile_count = 0
        migration_count = 0
        fraud_count = 0
        
        for pc in pincodes:
            insight = self.generate_pincode_insights(pc)
            if insight:
                ovs_values.append(insight.ovs)
                if insight.is_volatile_camp:
                    volatile_count += 1
                if insight.is_migration_hotspot:
                    migration_count += 1
                if insight.is_fraud_risk:
                    fraud_count += 1
        
        avg_ovs = sum(ovs_values) / len(ovs_values) if ovs_values else 0.0
        avg_mii = self.calculate_mii(enrolment_18_plus, total_enrolment)
        avg_dhr = self.calculate_dhr(total_demographic, total_biometric)
        
        enrolment_rate = total_enrolment / max(total_enrolment, 1)
        update_rate = (total_biometric + total_demographic) / max(total_enrolment, 1)
        adult_ratio = enrolment_18_plus / max(total_enrolment, 1)
        
        if enrolment_rate > 0.5:
            sml_cluster = SMLCluster.EMERGING
            sml_desc = SML_CLUSTER_DESCRIPTIONS[0]
        elif update_rate > 0.3:
            sml_cluster = SMLCluster.SATURATED
            sml_desc = SML_CLUSTER_DESCRIPTIONS[1]
        else:
            sml_cluster = SMLCluster.HIGH_CHURN
            sml_desc = SML_CLUSTER_DESCRIPTIONS[2]
        
        return DistrictInsight(
            state=state_name,
            district=district.upper(),
            pincode_count=pincode_count,
            total_enrolment=total_enrolment,
            total_biometric=total_biometric,
            total_demographic=total_demographic,
            avg_ovs=round(avg_ovs, 4),
            avg_mii=round(avg_mii, 4),
            avg_dhr=round(avg_dhr, 4),
            sml_cluster=sml_cluster,
            sml_description=sml_desc,
            normalized_enrolment_rate=round(min(enrolment_rate, 1.0), 4),
            normalized_update_rate=round(min(update_rate, 1.0), 4),
            normalized_adult_ratio=round(adult_ratio, 4),
            volatile_camp_count=volatile_count,
            migration_hotspot_count=migration_count,
            fraud_risk_count=fraud_count
        )
    
    
    def generate_national_summary(self) -> NationalSummary:
        
        try:
            pincode_insights = self.delta_ops.read_delta_as_polars("gold", "pincode_insights")
            district_insights = self.delta_ops.read_delta_as_polars("gold", "district_insights")
            
            if not pincode_insights.is_empty() and not district_insights.is_empty():
                return self._summary_from_gold(pincode_insights, district_insights)
        except Exception:
            pass
        
        return self._compute_national_summary_fast()
    
    def _summary_from_gold(
        self, 
        pincode_df: pl.DataFrame, 
        district_df: pl.DataFrame
    ) -> NationalSummary:
        
        total_states = pincode_df["state"].n_unique()
        total_districts = pincode_df["district"].n_unique()
        total_pincodes = len(pincode_df)
        
        total_enrolment = int(pincode_df["total_enrolment"].sum())
        total_biometric = int(pincode_df["total_biometric"].sum())
        total_demographic = int(pincode_df["total_demographic"].sum())
        
        emerging_count = 0
        saturated_count = 0
        high_churn_count = 0
        
        if "sml_label" in district_df.columns:
            cluster_counts = district_df.group_by("sml_label").len()
            for row in cluster_counts.iter_rows(named=True):
                label = row["sml_label"]
                count = row["len"]
                if label == SML_CLUSTER_LABELS[0]:
                    emerging_count = count
                elif label == SML_CLUSTER_LABELS[1]:
                    saturated_count = count
                elif label == SML_CLUSTER_LABELS[2]:
                    high_churn_count = count
        
        volatile_camp_count = int(pincode_df["is_volatile_camp"].sum()) if "is_volatile_camp" in pincode_df.columns else 0
        migration_hotspot_count = int(pincode_df["is_migration_hotspot"].sum()) if "is_migration_hotspot" in pincode_df.columns else 0
        high_fraud_risk_count = int(pincode_df["is_fraud_risk"].sum()) if "is_fraud_risk" in pincode_df.columns else 0
        
        top_volatile = (
            pincode_df
            .filter(pl.col("is_volatile_camp") == True)
            .sort("ovs", descending=True)
            .head(10)["pincode"].to_list()
        ) if "is_volatile_camp" in pincode_df.columns else []
        
        top_migration = (
            district_df
            .filter(pl.col("migration_hotspot_count") > 0)
            .sort("migration_hotspot_count", descending=True)
            .head(10)["district"].to_list()
        ) if "migration_hotspot_count" in district_df.columns else []
        
        top_fraud = (
            district_df
            .filter(pl.col("fraud_risk_count") > 0)
            .sort("fraud_risk_count", descending=True)
            .head(10)["district"].to_list()
        ) if "fraud_risk_count" in district_df.columns else []
        
        total_volume = total_enrolment + total_biometric + total_demographic
        update_volume = total_biometric + total_demographic
        avg_saturation = update_volume / total_volume if total_volume > 0 else 0.0
        
        return NationalSummary(
            total_states=total_states,
            total_districts=total_districts,
            total_pincodes=total_pincodes,
            total_enrolment=total_enrolment,
            total_biometric=total_biometric,
            total_demographic=total_demographic,
            avg_saturation_rate=round(avg_saturation, 4),
            emerging_districts=emerging_count,
            saturated_districts=saturated_count,
            high_churn_districts=high_churn_count,
            volatile_camp_count=volatile_camp_count,
            migration_hotspot_count=migration_hotspot_count,
            high_fraud_risk_count=high_fraud_risk_count,
            top_volatile_pincodes=top_volatile,
            top_migration_districts=top_migration,
            top_fraud_risk_districts=top_fraud
        )
    
    def _compute_national_summary_fast(self) -> NationalSummary:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        if enrolment_df.is_empty():
            return NationalSummary(
                total_states=0,
                total_districts=0,
                total_pincodes=0,
                total_enrolment=0,
                total_biometric=0,
                total_demographic=0,
                avg_saturation_rate=0.0,
                emerging_districts=0,
                saturated_districts=0,
                high_churn_districts=0,
                volatile_camp_count=0,
                migration_hotspot_count=0,
                high_fraud_risk_count=0,
                top_volatile_pincodes=[],
                top_migration_districts=[],
                top_fraud_risk_districts=[]
            )
        
        total_states = enrolment_df["state"].n_unique()
        total_districts = enrolment_df["district"].n_unique()
        total_pincodes = enrolment_df["pincode"].n_unique()
        
        total_enrolment = int(
            enrolment_df.select([
                pl.col("age_0_5").sum(),
                pl.col("age_5_17").sum(),
                pl.col("age_18_greater").sum()
            ]).sum_horizontal().item()
        )
        
        total_biometric = 0
        if not biometric_df.is_empty():
            total_biometric = int(
                biometric_df.select([
                    pl.col("bio_age_5_17").sum(),
                    pl.col("bio_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        total_demographic = 0
        if not demographic_df.is_empty():
            total_demographic = int(
                demographic_df.select([
                    pl.col("demo_age_5_17").sum(),
                    pl.col("demo_age_17_").sum()
                ]).sum_horizontal().item()
            )
        
        total_volume = total_enrolment + total_biometric + total_demographic
        update_volume = total_biometric + total_demographic
        avg_saturation = update_volume / total_volume if total_volume > 0 else 0.0
        
        return NationalSummary(
            total_states=total_states,
            total_districts=total_districts,
            total_pincodes=total_pincodes,
            total_enrolment=total_enrolment,
            total_biometric=total_biometric,
            total_demographic=total_demographic,
            avg_saturation_rate=round(avg_saturation, 4),
            emerging_districts=0,
            saturated_districts=0,
            high_churn_districts=0,
            volatile_camp_count=0,
            migration_hotspot_count=0,
            high_fraud_risk_count=0,
            top_volatile_pincodes=[],
            top_migration_districts=[],
            top_fraud_risk_districts=[]
        )
    
    
    def aggregate_to_gold(self) -> dict:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        if enrolment_df.is_empty():
            return {"pincode_insights": 0, "district_insights": 0}
        
        
        pincode_enrolment = (
            enrolment_df
            .group_by(["pincode", "state", "district"])
            .agg([
                pl.col("age_0_5").sum().alias("age_0_5_total"),
                pl.col("age_5_17").sum().alias("age_5_17_total"),
                pl.col("age_18_greater").sum().alias("age_18_greater_total"),
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater")).std().alias("daily_std"),
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater")).mean().alias("daily_mean"),
            ])
        )
        
        pincode_enrolment = pincode_enrolment.with_columns([
            (pl.col("age_0_5_total") + pl.col("age_5_17_total") + pl.col("age_18_greater_total")).alias("total_enrolment"),
            pl.when(pl.col("daily_mean") > 0)
            .then(pl.col("daily_std") / pl.col("daily_mean"))
            .otherwise(0.0)
            .alias("ovs"),
        ])
        
        pincode_enrolment = pincode_enrolment.with_columns([
            pl.when(pl.col("total_enrolment") > 0)
            .then(pl.col("age_18_greater_total") / pl.col("total_enrolment"))
            .otherwise(0.0)
            .alias("mii"),
        ])
        
        if not biometric_df.is_empty():
            pincode_biometric = (
                biometric_df
                .group_by("pincode")
                .agg([
                    (pl.col("bio_age_5_17").sum() + pl.col("bio_age_17_").sum()).alias("total_biometric")
                ])
            )
            pincode_enrolment = pincode_enrolment.join(
                pincode_biometric, on="pincode", how="left"
            ).with_columns(pl.col("total_biometric").fill_null(0))
        else:
            pincode_enrolment = pincode_enrolment.with_columns(pl.lit(0).alias("total_biometric"))
        
        if not demographic_df.is_empty():
            pincode_demographic = (
                demographic_df
                .group_by("pincode")
                .agg([
                    (pl.col("demo_age_5_17").sum() + pl.col("demo_age_17_").sum()).alias("total_demographic")
                ])
            )
            pincode_enrolment = pincode_enrolment.join(
                pincode_demographic, on="pincode", how="left"
            ).with_columns(pl.col("total_demographic").fill_null(0))
        else:
            pincode_enrolment = pincode_enrolment.with_columns(pl.lit(0).alias("total_demographic"))
        
        pincode_enrolment = pincode_enrolment.with_columns([
            pl.when(pl.col("total_biometric") > 0)
            .then(pl.col("total_demographic") / pl.col("total_biometric"))
            .otherwise(0.0)
            .alias("dhr"),
        ])
        
        pincode_enrolment = pincode_enrolment.with_columns([
            pl.when((pl.col("ovs") > OVS_CAMP_THRESHOLD) & (pl.col("total_enrolment") >= MIN_VOLUME_FOR_CAMP_FLAG))
            .then(pl.lit(OVS_LABEL_CAMP))
            .when(pl.col("ovs") < OVS_CENTER_THRESHOLD)
            .then(pl.lit(OVS_LABEL_CENTER))
            .otherwise(pl.lit(OVS_LABEL_NORMAL))
            .alias("ovs_classification"),
        ])
        
        pincode_enrolment = pincode_enrolment.with_columns([
            pl.when((pl.col("mii") > MII_HOTSPOT_THRESHOLD) & (pl.col("total_enrolment") >= MIN_ENROLMENT_FOR_MIGRATION_FLAG))
            .then(pl.lit(MII_LABEL_HOTSPOT))
            .when(pl.col("mii") < MII_NORMAL_THRESHOLD)
            .then(pl.lit(MII_LABEL_NORMAL))
            .otherwise(pl.lit(MII_LABEL_MIXED))
            .alias("mii_classification"),
        ])
        
        pincode_enrolment = pincode_enrolment.with_columns([
            pl.when(pl.col("dhr") > DHR_FRAUD_THRESHOLD)
            .then(pl.lit(DHR_LABEL_HIGH_RISK))
            .when(pl.col("dhr") < 0.3)
            .then(pl.lit(DHR_LABEL_OVER_VERIFIED))
            .otherwise(pl.lit(DHR_LABEL_NORMAL))
            .alias("dhr_classification"),
        ])
        
        pincode_enrolment = pincode_enrolment.with_columns([
            (pl.col("ovs_classification") == OVS_LABEL_CAMP).alias("is_volatile_camp"),
            (pl.col("mii_classification") == MII_LABEL_HOTSPOT).alias("is_migration_hotspot"),
            (pl.col("dhr_classification") == DHR_LABEL_HIGH_RISK).alias("is_fraud_risk"),
            pl.lit(TLP_LABEL_BALANCED).alias("tlp_classification"),
        ])
        
        pincode_df = pincode_enrolment.select([
            "pincode", "state", "district",
            "total_enrolment", "total_biometric", "total_demographic",
            pl.col("ovs").round(4),
            "ovs_classification",
            pl.col("mii").round(4),
            "mii_classification",
            pl.col("dhr").round(4),
            "dhr_classification",
            "tlp_classification",
            "is_volatile_camp",
            "is_migration_hotspot",
            "is_fraud_risk",
        ])
        
        
        district_df = (
            pincode_df
            .group_by(["state", "district"])
            .agg([
                pl.col("pincode").n_unique().alias("pincode_count"),
                pl.col("total_enrolment").sum().alias("total_enrolment"),
                pl.col("total_biometric").sum().alias("total_biometric"),
                pl.col("total_demographic").sum().alias("total_demographic"),
                pl.col("ovs").mean().alias("avg_ovs"),
                pl.col("mii").mean().alias("avg_mii"),
                pl.col("dhr").mean().alias("avg_dhr"),
                pl.col("is_volatile_camp").sum().alias("volatile_camp_count"),
                pl.col("is_migration_hotspot").sum().alias("migration_hotspot_count"),
                pl.col("is_fraud_risk").sum().alias("fraud_risk_count"),
            ])
        )
        
        max_enrolment = district_df["total_enrolment"].max()
        max_enrolment = max_enrolment if max_enrolment and max_enrolment > 0 else 1
        
        district_df = district_df.with_columns([
            (pl.col("total_enrolment") / max_enrolment).alias("normalized_enrolment_rate"),
            pl.when(pl.col("total_enrolment") > 0)
            .then((pl.col("total_biometric") + pl.col("total_demographic")) / pl.col("total_enrolment"))
            .otherwise(0.0)
            .clip(0, 1)
            .alias("normalized_update_rate"),
            pl.col("avg_mii").alias("normalized_adult_ratio"),
        ])
        
        district_df = district_df.with_columns([
            pl.col("avg_ovs").round(4),
            pl.col("avg_mii").round(4),
            pl.col("avg_dhr").round(4),
            pl.col("normalized_enrolment_rate").round(4),
            pl.col("normalized_update_rate").round(4),
            pl.col("normalized_adult_ratio").round(4),
        ])
        
        if not district_df.is_empty() and len(district_df) >= 3:
            try:
                classifier = get_maturity_classifier()
                district_df = classifier.classify_districts(district_df)
            except Exception:
                district_df = district_df.with_columns([
                    pl.when(pl.col("normalized_enrolment_rate") > 0.5)
                    .then(pl.lit(SML_CLUSTER_LABELS[0]))
                    .when(pl.col("normalized_update_rate") > 0.3)
                    .then(pl.lit(SML_CLUSTER_LABELS[1]))
                    .otherwise(pl.lit(SML_CLUSTER_LABELS[2]))
                    .alias("sml_label"),
                    pl.lit("").alias("sml_description"),
                ])
        else:
            district_df = district_df.with_columns([
                pl.lit(SML_CLUSTER_LABELS[0]).alias("sml_label"),
                pl.lit("").alias("sml_description"),
            ])
        
        pincode_count = 0
        district_count = 0
        
        if not pincode_df.is_empty():
            pincode_count = self.delta_ops.write_to_delta(
                pincode_df, "gold", "pincode_insights", mode="overwrite"
            )
        
        if not district_df.is_empty():
            district_count = self.delta_ops.write_to_delta(
                district_df, "gold", "district_insights", mode="overwrite"
            )
        
        return {
            "pincode_insights": pincode_count,
            "district_insights": district_count
        }
    
    
    def detect_service_shadows(self) -> pl.DataFrame:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        if enrolment_df.is_empty():
            return pl.DataFrame()
        
        enrolment_by_district = (
            enrolment_df
            .group_by(["state", "district"])
            .agg([
                pl.col("age_18_greater").sum().alias("adult_enrolments"),
                (pl.col("age_0_5").sum() + pl.col("age_5_17").sum() + pl.col("age_18_greater").sum()).alias("total_enrolments")
            ])
        )
        
        adult_updates = pl.lit(0).alias("adult_updates")
        if not biometric_df.is_empty() and "bio_age_17_" in biometric_df.columns:
            bio_by_district = (
                biometric_df
                .group_by(["state", "district"])
                .agg([pl.col("bio_age_17_").sum().alias("adult_bio_updates")])
            )
            enrolment_by_district = enrolment_by_district.join(
                bio_by_district, on=["state", "district"], how="left"
            ).with_columns(pl.col("adult_bio_updates").fill_null(0))
        else:
            enrolment_by_district = enrolment_by_district.with_columns(pl.lit(0).alias("adult_bio_updates"))
        
        ghost_districts = enrolment_by_district.filter(
            (pl.col("total_enrolments") > 50) & (pl.col("adult_bio_updates") == 0)
        ).sort("total_enrolments", descending=True)
        
        return ghost_districts
    
    def calculate_utilization_rate(self) -> pl.DataFrame:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        if enrolment_df.is_empty():
            return pl.DataFrame()
        
        daily_volume = (
            enrolment_df
            .with_columns(
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater")).alias("daily_total")
            )
            .group_by(["pincode", "state", "district"])
            .agg([
                pl.col("daily_total").max().alias("max_daily_volume"),
                pl.col("daily_total").mean().alias("avg_daily_volume"),
                pl.col("daily_total").sum().alias("total_volume")
            ])
        )
        
        daily_volume = daily_volume.with_columns(
            pl.when(pl.col("max_daily_volume") > 0)
            .then((pl.col("avg_daily_volume") / pl.col("max_daily_volume") * 100))
            .otherwise(0.0)
            .alias("utilization_rate")
        )
        
        high_util = daily_volume.filter(pl.col("utilization_rate") > 90).sort("utilization_rate", descending=True).head(5)
        
        return high_util
    
    def get_age_ladder_data(self) -> pl.DataFrame:
        
        enrolment_df, _, _ = self._get_data()
        
        if enrolment_df.is_empty():
            return pl.DataFrame()
        
        state_age = (
            enrolment_df
            .group_by("state")
            .agg([
                pl.col("age_0_5").sum().alias("age_0_5"),
                pl.col("age_5_17").sum().alias("age_5_17"),
                pl.col("age_18_greater").sum().alias("age_18_greater"),
            ])
        )
        
        state_age = state_age.with_columns(
            (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater")).alias("total")
        ).with_columns(
            pl.when(pl.col("total") > 0)
            .then((pl.col("age_18_greater") / pl.col("total") * 100))
            .otherwise(0.0)
            .round(1)
            .alias("adult_pct")
        ).with_columns(
            (pl.col("adult_pct") > 10).alias("is_anomaly")
        ).sort("total", descending=True)
        
        return state_age
    
    def get_migration_hotspots(self) -> pl.DataFrame:
        
        try:
            district_df = self.delta_ops.read_delta_as_polars("gold", "district_insights")
        except Exception:
            return pl.DataFrame()
        
        if district_df.is_empty() or "avg_mii" not in district_df.columns:
            return pl.DataFrame()
        
        hotspots = district_df.filter(pl.col("avg_mii") > MII_HOTSPOT_THRESHOLD)
        
        hotspots = hotspots.with_columns(
            pl.when(pl.col("avg_mii") > 0.6)
            .then(pl.lit("Labor Influx"))
            .when(pl.col("avg_mii") > 0.5)
            .then(pl.lit("Settlement Zone"))
            .otherwise(pl.lit("Migration Detected"))
            .alias("verdict")
        ).sort("avg_mii", descending=True)
        
        return hotspots
    
    def get_zero_growth_districts(self) -> pl.DataFrame:
        
        try:
            district_df = self.delta_ops.read_delta_as_polars("gold", "district_insights")
        except Exception:
            return pl.DataFrame()
        
        if district_df.is_empty() or "total_enrolment" not in district_df.columns:
            return pl.DataFrame()
        
        zero_growth = district_df.filter(pl.col("total_enrolment") < 100).sort("total_enrolment")
        
        return zero_growth
    
    def predict_student_surge(self) -> dict:
        
        _, biometric_df, _ = self._get_data()
        
        if biometric_df.is_empty() or "date" not in biometric_df.columns:
            return {"monthly_data": [], "peak_month": None, "peak_value": 0}
        
        monthly = (
            biometric_df
            .with_columns(pl.col("date").cast(pl.Date).dt.month().alias("month"))
            .group_by("month")
            .agg(pl.col("bio_age_5_17").sum().alias("student_updates"))
            .sort("month")
        )
        
        if monthly.is_empty():
            return {"monthly_data": [], "peak_month": None, "peak_value": 0}
        
        peak_row = monthly.filter(pl.col("student_updates") == pl.col("student_updates").max())
        peak_month = int(peak_row[0, "month"]) if not peak_row.is_empty() else None
        peak_value = int(peak_row[0, "student_updates"]) if not peak_row.is_empty() else 0
        
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        return {
            "monthly_data": monthly.to_dicts(),
            "peak_month": month_names[peak_month - 1] if peak_month else None,
            "peak_value": peak_value
        }
    
    def calculate_digital_maturity(self) -> dict:
        
        _, biometric_df, demographic_df = self._get_data()
        
        total_biometric = 0
        total_demographic = 0
        
        if not biometric_df.is_empty():
            total_biometric = int(biometric_df.select(
                (pl.col("bio_age_5_17").sum() + pl.col("bio_age_17_").sum())
            ).item())
        
        if not demographic_df.is_empty():
            total_demographic = int(demographic_df.select(
                (pl.col("demo_age_5_17").sum() + pl.col("demo_age_17_").sum())
            ).item())
        
        if total_demographic == 0:
            maturity_score = 2.0
        else:
            maturity_score = total_biometric / total_demographic
        
        if maturity_score < 0.5:
            classification = "Fixing Phase"
            recommendation = "High address change volume - investigate data quality"
        elif maturity_score < 1.5:
            classification = "Normal Operation"
            recommendation = "Healthy balance of updates"
        else:
            classification = "Mature Usage"
            recommendation = "Strong biometric verification culture"
        
        return {
            "score": round(maturity_score, 2),
            "classification": classification,
            "recommendation": recommendation,
            "total_biometric": total_biometric,
            "total_demographic": total_demographic
        }
    
    def detect_synchronized_spikes(self) -> pl.DataFrame:
        
        enrolment_df, biometric_df, demographic_df = self._get_data()
        
        dfs_to_concat = []
        
        if not enrolment_df.is_empty():
            e = enrolment_df.with_columns(
                (pl.col("age_0_5") + pl.col("age_5_17") + pl.col("age_18_greater")).alias("volume")
            ).select(["date", "volume"])
            dfs_to_concat.append(e)
        
        if not biometric_df.is_empty():
            b = biometric_df.with_columns(
                (pl.col("bio_age_5_17") + pl.col("bio_age_17_")).alias("volume")
            ).select(["date", "volume"])
            dfs_to_concat.append(b)
        
        if not demographic_df.is_empty():
            d = demographic_df.with_columns(
                (pl.col("demo_age_5_17") + pl.col("demo_age_17_")).alias("volume")
            ).select(["date", "volume"])
            dfs_to_concat.append(d)
        
        if not dfs_to_concat:
            return pl.DataFrame()
        
        combined = pl.concat(dfs_to_concat).group_by("date").agg(pl.col("volume").sum())
        
        mean_vol = combined["volume"].mean()
        std_vol = combined["volume"].std()
        
        if std_vol is None or std_vol == 0:
            return pl.DataFrame()
        
        combined = combined.with_columns(
            ((pl.col("volume") - mean_vol) / std_vol).alias("z_score")
        ).filter(
            pl.col("z_score").abs() > 3
        ).sort("date")
        
        return combined

_insight_generator: Optional[InsightGenerator] = None

def get_insight_generator() -> InsightGenerator:
    
    global _insight_generator
    if _insight_generator is None:
        _insight_generator = InsightGenerator()
    return _insight_generator
