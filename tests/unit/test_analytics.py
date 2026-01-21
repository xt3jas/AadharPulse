"""
Unit tests for the analytics engine.

Tests:
- OVS (Operational Volatility Score) calculation
- MII (Migration Impact Index) calculation
- DHR (Data Hygiene Ratio) calculation
- Edge cases and division-by-zero handling
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.api.schemas import DHRClassification, MIIClassification, OVSClassification
from app.services.analytics import InsightGenerator


class TestOVSCalculation:
    """Test Operational Volatility Score calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = InsightGenerator()
    
    def test_ovs_high_volatility_spike_pattern(self):
        """
        Test OVS calculation with spike pattern.
        
        Given a daily volume pattern of [0, 0, 100, 0], the OVS should be high
        because there's sporadic activity (one big spike).
        
        Expected: OVS > 3.0 (indicates temporary camp behavior)
        """
        daily_volumes = pl.Series([0, 0, 100, 0, 0, 0, 0])
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        # High volatility pattern
        assert ovs > 3.0, f"Expected OVS > 3.0 for spike pattern, got {ovs}"
    
    def test_ovs_stable_pattern_zero_volatility(self):
        """
        Test OVS calculation with perfectly stable pattern.
        
        When all daily volumes are identical, standard deviation is 0,
        so OVS (coefficient of variation) should be 0.
        """
        daily_volumes = pl.Series([100, 100, 100, 100, 100, 100, 100])
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        assert ovs == 0.0, f"Expected OVS = 0.0 for stable pattern, got {ovs}"
    
    def test_ovs_division_by_zero_handling(self):
        """
        Test that OVS handles division by zero gracefully.
        
        When mean is 0 (all zeros), the function should return 0.0
        instead of raising an error.
        """
        daily_volumes = pl.Series([0, 0, 0, 0])
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        assert ovs == 0.0, f"Expected OVS = 0.0 when mean is 0, got {ovs}"
    
    def test_ovs_temporary_camp_pattern(self):
        """
        Test OVS with typical temporary camp pattern.
        
        One day of high activity surrounded by zeros indicates
        a mobile camp deployment.
        """
        # Pattern: 4 zeros, one spike of 500, more zeros
        daily_volumes = pl.Series([0, 0, 0, 0, 500, 0, 0, 0, 0, 0])
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        # Should exceed the camp threshold of 4.0
        assert ovs > 4.0, f"Expected OVS > 4.0 for camp pattern, got {ovs}"
    
    def test_ovs_moderate_volatility(self):
        """
        Test OVS with moderate volatility pattern.
        
        Some variation but not extreme should give moderate OVS.
        """
        daily_volumes = pl.Series([50, 75, 60, 80, 55, 70, 65])
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        # Moderate volatility - between stable and camp thresholds
        assert 0.1 < ovs < 2.0, f"Expected moderate OVS, got {ovs}"
    
    def test_ovs_empty_series_returns_zero(self):
        """Test that empty series returns 0.0."""
        daily_volumes = pl.Series([], dtype=pl.Int64)
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        assert ovs == 0.0
    
    def test_ovs_short_series_returns_zero(self):
        """Test that series shorter than minimum days returns 0.0."""
        daily_volumes = pl.Series([100, 200])  # Less than 7 days
        
        ovs = self.generator.calculate_ovs(daily_volumes)
        
        assert ovs == 0.0
    
    def test_ovs_classification_camp(self):
        """Test OVS classification as Temporary Camp."""
        classification = self.generator.classify_ovs(5.0, total_volume=1000)
        
        assert classification == OVSClassification.TEMPORARY_CAMP
    
    def test_ovs_classification_center(self):
        """Test OVS classification as Permanent Center."""
        classification = self.generator.classify_ovs(0.3, total_volume=1000)
        
        assert classification == OVSClassification.PERMANENT_CENTER
    
    def test_ovs_classification_normal(self):
        """Test OVS classification as Normal Activity."""
        classification = self.generator.classify_ovs(2.0, total_volume=1000)
        
        assert classification == OVSClassification.NORMAL_ACTIVITY


class TestMIICalculation:
    """Test Migration Impact Index calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = InsightGenerator()
    
    def test_mii_high_adult_ratio(self):
        """
        Test MII with high adult enrolment ratio.
        
        When adults dominate enrolment (> 40%), it indicates
        migration or regularization.
        """
        mii = self.generator.calculate_mii(
            enrolment_18_plus=500,
            total_enrolment=1000
        )
        
        assert mii == 0.5, f"Expected MII = 0.5, got {mii}"
        assert mii > 0.4, "Expected MII > 0.4 for migration hotspot threshold"
    
    def test_mii_low_adult_ratio(self):
        """
        Test MII with low adult enrolment (birth-rate driven).
        
        When children dominate (< 5% adults), it's normal birth-rate driven.
        """
        mii = self.generator.calculate_mii(
            enrolment_18_plus=30,
            total_enrolment=1000
        )
        
        assert mii == 0.03, f"Expected MII = 0.03, got {mii}"
        assert mii < 0.05, "Expected MII < 0.05 for birth-rate driven"
    
    def test_mii_division_by_zero_handling(self):
        """Test that MII handles zero total gracefully."""
        mii = self.generator.calculate_mii(
            enrolment_18_plus=100,
            total_enrolment=0
        )
        
        assert mii == 0.0
    
    def test_mii_clamped_to_one(self):
        """Test that MII is clamped to maximum of 1.0."""
        # Edge case: adult count > total (data error)
        mii = self.generator.calculate_mii(
            enrolment_18_plus=1500,
            total_enrolment=1000
        )
        
        assert mii == 1.0, f"Expected MII clamped to 1.0, got {mii}"
    
    def test_mii_classification_hotspot(self):
        """Test MII classification as Migration Hotspot."""
        classification = self.generator.classify_mii(0.5, total_enrolment=500)
        
        assert classification == MIIClassification.MIGRATION_HOTSPOT
    
    def test_mii_classification_birth_rate(self):
        """Test MII classification as Birth-Rate Driven."""
        classification = self.generator.classify_mii(0.03, total_enrolment=500)
        
        assert classification == MIIClassification.BIRTH_RATE_DRIVEN
    
    def test_mii_classification_mixed(self):
        """Test MII classification as Mixed Population."""
        classification = self.generator.classify_mii(0.2, total_enrolment=500)
        
        assert classification == MIIClassification.MIXED_POPULATION


class TestDHRCalculation:
    """Test Data Hygiene Ratio calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = InsightGenerator()
    
    def test_dhr_high_fraud_risk(self):
        """
        Test DHR indicating high fraud risk.
        
        When demographic updates >> biometric updates, it suggests
        address/name changes without mandatory biometric verification.
        """
        dhr = self.generator.calculate_dhr(
            demographic_updates=2000,
            biometric_updates=1000
        )
        
        assert dhr == 2.0, f"Expected DHR = 2.0, got {dhr}"
        assert dhr > 1.5, "Expected DHR > 1.5 for fraud risk threshold"
    
    def test_dhr_normal_maintenance(self):
        """
        Test DHR indicating normal maintenance.
        
        When demographic updates ≈ 50% of biometric updates, it's normal.
        """
        dhr = self.generator.calculate_dhr(
            demographic_updates=500,
            biometric_updates=1000
        )
        
        assert dhr == 0.5
    
    def test_dhr_division_by_zero_with_demographics(self):
        """
        Test DHR when biometric updates is zero but demographics exist.
        
        This is a red flag - demographic changes without any biometric checks.
        """
        dhr = self.generator.calculate_dhr(
            demographic_updates=100,
            biometric_updates=0
        )
        
        # Returns the demographic count as indicator of risk
        assert dhr == 100.0
    
    def test_dhr_both_zero(self):
        """Test DHR when both counts are zero."""
        dhr = self.generator.calculate_dhr(
            demographic_updates=0,
            biometric_updates=0
        )
        
        assert dhr == 0.0
    
    def test_dhr_classification_high_risk(self):
        """Test DHR classification as High Fraud Risk."""
        classification = self.generator.classify_dhr(2.0, total_transactions=2000)
        
        assert classification == DHRClassification.HIGH_FRAUD_RISK
    
    def test_dhr_classification_normal(self):
        """Test DHR classification as Normal Maintenance."""
        classification = self.generator.classify_dhr(0.5, total_transactions=2000)
        
        assert classification == DHRClassification.NORMAL_MAINTENANCE
    
    def test_dhr_classification_over_verified(self):
        """Test DHR classification as Over-Verified."""
        classification = self.generator.classify_dhr(0.1, total_transactions=2000)
        
        assert classification == DHRClassification.OVER_VERIFIED


class TestTLPCalculation:
    """Test Temporal Load Profile calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = InsightGenerator()
    
    def test_tlp_weekend_warrior_pattern(self):
        """
        Test TLP detection of weekend warrior pattern.
        
        When Saturday + Sunday > 60% of volume, classify as weekend zone.
        """
        # Create data with heavy weekend activity
        transactions = pl.DataFrame({
            "date": [
                "2024-01-13",  # Saturday
                "2024-01-14",  # Sunday
                "2024-01-15",  # Monday
                "2024-01-16",  # Tuesday
            ],
            "volume": [400, 300, 100, 200]
        }).with_columns(pl.col("date").str.to_date())
        
        tlp = self.generator.calculate_tlp(transactions)
        
        # Weekend total = 700/1000 = 70%
        weekend_pct = tlp.saturday + tlp.sunday
        assert weekend_pct > 0.6, f"Expected weekend > 60%, got {weekend_pct}"
    
    def test_tlp_balanced_pattern(self):
        """Test TLP with balanced distribution."""
        transactions = pl.DataFrame({
            "date": [
                "2024-01-15",  # Monday
                "2024-01-16",  # Tuesday
                "2024-01-17",  # Wednesday
                "2024-01-18",  # Thursday
                "2024-01-19",  # Friday
            ],
            "volume": [100, 100, 100, 100, 100]
        }).with_columns(pl.col("date").str.to_date())
        
        tlp = self.generator.calculate_tlp(transactions)
        
        # Each day should be ~20%
        assert abs(tlp.monday - 0.2) < 0.01
        assert tlp.recommendation is not None
    
    def test_tlp_empty_dataframe(self):
        """Test TLP with empty DataFrame."""
        transactions = pl.DataFrame({"date": [], "volume": []})
        
        tlp = self.generator.calculate_tlp(transactions)
        
        assert tlp.monday == 0.0
        assert tlp.saturday == 0.0


class TestClusterClassification:
    """Test SML clustering functionality."""
    
    def test_maturity_classifier_basic(self):
        """Test basic MaturityClassifier functionality."""
        from app.services.clustering import MaturityClassifier
        import numpy as np
        
        classifier = MaturityClassifier(n_clusters=3)
        
        # Create sample feature data
        features = np.array([
            [0.9, 0.1, 0.3],  # High enrolment (Emerging)
            [0.2, 0.8, 0.5],  # High updates (Saturated)
            [0.5, 0.5, 0.9],  # High adult ratio (High Churn)
            [0.8, 0.2, 0.2],  # Emerging
            [0.1, 0.9, 0.4],  # Saturated
        ])
        
        labels = classifier.fit_predict(features)
        
        assert len(labels) == 5
        assert set(labels).issubset({0, 1, 2})
    
    def test_maturity_classifier_insufficient_samples(self):
        """Test that classifier handles insufficient samples."""
        from app.services.clustering import MaturityClassifier
        import numpy as np
        
        classifier = MaturityClassifier(n_clusters=3)
        
        # Only 2 samples but need 3 clusters
        features = np.array([
            [0.5, 0.5, 0.5],
            [0.6, 0.4, 0.5],
        ])
        
        with pytest.raises(ValueError):
            classifier.fit_predict(features)
