"""
Unit tests for the ingestion service.

Tests:
- Schema auto-detection for 3 CSV types
- Rejection of negative values
- Date standardization
"""

import io
from datetime import date

import polars as pl
import pytest

# Add app to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.api.schemas import SchemaType
from app.services.ingestion import IngestionService, SchemaDetectionError


class TestSchemaDetection:
    """Test schema auto-detection from CSV headers."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = IngestionService()
    
    def test_detect_enrolment_schema(self):
        """Test detection of Enrolment CSV schema."""
        headers = ["date", "state", "district", "pincode", "age_0_5", "age_5_17", "age_18_greater"]
        
        result = self.service.detect_schema(headers)
        
        assert result == SchemaType.ENROLMENT
    
    def test_detect_biometric_schema(self):
        """Test detection of Biometric CSV schema."""
        headers = ["date", "state", "district", "pincode", "bio_age_5_17", "bio_age_17_"]
        
        result = self.service.detect_schema(headers)
        
        assert result == SchemaType.BIOMETRIC
    
    def test_detect_demographic_schema(self):
        """Test detection of Demographic CSV schema."""
        headers = ["date", "state", "district", "pincode", "demo_age_5_17", "demo_age_17_"]
        
        result = self.service.detect_schema(headers)
        
        assert result == SchemaType.DEMOGRAPHIC
    
    def test_detect_schema_case_insensitive(self):
        """Test that schema detection is case-insensitive."""
        headers = ["DATE", "State", "DISTRICT", "Pincode", "AGE_0_5", "age_5_17", "Age_18_Greater"]
        
        result = self.service.detect_schema(headers)
        
        assert result == SchemaType.ENROLMENT
    
    def test_detect_schema_with_extra_columns(self):
        """Test detection with additional unrecognized columns."""
        headers = ["date", "state", "district", "pincode", "age_0_5", "age_5_17", "age_18_greater", "extra_col"]
        
        result = self.service.detect_schema(headers)
        
        assert result == SchemaType.ENROLMENT
    
    def test_detect_schema_unknown_raises_error(self):
        """Test that unknown schemas raise SchemaDetectionError."""
        headers = ["unknown", "columns", "here"]
        
        with pytest.raises(SchemaDetectionError):
            self.service.detect_schema(headers)


class TestValidation:
    """Test data validation rules."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = IngestionService()
    
    def test_reject_negative_values(self):
        """Test that rows with negative numerical values are rejected."""
        df = pl.DataFrame({
            "date": ["2024-01-15", "2024-01-16"],
            "state": ["Karnataka", "Karnataka"],
            "district": ["Bangalore", "Bangalore"],
            "pincode": ["560001", "560002"],
            "age_0_5": [100, -50],  # Second row has negative value
            "age_5_17": [200, 200],
            "age_18_greater": [300, 300],
        })
        
        valid_df, errors = self.service.validate_and_transform(df, SchemaType.ENROLMENT)
        
        # Only first row should remain
        assert len(valid_df) == 1
        assert len(errors) > 0
        assert any(e.column == "age_0_5" for e in errors)
    
    def test_date_standardization(self):
        """Test that dates are standardized to ISO-8601."""
        df = pl.DataFrame({
            "date": ["15-01-2024", "16/01/2024", "2024-01-17"],
            "state": ["Karnataka", "Karnataka", "Karnataka"],
            "district": ["Bangalore", "Bangalore", "Bangalore"],
            "pincode": ["560001", "560002", "560003"],
            "age_0_5": [100, 100, 100],
            "age_5_17": [200, 200, 200],
            "age_18_greater": [300, 300, 300],
        })
        
        valid_df, errors = self.service.validate_and_transform(df, SchemaType.ENROLMENT)
        
        # All rows should be valid
        assert len(valid_df) == 3
        
        # Dates should be Date type
        assert valid_df["date"].dtype == pl.Date
    
    def test_pincode_preserved_as_string(self):
        """Test that pincodes preserve leading zeros as strings."""
        df = pl.DataFrame({
            "date": ["2024-01-15"],
            "state": ["Assam"],
            "district": ["Test"],
            "pincode": ["001234"],  # Leading zeros
            "age_0_5": [100],
            "age_5_17": [200],
            "age_18_greater": [300],
        })
        
        valid_df, errors = self.service.validate_and_transform(df, SchemaType.ENROLMENT)
        
        assert valid_df["pincode"].dtype == pl.Utf8
        assert valid_df["pincode"][0] == "001234"
    
    def test_state_district_normalized_uppercase(self):
        """Test that state and district are normalized to uppercase."""
        df = pl.DataFrame({
            "date": ["2024-01-15"],
            "state": ["  karnataka  "],
            "district": ["bangalore urban"],
            "pincode": ["560001"],
            "age_0_5": [100],
            "age_5_17": [200],
            "age_18_greater": [300],
        })
        
        valid_df, errors = self.service.validate_and_transform(df, SchemaType.ENROLMENT)
        
        assert valid_df["state"][0] == "KARNATAKA"
        assert valid_df["district"][0] == "BANGALORE URBAN"


class TestDateParser:
    """Test the date parser utility."""
    
    def test_standardize_iso_format(self):
        """Test parsing ISO-8601 format."""
        from app.utils.date_parser import standardize_date
        
        result = standardize_date("2024-01-15")
        
        assert result == "2024-01-15"
    
    def test_standardize_indian_format(self):
        """Test parsing Indian DD-MM-YYYY format."""
        from app.utils.date_parser import standardize_date
        
        result = standardize_date("15-01-2024")
        
        assert result == "2024-01-15"
    
    def test_standardize_slash_format(self):
        """Test parsing DD/MM/YYYY format."""
        from app.utils.date_parser import standardize_date
        
        result = standardize_date("15/01/2024")
        
        assert result == "2024-01-15"
    
    def test_standardize_date_object(self):
        """Test passing Python date object."""
        from app.utils.date_parser import standardize_date
        
        result = standardize_date(date(2024, 1, 15))
        
        assert result == "2024-01-15"
    
    def test_invalid_date_raises_error(self):
        """Test that invalid dates raise DateParseError."""
        from app.utils.date_parser import DateParseError, standardize_date
        
        with pytest.raises(DateParseError):
            standardize_date("not a date")
    
    def test_invalid_date_returns_none_when_not_raising(self):
        """Test that invalid dates return None with raise_on_error=False."""
        from app.utils.date_parser import standardize_date
        
        result = standardize_date("invalid", raise_on_error=False)
        
        assert result is None
