from datetime import date, datetime
from typing import Optional, Union

from dateutil import parser as dateutil_parser
from dateutil.parser import ParserError

class DateParseError(Exception):
    
    
    def __init__(self, value: str, reason: str = "Unknown format"):
        self.value = value
        self.reason = reason
        super().__init__(f"Cannot parse date '{value}': {reason}")

KNOWN_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
    "%d-%b-%Y",
    "%d %b %Y",
    "%d-%B-%Y",
    "%Y%m%d",
)

def standardize_date(
    value: Union[str, date, datetime, None],
    dayfirst: bool = True,
    raise_on_error: bool = True
) -> Optional[str]:
    
    if value is None:
        if raise_on_error:
            raise DateParseError("None", "Null value provided")
        return None
    
    if isinstance(value, datetime):
        return value.date().isoformat()
    
    if isinstance(value, date):
        return value.isoformat()
    
    if not isinstance(value, str):
        value = str(value)
    
    value = value.strip()
    
    if not value:
        if raise_on_error:
            raise DateParseError("", "Empty string provided")
        return None
    
    parsed_date = _try_known_formats(value)
    if parsed_date:
        return parsed_date.isoformat()
    
    try:
        parsed = dateutil_parser.parse(value, dayfirst=dayfirst)
        return parsed.date().isoformat()
    except (ParserError, ValueError, OverflowError) as e:
        if raise_on_error:
            raise DateParseError(value, str(e))
        return None

def _try_known_formats(value: str) -> Optional[date]:
    
    for fmt in KNOWN_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None

def validate_date_range(
    date_str: str,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None
) -> tuple[bool, str]:
    
    try:
        parsed = date.fromisoformat(date_str)
    except ValueError:
        return False, f"Invalid ISO-8601 format: {date_str}"
    
    if min_date and parsed < min_date:
        return False, f"Date {date_str} is before minimum {min_date.isoformat()}"
    
    if max_date and parsed > max_date:
        return False, f"Date {date_str} is after maximum {max_date.isoformat()}"
    
    return True, ""

def extract_day_of_week(date_str: str) -> int:
    
    return date.fromisoformat(date_str).weekday()

def get_date_range(date_strings: list[str]) -> tuple[Optional[str], Optional[str]]:
    
    if not date_strings:
        return None, None
    
    dates = [date.fromisoformat(d) for d in date_strings if d]
    if not dates:
        return None, None
    
    return min(dates).isoformat(), max(dates).isoformat()
