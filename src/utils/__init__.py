"""UNCTAD connector utilities.

Shared helper functions for parsing and transforming UNCTAD data.
"""

from subsets_utils import load_raw_parquet, load_raw_json


def load_raw(asset_id: str) -> list[dict]:
    """Load raw data, trying Parquet first then JSON.

    Returns list of dicts for backward compatibility with existing transforms.
    """
    try:
        table = load_raw_parquet(asset_id)
        return table.to_pylist()
    except FileNotFoundError:
        return load_raw_json(asset_id)


MONTH_MAP = {
    "Jan.": "01", "January": "01",
    "Feb.": "02", "February": "02",
    "Mar.": "03", "March": "03",
    "Apr.": "04", "April": "04",
    "May": "05",
    "Jun.": "06", "June": "06",
    "Jul.": "07", "July": "07",
    "Aug.": "08", "August": "08",
    "Sep.": "09", "September": "09",
    "Oct.": "10", "October": "10",
    "Nov.": "11", "November": "11",
    "Dec.": "12", "December": "12",
}


def parse_value(val) -> float | None:
    """Parse UNCTAD value to float, returning None for empty/missing.

    Handles both string values (from JSON) and numeric values (from Parquet).
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        if not val or val.strip() == "":
            return None
        try:
            return float(val)
        except ValueError:
            return None
    return None


def parse_month(val: str) -> str:
    """Convert 'Jan. 1995' to '1995-01' format."""
    parts = val.split()
    if len(parts) == 2:
        month_abbr, year = parts
        if month_abbr in MONTH_MAP:
            return f"{year}-{MONTH_MAP[month_abbr]}"
    return val


def parse_quarter(val: str) -> str:
    """Convert 'Q1 2005' to '2005-Q1' format."""
    parts = val.split()
    if len(parts) == 2 and parts[0].startswith("Q"):
        return f"{parts[1]}-{parts[0]}"
    return val


def to_str(val) -> str:
    """Convert value to string. Handles int/float from Parquet."""
    if val is None:
        return ""
    return str(val)
