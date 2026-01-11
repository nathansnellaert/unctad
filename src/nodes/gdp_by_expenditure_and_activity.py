"""Transform Gdp By Expenditure And Activity data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_gdp_by_expenditure_and_activity"

METADATA = {
    "title": "UNCTAD GDP by Expenditure and Activity",
    "description": "GDP breakdown by expenditure component and economic activity by economy.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "component": "GDP component (expenditure or activity)",
        "value_current_usd_millions": "Value in US$ millions at current prices",
        "value_constant_2015_usd_millions": "Value in US$ millions at constant 2015 prices",
        "value_pct_gdp": "Value as percentage of GDP",
    },
}


def test(table: pa.Table) -> None:
    """Validate GDP by expenditure and activity dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "component": "string",
            "value_current_usd_millions": "double",
            "value_constant_2015_usd_millions": "double",
            "value_pct_gdp": "double",
        },
        "not_null": ["year", "economy_code", "economy", "component"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform GDP by expenditure and activity data."""
    raw = load_raw("gdp_by_expenditure_and_activity")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "component": row.get("Component Label", ""),
            "value_current_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_constant_2015_usd_millions": parse_value(row.get("US$ at constant prices (2015) in millions", "")),
            "value_pct_gdp": parse_value(row.get("Percentage of Gross Domestic Product", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
