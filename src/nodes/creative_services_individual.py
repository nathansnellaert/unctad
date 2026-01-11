"""Transform Creative Services Individual data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_set

DATASET_ID = "unctad_creative_services_individual"

METADATA = {
    "title": "UNCTAD Creative Services Trade by Economy",
    "description": "Creative services trade values and growth rates by individual economy and trade flow.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow: Imports or Exports",
        "value_usd_millions": "Trade value in US$ millions at current prices",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate creative services individual dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "value_usd_millions": "double",
            "growth_rate_yoy": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow"],
        "min_rows": 100,
    })

    assert_in_set(table, "flow", {"Imports", "Exports"})

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 2000, f"Years before 2000 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"


def run():
    """Transform creative services individual data."""
    raw = load_raw("creative_services_individual")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row["Flow Label"],
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "growth_rate_yoy": parse_value(row.get("Growth rate, year-on-year", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
