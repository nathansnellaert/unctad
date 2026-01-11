"""Transform Gni Total And Per Capita data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_gni_total_and_per_capita"

METADATA = {
    "title": "UNCTAD GNI Total and Per Capita",
    "description": "Gross national income by economy. Values in US$ millions and per capita at current prices.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "gni_current_usd_millions": "GNI in US$ millions at current prices",
        "gni_per_capita_current_usd": "GNI per capita in US$ at current prices",
    },
}


def test(table: pa.Table) -> None:
    """Validate GNI total and per capita dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "gni_current_usd_millions": "double",
            "gni_per_capita_current_usd": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform GNI total and per capita data."""
    raw = load_raw("gni_total_and_per_capita")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "gni_current_usd_millions": parse_value(row.get("US dollars at current prices in millions", "")),
            "gni_per_capita_current_usd": parse_value(row.get("US dollars at current prices per capita", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
