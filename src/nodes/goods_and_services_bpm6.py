"""Transform Goods And Services Bpm6 data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_goods_and_services_bpm6"

METADATA = {
    "title": "UNCTAD Goods and Services (BPM6)",
    "description": "International trade in goods and services by economy using BPM6 methodology. Values in US$ millions and as percentage of total trade.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "series": "Trade series type",
        "flow": "Flow direction (Exports/Imports)",
        "value_usd_millions": "Value in US$ millions at current prices",
        "value_pct_total_trade": "Value as percentage of total trade in goods and services",
    },
}


def test(table: pa.Table) -> None:
    """Validate goods and services BPM6 dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "series": "string",
            "flow": "string",
            "value_usd_millions": "double",
            "value_pct_total_trade": "double",
        },
        "not_null": ["year", "economy_code", "economy", "series", "flow"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform goods and services BPM6 data."""
    raw = load_raw("goods_and_services_bpm6")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "series": row.get("Series Label", ""),
            "flow": row.get("Flow Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_total_trade": parse_value(row.get("Percentage of total trade in goods and services", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
