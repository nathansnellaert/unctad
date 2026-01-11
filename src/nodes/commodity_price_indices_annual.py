"""Transform Commodity Price Indices Annual data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_commodity_price_indices_annual"

METADATA = {
    "title": "UNCTAD Commodity Price Indices (Annual)",
    "description": "Annual commodity price indices with base year 2015=100 and growth rates.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "commodity_code": "UNCTAD commodity code",
        "commodity": "Commodity name",
        "index_base_2015": "Price index with base 2015=100",
        "growth_rate": "Growth rate over previous period (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate commodity price indices annual dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "commodity_code": "string",
            "commodity": "string",
            "index_base_2015": "double",
            "growth_rate": "double",
        },
        "not_null": ["year", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_year(table, "year")


def run():
    """Transform commodity price indices annual data."""
    raw = load_raw("commodity_price_indices_annual")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "commodity_code": row["Commodity"],
            "commodity": row["Commodity Label"],
            "index_base_2015": parse_value(row.get("Index Base 2015", "")),
            "growth_rate": parse_value(row.get("Growth rate (over previous period)", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
