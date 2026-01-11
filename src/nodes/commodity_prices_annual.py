"""Transform Commodity Prices Annual data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_commodity_prices_annual"

METADATA = {
    "title": "UNCTAD Commodity Prices (Annual)",
    "description": "Annual commodity prices in various units as specified in commodity label.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "commodity_code": "UNCTAD commodity code",
        "commodity": "Commodity name and price specification",
        "price": "Price value in units specified in commodity label",
    },
}


def test(table: pa.Table) -> None:
    """Validate commodity prices annual dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "commodity_code": "string",
            "commodity": "string",
            "price": "double",
        },
        "not_null": ["year", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_year(table, "year")


def run():
    """Transform commodity prices annual data."""
    raw = load_raw("commodity_prices_annual")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "commodity_code": row["Commodity"],
            "commodity": row["Commodity Label"],
            "price": parse_value(row.get("Prices", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
