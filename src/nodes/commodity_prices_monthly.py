"""Transform Commodity Prices Monthly data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, parse_month
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month

DATASET_ID = "unctad_commodity_prices_monthly"

METADATA = {
    "title": "UNCTAD Commodity Prices (Monthly)",
    "description": "Monthly commodity prices in various units as specified in commodity label.",
    "column_descriptions": {
        "month": "Month (YYYY-MM)",
        "commodity_code": "UNCTAD commodity code",
        "commodity": "Commodity name and price specification",
        "price": "Price value in units specified in commodity label",
    },
}


def test(table: pa.Table) -> None:
    """Validate commodity prices monthly dataset."""
    validate(table, {
        "columns": {
            "month": "string",
            "commodity_code": "string",
            "commodity": "string",
            "price": "double",
        },
        "not_null": ["month", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_month(table, "month")


def run():
    """Transform commodity prices monthly data."""
    raw = load_raw("commodity_prices_monthly")

    records = []
    for row in raw:
        records.append({
            "month": parse_month(row["Period Label"]),
            "commodity_code": row["Commodity"],
            "commodity": row["Commodity Label"],
            "price": parse_value(row.get("Prices", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
