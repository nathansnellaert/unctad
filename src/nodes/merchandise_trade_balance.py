"""Transform Merchandise Trade Balance data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_merchandise_trade_balance"

METADATA = {
    "title": "UNCTAD Merchandise Trade Balance",
    "description": "Merchandise trade balance by economy. Values in US$ millions and as percentage of imports.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Balance type",
        "value_usd_millions": "Trade balance in US$ millions at current prices",
        "value_pct_imports": "Trade balance as percentage of imports",
    },
}


def test(table: pa.Table) -> None:
    """Validate merchandise trade balance dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "value_usd_millions": "double",
            "value_pct_imports": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform merchandise trade balance data."""
    raw = load_raw("merchandise_trade_balance")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_imports": parse_value(row.get("Percentage of imports", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
