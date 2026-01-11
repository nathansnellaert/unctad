"""Transform Merchandise Trade Growth Rates data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate

DATASET_ID = "unctad_merchandise_trade_growth_rates"

METADATA = {
    "title": "UNCTAD Merchandise Trade Growth Rates",
    "description": "Merchandise trade growth rates by economy, period, and flow direction.",
    "column_descriptions": {
        "period": "Time period (e.g., 1995-2000)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "growth_rate": "Annual average growth rate (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate merchandise trade growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "growth_rate": "double",
        },
        "not_null": ["period", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })


def run():
    """Transform merchandise trade growth rates data."""
    raw = load_raw("merchandise_trade_growth_rates")

    records = []
    for row in raw:
        records.append({
            "period": row.get("Year Label", row.get("Year", "")),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "growth_rate": parse_value(row.get("Annual average growth rate", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
