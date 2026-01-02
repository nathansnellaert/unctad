import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_merchandise_trade_growth_rates"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform merchandise trade growth rates data."""
    raw = load_raw("merchandise_trade_growth_rates")

    records = []
    for row in raw:
        records.append({
            "period": row.get("Year Label", row.get("Year", "")),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "growth_rate": parse_value(row.get("Annual average growth rate", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
