import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_goods_and_services_bpm6"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform goods and services BPM6 data."""
    raw = load_raw("goods_and_services_bpm6")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
