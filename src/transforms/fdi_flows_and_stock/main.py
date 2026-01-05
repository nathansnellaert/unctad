import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_fdi_flows_and_stock"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Foreign Direct Investment Flows and Stock",
    "description": "Foreign direct investment flows and stock by economy, direction (inward/outward), and type (flows/stock). Values in US$ millions and as percentage of world total.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow_type": "Type of FDI measure (Flows/Stock)",
        "direction": "Direction (Inward/Outward)",
        "value_usd_millions": "Value in US$ millions at current prices",
        "value_pct_world": "Value as percentage of total world",
    },
}


def run():
    """Transform FDI flows and stock data."""
    raw = load_raw("fdi_flows_and_stock")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow_type": row.get("Flow Label", ""),
            "direction": row.get("Direction Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_world": parse_value(row.get("Percentage of total world", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
