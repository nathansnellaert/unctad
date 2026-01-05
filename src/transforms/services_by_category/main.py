import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_services_by_category"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Services Trade by Category",
    "description": "Annual international trade in services by economy, flow direction, and service category.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "category": "Service category",
        "value_usd_millions": "Value in US$ millions at current prices",
        "value_pct_world": "Value as percentage of total world",
        "value_pct_services": "Value as percentage of total trade in services",
        "growth_rate": "Growth rate over previous period (%)",
        "value_pct_gdp": "Value as percentage of GDP",
    },
}


def run():
    """Transform services by category data."""
    raw = load_raw("services_by_category")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "category": row.get("Category Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_world": parse_value(row.get("Percentage of total world", "")),
            "value_pct_services": parse_value(row.get("Percentage of total trade in services", "")),
            "growth_rate": parse_value(row.get("Growth rate (over previous period)", "")),
            "value_pct_gdp": parse_value(row.get("Percentage of Gross Domestic Product", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
