import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_import_tariff_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Import Tariff Rates",
    "description": "Import tariff rates by market, origin, duty type, and product category.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "duty_type": "Type of duty (MFN, preferential, etc.)",
        "market_code": "Importing market code",
        "market": "Importing market name",
        "origin_code": "Origin economy code",
        "origin": "Origin economy name",
        "product_category": "Product category",
        "simple_avg_rate": "Simple average tariff rate (%)",
        "weighted_avg_rate": "Trade-weighted average tariff rate (%)",
    },
}


def run():
    """Transform import tariff rates data."""
    raw = load_raw("import_tariff_rates")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "duty_type": row.get("DutyType Label", ""),
            "market_code": row.get("Market", ""),
            "market": row.get("Market Label", ""),
            "origin_code": row.get("Origin", ""),
            "origin": row.get("Origin Label", ""),
            "product_category": row.get("ProductCategory Label", ""),
            "simple_avg_rate": parse_value(row.get("Simple average of rates", "")),
            "weighted_avg_rate": parse_value(row.get("Weighted average of rates", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
