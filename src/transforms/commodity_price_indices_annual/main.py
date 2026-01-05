import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_commodity_price_indices_annual"

METADATA = {
    "id": DATASET_ID,
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
