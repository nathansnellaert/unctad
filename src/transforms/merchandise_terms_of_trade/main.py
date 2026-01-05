import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_merchandise_terms_of_trade"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Merchandise Terms of Trade",
    "description": "Merchandise terms of trade indices by economy and index type. Base year 2015=100.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "index_type": "Type of index (e.g., terms of trade, export prices, import prices)",
        "index_value": "Index value (2015=100)",
    },
}


def run():
    """Transform merchandise terms of trade data."""
    raw = load_raw("merchandise_terms_of_trade")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "index_type": row.get("Index Label", ""),
            "index_value": parse_value(row.get("Index Base 2015", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
