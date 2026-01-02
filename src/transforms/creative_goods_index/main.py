import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_creative_goods_index"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Creative Goods Concentration Index",
    "description": "Concentration index for creative goods trade by product category and trade flow (imports/exports).",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "flow": "Trade flow: Imports or Exports",
        "product_code": "Creative goods product code",
        "product": "Creative goods product category",
        "concentration_index": "Trade concentration index value",
    },
}


def run():
    """Transform creative goods index data."""
    raw = load_raw("creative_goods_index")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "flow": row["Flow Label"],
            "product_code": row["Product"],
            "product": row["Product Label"],
            "concentration_index": parse_value(row.get("Concentration Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
