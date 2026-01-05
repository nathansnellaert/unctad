import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_merchandise_structural_indices"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Merchandise Structural Indices",
    "description": "Merchandise trade structural indices by flow direction and product category.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "flow": "Trade flow (Exports/Imports)",
        "product": "Product category",
        "concentration_index": "Concentration index",
        "structural_change_index": "Structural change index (1995=0)",
    },
}


def run():
    """Transform merchandise structural indices data."""
    raw = load_raw("merchandise_structural_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "flow": row.get("Flow Label", ""),
            "product": row.get("Product Label", ""),
            "concentration_index": parse_value(row.get("Concentration Index", "")),
            "structural_change_index": parse_value(row.get("Structural Change Index (1995=0)", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
