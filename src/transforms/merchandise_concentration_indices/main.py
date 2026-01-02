import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_merchandise_concentration_indices"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Merchandise Concentration and Diversification Indices",
    "description": "Trade concentration and diversification indices by economy and flow direction.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "num_products": "Number of products traded",
        "concentration_index": "Herfindahl-Hirschmann concentration index (0-1)",
        "diversification_index": "Diversification index (0-1)",
    },
}


def run():
    """Transform merchandise concentration indices data."""
    raw = load_raw("merchandise_concentration_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "num_products": parse_value(row.get("Number of products", "")),
            "concentration_index": parse_value(row.get("Concentration Index", "")),
            "diversification_index": parse_value(row.get("Diversification Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
