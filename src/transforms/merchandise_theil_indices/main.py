import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_merchandise_theil_indices"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Merchandise Trade Theil Indices",
    "description": "Theil indices measuring product and market concentration for merchandise trade by economy.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow: Imports or Exports",
        "series_code": "Index series code",
        "series": "Index series name (Product concentration, Market concentration, or Overall)",
        "index_value": "Theil index value",
    },
}


def run():
    """Transform merchandise theil indices data."""
    raw = load_raw("merchandise_theil_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row["Flow Label"],
            "series_code": to_str(row["Series"]),
            "series": row["Series Label"],
            "index_value": parse_value(row.get("Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
