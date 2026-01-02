import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_gni_total_and_per_capita"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD GNI Total and Per Capita",
    "description": "Gross national income by economy. Values in US$ millions and per capita at current prices.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "gni_current_usd_millions": "GNI in US$ millions at current prices",
        "gni_per_capita_current_usd": "GNI per capita in US$ at current prices",
    },
}


def run():
    """Transform GNI total and per capita data."""
    raw = load_raw("gni_total_and_per_capita")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "gni_current_usd_millions": parse_value(row.get("US dollars at current prices in millions", "")),
            "gni_per_capita_current_usd": parse_value(row.get("US dollars at current prices per capita", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
