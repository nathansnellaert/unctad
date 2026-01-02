import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_creative_services_individual"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Creative Services Trade by Economy",
    "description": "Creative services trade values and growth rates by individual economy and trade flow.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow: Imports or Exports",
        "value_usd_millions": "Trade value in US$ millions at current prices",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
    },
}


def run():
    """Transform creative services individual data."""
    raw = load_raw("creative_services_individual")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row["Flow Label"],
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "growth_rate_yoy": parse_value(row.get("Growth rate, year-on-year", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
