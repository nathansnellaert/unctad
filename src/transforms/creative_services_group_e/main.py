import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_creative_services_group_e"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Creative Services by Category (Regional)",
    "description": "Creative services trade by service category and regional aggregates (World, developing economies, etc.).",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy/region code",
        "economy": "Economy or region name",
        "service_code": "Creative service category code",
        "service": "Creative service category name",
        "value_usd_millions": "Trade value in US$ millions at current prices",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
    },
}


def run():
    """Transform creative services group E data."""
    raw = load_raw("creative_services_group_e")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "service_code": to_str(row["CreativeService"]),
            "service": row["CreativeService Label"],
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
