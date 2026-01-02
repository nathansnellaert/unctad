import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_gdp_total_and_per_capita"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD GDP Total and Per Capita",
    "description": "Gross domestic product by economy. Values in US$ millions (current and constant 2015 prices) and per capita.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "gdp_current_usd_millions": "GDP in US$ millions at current prices",
        "gdp_per_capita_current_usd": "GDP per capita in US$ at current prices",
        "gdp_constant_2015_usd_millions": "GDP in US$ millions at constant 2015 prices",
        "gdp_per_capita_constant_2015_usd": "GDP per capita in US$ at constant 2015 prices",
    },
}


def run():
    """Transform GDP total and per capita data."""
    raw = load_raw("gdp_total_and_per_capita")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "gdp_current_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "gdp_per_capita_current_usd": parse_value(row.get("US$ at current prices per capita", "")),
            "gdp_constant_2015_usd_millions": parse_value(row.get("US$ at constant prices (2015) in millions", "")),
            "gdp_per_capita_constant_2015_usd": parse_value(row.get("US$ at constant prices (2015) per capita", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
