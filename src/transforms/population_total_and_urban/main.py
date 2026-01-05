import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_population_total_and_urban"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Population Total and Urban",
    "description": "Population statistics by economy including total population and urbanization rate.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "population_thousands": "Total population in thousands",
        "urban_pct": "Urban population as percentage of total population",
    },
}


def run():
    """Transform population total and urban data."""
    raw = load_raw("population_total_and_urban")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "population_thousands": parse_value(row.get("Absolute value in thousands", "")),
            "urban_pct": parse_value(row.get("Urban population as percentage of total population", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
