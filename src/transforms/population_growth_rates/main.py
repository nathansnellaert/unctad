import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_population_growth_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Population Growth Rates",
    "description": "Population growth rates by economy and time period.",
    "column_descriptions": {
        "period": "Time period (e.g., 1950-1955, 2020-2025)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "growth_rate": "Annual average population growth rate (%)",
    },
}


def run():
    """Transform population growth rates data."""
    raw = load_raw("population_growth_rates")

    records = []
    for row in raw:
        records.append({
            "period": row["Period Label"],
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "growth_rate": parse_value(row.get("Annual average growth rate", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
