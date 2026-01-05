import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_population_structure_by_age_gender"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Population Structure by Age and Gender",
    "description": "Population breakdown by economy, sex, and age class.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "sex": "Sex (Male/Female/Both sexes)",
        "age_class": "Age class",
        "population_thousands": "Population in thousands",
    },
}


def run():
    """Transform population structure by age and gender data."""
    raw = load_raw("population_structure_by_age_gender")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "sex": row.get("Sex Label", ""),
            "age_class": row.get("AgeClass Label", ""),
            "population_thousands": parse_value(row.get("Absolute value in thousands", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
