"""Transform Population Structure By Age Gender data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_population_structure_by_age_gender"

METADATA = {
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


def test(table: pa.Table) -> None:
    """Validate population structure by age and gender dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "sex": "string",
            "age_class": "string",
            "population_thousands": "double",
        },
        "not_null": ["year", "economy_code", "economy", "sex", "age_class"],
        "min_rows": 10000,
    })

    assert_valid_year(table, "year")


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
    sync_metadata(DATASET_ID, METADATA)
