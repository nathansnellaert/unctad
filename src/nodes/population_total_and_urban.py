"""Transform Population Total And Urban data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_range

DATASET_ID = "unctad_population_total_and_urban"

METADATA = {
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


def test(table: pa.Table) -> None:
    """Validate population total and urban dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "population_thousands": "double",
            "urban_pct": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
    assert_in_range(table, "urban_pct", 0, 100)


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
    sync_metadata(DATASET_ID, METADATA)
