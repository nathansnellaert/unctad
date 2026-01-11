"""Transform Population Growth Rates data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate

DATASET_ID = "unctad_population_growth_rates"

METADATA = {
    "title": "UNCTAD Population Growth Rates",
    "description": "Population growth rates by economy and time period.",
    "column_descriptions": {
        "period": "Time period (e.g., 1950-1955, 2020-2025)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "growth_rate": "Annual average population growth rate (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate population growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "growth_rate": "double",
        },
        "not_null": ["period", "economy_code", "economy"],
        "min_rows": 1000,
    })


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
    sync_metadata(DATASET_ID, METADATA)
