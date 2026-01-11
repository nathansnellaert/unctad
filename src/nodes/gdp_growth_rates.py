"""Transform Gdp Growth Rates data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate

DATASET_ID = "unctad_gdp_growth_rates"

METADATA = {
    "title": "UNCTAD GDP Growth Rates",
    "description": "GDP growth rates by economy and time period. Includes total GDP growth and per capita growth rates.",
    "column_descriptions": {
        "period": "Time period (e.g., 1970-1980, 2020-2024)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "growth_rate": "Annual average GDP growth rate (%)",
        "growth_rate_per_capita": "Annual average GDP growth rate per capita (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate GDP growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "growth_rate": "double",
            "growth_rate_per_capita": "double",
        },
        "not_null": ["period", "economy_code", "economy"],
        "min_rows": 1000,
    })


def run():
    """Transform GDP growth rates data."""
    raw = load_raw("gdp_growth_rates")

    records = []
    for row in raw:
        records.append({
            "period": row["Period Label"],
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "growth_rate": parse_value(row.get("Annual average growth rate", "")),
            "growth_rate_per_capita": parse_value(row.get("Annual average growth rate per capita", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
