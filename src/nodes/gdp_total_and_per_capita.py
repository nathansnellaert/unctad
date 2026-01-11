"""Transform Gdp Total And Per Capita data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_gdp_total_and_per_capita"

METADATA = {
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


def test(table: pa.Table) -> None:
    """Validate GDP total and per capita dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "gdp_current_usd_millions": "double",
            "gdp_per_capita_current_usd": "double",
            "gdp_constant_2015_usd_millions": "double",
            "gdp_per_capita_constant_2015_usd": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 1950, f"Years before 1950 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"


def run():
    """Transform GDP total and per capita data."""
    raw = load_raw("gdp_total_and_per_capita")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
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
    sync_metadata(DATASET_ID, METADATA)
