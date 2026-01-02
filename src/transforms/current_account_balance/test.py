import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate current account balance dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "value_usd_millions": "double",
            "value_pct_gdp": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 1950, f"Years before 1950 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"
