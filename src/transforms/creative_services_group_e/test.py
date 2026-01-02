import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate creative services group E dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "service_code": "string",
            "service": "string",
            "value_usd_millions": "double",
            "growth_rate_yoy": "double",
        },
        "not_null": ["year", "economy_code", "economy", "service_code", "service"],
        "min_rows": 100,
    })

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 2000, f"Years before 2000 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"
