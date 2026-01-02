import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_range


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
