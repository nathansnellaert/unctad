import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_set


def test(table: pa.Table) -> None:
    """Validate merchandise theil indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "series_code": "string",
            "series": "string",
            "index_value": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow", "series_code", "series"],
        "min_rows": 1000,
    })

    assert_in_set(table, "flow", {"Imports", "Exports"})

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 1950, f"Years before 1950 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"
