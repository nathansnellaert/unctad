import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate merchandise terms of trade dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "index_type": "string",
            "index_value": "double",
        },
        "not_null": ["year", "economy_code", "economy", "index_type"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
