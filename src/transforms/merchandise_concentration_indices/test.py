import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_range


def test(table: pa.Table) -> None:
    """Validate merchandise concentration indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "num_products": "double",
            "concentration_index": "double",
            "diversification_index": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
    assert_in_range(table, "concentration_index", 0, 1)
    assert_in_range(table, "diversification_index", 0, 1)
