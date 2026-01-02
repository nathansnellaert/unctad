import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate merchandise structural indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "flow": "string",
            "product": "string",
            "concentration_index": "double",
            "structural_change_index": "double",
        },
        "not_null": ["year", "flow", "product"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
