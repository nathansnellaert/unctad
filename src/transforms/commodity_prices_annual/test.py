import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate commodity prices annual dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "commodity_code": "string",
            "commodity": "string",
            "price": "double",
        },
        "not_null": ["year", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_year(table, "year")
