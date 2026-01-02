import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month


def test(table: pa.Table) -> None:
    """Validate commodity prices monthly dataset."""
    validate(table, {
        "columns": {
            "month": "string",
            "commodity_code": "string",
            "commodity": "string",
            "price": "double",
        },
        "not_null": ["month", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_month(table, "month")
