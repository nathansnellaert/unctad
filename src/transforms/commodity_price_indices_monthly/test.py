import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month


def test(table: pa.Table) -> None:
    """Validate commodity price indices monthly dataset."""
    validate(table, {
        "columns": {
            "month": "string",
            "commodity_code": "string",
            "commodity": "string",
            "index_base_2015": "double",
            "growth_rate_yoy": "double",
            "growth_rate_period": "double",
        },
        "not_null": ["month", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_month(table, "month")
