import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_quarter


def test(table: pa.Table) -> None:
    """Validate services trade quarterly dataset."""
    validate(table, {
        "columns": {
            "quarter": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "category": "string",
            "value_usd_millions": "double",
            "growth_rate_yoy": "double",
            "value_usd_millions_sa": "double",
            "growth_rate_period_sa": "double",
        },
        "not_null": ["quarter", "economy_code", "economy", "flow", "category"],
        "min_rows": 1000,
    })

    assert_valid_quarter(table, "quarter")
