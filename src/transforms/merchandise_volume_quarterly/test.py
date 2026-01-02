import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_quarter


def test(table: pa.Table) -> None:
    """Validate merchandise volume quarterly dataset."""
    validate(table, {
        "columns": {
            "quarter": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "growth_rate_period": "double",
            "growth_rate_yoy": "double",
            "volume_index": "double",
            "volume_index_sa": "double",
            "growth_rate_period_sa": "double",
        },
        "not_null": ["quarter", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })

    assert_valid_quarter(table, "quarter")
