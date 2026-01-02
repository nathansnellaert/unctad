import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate merchandise trade growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "growth_rate": "double",
        },
        "not_null": ["period", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })
