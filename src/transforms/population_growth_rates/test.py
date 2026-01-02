import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate population growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "growth_rate": "double",
        },
        "not_null": ["period", "economy_code", "economy"],
        "min_rows": 1000,
    })
