import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate GDP growth rates dataset."""
    validate(table, {
        "columns": {
            "period": "string",
            "economy_code": "string",
            "economy": "string",
            "growth_rate": "double",
            "growth_rate_per_capita": "double",
        },
        "not_null": ["period", "economy_code", "economy"],
        "min_rows": 1000,
    })
