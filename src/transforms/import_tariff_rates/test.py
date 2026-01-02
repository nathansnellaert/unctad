import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate import tariff rates dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "duty_type": "string",
            "market_code": "string",
            "market": "string",
            "origin_code": "string",
            "origin": "string",
            "product_category": "string",
            "simple_avg_rate": "double",
            "weighted_avg_rate": "double",
        },
        "not_null": ["year", "duty_type", "market_code", "market"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
