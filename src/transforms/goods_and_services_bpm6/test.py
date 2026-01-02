import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate goods and services BPM6 dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "series": "string",
            "flow": "string",
            "value_usd_millions": "double",
            "value_pct_total_trade": "double",
        },
        "not_null": ["year", "economy_code", "economy", "series", "flow"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
