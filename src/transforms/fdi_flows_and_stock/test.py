import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate FDI flows and stock dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow_type": "string",
            "direction": "string",
            "value_usd_millions": "double",
            "value_pct_world": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow_type", "direction"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
