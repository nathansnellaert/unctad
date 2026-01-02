import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate services by category dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "category": "string",
            "value_usd_millions": "double",
            "value_pct_world": "double",
            "value_pct_services": "double",
            "growth_rate": "double",
            "value_pct_gdp": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow", "category"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
