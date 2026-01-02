import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate GDP by expenditure and activity dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "component": "string",
            "value_current_usd_millions": "double",
            "value_constant_2015_usd_millions": "double",
            "value_pct_gdp": "double",
        },
        "not_null": ["year", "economy_code", "economy", "component"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
