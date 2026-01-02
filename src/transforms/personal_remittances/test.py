import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate personal remittances dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "value_usd_millions": "double",
            "value_pct_gdp": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")

    flows = set(table.column("flow").to_pylist())
    assert len(flows) > 0, "Expected at least one flow direction"
