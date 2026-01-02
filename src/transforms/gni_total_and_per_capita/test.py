import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate GNI total and per capita dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "gni_current_usd_millions": "double",
            "gni_per_capita_current_usd": "double",
        },
        "not_null": ["year", "economy_code", "economy"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
