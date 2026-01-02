import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year


def test(table: pa.Table) -> None:
    """Validate population structure by age and gender dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "sex": "string",
            "age_class": "string",
            "population_thousands": "double",
        },
        "not_null": ["year", "economy_code", "economy", "sex", "age_class"],
        "min_rows": 10000,
    })

    assert_valid_year(table, "year")
