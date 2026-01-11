"""Transform Merchandise Theil Indices data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_set

DATASET_ID = "unctad_merchandise_theil_indices"

METADATA = {
    "title": "UNCTAD Merchandise Trade Theil Indices",
    "description": "Theil indices measuring product and market concentration for merchandise trade by economy.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow: Imports or Exports",
        "series_code": "Index series code",
        "series": "Index series name (Product concentration, Market concentration, or Overall)",
        "index_value": "Theil index value",
    },
}


def test(table: pa.Table) -> None:
    """Validate merchandise theil indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "series_code": "string",
            "series": "string",
            "index_value": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow", "series_code", "series"],
        "min_rows": 1000,
    })

    assert_in_set(table, "flow", {"Imports", "Exports"})

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 1950, f"Years before 1950 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"


def run():
    """Transform merchandise theil indices data."""
    raw = load_raw("merchandise_theil_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row["Flow Label"],
            "series_code": to_str(row["Series"]),
            "series": row["Series Label"],
            "index_value": parse_value(row.get("Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
