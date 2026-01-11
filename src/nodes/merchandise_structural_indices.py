"""Transform Merchandise Structural Indices data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_merchandise_structural_indices"

METADATA = {
    "title": "UNCTAD Merchandise Structural Indices",
    "description": "Merchandise trade structural indices by flow direction and product category.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "flow": "Trade flow (Exports/Imports)",
        "product": "Product category",
        "concentration_index": "Concentration index",
        "structural_change_index": "Structural change index (1995=0)",
    },
}


def test(table: pa.Table) -> None:
    """Validate merchandise structural indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "flow": "string",
            "product": "string",
            "concentration_index": "double",
            "structural_change_index": "double",
        },
        "not_null": ["year", "flow", "product"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform merchandise structural indices data."""
    raw = load_raw("merchandise_structural_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "flow": row.get("Flow Label", ""),
            "product": row.get("Product Label", ""),
            "concentration_index": parse_value(row.get("Concentration Index", "")),
            "structural_change_index": parse_value(row.get("Structural Change Index (1995=0)", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
