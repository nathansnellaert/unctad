"""Transform Merchandise Concentration Indices data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_range

DATASET_ID = "unctad_merchandise_concentration_indices"

METADATA = {
    "title": "UNCTAD Merchandise Concentration and Diversification Indices",
    "description": "Trade concentration and diversification indices by economy and flow direction.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "num_products": "Number of products traded",
        "concentration_index": "Herfindahl-Hirschmann concentration index (0-1)",
        "diversification_index": "Diversification index (0-1)",
    },
}


def test(table: pa.Table) -> None:
    """Validate merchandise concentration indices dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "num_products": "double",
            "concentration_index": "double",
            "diversification_index": "double",
        },
        "not_null": ["year", "economy_code", "economy", "flow"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")
    assert_in_range(table, "concentration_index", 0, 1)
    assert_in_range(table, "diversification_index", 0, 1)


def run():
    """Transform merchandise concentration indices data."""
    raw = load_raw("merchandise_concentration_indices")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "num_products": parse_value(row.get("Number of products", "")),
            "concentration_index": parse_value(row.get("Concentration Index", "")),
            "diversification_index": parse_value(row.get("Diversification Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
