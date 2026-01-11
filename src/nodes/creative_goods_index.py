"""Transform Creative Goods Index data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_set

DATASET_ID = "unctad_creative_goods_index"

METADATA = {
    "title": "UNCTAD Creative Goods Concentration Index",
    "description": "Concentration index for creative goods trade by product category and trade flow (imports/exports).",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "flow": "Trade flow: Imports or Exports",
        "product_code": "Creative goods product code",
        "product": "Creative goods product category",
        "concentration_index": "Trade concentration index value",
    },
}


def test(table: pa.Table) -> None:
    """Validate creative goods index dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "flow": "string",
            "product_code": "string",
            "product": "string",
            "concentration_index": "double",
        },
        "not_null": ["year", "flow", "product_code", "product"],
        "min_rows": 100,
    })

    assert_in_set(table, "flow", {"Imports", "Exports"})

    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 2000, f"Years before 2000 are suspect: {min(years)}"
    assert max(years) <= 2030, f"Future years are suspect: {max(years)}"


def run():
    """Transform creative goods index data."""
    raw = load_raw("creative_goods_index")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "flow": row["Flow Label"],
            "product_code": row["Product"],
            "product": row["Product Label"],
            "concentration_index": parse_value(row.get("Concentration Index", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
