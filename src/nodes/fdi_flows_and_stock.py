"""Transform Fdi Flows And Stock data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_fdi_flows_and_stock"

METADATA = {
    "title": "UNCTAD Foreign Direct Investment Flows and Stock",
    "description": "Foreign direct investment flows and stock by economy, direction (inward/outward), and type (flows/stock). Values in US$ millions and as percentage of world total.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow_type": "Type of FDI measure (Flows/Stock)",
        "direction": "Direction (Inward/Outward)",
        "value_usd_millions": "Value in US$ millions at current prices",
        "value_pct_world": "Value as percentage of total world",
    },
}


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


def run():
    """Transform FDI flows and stock data."""
    raw = load_raw("fdi_flows_and_stock")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow_type": row.get("Flow Label", ""),
            "direction": row.get("Direction Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_world": parse_value(row.get("Percentage of total world", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
