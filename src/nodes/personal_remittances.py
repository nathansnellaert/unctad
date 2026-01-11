"""Transform Personal Remittances data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_personal_remittances"

METADATA = {
    "title": "UNCTAD Personal Remittances",
    "description": "Personal remittances by economy and flow direction (inward/outward). Values in US$ millions and as percentage of GDP.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Flow direction (Inward/Outward)",
        "value_usd_millions": "Personal remittances in US$ millions at current prices",
        "value_pct_gdp": "Personal remittances as percentage of GDP",
    },
}


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


def run():
    """Transform personal remittances data."""
    raw = load_raw("personal_remittances")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_gdp": parse_value(row.get("Percentage of Gross Domestic Product", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
