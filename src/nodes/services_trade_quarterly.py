"""Transform Services Trade Quarterly data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, parse_quarter, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_quarter

DATASET_ID = "unctad_services_trade_quarterly"

METADATA = {
    "title": "UNCTAD Services Trade (Quarterly)",
    "description": "Quarterly international trade in services by economy, flow direction, and category.",
    "column_descriptions": {
        "quarter": "Quarter (YYYY-QN)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "category": "Service category",
        "value_usd_millions": "Value in US$ millions at current prices",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
        "value_usd_millions_sa": "Value in US$ millions, seasonally adjusted",
        "growth_rate_period_sa": "Growth rate over previous period, seasonally adjusted (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate services trade quarterly dataset."""
    validate(table, {
        "columns": {
            "quarter": "string",
            "economy_code": "string",
            "economy": "string",
            "flow": "string",
            "category": "string",
            "value_usd_millions": "double",
            "growth_rate_yoy": "double",
            "value_usd_millions_sa": "double",
            "growth_rate_period_sa": "double",
        },
        "not_null": ["quarter", "economy_code", "economy", "flow", "category"],
        "min_rows": 1000,
    })

    assert_valid_quarter(table, "quarter")


def run():
    """Transform services trade quarterly data."""
    raw = load_raw("services_trade_quarterly")

    records = []
    for row in raw:
        records.append({
            "quarter": parse_quarter(row["Period Label"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "category": row.get("Category Label", ""),
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "growth_rate_yoy": parse_value(row.get("Growth rate, year-on-year", "")),
            "value_usd_millions_sa": parse_value(row.get("US$ at current prices, seasonally adjusted, in millions", "")),
            "growth_rate_period_sa": parse_value(row.get("Growth rate (over previous period), seasonally adjusted", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
