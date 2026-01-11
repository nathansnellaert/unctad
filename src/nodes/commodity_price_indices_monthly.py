"""Transform Commodity Price Indices Monthly data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, parse_month
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month

DATASET_ID = "unctad_commodity_price_indices_monthly"

METADATA = {
    "title": "UNCTAD Commodity Price Indices (Monthly)",
    "description": "Monthly commodity price indices with base year 2015=100, year-on-year and period growth rates.",
    "column_descriptions": {
        "month": "Month (YYYY-MM)",
        "commodity_code": "UNCTAD commodity code",
        "commodity": "Commodity name",
        "index_base_2015": "Price index with base 2015=100",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
        "growth_rate_period": "Growth rate over previous period (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate commodity price indices monthly dataset."""
    validate(table, {
        "columns": {
            "month": "string",
            "commodity_code": "string",
            "commodity": "string",
            "index_base_2015": "double",
            "growth_rate_yoy": "double",
            "growth_rate_period": "double",
        },
        "not_null": ["month", "commodity_code", "commodity"],
        "min_rows": 100,
    })

    assert_valid_month(table, "month")


def run():
    """Transform commodity price indices monthly data."""
    raw = load_raw("commodity_price_indices_monthly")

    records = []
    for row in raw:
        records.append({
            "month": parse_month(row["Period Label"]),
            "commodity_code": row["Commodity"],
            "commodity": row["Commodity Label"],
            "index_base_2015": parse_value(row.get("Index Base 2015", "")),
            "growth_rate_yoy": parse_value(row.get("Growth rate, year-on-year", "")),
            "growth_rate_period": parse_value(row.get("Growth rate (over previous period)", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
