"""Transform Import Tariff Rates data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value, to_str
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year

DATASET_ID = "unctad_import_tariff_rates"

METADATA = {
    "title": "UNCTAD Import Tariff Rates",
    "description": "Import tariff rates by market, origin, duty type, and product category.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "duty_type": "Type of duty (MFN, preferential, etc.)",
        "market_code": "Importing market code",
        "market": "Importing market name",
        "origin_code": "Origin economy code",
        "origin": "Origin economy name",
        "product_category": "Product category",
        "simple_avg_rate": "Simple average tariff rate (%)",
        "weighted_avg_rate": "Trade-weighted average tariff rate (%)",
    },
}


def test(table: pa.Table) -> None:
    """Validate import tariff rates dataset."""
    validate(table, {
        "columns": {
            "year": "string",
            "duty_type": "string",
            "market_code": "string",
            "market": "string",
            "origin_code": "string",
            "origin": "string",
            "product_category": "string",
            "simple_avg_rate": "double",
            "weighted_avg_rate": "double",
        },
        "not_null": ["year", "duty_type", "market_code", "market"],
        "min_rows": 1000,
    })

    assert_valid_year(table, "year")


def run():
    """Transform import tariff rates data."""
    raw = load_raw("import_tariff_rates")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "duty_type": row.get("DutyType Label", ""),
            "market_code": to_str(row.get("Market", "")),
            "market": row.get("Market Label", ""),
            "origin_code": to_str(row.get("Origin", "")),
            "origin": row.get("Origin Label", ""),
            "product_category": row.get("ProductCategory Label", ""),
            "simple_avg_rate": parse_value(row.get("Simple average of rates", "")),
            "weighted_avg_rate": parse_value(row.get("Weighted average of rates", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    sync_metadata(DATASET_ID, METADATA)
