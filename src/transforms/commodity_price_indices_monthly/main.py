import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, parse_month
from .test import test

DATASET_ID = "unctad_commodity_price_indices_monthly"

METADATA = {
    "id": DATASET_ID,
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
