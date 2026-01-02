import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value, parse_quarter
from .test import test

DATASET_ID = "unctad_services_trade_quarterly"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform services trade quarterly data."""
    raw = load_raw("services_trade_quarterly")

    records = []
    for row in raw:
        records.append({
            "quarter": parse_quarter(row["Period Label"]),
            "economy_code": row["Economy"],
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
