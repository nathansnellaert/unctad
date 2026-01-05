import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, to_str
from .test import test

DATASET_ID = "unctad_current_account_balance"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Current Account Balance",
    "description": "Current account balance by economy. Values in US$ millions at current prices and as percentage of GDP.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "value_usd_millions": "Current account balance in US$ millions at current prices",
        "value_pct_gdp": "Current account balance as percentage of GDP",
    },
}


def run():
    """Transform current account balance data."""
    raw = load_raw("current_account_balance")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "value_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_pct_gdp": parse_value(row.get("Percentage of Gross Domestic Product", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
