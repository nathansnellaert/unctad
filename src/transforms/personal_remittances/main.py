import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_personal_remittances"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform personal remittances data."""
    raw = load_raw("personal_remittances")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
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
