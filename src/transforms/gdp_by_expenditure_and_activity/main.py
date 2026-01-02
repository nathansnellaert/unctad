import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_gdp_by_expenditure_and_activity"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD GDP by Expenditure and Activity",
    "description": "GDP breakdown by expenditure component and economic activity by economy.",
    "column_descriptions": {
        "year": "Year (YYYY)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "component": "GDP component (expenditure or activity)",
        "value_current_usd_millions": "Value in US$ millions at current prices",
        "value_constant_2015_usd_millions": "Value in US$ millions at constant 2015 prices",
        "value_pct_gdp": "Value as percentage of GDP",
    },
}


def run():
    """Transform GDP by expenditure and activity data."""
    raw = load_raw("gdp_by_expenditure_and_activity")

    records = []
    for row in raw:
        records.append({
            "year": str(row["Year"]),
            "economy_code": row["Economy"],
            "economy": row["Economy Label"],
            "component": row.get("Component Label", ""),
            "value_current_usd_millions": parse_value(row.get("US$ at current prices in millions", "")),
            "value_constant_2015_usd_millions": parse_value(row.get("US$ at constant prices (2015) in millions", "")),
            "value_pct_gdp": parse_value(row.get("Percentage of Gross Domestic Product", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
