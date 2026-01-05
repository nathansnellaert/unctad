import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value, parse_quarter, to_str
from .test import test

DATASET_ID = "unctad_merchandise_volume_quarterly"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Merchandise Trade Volume (Quarterly)",
    "description": "Quarterly merchandise trade volume indices and growth rates by economy and flow direction.",
    "column_descriptions": {
        "quarter": "Quarter (YYYY-QN)",
        "economy_code": "UNCTAD economy code",
        "economy": "Economy name",
        "flow": "Trade flow (Exports/Imports)",
        "growth_rate_period": "Growth rate over previous period (%)",
        "growth_rate_yoy": "Year-on-year growth rate (%)",
        "volume_index": "Volume index (2005=100)",
        "volume_index_sa": "Volume index, seasonally adjusted (2005=100)",
        "growth_rate_period_sa": "Growth rate over previous period, seasonally adjusted (%)",
    },
}


def run():
    """Transform merchandise volume quarterly data."""
    raw = load_raw("merchandise_volume_quarterly")

    records = []
    for row in raw:
        records.append({
            "quarter": parse_quarter(row["Quarter Label"]),
            "economy_code": to_str(row["Economy"]),
            "economy": row["Economy Label"],
            "flow": row.get("Flow Label", ""),
            "growth_rate_period": parse_value(row.get("Growth rate (over previous period)", "")),
            "growth_rate_yoy": parse_value(row.get("Growth rate, year-on-year", "")),
            "volume_index": parse_value(row.get("Volume Index (2005=100)", "")),
            "volume_index_sa": parse_value(row.get("Volume Index, seasonally adjusted (2005=100)", "")),
            "growth_rate_period_sa": parse_value(row.get("Growth rate (over previous period), seasonally adjusted", "")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
