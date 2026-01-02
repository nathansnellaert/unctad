import pyarrow as pa
from subsets_utils import upload_data, publish
from transforms.common import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_fleet_beneficial_owners"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Fleet Beneficial Owners",
    "description": "",  # TODO: Add description after profiling
    "column_descriptions": {},  # TODO: Add column descriptions after profiling
}


def run():
    """Transform fleet_beneficial_owners data."""
    raise NotImplementedError("Transform not yet implemented - profile raw data first")

    # raw = load_raw("fleet_beneficial_owners")
    #
    # records = []
    # for row in raw:
    #     records.append({
    #         # TODO: Map columns after profiling
    #     })
    #
    # table = pa.Table.from_pylist(records)
    # print(f"  Transformed {len(table):,} records")
    #
    # test(table)
    #
    # upload_data(table, DATASET_ID)
    # publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
