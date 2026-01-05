import pyarrow as pa
from subsets_utils import upload_data, publish
from utils import load_raw, parse_value
from .test import test

DATASET_ID = "unctad_food_trade_rca_by_processing"

METADATA = {
    "id": DATASET_ID,
    "title": "UNCTAD Food Trade RCA by Processing",
    "description": "",  # TODO: Add description after profiling
    "column_descriptions": {},  # TODO: Add column descriptions after profiling
}


def run():
    """Transform food_trade_rca_by_processing data."""
    raise NotImplementedError("Transform not yet implemented - profile raw data first")

    # raw = load_raw("food_trade_rca_by_processing")
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
