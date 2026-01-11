"""Transform Port Calls Ships data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate

DATASET_ID = "unctad_port_calls_ships"

METADATA = {
    "title": "UNCTAD Port Calls by Ships",
    "description": "",  # TODO: Add description after profiling
    "column_descriptions": {},  # TODO: Add column descriptions after profiling
}


def test(table: pa.Table) -> None:
    """Validate port_calls_ships dataset."""
    raise NotImplementedError("Test not yet implemented - implement after transform")

    # validate(table, {
    #     "columns": {
    #         # TODO: Add columns after profiling
    #     },
    #     "not_null": [],
    #     "min_rows": 100,
    # })


def run():
    """Transform port_calls_ships data."""
    raise NotImplementedError("Transform not yet implemented - profile raw data first")

    # raw = load_raw("port_calls_ships")
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
    # sync_metadata(DATASET_ID, METADATA)
