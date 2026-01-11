"""Transform Container Port Throughput data."""
import pyarrow as pa
from subsets_utils import upload_data, sync_metadata
from ..utils import load_raw, parse_value
from subsets_utils import validate

DATASET_ID = "unctad_container_port_throughput"

METADATA = {
    "title": "UNCTAD Container Port Throughput",
    "description": "",  # TODO: Add description after profiling
    "column_descriptions": {},  # TODO: Add column descriptions after profiling
}


def test(table: pa.Table) -> None:
    """Validate container_port_throughput dataset."""
    raise NotImplementedError("Test not yet implemented - implement after transform")

    # validate(table, {
    #     "columns": {
    #         # TODO: Add columns after profiling
    #     },
    #     "not_null": [],
    #     "min_rows": 100,
    # })


def run():
    """Transform container_port_throughput data."""
    raise NotImplementedError("Transform not yet implemented - profile raw data first")

    # raw = load_raw("container_port_throughput")
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
