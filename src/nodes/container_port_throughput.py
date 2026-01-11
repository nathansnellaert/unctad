"""Fetch Container Port Throughput from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.ContPortThroughput"
DATASET_ID = "unctad_container_port_throughput"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("container_port_throughput", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
