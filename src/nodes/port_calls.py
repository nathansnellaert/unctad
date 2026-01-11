"""Fetch Port Calls from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PortCalls"
DATASET_ID = "unctad_port_calls"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("port_calls", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
