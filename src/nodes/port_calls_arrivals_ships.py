"""Fetch Port Calls Arrivals Ships from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PortCallsArrivals_S"
DATASET_ID = "unctad_port_calls_arrivals_ships"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("port_calls_arrivals_ships", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
