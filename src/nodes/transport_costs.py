"""Fetch Transport Costs from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TransportCosts"
DATASET_ID = "unctad_transport_costs"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("transport_costs", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
