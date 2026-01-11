"""Fetch Productive Capacities Index from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PCI"
DATASET_ID = "unctad_productive_capacities_index"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("productive_capacities_index", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
