"""Fetch Seafarers from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.Seafarers"
DATASET_ID = "unctad_seafarers"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("seafarers", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
