"""Fetch Inclusive Growth from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.InclusiveGrowth"
DATASET_ID = "unctad_inclusive_growth"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("inclusive_growth", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
