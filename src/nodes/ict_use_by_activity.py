"""Fetch Ict Use By Activity from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.IctUseEconActivity"
DATASET_ID = "unctad_ict_use_by_activity"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ict_use_by_activity", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
