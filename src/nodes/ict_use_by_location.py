"""Fetch Ict Use By Location from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.IctUseLocation"
DATASET_ID = "unctad_ict_use_by_location"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ict_use_by_location", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
