"""Fetch Merchandise Theil Indices from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.MerchTheilIndices"
DATASET_ID = "unctad_merchandise_theil_indices"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_theil_indices", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
