"""Fetch Merchandise Structural Indices from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.ConcentStructIndices"
DATASET_ID = "unctad_merchandise_structural_indices"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_structural_indices", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
