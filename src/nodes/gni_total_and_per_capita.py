"""Fetch Gni Total And Per Capita from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.GNI"
DATASET_ID = "unctad_gni_total_and_per_capita"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("gni_total_and_per_capita", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
