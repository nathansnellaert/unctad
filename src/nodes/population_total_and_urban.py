"""Fetch Population Total And Urban from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PopTotal"
DATASET_ID = "unctad_population_total_and_urban"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("population_total_and_urban", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
