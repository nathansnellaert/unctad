"""Fetch Population Dependency Ratios from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PopDependency"
DATASET_ID = "unctad_population_dependency_ratios"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("population_dependency_ratios", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
