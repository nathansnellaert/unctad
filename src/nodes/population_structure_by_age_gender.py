"""Fetch Population Structure By Age Gender from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PopAgeStruct"
DATASET_ID = "unctad_population_structure_by_age_gender"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("population_structure_by_age_gender", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
