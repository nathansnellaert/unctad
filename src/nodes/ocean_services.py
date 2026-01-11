"""Fetch Ocean Services from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.OceanServices"
DATASET_ID = "unctad_ocean_services"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ocean_services", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
