"""Fetch Unit Value Indices Annual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.UCPI_A"
DATASET_ID = "unctad_unit_value_indices_annual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("unit_value_indices_annual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
