"""Fetch Vessel Value By Ownership from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.VesselValueByOwnership"
DATASET_ID = "unctad_vessel_value_by_ownership"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("vessel_value_by_ownership", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
