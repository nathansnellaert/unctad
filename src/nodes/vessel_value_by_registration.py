"""Fetch Vessel Value By Registration from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.VesselValueByRegistration"
DATASET_ID = "unctad_vessel_value_by_registration"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("vessel_value_by_registration", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
