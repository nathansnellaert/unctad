"""Fetch Fleet Beneficial Owners from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.FleetBeneficialOwners"
DATASET_ID = "unctad_fleet_beneficial_owners"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("fleet_beneficial_owners", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
