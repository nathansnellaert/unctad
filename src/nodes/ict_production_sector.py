"""Fetch Ict Production Sector from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.IctProductionSector"
DATASET_ID = "unctad_ict_production_sector"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ict_production_sector", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
