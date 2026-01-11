"""Fetch Ship Building from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.ShipBuilding"
DATASET_ID = "unctad_ship_building"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ship_building", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
