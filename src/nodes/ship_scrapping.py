"""Fetch Ship Scrapping from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.ShipScrapping"
DATASET_ID = "unctad_ship_scrapping"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ship_scrapping", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
