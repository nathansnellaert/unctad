"""Fetch Seaborne Trade from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.SeaborneTrade"
DATASET_ID = "unctad_seaborne_trade"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("seaborne_trade", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
