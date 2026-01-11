"""Fetch Trade Facilitation Index from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.FTRI"
DATASET_ID = "unctad_trade_facilitation_index"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("trade_facilitation_index", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
