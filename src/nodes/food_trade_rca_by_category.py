"""Fetch Food Trade Rca By Category from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TradeFoodProcCat_Cat_RCA"
DATASET_ID = "unctad_food_trade_rca_by_category"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("food_trade_rca_by_category", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
