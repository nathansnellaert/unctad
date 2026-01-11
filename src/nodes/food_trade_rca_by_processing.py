"""Fetch Food Trade Rca By Processing from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TradeFoodProcCat_Proc_RCA"
DATASET_ID = "unctad_food_trade_rca_by_processing"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("food_trade_rca_by_processing", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
