"""Fetch Merchandise Trade Balance from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TradeMerchBalance"
DATASET_ID = "unctad_merchandise_trade_balance"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_trade_balance", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
