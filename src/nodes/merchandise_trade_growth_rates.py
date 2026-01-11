"""Fetch Merchandise Trade Growth Rates from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TradeMerchGR"
DATASET_ID = "unctad_merchandise_trade_growth_rates"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_trade_growth_rates", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
