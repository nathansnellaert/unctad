"""Fetch Commodity Prices Annual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.CommodityPrice_A"
DATASET_ID = "unctad_commodity_prices_annual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("commodity_prices_annual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
