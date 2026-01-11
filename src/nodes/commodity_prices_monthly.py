"""Fetch Commodity Prices Monthly from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.CommodityPrice_M"
DATASET_ID = "unctad_commodity_prices_monthly"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("commodity_prices_monthly", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
