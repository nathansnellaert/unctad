"""Fetch Merchant Fleet from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.MerchantFleet"
DATASET_ID = "unctad_merchant_fleet"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchant_fleet", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
