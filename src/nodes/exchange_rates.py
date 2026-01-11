"""Fetch Exchange Rates from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.ExchangeRateCrosstab"
DATASET_ID = "unctad_exchange_rates"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("exchange_rates", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
