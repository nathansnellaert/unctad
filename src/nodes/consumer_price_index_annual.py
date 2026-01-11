"""Fetch Consumer Price Index Annual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.Cpi_A"
DATASET_ID = "unctad_consumer_price_index_annual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("consumer_price_index_annual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
