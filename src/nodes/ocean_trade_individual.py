"""Fetch Ocean Trade Individual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.OceanTradeIndividualEconomies"
DATASET_ID = "unctad_ocean_trade_individual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ocean_trade_individual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
