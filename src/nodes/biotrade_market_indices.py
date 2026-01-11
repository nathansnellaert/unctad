"""Fetch Biotrade Market Indices from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.BioTradeMerchMarketIndices"
DATASET_ID = "unctad_biotrade_market_indices"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("biotrade_market_indices", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
