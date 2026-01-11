"""Fetch Biotrade Concentration from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.BioTradeMerchProdConcent"
DATASET_ID = "unctad_biotrade_concentration"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("biotrade_concentration", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
