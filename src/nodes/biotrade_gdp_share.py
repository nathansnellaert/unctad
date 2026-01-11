"""Fetch Biotrade Gdp Share from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.BioTradeMerchGDPShare"
DATASET_ID = "unctad_biotrade_gdp_share"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("biotrade_gdp_share", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
