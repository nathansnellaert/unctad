"""Fetch Merchandise Terms Of Trade from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.TermsOfTrade"
DATASET_ID = "unctad_merchandise_terms_of_trade"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_terms_of_trade", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
