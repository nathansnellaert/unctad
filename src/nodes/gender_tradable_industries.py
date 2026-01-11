"""Fetch Gender Tradable Industries from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.Gender_TradableIndustries"
DATASET_ID = "unctad_gender_tradable_industries"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("gender_tradable_industries", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
