"""Fetch Gender Domestic Value Added from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.Gender_DomesticValueAdded"
DATASET_ID = "unctad_gender_domestic_value_added"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("gender_domestic_value_added", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
