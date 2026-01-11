"""Fetch Government Expenditures from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.GovExpenditures"
DATASET_ID = "unctad_government_expenditures"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("government_expenditures", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
