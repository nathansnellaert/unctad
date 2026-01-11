"""Fetch Current Account Balance from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.CurrAccBalance"
DATASET_ID = "unctad_current_account_balance"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("current_account_balance", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
