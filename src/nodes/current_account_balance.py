"""Download and transform CurrAccBalance."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CurrAccBalance"
DATASET_ID = "unctad_current_account_balance"


def download():
    """Download US.CurrAccBalance from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "current_account_balance")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_current_account_balance."""
    table = load_raw_parquet("current_account_balance")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
