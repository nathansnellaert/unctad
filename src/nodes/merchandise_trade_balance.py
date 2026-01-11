"""Download and transform TradeMerchBalance."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeMerchBalance"
DATASET_ID = "unctad_merchandise_trade_balance"


def download():
    """Download US.TradeMerchBalance from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_trade_balance")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_trade_balance."""
    table = load_raw_parquet("merchandise_trade_balance")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
