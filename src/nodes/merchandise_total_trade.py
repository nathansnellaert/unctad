"""Download and transform TradeMerchTotal."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeMerchTotal"
DATASET_ID = "unctad_merchandise_total_trade"


def download():
    """Download US.TradeMerchTotal from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_total_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_total_trade."""
    table = load_raw_parquet("merchandise_total_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
