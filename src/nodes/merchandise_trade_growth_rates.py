"""Download and transform TradeMerchGR."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeMerchGR"
DATASET_ID = "unctad_merchandise_trade_growth_rates"


def download():
    """Download US.TradeMerchGR from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_trade_growth_rates")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_trade_growth_rates."""
    table = load_raw_parquet("merchandise_trade_growth_rates")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
