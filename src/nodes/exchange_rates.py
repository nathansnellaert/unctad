"""Download and transform ExchangeRateCrosstab."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.ExchangeRateCrosstab"
DATASET_ID = "unctad_exchange_rates"


def download():
    """Download US.ExchangeRateCrosstab from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "exchange_rates")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_exchange_rates."""
    table = load_raw_parquet("exchange_rates")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
