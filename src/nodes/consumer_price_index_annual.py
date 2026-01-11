"""Download and transform Cpi_A."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.Cpi_A"
DATASET_ID = "unctad_consumer_price_index_annual"


def download():
    """Download US.Cpi_A from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "consumer_price_index_annual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_consumer_price_index_annual."""
    table = load_raw_parquet("consumer_price_index_annual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
