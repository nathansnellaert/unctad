"""Download and transform MerchantFleet."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.MerchantFleet"
DATASET_ID = "unctad_merchant_fleet"


def download():
    """Download US.MerchantFleet from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchant_fleet")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchant_fleet."""
    table = load_raw_parquet("merchant_fleet")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
