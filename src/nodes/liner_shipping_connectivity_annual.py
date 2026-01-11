"""Download and transform LSCI."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.LSCI"
DATASET_ID = "unctad_liner_shipping_connectivity_annual"


def download():
    """Download US.LSCI from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "liner_shipping_connectivity_annual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_liner_shipping_connectivity_annual."""
    table = load_raw_parquet("liner_shipping_connectivity_annual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
