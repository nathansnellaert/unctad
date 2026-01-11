"""Download and transform Seafarers."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.Seafarers"
DATASET_ID = "unctad_seafarers"


def download():
    """Download US.Seafarers from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "seafarers")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_seafarers."""
    table = load_raw_parquet("seafarers")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
