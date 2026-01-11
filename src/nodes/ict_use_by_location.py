"""Download and transform IctUseLocation."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.IctUseLocation"
DATASET_ID = "unctad_ict_use_by_location"


def download():
    """Download US.IctUseLocation from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ict_use_by_location")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ict_use_by_location."""
    table = load_raw_parquet("ict_use_by_location")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
