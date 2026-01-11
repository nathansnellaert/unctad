"""Download and transform IctUseEconActivity."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.IctUseEconActivity"
DATASET_ID = "unctad_ict_use_by_activity"


def download():
    """Download US.IctUseEconActivity from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ict_use_by_activity")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ict_use_by_activity."""
    table = load_raw_parquet("ict_use_by_activity")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
