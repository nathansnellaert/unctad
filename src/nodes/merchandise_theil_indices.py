"""Download and transform MerchTheilIndices."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.MerchTheilIndices"
DATASET_ID = "unctad_merchandise_theil_indices"


def download():
    """Download US.MerchTheilIndices from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_theil_indices")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_theil_indices."""
    table = load_raw_parquet("merchandise_theil_indices")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
