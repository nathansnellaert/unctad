"""Download and transform InclusiveGrowth."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.InclusiveGrowth"
DATASET_ID = "unctad_inclusive_growth"


def download():
    """Download US.InclusiveGrowth from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "inclusive_growth")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_inclusive_growth."""
    table = load_raw_parquet("inclusive_growth")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
