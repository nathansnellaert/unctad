"""Download and transform GNI."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.GNI"
DATASET_ID = "unctad_gni_total_and_per_capita"


def download():
    """Download US.GNI from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "gni_total_and_per_capita")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_gni_total_and_per_capita."""
    table = load_raw_parquet("gni_total_and_per_capita")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
