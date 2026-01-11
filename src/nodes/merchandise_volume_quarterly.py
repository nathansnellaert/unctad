"""Download and transform MerchVolumeQuarterly."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.MerchVolumeQuarterly"
DATASET_ID = "unctad_merchandise_volume_quarterly"


def download():
    """Download US.MerchVolumeQuarterly from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_volume_quarterly")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_volume_quarterly."""
    table = load_raw_parquet("merchandise_volume_quarterly")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
