"""Download and transform VesselValueByOwnership."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.VesselValueByOwnership"
DATASET_ID = "unctad_vessel_value_by_ownership"


def download():
    """Download US.VesselValueByOwnership from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "vessel_value_by_ownership")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_vessel_value_by_ownership."""
    table = load_raw_parquet("vessel_value_by_ownership")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
