"""Download and transform VesselValueByRegistration."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.VesselValueByRegistration"
DATASET_ID = "unctad_vessel_value_by_registration"


def download():
    """Download US.VesselValueByRegistration from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "vessel_value_by_registration")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_vessel_value_by_registration."""
    table = load_raw_parquet("vessel_value_by_registration")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
