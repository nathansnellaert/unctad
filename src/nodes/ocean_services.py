"""Download and transform OceanServices."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.OceanServices"
DATASET_ID = "unctad_ocean_services"


def download():
    """Download US.OceanServices from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ocean_services")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ocean_services."""
    table = load_raw_parquet("ocean_services")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
