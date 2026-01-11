"""Download and transform ShipBuilding."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.ShipBuilding"
DATASET_ID = "unctad_ship_building"


def download():
    """Download US.ShipBuilding from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ship_building")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ship_building."""
    table = load_raw_parquet("ship_building")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
