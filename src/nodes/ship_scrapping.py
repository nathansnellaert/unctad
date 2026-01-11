"""Download and transform ShipScrapping."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.ShipScrapping"
DATASET_ID = "unctad_ship_scrapping"


def download():
    """Download US.ShipScrapping from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ship_scrapping")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ship_scrapping."""
    table = load_raw_parquet("ship_scrapping")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
