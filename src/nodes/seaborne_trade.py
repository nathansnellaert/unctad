"""Download and transform SeaborneTrade."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.SeaborneTrade"
DATASET_ID = "unctad_seaborne_trade"


def download():
    """Download US.SeaborneTrade from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "seaborne_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_seaborne_trade."""
    table = load_raw_parquet("seaborne_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
