"""Download and transform BiotradeMerchShare."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.BiotradeMerchShare"
DATASET_ID = "unctad_biotrade_share"


def download():
    """Download US.BiotradeMerchShare from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "biotrade_share")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_biotrade_share."""
    table = load_raw_parquet("biotrade_share")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
