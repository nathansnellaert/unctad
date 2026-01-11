"""Download and transform BiotradeMerchRCA."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.BiotradeMerchRCA"
DATASET_ID = "unctad_biotrade_rca"


def download():
    """Download US.BiotradeMerchRCA from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "biotrade_rca")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_biotrade_rca."""
    table = load_raw_parquet("biotrade_rca")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
