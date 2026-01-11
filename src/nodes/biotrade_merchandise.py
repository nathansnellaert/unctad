"""Download and transform BiotradeMerch."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.BiotradeMerch"
DATASET_ID = "unctad_biotrade_merchandise"


def download():
    """Download US.BiotradeMerch from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "biotrade_merchandise")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_biotrade_merchandise."""
    table = load_raw_parquet("biotrade_merchandise")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
