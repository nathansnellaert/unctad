"""Download and transform TermsOfTrade."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TermsOfTrade"
DATASET_ID = "unctad_merchandise_terms_of_trade"


def download():
    """Download US.TermsOfTrade from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_terms_of_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_terms_of_trade."""
    table = load_raw_parquet("merchandise_terms_of_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
