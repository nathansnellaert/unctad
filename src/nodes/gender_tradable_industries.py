"""Download and transform Gender_TradableIndustries."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.Gender_TradableIndustries"
DATASET_ID = "unctad_gender_tradable_industries"


def download():
    """Download US.Gender_TradableIndustries from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "gender_tradable_industries")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_gender_tradable_industries."""
    table = load_raw_parquet("gender_tradable_industries")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
