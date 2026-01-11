"""Download and transform NonPlasticSubstsTradeByPartner."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.NonPlasticSubstsTradeByPartner"
DATASET_ID = "unctad_non_plastic_substitutes_trade"


def download():
    """Download US.NonPlasticSubstsTradeByPartner from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "non_plastic_substitutes_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_non_plastic_substitutes_trade."""
    table = load_raw_parquet("non_plastic_substitutes_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
