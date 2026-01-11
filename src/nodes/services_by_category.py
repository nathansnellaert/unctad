"""Download and transform TradeServCatByPartner."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeServCatByPartner"
DATASET_ID = "unctad_services_by_category"


def download():
    """Download US.TradeServCatByPartner from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "services_by_category")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_services_by_category."""
    table = load_raw_parquet("services_by_category")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
