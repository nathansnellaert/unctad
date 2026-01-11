"""Download and transform CommodityPrice_A."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CommodityPrice_A"
DATASET_ID = "unctad_commodity_prices_annual"


def download():
    """Download US.CommodityPrice_A from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "commodity_prices_annual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_commodity_prices_annual."""
    table = load_raw_parquet("commodity_prices_annual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
