"""Download and transform CommodityPriceIndices_A."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CommodityPriceIndices_A"
DATASET_ID = "unctad_commodity_price_indices_annual"


def download():
    """Download US.CommodityPriceIndices_A from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "commodity_price_indices_annual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_commodity_price_indices_annual."""
    table = load_raw_parquet("commodity_price_indices_annual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
