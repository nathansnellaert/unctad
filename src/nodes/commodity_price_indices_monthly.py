"""Download and transform CommodityPriceIndices_M."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CommodityPriceIndices_M"
DATASET_ID = "unctad_commodity_price_indices_monthly"


def download():
    """Download US.CommodityPriceIndices_M from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "commodity_price_indices_monthly")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_commodity_price_indices_monthly."""
    table = load_raw_parquet("commodity_price_indices_monthly")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
