"""Download and transform CreativeGoodsValue."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CreativeGoodsValue"
DATASET_ID = "unctad_creative_goods_value"


def download():
    """Download US.CreativeGoodsValue from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "creative_goods_value")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_creative_goods_value."""
    table = load_raw_parquet("creative_goods_value")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
