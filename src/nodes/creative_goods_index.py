"""Download and transform CreativeGoodsIndex."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CreativeGoodsIndex"
DATASET_ID = "unctad_creative_goods_index"


def download():
    """Download US.CreativeGoodsIndex from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "creative_goods_index")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_creative_goods_index."""
    table = load_raw_parquet("creative_goods_index")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
