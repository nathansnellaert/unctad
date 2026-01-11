"""Download and transform IctGoodsShare."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.IctGoodsShare"
DATASET_ID = "unctad_ict_goods_share"


def download():
    """Download US.IctGoodsShare from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ict_goods_share")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ict_goods_share."""
    table = load_raw_parquet("ict_goods_share")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
