"""Fetch Creative Goods Value from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.CreativeGoodsValue"
DATASET_ID = "unctad_creative_goods_value"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("creative_goods_value", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
