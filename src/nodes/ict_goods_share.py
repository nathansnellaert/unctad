"""Fetch Ict Goods Share from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.IctGoodsShare"
DATASET_ID = "unctad_ict_goods_share"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ict_goods_share", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
