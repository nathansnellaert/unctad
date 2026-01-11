"""Fetch Goods And Services Balance Bpm6 from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.GoodsAndServBalanceBpm6"
DATASET_ID = "unctad_goods_and_services_balance_bpm6"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("goods_and_services_balance_bpm6", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
