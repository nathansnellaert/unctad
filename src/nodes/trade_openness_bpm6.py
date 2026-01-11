"""Fetch Trade Openness Bpm6 from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.GoodsAndServTradeOpennessBpm6"
DATASET_ID = "unctad_trade_openness_bpm6"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("trade_openness_bpm6", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
