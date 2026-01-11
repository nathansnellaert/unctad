"""Fetch Hidden Plastics Trade from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.HiddenPlasticsTradebyPartner"
DATASET_ID = "unctad_hidden_plastics_trade"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("hidden_plastics_trade", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
