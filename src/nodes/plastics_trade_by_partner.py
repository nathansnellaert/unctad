"""Fetch Plastics Trade By Partner from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PlasticsTradebyPartner"
DATASET_ID = "unctad_plastics_trade_by_partner"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("plastics_trade_by_partner", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
