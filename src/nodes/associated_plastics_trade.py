"""Fetch Associated Plastics Trade from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.AssociatedPlasticsTradebyPartner"
DATASET_ID = "unctad_associated_plastics_trade"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("associated_plastics_trade", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
