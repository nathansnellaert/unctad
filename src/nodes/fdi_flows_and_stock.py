"""Fetch Fdi Flows And Stock from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.FdiFlowsStock"
DATASET_ID = "unctad_fdi_flows_and_stock"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("fdi_flows_and_stock", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
