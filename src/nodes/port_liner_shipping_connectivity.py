"""Fetch Port Liner Shipping Connectivity from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.PLSCI"
DATASET_ID = "unctad_port_liner_shipping_connectivity"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("port_liner_shipping_connectivity", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
