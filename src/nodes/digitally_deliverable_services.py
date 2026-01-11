"""Fetch Digitally Deliverable Services from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.DigitallyDeliverableServices"
DATASET_ID = "unctad_digitally_deliverable_services"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("digitally_deliverable_services", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
