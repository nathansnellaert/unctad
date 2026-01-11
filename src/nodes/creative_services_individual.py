"""Fetch Creative Services Individual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.CreativeServ_Indiv_Tot"
DATASET_ID = "unctad_creative_services_individual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("creative_services_individual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
