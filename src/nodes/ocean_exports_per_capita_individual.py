"""Fetch Ocean Exports Per Capita Individual from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.OceanExportsPerCapitaIndividualEconomies"
DATASET_ID = "unctad_ocean_exports_per_capita_individual"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("ocean_exports_per_capita_individual", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
