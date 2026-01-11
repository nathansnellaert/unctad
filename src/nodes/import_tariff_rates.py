"""Fetch Import Tariff Rates from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.Tariff"
DATASET_ID = "unctad_import_tariff_rates"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("import_tariff_rates", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
