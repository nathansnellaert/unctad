"""Download and transform IctProductionSector."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.IctProductionSector"
DATASET_ID = "unctad_ict_production_sector"


def download():
    """Download US.IctProductionSector from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ict_production_sector")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ict_production_sector."""
    table = load_raw_parquet("ict_production_sector")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
