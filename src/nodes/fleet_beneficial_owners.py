"""Download and transform FleetBeneficialOwners."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.FleetBeneficialOwners"
DATASET_ID = "unctad_fleet_beneficial_owners"


def download():
    """Download US.FleetBeneficialOwners from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "fleet_beneficial_owners")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_fleet_beneficial_owners."""
    table = load_raw_parquet("fleet_beneficial_owners")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
