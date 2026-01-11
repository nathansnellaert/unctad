"""Download and transform UCPI_M."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.UCPI_M"
DATASET_ID = "unctad_unit_value_indices_monthly"


def download():
    """Download US.UCPI_M from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "unit_value_indices_monthly")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_unit_value_indices_monthly."""
    table = load_raw_parquet("unit_value_indices_monthly")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
