"""Download and transform UCPI_A."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.UCPI_A"
DATASET_ID = "unctad_unit_value_indices_annual"


def download():
    """Download US.UCPI_A from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "unit_value_indices_annual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_unit_value_indices_annual."""
    table = load_raw_parquet("unit_value_indices_annual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
