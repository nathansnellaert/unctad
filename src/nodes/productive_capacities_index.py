"""Download and transform PCI."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PCI"
DATASET_ID = "unctad_productive_capacities_index"


def download():
    """Download US.PCI from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "productive_capacities_index")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_productive_capacities_index."""
    table = load_raw_parquet("productive_capacities_index")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
