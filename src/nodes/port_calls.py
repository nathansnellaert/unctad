"""Download and transform PortCalls."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PortCalls"
DATASET_ID = "unctad_port_calls"


def download():
    """Download US.PortCalls from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "port_calls")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_port_calls."""
    table = load_raw_parquet("port_calls")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
