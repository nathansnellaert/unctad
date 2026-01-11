"""Download and transform PortCalls_S."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PortCalls_S"
DATASET_ID = "unctad_port_calls_ships"


def download():
    """Download US.PortCalls_S from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "port_calls_ships")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_port_calls_ships."""
    table = load_raw_parquet("port_calls_ships")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
