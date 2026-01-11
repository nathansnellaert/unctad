"""Download and transform ContPortThroughput."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.ContPortThroughput"
DATASET_ID = "unctad_container_port_throughput"


def download():
    """Download US.ContPortThroughput from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "container_port_throughput")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_container_port_throughput."""
    table = load_raw_parquet("container_port_throughput")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
