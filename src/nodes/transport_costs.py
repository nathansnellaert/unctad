"""Download and transform TransportCosts."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TransportCosts"
DATASET_ID = "unctad_transport_costs"


def download():
    """Download US.TransportCosts from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "transport_costs")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_transport_costs."""
    table = load_raw_parquet("transport_costs")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
