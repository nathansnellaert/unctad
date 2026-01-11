"""Download and transform PLSCI."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PLSCI"
DATASET_ID = "unctad_port_liner_shipping_connectivity"


def download():
    """Download US.PLSCI from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "port_liner_shipping_connectivity")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_port_liner_shipping_connectivity."""
    table = load_raw_parquet("port_liner_shipping_connectivity")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
