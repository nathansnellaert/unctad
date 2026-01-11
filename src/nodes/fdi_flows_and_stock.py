"""Download and transform FdiFlowsStock."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.FdiFlowsStock"
DATASET_ID = "unctad_fdi_flows_and_stock"


def download():
    """Download US.FdiFlowsStock from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "fdi_flows_and_stock")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_fdi_flows_and_stock."""
    table = load_raw_parquet("fdi_flows_and_stock")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
