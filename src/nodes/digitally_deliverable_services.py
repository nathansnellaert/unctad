"""Download and transform DigitallyDeliverableServices."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.DigitallyDeliverableServices"
DATASET_ID = "unctad_digitally_deliverable_services"


def download():
    """Download US.DigitallyDeliverableServices from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "digitally_deliverable_services")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_digitally_deliverable_services."""
    table = load_raw_parquet("digitally_deliverable_services")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
