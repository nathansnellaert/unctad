"""Download and transform Remittances."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.Remittances"
DATASET_ID = "unctad_personal_remittances"


def download():
    """Download US.Remittances from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "personal_remittances")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_personal_remittances."""
    table = load_raw_parquet("personal_remittances")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
