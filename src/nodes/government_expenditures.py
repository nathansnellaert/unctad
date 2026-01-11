"""Download and transform GovExpenditures."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.GovExpenditures"
DATASET_ID = "unctad_government_expenditures"


def download():
    """Download US.GovExpenditures from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "government_expenditures")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_government_expenditures."""
    table = load_raw_parquet("government_expenditures")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
