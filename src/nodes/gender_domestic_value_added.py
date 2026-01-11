"""Download and transform Gender_DomesticValueAdded."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.Gender_DomesticValueAdded"
DATASET_ID = "unctad_gender_domestic_value_added"


def download():
    """Download US.Gender_DomesticValueAdded from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "gender_domestic_value_added")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_gender_domestic_value_added."""
    table = load_raw_parquet("gender_domestic_value_added")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
