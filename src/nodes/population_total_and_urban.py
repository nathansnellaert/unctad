"""Download and transform PopTotal."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PopTotal"
DATASET_ID = "unctad_population_total_and_urban"


def download():
    """Download US.PopTotal from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "population_total_and_urban")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_population_total_and_urban."""
    table = load_raw_parquet("population_total_and_urban")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
