"""Download and transform PopGR."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PopGR"
DATASET_ID = "unctad_population_growth_rates"


def download():
    """Download US.PopGR from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "population_growth_rates")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_population_growth_rates."""
    table = load_raw_parquet("population_growth_rates")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
