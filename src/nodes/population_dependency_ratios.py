"""Download and transform PopDependency."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PopDependency"
DATASET_ID = "unctad_population_dependency_ratios"


def download():
    """Download US.PopDependency from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "population_dependency_ratios")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_population_dependency_ratios."""
    table = load_raw_parquet("population_dependency_ratios")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
