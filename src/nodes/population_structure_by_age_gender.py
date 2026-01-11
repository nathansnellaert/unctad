"""Download and transform PopAgeStruct."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PopAgeStruct"
DATASET_ID = "unctad_population_structure_by_age_gender"


def download():
    """Download US.PopAgeStruct from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "population_structure_by_age_gender")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_population_structure_by_age_gender."""
    table = load_raw_parquet("population_structure_by_age_gender")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
