"""Download and transform OceanRCAIndividualEconomies."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.OceanRCAIndividualEconomies"
DATASET_ID = "unctad_ocean_rca_individual"


def download():
    """Download US.OceanRCAIndividualEconomies from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ocean_rca_individual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ocean_rca_individual."""
    table = load_raw_parquet("ocean_rca_individual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
