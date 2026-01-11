"""Download and transform GDPTotal."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.GDPTotal"
DATASET_ID = "unctad_gdp_total_and_per_capita"


def download():
    """Download US.GDPTotal from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "gdp_total_and_per_capita")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_gdp_total_and_per_capita."""
    table = load_raw_parquet("gdp_total_and_per_capita")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
