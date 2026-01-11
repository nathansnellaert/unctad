"""Download and transform OceanExportsPerCapitaRegionalAggregates."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.OceanExportsPerCapitaRegionalAggregates"
DATASET_ID = "unctad_ocean_exports_per_capita_regional"


def download():
    """Download US.OceanExportsPerCapitaRegionalAggregates from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ocean_exports_per_capita_regional")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ocean_exports_per_capita_regional."""
    table = load_raw_parquet("ocean_exports_per_capita_regional")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
