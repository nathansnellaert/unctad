"""Download and transform GDPGR."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.GDPGR"
DATASET_ID = "unctad_gdp_growth_rates"


def download():
    """Download US.GDPGR from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "gdp_growth_rates")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_gdp_growth_rates."""
    table = load_raw_parquet("gdp_growth_rates")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
