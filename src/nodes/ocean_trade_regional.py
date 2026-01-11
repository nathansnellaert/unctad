"""Download and transform OceanTradeRegionalAggregates."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.OceanTradeRegionalAggregates"
DATASET_ID = "unctad_ocean_trade_regional"


def download():
    """Download US.OceanTradeRegionalAggregates from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "ocean_trade_regional")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_ocean_trade_regional."""
    table = load_raw_parquet("ocean_trade_regional")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
