"""Download and transform TradeFoodProcByCat."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeFoodProcByCat"
DATASET_ID = "unctad_food_trade_processing_by_category"


def download():
    """Download US.TradeFoodProcByCat from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "food_trade_processing_by_category")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_food_trade_processing_by_category."""
    table = load_raw_parquet("food_trade_processing_by_category")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
