"""Download and transform TradeFoodProcCat_Proc_RCA."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeFoodProcCat_Proc_RCA"
DATASET_ID = "unctad_food_trade_rca_by_processing"


def download():
    """Download US.TradeFoodProcCat_Proc_RCA from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "food_trade_rca_by_processing")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_food_trade_rca_by_processing."""
    table = load_raw_parquet("food_trade_rca_by_processing")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
