"""Download and transform GoodsAndServTradeOpennessBpm6."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.GoodsAndServTradeOpennessBpm6"
DATASET_ID = "unctad_trade_openness_bpm6"


def download():
    """Download US.GoodsAndServTradeOpennessBpm6 from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "trade_openness_bpm6")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_trade_openness_bpm6."""
    table = load_raw_parquet("trade_openness_bpm6")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
