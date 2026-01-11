"""Download and transform TradeServICT."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.TradeServICT"
DATASET_ID = "unctad_services_trade_ict"


def download():
    """Download US.TradeServICT from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "services_trade_ict")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_services_trade_ict."""
    table = load_raw_parquet("services_trade_ict")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
