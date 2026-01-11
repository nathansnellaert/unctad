"""Download and transform PlasticsTradebyPartner."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.PlasticsTradebyPartner"
DATASET_ID = "unctad_plastics_trade_by_partner"


def download():
    """Download US.PlasticsTradebyPartner from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "plastics_trade_by_partner")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_plastics_trade_by_partner."""
    table = load_raw_parquet("plastics_trade_by_partner")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
