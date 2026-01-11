"""Download and transform HiddenPlasticsTradebyPartner."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.HiddenPlasticsTradebyPartner"
DATASET_ID = "unctad_hidden_plastics_trade"


def download():
    """Download US.HiddenPlasticsTradebyPartner from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "hidden_plastics_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_hidden_plastics_trade."""
    table = load_raw_parquet("hidden_plastics_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
