"""Download and transform AssociatedPlasticsTradebyPartner."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.AssociatedPlasticsTradebyPartner"
DATASET_ID = "unctad_associated_plastics_trade"


def download():
    """Download US.AssociatedPlasticsTradebyPartner from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "associated_plastics_trade")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_associated_plastics_trade."""
    table = load_raw_parquet("associated_plastics_trade")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
