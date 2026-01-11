"""Download and transform BioTradeMerchMarketIndices."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.BioTradeMerchMarketIndices"
DATASET_ID = "unctad_biotrade_market_indices"


def download():
    """Download US.BioTradeMerchMarketIndices from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "biotrade_market_indices")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_biotrade_market_indices."""
    table = load_raw_parquet("biotrade_market_indices")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
