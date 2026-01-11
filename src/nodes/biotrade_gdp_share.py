"""Download and transform BioTradeMerchGDPShare."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.BioTradeMerchGDPShare"
DATASET_ID = "unctad_biotrade_gdp_share"


def download():
    """Download US.BioTradeMerchGDPShare from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "biotrade_gdp_share")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_biotrade_gdp_share."""
    table = load_raw_parquet("biotrade_gdp_share")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
