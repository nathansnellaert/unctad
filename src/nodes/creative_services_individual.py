"""Download and transform CreativeServ_Indiv_Tot."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CreativeServ_Indiv_Tot"
DATASET_ID = "unctad_creative_services_individual"


def download():
    """Download US.CreativeServ_Indiv_Tot from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "creative_services_individual")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_creative_services_individual."""
    table = load_raw_parquet("creative_services_individual")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
