"""Download and transform CreativeServ_Group_E."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.CreativeServ_Group_E"
DATASET_ID = "unctad_creative_services_group_e"


def download():
    """Download US.CreativeServ_Group_E from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "creative_services_group_e")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_creative_services_group_e."""
    table = load_raw_parquet("creative_services_group_e")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
