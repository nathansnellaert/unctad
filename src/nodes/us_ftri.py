from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, raw_asset_exists

DATASET_ID = "US.FTRI"


def download():
    if raw_asset_exists(DATASET_ID):
        print(f"  Skipping {DATASET_ID} - already downloaded")
        return
    table = download_dataset(DATASET_ID)
    save_raw_parquet(table, DATASET_ID)


NODES = {download: []}

if __name__ == "__main__":
    download()
