from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, upload_data, load_state, save_state, data_hash

## currently using generic scaffolded node file until we implement dataset specific transforms
UNCTAD_DATASET_ID = "US.BiotradeMerchRCA"
SUBSET_DATASET_ID = "unctad_biotrade_rca"

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)

    h = data_hash(table)
    if load_state(SUBSET_DATASET_ID).get("hash") == h:
        print(f"Skipping {SUBSET_DATASET_ID} - unchanged")
        return

    upload_data(table, SUBSET_DATASET_ID)
    save_state(SUBSET_DATASET_ID, {"hash": h})

NODES = {
    download: transform
}

if __name__ == "__main__":
    download()
    transform()
