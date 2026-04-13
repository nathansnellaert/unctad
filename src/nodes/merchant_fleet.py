from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.MerchantFleet"
SUBSET_DATASET_ID = "unctad_merchant_fleet"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Merchant Fleet",
    "description": "Merchant fleet capacity by economy and ship type from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Flag state economy",
        "shiptype": "Type of vessel",
        "value": "Fleet capacity value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'shiptype'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
