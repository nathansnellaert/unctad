from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CommodityPrice_A"
SUBSET_DATASET_ID = "unctad_commodity_prices_annual"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Commodity Prices (Annual)",
    "description": "Annual commodity prices from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "commodity": "Commodity group or item",
        "value": "Commodity price",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'commodity'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
