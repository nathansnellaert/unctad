from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CreativeGoodsIndex"
SUBSET_DATASET_ID = "unctad_creative_goods_index"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Creative Goods Index",
    "description": "Creative goods trade indices from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "product": "Product category code",
        "product_label": "Product category name",
        "concentration_index": "Concentration index",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'flow', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
