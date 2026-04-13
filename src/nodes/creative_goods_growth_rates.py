from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CreativeGoodsGR"
SUBSET_DATASET_ID = "unctad_creative_goods_growth_rates"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Creative Goods Growth Rates",
    "description": "Growth rates of creative goods trade by economy and partner from UNCTAD.",
    "column_descriptions": {
        "period": "Period of observation",
        "economy": "Reporting economy",
        "partner": "Partner economy",
        "flow": "Trade flow direction (e.g. export, import)",
        "product": "Creative goods category",
        "value": "Growth rate",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['period', 'economy', 'partner', 'flow', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
