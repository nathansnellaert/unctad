from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.GoodsAndServBalanceBpm6"
SUBSET_DATASET_ID = "unctad_goods_and_services_balance_bpm6"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Goods and Services Balance (BPM6)",
    "description": "Balance of goods and services trade using BPM6 methodology from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "series": "Series code",
        "series_label": "Series name",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "percentage_of_gross_domestic_product": "Percentage of gross domestic product",
        "normalized_trade_balance": "Normalized trade balance",
        "percentage_of_imports": "Percentage of imports",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'series', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
