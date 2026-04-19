from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TradeMerchBalance"
SUBSET_DATASET_ID = "unctad_merchandise_trade_balance"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Merchandise Trade Balance",
    "description": "Merchandise trade balance by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "percentage_of_imports": "Percentage of imports",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
