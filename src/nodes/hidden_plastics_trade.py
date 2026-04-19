from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.HiddenPlasticsTradebyPartner"
SUBSET_DATASET_ID = "unctad_hidden_plastics_trade"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Hidden Plastics Trade",
    "description": "Trade in products with hidden plastic content by partner economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "partner": "Partner economy code (UN M49)",
        "partner_label": "Partner economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "product": "Product category code",
        "product_label": "Product category name",
        "us_at_current_prices_in_thousands": "US dollars at current prices in thousands",
        "metric_tons_in_thousands": "Metric tons in thousands",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'partner', 'flow', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
