from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TotAndComServicesQuarterly"
SUBSET_DATASET_ID = "unctad_services_trade_quarterly"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Services Trade (Quarterly)",
    "description": "Quarterly total and commercial services trade by economy from UNCTAD.",
    "column_descriptions": {
        "period": "Period of observation",
        "period_label": "Period label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "category": "Category code",
        "category_label": "Category name",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "growth_rate_yearonyear": "Growth rate year-on-year",
        "us_at_current_prices_seasonally_adjusted_in_millions": "US dollars at current prices seasonally adjusted in millions",
        "growth_rate_over_previous_period_seasonally_adjusted": "Growth rate over previoUS period seasonally adjusted",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['period', 'economy', 'flow', 'category'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
