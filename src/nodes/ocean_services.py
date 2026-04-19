from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.OceanServices"
SUBSET_DATASET_ID = "unctad_ocean_services"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Ocean Services",
    "description": "Ocean-based services trade by economy and category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "category": "Category code",
        "category_label": "Category name",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "growth_rate_over_previous_period": "Growth rate over previoUS period",
        "us_at_current_prices_per_capita": "US dollars at current prices per capita",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow', 'category'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
