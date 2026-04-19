from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.GDPComponent"
SUBSET_DATASET_ID = "unctad_gdp_by_expenditure_and_activity"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD GDP by Expenditure and Activity",
    "description": "GDP broken down by expenditure component and economic activity from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "component": "Component code",
        "component_label": "Component name",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "us_at_constant_prices_2015_in_millions": "US dollars at constant prices 2015 in millions",
        "percentage_of_gross_domestic_product": "Percentage of gross domestic product",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'component'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
