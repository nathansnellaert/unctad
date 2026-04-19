from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.GNI"
SUBSET_DATASET_ID = "unctad_gni_total_and_per_capita"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD GNI Total and Per Capita",
    "description": "Gross national income total and per capita by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "us_dollars_at_current_prices_in_millions": "US dollars at current prices in millions",
        "us_dollars_at_current_prices_per_capita": "US dollars at current prices per capita",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
