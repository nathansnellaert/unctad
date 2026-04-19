from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PopTotal"
SUBSET_DATASET_ID = "unctad_population_total_and_urban"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Population Total and Urban",
    "description": "Total and urban population by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "absolute_value_in_thousands": "Absolute value in thousands",
        "urban_population_as_percentage_of_total_population": "Urban population as percentage of total population",
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
