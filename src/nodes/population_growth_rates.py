from connector_utils import download_dataset, filter_countries, format_year_range
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PopGR"
SUBSET_DATASET_ID = "unctad_population_growth_rates"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Population Growth Rates",
    "description": "Population growth rates by economy from UNCTAD.",
    "column_descriptions": {
        "period": "Year range (e.g. 1950-1951)",
        "period_label": "Period label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "annual_average_growth_rate": "Annual average population growth rate",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    table = format_year_range(table, "period")
    merge(table, SUBSET_DATASET_ID, key=['period', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
