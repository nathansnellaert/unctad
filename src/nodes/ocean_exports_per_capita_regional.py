from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.OceanExportsPerCapitaRegionalAggregates"
SUBSET_DATASET_ID = "unctad_ocean_exports_per_capita_regional"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Ocean Exports Per Capita (Regional)",
    "description": "Ocean-based exports per capita for regional aggregates from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "us_at_current_prices_per_capita": "US dollars at current prices per capita",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
