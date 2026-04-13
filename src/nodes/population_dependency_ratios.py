from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PopDependency"
SUBSET_DATASET_ID = "unctad_population_dependency_ratios"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Population Dependency Ratios",
    "description": "Population dependency ratios by economy from UNCTAD.",
    "column_descriptions": {
        "series": "Dependency ratio series",
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "value": "Dependency ratio",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['series', '_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
