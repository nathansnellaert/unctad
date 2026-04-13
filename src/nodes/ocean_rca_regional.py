from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.OceanRCARegionalAggregates"
SUBSET_DATASET_ID = "unctad_ocean_rca_regional"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Ocean RCA (Regional)",
    "description": "Revealed comparative advantage in ocean-based trade for regional aggregates from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Regional aggregate",
        "product": "Ocean-based product category",
        "value": "RCA index value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
