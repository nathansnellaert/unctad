from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CreativeServ_Group_E"
SUBSET_DATASET_ID = "unctad_creative_services_group_e"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Creative Services (Group E)",
    "description": "Creative services trade data grouped by EBOPS category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "creativeservice": "Creative service category",
        "value": "Trade value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'creativeservice'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
