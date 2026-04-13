from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CreativeServ_Indiv_Tot"
SUBSET_DATASET_ID = "unctad_creative_services_individual"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Creative Services (Individual)",
    "description": "Individual creative services trade totals by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "flow": "Trade flow direction (e.g. export, import)",
        "value": "Trade value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
