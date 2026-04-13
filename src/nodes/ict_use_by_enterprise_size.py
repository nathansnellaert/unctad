from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.IctUseEnterprSize"
SUBSET_DATASET_ID = "unctad_ict_use_by_enterprise_size"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD ICT Use by Enterprise Size",
    "description": "ICT usage by enterprise size from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "useofict": "Type of ICT use",
        "enterprisesize": "Enterprise size category",
        "value": "Usage value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'useofict', 'enterprisesize'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
