from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TradeFoodProcCat_Cat_RCA"
SUBSET_DATASET_ID = "unctad_food_trade_rca_by_category"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Food Trade RCA by Category",
    "description": "Revealed comparative advantage in processed food trade by category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "processfoodcategory": "Food processing category code",
        "processfoodcategory_label": "Food processing category name",
        "rca_food_process": "RCA food process",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'processfoodcategory'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
