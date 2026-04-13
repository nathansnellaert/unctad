from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TradeFoodProcCat_Cat_RCA"
SUBSET_DATASET_ID = "unctad_food_trade_rca_by_category"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Food Trade RCA by Category",
    "description": "Revealed comparative advantage in processed food trade by category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "processfoodcategory": "Processed food category",
        "value": "RCA index value",
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
