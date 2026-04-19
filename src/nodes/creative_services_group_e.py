from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CreativeServ_Group_E"
SUBSET_DATASET_ID = "unctad_creative_services_group_e"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Creative Services (Group E)",
    "description": "Creative services trade data grouped by EBOPS category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "creativeservice": "Creative service type code",
        "creativeservice_label": "Creative service type name",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "growth_rate_yearonyear": "Growth rate year-on-year",
        "percentage_of_total_trade_in_services": "Percentage of total trade in services",
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
