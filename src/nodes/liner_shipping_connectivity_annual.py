from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.LSCI"
SUBSET_DATASET_ID = "unctad_liner_shipping_connectivity_annual"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Liner Shipping Connectivity Index (Annual)",
    "description": "Annual liner shipping connectivity index by economy from UNCTAD.",
    "column_descriptions": {
        "_quarter": "Quarter of observation",
        "quarter_label": "Quarter label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "index_average_q1_2023_100": "Index average q1 2023 100",
        "growth_rate_yearonyear": "Growth rate year-on-year",
        "growth_rate_over_previous_period": "Growth rate over previoUS period",
        "ranking_per_quarter": "Ranking per quarter",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_quarter', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
