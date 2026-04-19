from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.FdiFlowsStock"
SUBSET_DATASET_ID = "unctad_fdi_flows_and_stock"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD FDI Flows and Stock",
    "description": "Foreign direct investment flows and stock by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "direction": "Direction code",
        "direction_label": "Direction",
        "us_at_current_prices_in_millions": "US dollars at current prices in millions",
        "percentage_of_total_world": "Percentage of total world",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow', 'direction'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
