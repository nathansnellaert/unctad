from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PortCallsArrivals_S"
SUBSET_DATASET_ID = "unctad_port_calls_arrivals_ships"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Port Calls Arrivals (Ships)",
    "description": "Port call arrival statistics for ships by economy and commercial market from UNCTAD.",
    "column_descriptions": {
        "period": "Period of observation",
        "period_label": "Period label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "commercialmarket": "Commercial market segment code",
        "commercialmarket_label": "Commercial market segment name",
        "number_of_port_calls": "Number of port calls",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['period', 'economy', 'commercialmarket'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
