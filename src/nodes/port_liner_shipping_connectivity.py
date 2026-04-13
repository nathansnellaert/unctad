from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PLSCI"
SUBSET_DATASET_ID = "unctad_port_liner_shipping_connectivity"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Port Liner Shipping Connectivity",
    "description": "Port-level liner shipping connectivity index from UNCTAD.",
    "column_descriptions": {
        "_quarter": "Quarter of observation",
        "port": "Port name",
        "value": "Connectivity index value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_quarter', 'port'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
