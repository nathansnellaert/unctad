from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.BiotradeMerchRCA"
SUBSET_DATASET_ID = "unctad_biotrade_rca"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD BioTrade Revealed Comparative Advantage",
    "description": "Revealed comparative advantage indices for biodiversity-based merchandise trade from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "product": "Product category code",
        "product_label": "Product category name",
        "revealed_comparative_advantage": "Revealed comparative advantage",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
