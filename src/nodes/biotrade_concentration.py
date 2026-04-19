from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.BioTradeMerchProdConcent"
SUBSET_DATASET_ID = "unctad_biotrade_concentration"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD BioTrade Concentration",
    "description": "Merchandise product concentration indices for biodiversity-based trade from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "product_concentration_index": "Product concentration index",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    if "year" in table.column_names:
        table = table.drop("year")
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
