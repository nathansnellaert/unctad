from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.BioTradeMerchMarketIndices"
SUBSET_DATASET_ID = "unctad_biotrade_market_indices"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD BioTrade Market Indices",
    "description": "Market indices for biodiversity-based merchandise trade from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "product": "Product category code",
        "product_label": "Product category name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "market_concentration_index": "Market concentration index",
        "structural_change_index": "Structural change index",
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
    merge(table, SUBSET_DATASET_ID, key=['_year', 'product', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
