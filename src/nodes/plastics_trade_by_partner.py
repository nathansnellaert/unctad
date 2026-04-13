from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PlasticsTradebyPartner"
SUBSET_DATASET_ID = "unctad_plastics_trade_by_partner"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Plastics Trade by Partner",
    "description": "Plastics trade by economy, partner, and product category from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "partner": "Partner economy",
        "flow": "Trade flow direction (e.g. export, import)",
        "product": "Plastics product category",
        "value": "Trade value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'partner', 'flow', 'product'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
