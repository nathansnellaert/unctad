from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.BioTradeMerchGDPShare"
SUBSET_DATASET_ID = "unctad_biotrade_gdp_share"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD BioTrade GDP Share",
    "description": "Share of biodiversity-based merchandise trade in GDP from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy",
        "value": "BioTrade share of GDP",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
