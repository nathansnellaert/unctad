from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.CommodityPriceIndices_M"
SUBSET_DATASET_ID = "unctad_commodity_price_indices_monthly"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Commodity Price Indices (Monthly)",
    "description": "Monthly commodity price indices from UNCTAD.",
    "column_descriptions": {
        "period": "Period of observation",
        "period_label": "Period label",
        "commodity": "Commodity code",
        "commodity_label": "Commodity name",
        "index_base_2015": "Index base 2015",
        "growth_rate_yearonyear": "Growth rate year-on-year",
        "growth_rate_over_previous_period": "Growth rate over previoUS period",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['period', 'commodity'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
