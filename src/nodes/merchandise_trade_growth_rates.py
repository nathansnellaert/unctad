from connector_utils import download_dataset, filter_countries, format_year_range
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TradeMerchGR"
SUBSET_DATASET_ID = "unctad_merchandise_trade_growth_rates"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Merchandise Trade Growth Rates",
    "description": "Growth rates of merchandise trade by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year range (e.g. 1980-1981)",
        "year_label": "Year range label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "annual_average_growth_rate": "Annual average growth rate",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    table = format_year_range(table, "_year")
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
