import pyarrow as pa
import pyarrow.compute as pc
from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.TradeServCatByPartner"
SUBSET_DATASET_ID = "unctad_services_by_category"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Services Trade by Category",
    "description": "Services trade by category and partner economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "partner": "Partner economy code (UN M49)",
        "partner_label": "Partner economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "category": "Service category code",
        "category_label": "Service category name",
        "us_at_current_prices_in_millions": "Trade value in US dollars at current prices, in millions",
        "percentage_of_total_world": "Percentage of total world trade",
        "percentage_of_total_trade_in_services": "Percentage of total trade in services",
        "growth_rate_over_previous_period": "Growth rate over previous period",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    if "growth_rate_over_previous_period" in table.column_names:
        col = table["growth_rate_over_previous_period"]
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            idx = table.column_names.index("growth_rate_over_previous_period")
            table = table.set_column(idx, "growth_rate_over_previous_period", pc.cast(col, pa.float64(), safe=False))
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'partner', 'flow', 'category'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
