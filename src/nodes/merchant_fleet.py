from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.MerchantFleet"
SUBSET_DATASET_ID = "unctad_merchant_fleet"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Merchant Fleet",
    "description": "Merchant fleet capacity by economy and ship type from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "shiptype": "Ship type code",
        "shiptype_label": "Ship type name",
        "dead_weight_tons_in_thousands": "Dead weight tons in thousands",
        "percentage_of_total_world": "Percentage of total world",
        "percentage_of_total_fleet": "Percentage of total fleet",
        "number_of_ships": "Number of ships",
        "gross_tonnage_in_thousands": "Gross tonnage in thousands",
        "average_age_of_vessels_years": "Average age of vessels years",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'shiptype'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
