from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.FleetBeneficialOwners"
SUBSET_DATASET_ID = "unctad_fleet_beneficial_owners"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Fleet Beneficial Owners",
    "description": "Merchant fleet data by flag of registration and beneficial ownership from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "flagofregistration": "Flag of registration code",
        "flagofregistration_label": "Flag of registration name",
        "beneficialownership": "Beneficial ownership code",
        "beneficialownership_label": "Beneficial ownership name",
        "dead_weight_tons_in_thousands": "Dead weight tons in thousands",
        "percentage_of_total_fleet": "Percentage of total fleet",
        "number_of_ships": "Number of ships",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'flagofregistration', 'beneficialownership'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
