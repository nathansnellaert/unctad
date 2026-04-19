from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.VesselValueByRegistration"
SUBSET_DATASET_ID = "unctad_vessel_value_by_registration"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Vessel Value by Registration",
    "description": "Commercial vessel fleet value by flag of registration from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "flagofregistration": "Flag of registration code",
        "flagofregistration_label": "Flag of registration name",
        "percentage_of_global_fleet_value": "Percentage of global fleet value",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'flagofregistration'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
